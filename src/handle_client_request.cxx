/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/datatechnology/cornerstone

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "handle_client_request.hxx"

#include "cluster_config.hxx"
#include "context.hxx"
#include "debugging_options.hxx"
#include "error_code.hxx"
#include "global_mgr.hxx"
#include "state_machine.hxx"
#include "state_mgr.hxx"
#include "tracer.hxx"

#include <cassert>
#include <chrono>
#include <sstream>

namespace nuraft {

ptr<resp_msg> raft_server::handle_cli_req_prelock(req_msg& req,
                                                  const req_ext_params& ext_params)
{
    ptr<resp_msg> resp = nullptr;
    ptr<raft_params> params = ctx_->get_params();
    uint64_t timestamp_us = timer_helper::get_timeofday_us();

    switch (params->locking_method_type_) {
        case raft_params::single_mutex: {
            recur_lock(lock_);
            resp = handle_cli_req(req, ext_params, timestamp_us);
            break;
        }
        case raft_params::dual_mutex:
        default: {
            // TODO: Use RW lock here.
            recur_lock(cli_lock_);
            resp = handle_cli_req(req, ext_params, timestamp_us);
            break;
        }
    }

    // Urgent commit, so that the commit will not depend on hb.
    request_append_entries_for_all();

    return resp;
}

void raft_server::request_append_entries_for_all() {
    ptr<raft_params> params = ctx_->get_params();
    if (params->use_bg_thread_for_urgent_commit_) {
        // Let background generate request (some delay may happen).
        global_mgr* mgr = get_global_mgr();
        if (mgr) {
            // Global thread pool exists, request it.
            p_tr("found global thread pool");
            mgr->request_append( this->shared_from_this() );
        } else {
            bg_append_ea_->invoke();
        }
    } else {
        // Directly generate request in user thread.
        recur_lock(lock_);
        request_append_entries();
    }
}

ptr<resp_msg> raft_server::handle_cli_req(req_msg& req,
                                          const req_ext_params& ext_params,
                                          uint64_t timestamp_us)
{
    ptr<resp_msg> resp = nullptr;
    ulong last_idx = 0;
    ptr<buffer> ret_value = nullptr;
    ulong resp_idx = 1;
    ulong cur_term = state_->get_term();
    ptr<raft_params> params = ctx_->get_params();

    resp = cs_new<resp_msg>( cur_term,
                             msg_type::append_entries_response,
                             id_,
                             leader_ );
    if (role_ != srv_role::leader || write_paused_) {
        resp->set_result_code( cmd_result_code::NOT_LEADER );
        return resp;
    }

    if (ext_params.expected_term_) {
        // If expected term is given, check the current term.
        if (ext_params.expected_term_ != cur_term) {
            resp->set_result_code( cmd_result_code::TERM_MISMATCH );
            return resp;
        }
    }

    std::vector< ptr<log_entry> >& entries = req.log_entries();
    size_t num_entries = entries.size();

    for (size_t i = 0; i < num_entries; ++i) {
        // force the log's term to current term
        entries.at(i)->set_term(cur_term);
        entries.at(i)->set_timestamp(timestamp_us);

        ulong next_slot = store_log_entry(entries.at(i));
        p_in("append at log_idx %" PRIu64 ", timestamp %" PRIu64,
             next_slot, timestamp_us);
        last_idx = next_slot;

        ptr<buffer> buf = entries.at(i)->get_buf_ptr();
        buf->pos(0);
        ret_value = state_machine_->pre_commit_ext
                    ( state_machine::ext_op_params( last_idx, buf ) );

        if (ext_params.after_precommit_) {
            req_ext_cb_params cb_params;
            cb_params.log_idx = last_idx;
            cb_params.log_term = cur_term;
            cb_params.context = ext_params.context_;
            ext_params.after_precommit_(cb_params);
        }
    }
    if (num_entries) {
        log_store_->end_of_append_batch(last_idx - num_entries + 1, num_entries);
    }
    try_update_precommit_index(last_idx);
    resp_idx = log_store_->next_slot();

    // Finished appending logs and pre_commit of itself.
    cb_func::Param param(id_, leader_);
    param.ctx = &last_idx;
    CbReturnCode rc = ctx_->cb_func_.call(cb_func::AppendLogs, &param);
    if (rc == CbReturnCode::ReturnNull) return nullptr;

    size_t sleep_us = debugging_options::get_instance()
                      .handle_cli_req_sleep_us_.load(std::memory_order_relaxed);
    if (sleep_us) {
        // Sleep if the debugging option is given.
        timer_helper::sleep_us(sleep_us);
    }

    if (!get_config()->is_async_replication()) {
        // Sync replication:
        //   Set callback function for `last_idx`.
        ptr<commit_ret_elem> elem = cs_new<commit_ret_elem>();
        elem->idx_ = last_idx;
        elem->result_code_ = cmd_result_code::TIMEOUT;

        {   auto_lock(commit_ret_elems_lock_);
            auto entry = commit_ret_elems_.find(last_idx);
            if (entry != commit_ret_elems_.end()) {
                // Commit thread was faster than this.
                elem = entry->second;
                p_tr("commit thread was faster than this thread: %p", elem.get());
            } else {
                commit_ret_elems_.insert( std::make_pair(last_idx, elem) );
            }

            switch (ctx_->get_params()->return_method_) {
            case raft_params::blocking:
            default:
                // Blocking call: set callback function waiting for the result.
                resp->set_cb( std::bind( &raft_server::handle_cli_req_callback,
                                         this,
                                         elem,
                                         std::placeholders::_1 ) );
                break;

            case raft_params::async_handler:
                // Async handler: create & set async result object.
                if (!elem->async_result_) {
                    elem->async_result_ = cs_new< cmd_result< ptr<buffer> > >();
                }
                resp->set_async_cb
                      ( std::bind( &raft_server::handle_cli_req_callback_async,
                                   this,
                                   elem->async_result_ ) );
                break;
            }
        }

    } else {
        // Async replication:
        //   Immediately return with the result of pre-commit.
        p_dv( "asynchronously replicated %" PRIu64 ", return value %p",
              last_idx, ret_value.get() );
        resp->set_ctx(ret_value);
    }

    resp->accept(resp_idx);
    return resp;
}

ptr<resp_msg> raft_server::handle_cli_req_callback(ptr<commit_ret_elem> elem,
                                                   ptr<resp_msg> resp) {
    p_dv("commit_ret_cv %" PRIu64 " %p sleep", elem->idx_, &elem->awaiter_);

    // Will wake up after timeout.
    elem->awaiter_.wait_ms(ctx_->get_params()->client_req_timeout_);

    uint64_t idx = 0;
    uint64_t elapsed_us = 0;
    ptr<buffer> ret_value = nullptr;
    {   auto_lock(commit_ret_elems_lock_);
        idx = elem->idx_;
        elapsed_us = elem->timer_.get_us();
        ret_value = elem->ret_value_;
        elem->callback_invoked_ = true;
        if (elem->result_code_ != cmd_result_code::TIMEOUT) {
            commit_ret_elems_.erase(elem->idx_);
        } else {
            p_dv("Client timeout leave commit thread to remove commit_ret_elem %" PRIu64,
                 idx);
        }
        p_dv("remaining elems in waiting queue: %zu", commit_ret_elems_.size());
    }

    if (elem->result_code_ == cmd_result_code::OK) {
        p_dv( "[OK] commit_ret_cv %" PRIu64 " wake up (%" PRIu64 " us), return value %p",
              idx, elapsed_us, ret_value.get() );
    } else {
        // Null `ret_value`, most likely timeout.
        p_wn( "[NOT OK] commit_ret_cv %" PRIu64 " wake up (%" PRIu64 " us), "
              "return value %p, result code %d",
              idx, elapsed_us, ret_value.get(), elem->result_code_ );
        bool valid_leader = check_leadership_validity();
        if (valid_leader) {
            p_in("leadership is still valid");
        } else {
            p_er("leadership is invalid");
        }
    }
    resp->set_ctx(ret_value);
    resp->set_result_code(elem->result_code_);

    return resp;
}

ptr< cmd_result< ptr<buffer> > >
    raft_server::handle_cli_req_callback_async(ptr< cmd_result< ptr<buffer> > > async_res)
{
    async_res->accept();
    return async_res;
}

void raft_server::drop_all_pending_commit_elems() {
    // Blocking mode:
    //   Invoke all awaiting requests to return `CANCELLED`.
    if (ctx_->get_params()->return_method_ == raft_params::blocking) {
        auto_lock(commit_ret_elems_lock_);
        ulong min_idx = std::numeric_limits<ulong>::max();
        ulong max_idx = 0;
        for (auto& entry: commit_ret_elems_) {
            ptr<commit_ret_elem>& elem = entry.second;
            elem->ret_value_ = nullptr;
            elem->result_code_ = cmd_result_code::CANCELLED;
            elem->awaiter_.invoke();
            if (min_idx > elem->idx_) {
                min_idx = elem->idx_;
            }
            if (max_idx < elem->idx_) {
                max_idx = elem->idx_;
            }
            p_db("cancelled blocking client request %" PRIu64 ", waited %" PRIu64 " us",
                 elem->idx_, elem->timer_.get_us());
        }
        if (!commit_ret_elems_.empty()) {
            p_wn("cancelled %zu blocking client requests from %" PRIu64
                 " to %" PRIu64 ".",
                 commit_ret_elems_.size(), min_idx, max_idx);
        }
        commit_ret_elems_.clear();
        return;
    }

    // Non-blocking mode:
    //   Set `CANCELLED` and set result & error.
    std::list< ptr<commit_ret_elem> > elems;

    {   auto_lock(commit_ret_elems_lock_);
        for (auto& entry: commit_ret_elems_) {
            ptr<commit_ret_elem>& ee = entry.second;
            elems.push_back(ee);
        }
        commit_ret_elems_.clear();
    }

    // Calling handler should be done outside the mutex.
    for (auto& entry: elems) {
        ptr<commit_ret_elem>&ee = entry;
        p_wn("cancelled non-blocking client request %" PRIu64, ee->idx_);

        ptr<buffer> result = nullptr;
        ptr<std::exception> err =
            cs_new<std::runtime_error>("Request cancelled.");
        ee->async_result_->set_result(result, err, cmd_result_code::CANCELLED);
    }
}

void raft_server::drop_all_sm_watcher_elems() {
    // Drop all state machine watchers.
    std::list<sm_watcher_elem> elems;

    {
        auto_lock(sm_watchers_lock_);
        for (auto& entry: sm_watchers_) {
            elems.push_back(entry.second);
        }
        sm_watchers_.clear();
    }

    // Calling handler should be done outside the mutex.
    for (auto& entry: elems) {
        sm_watcher_elem& ee = entry;
        p_wn("cancelled state machine watcher for idx %" PRIu64, ee.idx_);

        for (auto& watcher: ee.watchers_) {
            bool ret_bool = false;
            ptr<std::exception> exp =
                cs_new<std::runtime_error>("watcher has been cancelled.");
            watcher->set_result(ret_bool, exp, cmd_result_code::CANCELLED);
        }
    }
}

ptr<cmd_result<bool>> raft_server::wait_for_state_machine_commit(uint64_t target_idx) {
    auto ret = cs_new<cmd_result<bool>>();

    std::lock_guard<std::mutex> l(sm_watchers_lock_);
    uint64_t sm_commit_index = sm_commit_index_;
    if (target_idx <= sm_commit_index) {
        // If the target index is already committed, return immediately.
        p_tr("sm watcher for idx %" PRIu64 " already committed, return true",
             target_idx);
        bool ret_bool = true;
        ptr<std::exception> exp = nullptr;
        ret->set_result(ret_bool, exp, cmd_result_code::OK);
        return ret;
    }

    auto entry = sm_watchers_.find(target_idx);
    if (entry != sm_watchers_.end()) {
        // If watcher already exists, add it to the list.
        p_tr("sm watcher for idx %" PRIu64 " already exists, add to the list",
             target_idx);
        entry->second.watchers_.push_back(ret);
    } else {
        // If watcher does not exist, create a new one.
        p_tr("sm watcher for idx %" PRIu64 " does not exist, create a new one",
             target_idx);
        sm_watcher_elem elem;
        elem.idx_ = target_idx;
        elem.watchers_.push_back(ret);
        sm_watchers_.insert(std::make_pair(target_idx, elem));
    }

    return ret;
}

} // namespace nuraft;

