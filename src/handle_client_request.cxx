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

std::shared_ptr< resp_msg > raft_server::handle_cli_req_prelock(req_msg& req, const req_ext_params& ext_params) {
    std::shared_ptr< resp_msg > resp = nullptr;
    std::shared_ptr< raft_params > params = ctx_->get_params();
    uint64_t timestamp_us = timer_helper::get_timeofday_us();

    switch (params->locking_method_type_) {
    case raft_params::single_mutex: {
        auto guard = recur_lock(lock_);
        resp = handle_cli_req(req, ext_params, timestamp_us);
        break;
    }
    case raft_params::dual_mutex:
    default: {
        // TODO: Use RW lock here.
        auto guard = auto_lock(cli_lock_);
        resp = handle_cli_req(req, ext_params, timestamp_us);
        break;
    }
    }

    // Urgent commit, so that the commit will not depend on hb.
    request_append_entries_for_all();

    return resp;
}

void raft_server::request_append_entries_for_all() {
    std::shared_ptr< raft_params > params = ctx_->get_params();
    if (params->use_bg_thread_for_urgent_commit_) {
        // Let background generate request (some delay may happen).
        nuraft_global_mgr* mgr = nuraft_global_mgr::get_instance();
        if (mgr) {
            // Global thread pool exists, request it.
            p_tr("found global thread pool");
            mgr->request_append(this->shared_from_this());
        } else {
            bg_append_ea_->invoke();
        }
    } else {
        // Directly generate request in user thread.
        auto guard = recur_lock(lock_);
        request_append_entries();
    }
}

std::shared_ptr< resp_msg > raft_server::handle_cli_req(req_msg& req, const req_ext_params& ext_params,
                                                        uint64_t timestamp_us) {
    std::shared_ptr< resp_msg > resp = nullptr;
    uint64_t last_idx = 0;
    std::shared_ptr< buffer > ret_value = nullptr;
    uint64_t resp_idx = 1;
    uint64_t cur_term = state_->get_term();
    std::shared_ptr< raft_params > params = ctx_->get_params();

    resp = std::make_shared< resp_msg >(cur_term, msg_type::append_entries_response, id_, leader_);
    if (role_ != srv_role::leader || write_paused_) {
        resp->set_result_code(cmd_result_code::NOT_LEADER);
        return resp;
    }

    if (ext_params.expected_term_) {
        // If expected term is given, check the current term.
        if (ext_params.expected_term_ != cur_term) {
            resp->set_result_code(cmd_result_code::TERM_MISMATCH);
            return resp;
        }
    }

    std::vector< std::shared_ptr< log_entry > >& entries = req.log_entries();
    size_t num_entries = entries.size();

    for (size_t i = 0; i < num_entries; ++i) {
        // force the log's term to current term
        entries.at(i)->set_term(cur_term);
        entries.at(i)->set_timestamp(timestamp_us);

        uint64_t next_slot = store_log_entry(entries.at(i));
        p_db("append at log_idx %" PRIu64 ", timestamp %" PRIu64, next_slot, timestamp_us);
        last_idx = next_slot;

        std::shared_ptr< buffer > buf = entries.at(i)->get_buf_ptr();
        buf->pos(0);
        ret_value = state_machine_->pre_commit_ext(state_machine::ext_op_params(last_idx, buf));

        if (ext_params.after_precommit_) {
            req_ext_cb_params cb_params;
            cb_params.log_idx = last_idx;
            cb_params.log_term = cur_term;
            cb_params.context = ext_params.context_;
            ext_params.after_precommit_(cb_params);
        }
    }
    if (num_entries) { log_store_->end_of_append_batch(last_idx - num_entries, num_entries); }
    try_update_precommit_index(last_idx);
    resp_idx = log_store_->next_slot();

    // Finished appending logs and pre_commit of itself.
    cb_func::Param param(id_, leader_);
    param.ctx = &last_idx;
    CbReturnCode rc = ctx_->cb_func_.call(cb_func::AppendLogs, &param);
    if (rc == CbReturnCode::ReturnNull) return nullptr;

    size_t sleep_us = debugging_options::get_instance().handle_cli_req_sleep_us_.load(std::memory_order_relaxed);
    if (sleep_us) {
        // Sleep if the debugging option is given.
        timer_helper::sleep_us(sleep_us);
    }

    if (!get_config()->is_async_replication()) {
        // Sync replication:
        //   Set callback function for `last_idx`.
        std::shared_ptr< commit_ret_elem > elem = std::make_shared< commit_ret_elem >();
        elem->idx_ = last_idx;
        elem->result_code_ = cmd_result_code::TIMEOUT;

        {
            auto guard = auto_lock(commit_ret_elems_lock_);
            auto entry = commit_ret_elems_.find(last_idx);
            if (entry != commit_ret_elems_.end()) {
                // Commit thread was faster than this.
                elem = entry->second;
                p_tr("commit thread was faster than this thread: %p", elem.get());
            } else {
                commit_ret_elems_.insert(std::make_pair(last_idx, elem));
            }

            switch (ctx_->get_params()->return_method_) {
            case raft_params::blocking:
            default:
                // Blocking call: set callback function waiting for the result.
                resp->set_cb(std::bind(&raft_server::handle_cli_req_callback, this, elem, std::placeholders::_1));
                break;

            case raft_params::async_handler:
                // Async handler: create & set async result object.
                if (!elem->async_result_) {
                    elem->async_result_ = std::make_shared< cmd_result< std::shared_ptr< buffer > > >();
                }
                resp->set_async_cb(std::bind(&raft_server::handle_cli_req_callback_async, this, elem->async_result_));
                break;
            }
        }

    } else {
        // Async replication:
        //   Immediately return with the result of pre-commit.
        p_dv("asynchronously replicated %" PRIu64 ", return value %p", last_idx, ret_value.get());
        resp->set_ctx(ret_value);
    }

    resp->accept(resp_idx);
    return resp;
}

std::shared_ptr< resp_msg > raft_server::handle_cli_req_callback(std::shared_ptr< commit_ret_elem > elem,
                                                                 std::shared_ptr< resp_msg > resp) {
    p_dv("commit_ret_cv %" PRIu64 " %p sleep", elem->idx_, &elem->awaiter_);

    // Will wake up after timeout.
    elem->awaiter_.wait_ms(ctx_->get_params()->client_req_timeout_);

    uint64_t idx = 0;
    uint64_t elapsed_us = 0;
    std::shared_ptr< buffer > ret_value = nullptr;
    {
        auto guard = auto_lock(commit_ret_elems_lock_);
        idx = elem->idx_;
        elapsed_us = elem->timer_.get_us();
        ret_value = elem->ret_value_;
        elem->callback_invoked_ = true;
        if (elem->result_code_ != cmd_result_code::TIMEOUT) {
            commit_ret_elems_.erase(elem->idx_);
        } else {
            p_dv("Client timeout leave commit thread to remove commit_ret_elem %" PRIu64, idx);
        }
        p_dv("remaining elems in waiting queue: %zu", commit_ret_elems_.size());
    }

    if (elem->result_code_ == cmd_result_code::OK) {
        p_dv("[OK] commit_ret_cv %" PRIu64 " wake up (%" PRIu64 " us), return value %p", idx, elapsed_us,
             ret_value.get());
    } else {
        // Null `ret_value`, most likely timeout.
        p_wn("[NOT OK] commit_ret_cv %" PRIu64 " wake up (%" PRIu64 " us), "
             "return value %p, result code %d",
             idx, elapsed_us, ret_value.get(), elem->result_code_);
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

std::shared_ptr< cmd_result< std::shared_ptr< buffer > > >
raft_server::handle_cli_req_callback_async(std::shared_ptr< cmd_result< std::shared_ptr< buffer > > > async_res) {
    async_res->accept();
    return async_res;
}

void raft_server::drop_all_pending_commit_elems() {
    // Blocking mode:
    //   Invoke all awaiting requests to return `CANCELLED`.
    if (ctx_->get_params()->return_method_ == raft_params::blocking) {
        auto guard = auto_lock(commit_ret_elems_lock_);
        uint64_t min_idx = std::numeric_limits< uint64_t >::max();
        uint64_t max_idx = 0;
        for (auto& entry : commit_ret_elems_) {
            std::shared_ptr< commit_ret_elem >& elem = entry.second;
            elem->ret_value_ = nullptr;
            elem->result_code_ = cmd_result_code::CANCELLED;
            elem->awaiter_.invoke();
            if (min_idx > elem->idx_) { min_idx = elem->idx_; }
            if (max_idx < elem->idx_) { max_idx = elem->idx_; }
            p_db("cancelled blocking client request %" PRIu64 ", waited %" PRIu64 " us", elem->idx_,
                 elem->timer_.get_us());
        }
        if (!commit_ret_elems_.empty()) {
            p_wn("cancelled %zu blocking client requests from %" PRIu64 " to %" PRIu64 ".", commit_ret_elems_.size(),
                 min_idx, max_idx);
        }
        commit_ret_elems_.clear();
        return;
    }

    // Non-blocking mode:
    //   Set `CANCELLED` and set result & error.
    std::list< std::shared_ptr< commit_ret_elem > > elems;

    {
        auto guard = auto_lock(commit_ret_elems_lock_);
        for (auto& entry : commit_ret_elems_) {
            std::shared_ptr< commit_ret_elem >& ee = entry.second;
            elems.push_back(ee);
        }
        commit_ret_elems_.clear();
    }

    // Calling handler should be done outside the mutex.
    for (auto& entry : elems) {
        std::shared_ptr< commit_ret_elem >& ee = entry;
        p_wn("cancelled non-blocking client request %" PRIu64, ee->idx_);

        std::shared_ptr< buffer > result = nullptr;
        std::shared_ptr< std::exception > err = std::make_shared< std::runtime_error >("Request cancelled.");
        ee->async_result_->set_result(result, err, cmd_result_code::CANCELLED);
    }
}

} // namespace nuraft
