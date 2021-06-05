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

#include "raft_server.hxx"

#include "cluster_config.hxx"
#include "error_code.hxx"
#include "handle_client_request.hxx"
#include "global_mgr.hxx"
#include "peer.hxx"
#include "snapshot.hxx"
#include "state_machine.hxx"
#include "state_mgr.hxx"
#include "tracer.hxx"

#include <cassert>
#include <list>
#include <sstream>

namespace nuraft {

void raft_server::commit(ulong target_idx) {
    if (target_idx > quick_commit_index_) {
        quick_commit_index_ = target_idx;
        lagging_sm_target_index_ = target_idx;
        p_db( "trigger commit upto %lu", quick_commit_index_.load() );

        // if this is a leader notify peers to commit as well
        // for peers that are free, send the request, otherwise,
        // set pending commit flag for that peer
        if (role_ == srv_role::leader) {
            for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
                ptr<peer> pp = it->second;
                if (!request_append_entries(pp)) {
                    pp->set_pending_commit();
                }
            }
        }
    }

    p_tr( "local log idx %lu, target_commit_idx %lu, "
          "quick_commit_index_ %lu, state_->get_commit_idx() %lu",
          log_store_->next_slot() - 1, target_idx,
          quick_commit_index_.load(), sm_commit_index_.load() );

    if ( log_store_->next_slot() - 1 > sm_commit_index_ &&
         quick_commit_index_ > sm_commit_index_ ) {

        nuraft_global_mgr* mgr = nuraft_global_mgr::get_instance();
        if (mgr) {
            // Global thread pool exists, request it.
            p_tr("request commit to global thread pool");
            mgr->request_commit( this->shared_from_this() );
        } else {
            p_tr("commit_cv_ notify (local thread)");
            std::unique_lock<std::mutex> lock(commit_cv_lock_);
            commit_cv_.notify_one();
        }

    } else {
        /*
         * After raft server (re-)start, if its log entry is as fresh as leader,
         * then commit thread will not be notified. But we need to tell the app,
         * whose status rely on raft server, that now raft server is fresh.
         */
        if (role_ == srv_role::follower) {
            ulong leader_idx = leader_commit_index_.load();
            ulong local_idx = sm_commit_index_.load();
            if (!data_fresh_.load() &&
                leader_idx < local_idx + ctx_->get_params()->fresh_log_gap_) {
                data_fresh_.store(true);
                cb_func::Param param(id_, leader_);
                (void) ctx_->cb_func_.call(cb_func::BecomeFresh, &param);
            }
        }
    }
}

void raft_server::commit_in_bg() {
    std::string thread_name = "nuraft_commit";
#ifdef __linux__
    pthread_setname_np(pthread_self(), thread_name.c_str());
#elif __APPLE__
    pthread_setname_np(thread_name.c_str());
#endif

    while (true) {
     try {
        while ( quick_commit_index_ <= sm_commit_index_ ||
                sm_commit_index_ >= log_store_->next_slot() - 1 ) {
            std::unique_lock<std::mutex> lock(commit_cv_lock_);

            auto wait_check = [this] () {
                return (log_store_->next_slot() - 1 > sm_commit_index_ &&
                        quick_commit_index_ > sm_commit_index_) || stopping_;
            };
            p_tr("commit_cv_ sleep\n");
            commit_cv_.wait(lock, wait_check);

            p_tr("commit_cv_ wake up\n");
            if (stopping_) {
                lock.unlock();
                lock.release();
                { std::unique_lock<std::mutex> lock2(ready_to_stop_cv_lock_);
                  ready_to_stop_cv_.notify_all(); }
                commit_bg_stopped_ = true;
                return;
            }

            // NOTE:
            //   Even though commit_cv_ is invoked (by commit()), we don't
            //   need to execute it if the current commit index number of
            //   the state machine is greater than either
            //     1) requested commit index or
            //     2) log store's latest log index.
        }

        commit_in_bg_exec();

     } catch (std::exception& err) {
        // LCOV_EXCL_START
        commit_bg_stopped_ = true;
        p_er( "background committing thread encounter err %s, "
              "exiting to protect the system",
              err.what() );
        ctx_->state_mgr_->system_exit(raft_err::N20_background_commit_err);
        ::exit(-1);
        // LCOV_EXCL_STOP
     }
    }
    commit_bg_stopped_ = true;
}

bool raft_server::commit_in_bg_exec(size_t timeout_ms) {
    std::unique_lock<std::mutex> ll(commit_lock_, std::try_to_lock);
    if (!ll.owns_lock()) {
        // Other thread is already doing commit.
        // This is caused by global workers only, as there is only one
        // thread running `commit_in_bg`. Raft server can request a new commit
        // while other worker is doing commit for that Raft server, and the
        // new request will be accepted since the global queue doesn't contain
        // the request for that Raft server (as it is popped by the worker).
        // In such a case, we can just ignore it.
        return true;
    }

    p_db( "commit upto %ld, curruent idx %ld\n",
          quick_commit_index_.load(), sm_commit_index_.load() );

    ulong log_start_idx = log_store_->start_index();
    if ( log_start_idx &&
         sm_commit_index_ < log_start_idx - 1 ) {
        p_wn("current commit idx %llu is smaller than log start idx %llu - 1, "
             "adjust it to %llu",
             sm_commit_index_.load(),
             log_start_idx,
             log_start_idx - 1);
        sm_commit_index_ = log_start_idx - 1;
    }

    ptr<cluster_config> cur_config = get_config();
    bool need_to_handle_commit_elem = ( is_leader() &&
                                        !cur_config->is_async_replication() );

    bool first_loop_exec = true;
    bool finished_in_time = true;
    timer_helper tt(timeout_ms * 1000);
    while ( sm_commit_index_ < quick_commit_index_ &&
            sm_commit_index_ < log_store_->next_slot() - 1 ) {
        // NOTE: Skip timeout checking for the first loop execution.
        if (!first_loop_exec && timeout_ms && tt.timeout()) {
            p_wn( "abort commit due to timeout (%zu ms), %zu ms elapsed\n",
                  timeout_ms, tt.get_ms() );
            finished_in_time = false;
            break;
        }
        first_loop_exec = false;

        ulong index_to_commit = sm_commit_index_ + 1;
        ptr<log_entry> le = log_store_->entry_at(index_to_commit);
        p_tr( "commit upto %llu, curruent idx %llu\n",
              quick_commit_index_.load(), index_to_commit );

        if (le->get_term() == 0) {
            // LCOV_EXCL_START
            // Zero term means that log store is corrupted
            // (failed to read log).
            p_ft( "empty log at idx %llu, must be log corruption",
                  index_to_commit );
            ctx_->state_mgr_->system_exit(raft_err::N19_bad_log_idx_for_term);
            ::exit(-1);
            // LCOV_EXCL_STOP
        }

        if (le->get_val_type() == log_val_type::app_log) {
            commit_app_log(index_to_commit, le, need_to_handle_commit_elem);

        } else if (le->get_val_type() == log_val_type::conf) {
            commit_conf(index_to_commit, le);
        }

        ulong exp_idx = index_to_commit - 1;
        if (sm_commit_index_.compare_exchange_strong(exp_idx, index_to_commit)) {
            snapshot_and_compact(sm_commit_index_);

            cb_func::Param param(id_, leader_);
            // Copy to other local variable to be safe.
            uint64_t log_idx = index_to_commit;
            param.ctx = &log_idx;
            ctx_->cb_func_.call(cb_func::StateMachineExecution, &param);
        } else {
            p_er("sm_commit_index_ has been changed from %zu to %zu, "
                 "this thread attempted %zu",
                 index_to_commit - 1,
                 exp_idx,
                 index_to_commit);
        }
    }
    p_db( "DONE: commit upto %ld, curruent idx %ld\n",
          quick_commit_index_.load(), sm_commit_index_.load() );
    if (role_ == srv_role::follower) {
        ulong leader_idx = leader_commit_index_.load();
        ulong local_idx = sm_commit_index_.load();
        ptr<raft_params> params = ctx_->get_params();

        if (data_fresh_.load() &&
            leader_idx > local_idx + params->stale_log_gap_) {
            data_fresh_.store(false);
            cb_func::Param param(id_, leader_);
            (void) ctx_->cb_func_.call(cb_func::BecomeStale, &param);

        } else if (!data_fresh_.load() &&
                   leader_idx < local_idx + params->fresh_log_gap_) {
            data_fresh_.store(true);
            cb_func::Param param(id_, leader_);
            (void) ctx_->cb_func_.call(cb_func::BecomeFresh, &param);
        }
    }
    return finished_in_time;
}

void raft_server::commit_app_log(ulong idx_to_commit,
                                 ptr<log_entry>& le,
                                 bool need_to_handle_commit_elem)
{
    ptr<buffer> ret_value = nullptr;
    ptr<buffer> buf = le->get_buf_ptr();
    buf->pos(0);
    ulong sm_idx = idx_to_commit;
    ulong pc_idx = precommit_index_.load();
    if (pc_idx < sm_idx) {
        // Pre-commit should have been invoked, must be a bug.
        p_ft( "pre-commit index %zu is smaller than commit index %zu",
              pc_idx, sm_idx );
        ctx_->state_mgr_->system_exit(raft_err::N23_precommit_order_inversion);
        ::exit(-1);
    }
    ret_value = state_machine_->commit_ext
                ( state_machine::ext_op_params( sm_idx, buf ) );
    if (ret_value) ret_value->pos(0);

    std::list< ptr<commit_ret_elem> > async_elems;
    if (need_to_handle_commit_elem) {
        std::unique_lock<std::mutex> cre_lock(commit_ret_elems_lock_);
        bool match_found = false;
        auto entry = commit_ret_elems_.find(sm_idx);
        if (entry != commit_ret_elems_.end()) {
            ptr<commit_ret_elem> elem = entry->second;
            if (elem->idx_ == sm_idx) {
                elem->result_code_ = cmd_result_code::OK;
                elem->ret_value_ = ret_value;
                match_found = true;
                p_dv("notify cb %ld %p", sm_idx, &elem->awaiter_);

                switch (ctx_->get_params()->return_method_) {
                case raft_params::blocking:
                default:
                    // Blocking mode: invoke waiting function.
                    elem->awaiter_.invoke();
                    break;

                case raft_params::async_handler:
                    // Async handler: put into list.
                    async_elems.push_back(elem);
                    commit_ret_elems_.erase(entry);
                    break;
                }
            }
        }

        if (!match_found) {
            // If not found, commit thread is invoked earlier than user thread.
            // Create one here.
            ptr<commit_ret_elem> elem = cs_new<commit_ret_elem>();
            elem->idx_ = sm_idx;
            elem->result_code_ = cmd_result_code::OK;
            elem->ret_value_ = ret_value;
            p_tr("commit thread is invoked earlier than user thread, "
                 "log %lu, elem %p", sm_idx, elem.get());

            switch (ctx_->get_params()->return_method_) {
            case raft_params::blocking:
            default:
                elem->awaiter_.invoke(); // Callback will not sleep.
                break;
            case raft_params::async_handler:
                // Async handler:
                //   Set the result, but should not put it into the
                //   `async_elems` list, as the user thread (supposed to be
                //   executed right after this) will invoke the callback immediately.
                elem->async_result_ =
                    cs_new< cmd_result< ptr<buffer> > >( elem->ret_value_ );
                break;
            }
            commit_ret_elems_.insert( std::make_pair(sm_idx, elem) );
        }
    }

    // Calling handler should be done outside the mutex.
    for (auto& entry: async_elems) {
        ptr<commit_ret_elem>& elem = entry;
        if (elem->async_result_) {
            ptr<std::exception> err = nullptr;
            elem->async_result_->set_result_code(cmd_result_code::OK);
            elem->async_result_->set_result( elem->ret_value_, err );
            elem->ret_value_.reset();
            elem->async_result_.reset();
        }
    }
}

void raft_server::commit_conf(ulong idx_to_commit,
                              ptr<log_entry>& le) {
    recur_lock(lock_);
    le->get_buf().pos(0);
    ptr<cluster_config> new_conf =
        cluster_config::deserialize(le->get_buf());

    ptr<cluster_config> cur_conf = get_config();
    p_in( "config at index %llu is committed, prev config log idx %llu",
          new_conf->get_log_idx(), cur_conf->get_log_idx() );

    ctx_->state_mgr_->save_config(*new_conf);
    config_changing_ = false;
    if (cur_conf->get_log_idx() < new_conf->get_log_idx()) {
        reconfigure(new_conf);
    }

    cb_func::Param param(id_, leader_);
    uint64_t log_idx = idx_to_commit;
    param.ctx = &log_idx;
    ctx_->cb_func_.call(cb_func::NewConfig, &param);

    state_machine_->commit_config(idx_to_commit, new_conf);

    // Modified by Jung-Sang Ahn, May 18 2018:
    //   This causes an endless catch-up issue when we add a new node,
    //   as configuration always has itself in it.
    //   Instead, we can clear catch-up flag when this node receives
    //   normal append_entries() request, as receiving log entry
    //   means that catch-up process is already done.
    //
    // if (catching_up_ && new_conf->get_server(id_) != nilptr) {
    //     p_in("this server is committed as one of cluster members");
    //     catching_up_ = false;
    // }
}

bool raft_server::apply_config_log_entry(ptr<log_entry>& le,
                                         ptr<state_mgr>& s_mgr,
                                         std::string& err_msg)
{
    if (!le.get() || !s_mgr.get()) {
        err_msg = "Invalid arguments";
        return false;
    }
    if (le->get_val_type() != log_val_type::conf) {
        err_msg = "Invalid log type: " + std::to_string(le->get_val_type());
        return false;
    }
    if (le->is_buf_null()) {
        err_msg = "Context is empty";
        return false;
    }

    buffer& buf = le->get_buf();
    buf.pos(0);
    ptr<cluster_config> new_conf = cluster_config::deserialize(buf);
    s_mgr->save_config(*new_conf);
    return true;
}

void raft_server::snapshot_and_compact(ulong committed_idx) {
    ptr<raft_params> params = ctx_->get_params();
    if ( params->snapshot_distance_ == 0 ||
         ( committed_idx - log_store_->start_index() + 1 ) <
               (ulong)params->snapshot_distance_ ) {
        // snapshot is disabled or the log store is not long enough
        return;
    }
    if ( !state_machine_->chk_create_snapshot() ) {
        // User-defined state machine doesn't want to create a snapshot.
        return;
    }

    bool snapshot_in_action = false;
 try {
    bool f = false;
    ptr<snapshot> local_snp = get_last_snapshot();
    if ( ( !local_snp ||
           ( committed_idx - local_snp->get_last_log_idx() ) >=
                 (ulong)params->snapshot_distance_ ) &&
         snp_in_progress_.compare_exchange_strong(f, true) )
    {
        snapshot_in_action = true;
        p_in("creating a snapshot for index %llu", committed_idx);

        // get the latest configuration info
        ptr<cluster_config> conf = get_config();
        while ( conf->get_log_idx() > committed_idx &&
                conf->get_prev_log_idx() >= log_store_->start_index() ) {
            ptr<log_entry> conf_log
                ( log_store_->entry_at( conf->get_prev_log_idx() ) );
            conf = cluster_config::deserialize(conf_log->get_buf());
        }

        if ( conf->get_log_idx() > committed_idx &&
             conf->get_prev_log_idx() > 0 &&
             conf->get_prev_log_idx() < log_store_->start_index() ) {
            if (!local_snp) {
                // LCOV_EXCL_START
                p_er("No snapshot could be found while no configuration "
                     "cannot be found in current committed logs, "
                     "this is a system error, exiting");
                ctx_->state_mgr_->system_exit(raft_err::N6_no_snapshot_found);
                ::exit(-1);
                return;
                // LCOV_EXCL_STOP
            }
            conf = local_snp->get_last_config();

        } else if ( conf->get_log_idx() > committed_idx &&
                    conf->get_prev_log_idx() == 0 ) {
            // Modified by Jung-Sang Ahn in May, 2018:
            //  Since we remove configure from state machine
            //  (necessary when we clone a node to another node),
            //  config at log idx 1 may not be visiable in some condition.
            p_wn("config at log idx 1 is not availabe, "
                 "config log idx %zu, prev log idx %zu, committed idx %zu",
                 conf->get_log_idx(), conf->get_prev_log_idx(), committed_idx);
            //ctx_->state_mgr_->system_exit(raft_err::N7_no_config_at_idx_one);
            //::exit(-1);
            //return;
        }

        ulong log_term_to_compact = log_store_->term_at(committed_idx);
        ptr<snapshot> new_snapshot
            ( cs_new<snapshot>(committed_idx, log_term_to_compact, conf) );
        p_in( "create snapshot idx %ld log_term %ld\n",
              committed_idx, log_term_to_compact );
        cmd_result<bool>::handler_type handler =
            (cmd_result<bool>::handler_type)
            std::bind( &raft_server::on_snapshot_completed,
                       this,
                       new_snapshot,
                       std::placeholders::_1,
                       std::placeholders::_2 );
        timer_helper tt;
        state_machine_->create_snapshot(*new_snapshot, handler);
        p_in( "create snapshot idx %ld log_term %ld done: %lu us elapsed\n",
              committed_idx, log_term_to_compact, tt.get_us() );

        snapshot_in_action = false;
    }

 } catch (...) {
    p_er( "failed to compact logs at index %llu due to errors",
          committed_idx );
    if (snapshot_in_action) {
        bool val = true;
        snp_in_progress_.compare_exchange_strong(val, false);
    }
 }
}

void raft_server::on_snapshot_completed
     ( ptr<snapshot>& s, bool result, ptr<std::exception>& err )
{
 do { // Dummy loop
    if (err != nilptr) {
        p_er( "failed to create a snapshot due to %s",
              err->what() );
        break;
    }

    if (!result) {
        p_in("the state machine rejects to create the snapshot");
        break;
    }

    {
        recur_lock(lock_);
        p_db("snapshot created, compact the log store");

        ptr<snapshot> new_snp = state_machine_->last_snapshot();
        set_last_snapshot(new_snp);
        ptr<raft_params> params = ctx_->get_params();
        if ( new_snp->get_last_log_idx() >
                 (ulong)params->reserved_log_items_ ) {
            ulong compact_upto = new_snp->get_last_log_idx() -
                                     (ulong)params->reserved_log_items_;
            p_db("log_store_ compact upto %ld", compact_upto);
            log_store_->compact(compact_upto);
        }
    }
 } while (false);

    snp_in_progress_.store(false);
}

void raft_server::reconfigure(const ptr<cluster_config>& new_config) {
    ptr<cluster_config> cur_config = get_config();
    p_in( "new config log idx %zu, prev log idx %zu, "
          "cur config log idx %zu, prev log idx %zu",
          new_config->get_log_idx(), new_config->get_prev_log_idx(),
          cur_config->get_log_idx(), cur_config->get_prev_log_idx() );
    p_db( "system is reconfigured to have %d servers, "
          "last config index: %llu, this config index: %llu",
          new_config->get_servers().size(),
          new_config->get_prev_log_idx(),
          new_config->get_log_idx() );

    thread_local char temp_buf[1024];
    std::string str_buf;

    // Compare old and new configs, to check if
    // the configuration change is for adding this node.
    bool invoke_join_cb =
        ( !cur_config->get_server(id_) && new_config->get_server(id_) );

    // we only allow one server to be added or removed at a time
    std::vector<int32> srvs_removed;
    std::vector< ptr<srv_config> > srvs_added;
    std::list< ptr<srv_config> >& new_srvs(new_config->get_servers());
    for ( std::list<ptr<srv_config>>::const_iterator it = new_srvs.begin();
          it != new_srvs.end(); ++it ) {
        peer_itor pit = peers_.find((*it)->get_id());
        if (pit == peers_.end() && id_ != (*it)->get_id()) {
            srvs_added.push_back(*it);
        }
        if (id_ == (*it)->get_id()) {
            my_priority_ = (*it)->get_priority();
            steps_to_down_ = 0;
            if (role_ == srv_role::follower &&
                catching_up_) {
                // If this node is newly added, start election timer
                // without waiting for the next append_entries message.
                p_in("now this node is the part of cluster, "
                     "catch-up process is done, clearing the flag");
                catching_up_ = false;
                restart_election_timer();
            }
        }
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        if (!new_config->get_server(it->first)) {
            srvs_removed.push_back(it->first);
        }
    }

    if (!new_config->get_server(id_)) {
        srvs_removed.push_back(id_);
    }

    // ===== Adding new server =====
    for ( std::vector<ptr<srv_config>>::const_iterator it = srvs_added.begin();
          it != srvs_added.end(); ++it ) {
        ptr<srv_config> srv_added = *it;
        timer_task<int32>::executor exec =
            (timer_task<int32>::executor)
            std::bind( &raft_server::handle_hb_timeout,
                       this,
                       std::placeholders::_1 );
        ptr<peer> p = cs_new< peer,
                              ptr<srv_config>&,
                              context&,
                              timer_task<int32>::executor&,
                              ptr<logger>& >
                            ( srv_added, *ctx_, exec, l_ );
        p->set_next_log_idx(log_store_->next_slot());

        sprintf(temp_buf,
                "add peer %d, %s, %s\n",
                (int)srv_added->get_id(),
                srv_added->get_endpoint().c_str(),
                srv_added->is_learner() ? "learner" : "voting member");
        str_buf += temp_buf;

        peers_.insert(std::make_pair(srv_added->get_id(), p));
        p_in("server %d is added to cluster", srv_added->get_id());
        if (role_ == srv_role::leader) {
            // Suppress following RPC error as it is expected.
            p->set_suppress_following_error();
            p_in("enable heartbeating for server %d", srv_added->get_id());
            enable_hb_for_peer(*p);
            if (srv_to_join_ && srv_to_join_->get_id() == p->get_id()) {
                p->set_next_log_idx(srv_to_join_->get_next_log_idx());
                srv_to_join_.reset();
            }
        }
    }

    // ===== Removing server =====
    for ( std::vector<int32>::const_iterator it = srvs_removed.begin();
          it != srvs_removed.end(); ++it ) {
        int32 srv_removed = *it;
        if (srv_removed == id_ && !catching_up_) {
            p_in("this server (%d) has been removed from the cluster, "
                 "will step down itself soon. config log idx %zu",
                 id_,
                 new_config->get_log_idx());
            // this server is removed from cluster

            // Modified by Jung-Sang Ahn (Oct 25, 2017):
            // Reset cluster config and remove all other peer info.
            // If not, this server will repeatedly request leader
            // election of the cluster that this server doesn't belong
            // to anymore.

            // Modified by Jung-Sang Ahn (Dec 24, 2019):
            // Now we have a persistent flag for election timer,
            // we don't need to append any dummy config log at the end,
            // for the case re-joining this replica to the original cluster.
            //reset_peer_info();

            cb_func::Param param(id_, leader_);
            CbReturnCode rc = ctx_->cb_func_.call( cb_func::RemovedFromCluster,
                                                   &param );
            (void)rc;
            steps_to_down_ = 2;
        }

        peer_itor pit = peers_.find(srv_removed);
        if (pit != peers_.end()) {
            // WARNING:
            //   We should not remove the peer from the list immediately,
            //   due to the issue described below:
            //
            // 0) Let's suppose there are 3 servers: S1, S2, and S3,
            //    where S1 is the leader and S3 is going to leave.
            // 1) Generate a conf log for removing server S3.
            // 2) The conf log is committed by S1 and S2 only.
            // 3) Before delivering the conf log to S3, S1 removes
            //    the S3 peer info from the list.
            // 4) It closes the connection to S3.
            // 5) S3 cannot commit the config (containing removing S3).
            // 6) Callback function for `RemovedFromCluster` will be missing,
            //    but S3 will step down itself after 2 timeout period.
            //
            // To address it, we will remove S3 only after the commit index
            // of the last config is delivered to S3.
            // Also we will have timeout for it. If we fail to deliver the
            // commit index, S3 will be just force removed.
            const ptr<peer>& pp = pit->second;

            if (role_ == srv_role::leader && srv_to_leave_) {
                // If leader, keep the to-be-removed server in peer list
                // until 1) catch-up is done, or 2) timeout.
                p_in("srv_to_leave_: %d", srv_to_leave_->get_id());
                ptr<snapshot_sync_ctx> snp_ctx = srv_to_leave_->get_snapshot_sync_ctx();
                if (snp_ctx) {
                    void* user_ctx = snp_ctx->get_user_snp_ctx();
                    p_in("srv_to_leave_ has snapshot context %p and user context %p, "
                         "destroy them",
                         snp_ctx.get(), user_ctx);
                    clear_snapshot_sync_ctx(*srv_to_leave_);
                }

                // However, if `srv_to_leave_` is NULL,
                // it is replaying old config. We can remove it
                // immediately without setting `srv_to_leave_`.

            } else {
                if (!srv_to_leave_) {
                    p_in("srv_to_leave_ is currently empty "
                         "on config for removing %d",
                         pp->get_id());
                }
                remove_peer_from_peers(pp);
                sprintf(temp_buf, "remove peer %d\n", srv_removed);
                str_buf += temp_buf;
            }
        } else {
            p_in("peer %d cannot be found, no action for removing", srv_removed);
        }
    }

    if (!str_buf.empty()) {
        p_in("%s", str_buf.c_str());
    }

    set_config(new_config);

    if ( uncommitted_config_ &&
         uncommitted_config_->get_log_idx() == new_config->get_log_idx() ) {
        // All configs are committed.
        p_in("clearing uncommitted config at log %zu, prev %zu",
             uncommitted_config_->get_log_idx(),
             uncommitted_config_->get_prev_log_idx());
        uncommitted_config_.reset();
    }

    if (invoke_join_cb) {
        cb_func::Param param(id_, leader_);
        ptr<cluster_config> c_conf = get_config();
        param.ctx = (void*)c_conf.get();
        CbReturnCode rc = ctx_->cb_func_.call(cb_func::JoinedCluster, &param);
        (void)rc;
    }

    str_buf = "";
    for (auto& entry: new_config->get_servers()) {
        srv_config* s_conf = entry.get();

        // SHOULD update peer's srv_config.
        for (auto& entry_peer: peers_) {
            peer* pp = entry_peer.second.get();
            std::lock_guard<std::mutex> l(pp->get_lock());
            if (pp->get_id() == s_conf->get_id()) {
                pp->set_config(entry);
            }
        }

        sprintf(temp_buf, "peer %d, DC ID %d, %s, %s, %d\n",
                (int)s_conf->get_id(),
                (int)s_conf->get_dc_id(),
                s_conf->get_endpoint().c_str(),
                s_conf->is_learner() ? "learner" : "voting member",
                s_conf->get_priority());
        str_buf += temp_buf;
    }
    p_in("new configuration: log idx %ld, prev log idx %ld\n"
         "%smy id: %d, leader: %d, term: %zu",
         new_config->get_log_idx(), new_config->get_prev_log_idx(),
         str_buf.c_str(), id_, leader_.load(), state_->get_term());

    update_target_priority();
}

void raft_server::remove_peer_from_peers(const ptr<peer>& pp) {
    p_in("server %d is removed from cluster", pp->get_id());
    pp->enable_hb(false);
    clear_snapshot_sync_ctx(*pp);
    peers_.erase(pp->get_id());
}

} // namespace nuraft;

