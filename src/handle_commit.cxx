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

    p_tr( "local log idx %lu, target_commit_dx %lu\n"
          "quick_commit_index_ %lu, state_->get_commit_idx() %lu\n",
          log_store_->next_slot() - 1, target_idx,
          quick_commit_index_.load(), sm_commit_index_.load() );

    if ( log_store_->next_slot() - 1 > sm_commit_index_ &&
         quick_commit_index_ > sm_commit_index_ ) {
        p_tr("commit_cv_ notify\n");
        std::unique_lock<std::mutex> lock(commit_cv_lock_);
        commit_cv_.notify_one();
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
    while (true) {
     try {
        while ( quick_commit_index_ <= sm_commit_index_ ||
                sm_commit_index_ >= log_store_->next_slot() - 1 ) {
            std::unique_lock<std::mutex> lock(commit_cv_lock_);

            p_tr("commit_cv_ sleep\n");
            commit_cv_.wait(lock);

            p_tr("commit_cv_ wake up\n");
            if (stopping_) {
                lock.unlock();
                lock.release();
                { std::unique_lock<std::mutex> lock2(ready_to_stop_cv_lock_);
                  ready_to_stop_cv_.notify_all(); }
                commit_bg_stopped_ = true;
                return;
            }
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

        while ( sm_commit_index_ < quick_commit_index_ &&
                sm_commit_index_ < log_store_->next_slot() - 1 ) {
            sm_commit_index_ += 1;
            ptr<log_entry> le = log_store_->entry_at(sm_commit_index_);
            p_tr( "commit upto %llu, curruent idx %llu\n",
                  quick_commit_index_.load(), sm_commit_index_.load() );

            if (le->get_term() == 0) {
                // Zero term means that log store is corrupted
                // (failed to read log).
                p_ft( "empty log at idx %llu, must be log corruption",
                      sm_commit_index_.load() );
                ctx_->state_mgr_->system_exit(raft_err::N19_bad_log_idx_for_term);
                ::exit(-1);
            }

            if (le->get_val_type() == log_val_type::app_log) {
                commit_app_log(le);

            } else if (le->get_val_type() == log_val_type::conf) {
                commit_conf(le);
            }

            snapshot_and_compact(sm_commit_index_);
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

     } catch (std::exception& err) {
        commit_bg_stopped_ = true;
        p_er( "background committing thread encounter err %s, "
              "exiting to protect the system",
              err.what() );
        ctx_->state_mgr_->system_exit(raft_err::N20_background_commit_err);
        ::exit(-1);
     }
    }
    commit_bg_stopped_ = true;
}

void raft_server::commit_app_log(ptr<log_entry>& le) {
    ptr<buffer> ret_value = nullptr;
    ptr<buffer> buf = le->get_buf_ptr();
    buf->pos(0);
    ret_value = state_machine_->commit_ext
                ( state_machine::ext_op_params( sm_commit_index_, buf ) );
    if (ret_value) ret_value->pos(0);

    std::list< ptr<commit_ret_elem> > async_elems;
    {   std::unique_lock<std::mutex> cre_lock(commit_ret_elems_lock_);
        auto entry = commit_ret_elems_.begin();
        while (entry != commit_ret_elems_.end()) {
            ptr<commit_ret_elem> elem = entry->second;
            if (elem->idx_ > sm_commit_index_) {
                break;
            } else if (elem->idx_ == sm_commit_index_) {
                elem->result_code_ = cmd_result_code::OK;
                elem->ret_value_ = ret_value;
            }
            p_dv("notify cb %ld %p",
                 sm_commit_index_.load(), &elem->awaiter_);

            switch (ctx_->get_params()->return_method_) {
            case raft_params::blocking:
            default:
                // Blocking mode: invoke waiting function.
                elem->awaiter_.invoke();
                entry++;
                break;

            case raft_params::async_handler:
                // Async handler: put into list.
                async_elems.push_back(elem);
                entry = commit_ret_elems_.erase(entry);
                break;
            }
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

void raft_server::commit_conf(ptr<log_entry>& le) {
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
    uint64_t log_idx = sm_commit_index_;
    param.ctx = &log_idx;
    ctx_->cb_func_.call(cb_func::NewConfig, &param);

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
                p_er("No snapshot could be found while no configuration "
                     "cannot be found in current committed logs, "
                     "this is a system error, exiting");
                ctx_->state_mgr_->system_exit(raft_err::N6_no_snapshot_found);
                ::exit(-1);
                return;
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
            p_in("this server (%d) has been removed from the cluster\n", id_);
            // this server is removed from cluster

            // Modified by Jung-Sang Ahn (Oct 25, 2017):
            // Reset cluster config and remove all other peer info.
            // If not, this server will repeatedly request leader
            // election of the cluster that this server doesn't belong
            // to anymore.
            reset_peer_info();

            cb_func::Param param(id_, leader_);
            CbReturnCode rc = ctx_->cb_func_.call( cb_func::RemovedFromCluster,
                                                   &param );
            (void)rc;

            p_in("server has been removed, step down");
            return;
        }

        peer_itor pit = peers_.find(srv_removed);
        if (pit != peers_.end()) {
            pit->second->enable_hb(false);

            sprintf(temp_buf, "remove peer %d\n", srv_removed);
            str_buf += temp_buf;

            peers_.erase(pit);
            p_in("server %d is removed from cluster", srv_removed);
        } else {
            p_in("peer %d cannot be found, no action for removing", srv_removed);
        }
    }

    if (!str_buf.empty()) {
        p_in("%s", str_buf.c_str());
    }

    set_config(new_config);

    str_buf = "";
    for (auto& entry: new_config->get_servers()) {
        srv_config* s_conf = entry.get();

        // SHOULD update peer's srv_config.
        for (auto& entry_peer: peers_) {
            peer* pp = entry_peer.second.get();
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

} // namespace nuraft;

