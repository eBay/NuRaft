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
#include "event_awaiter.h"
#include "peer.hxx"
#include "state_machine.hxx"
#include "state_mgr.hxx"
#include "tracer.hxx"

#include <cassert>
#include <sstream>

namespace nuraft {

ptr<resp_msg> raft_server::handle_add_srv_req(req_msg& req) {
    std::vector< ptr<log_entry> >& entries = req.log_entries();
    ptr<resp_msg> resp = cs_new<resp_msg>
                         ( state_->get_term(),
                           msg_type::add_server_response,
                           id_,
                           leader_ );

    if ( entries.size() != 1 ||
         entries[0]->get_val_type() != log_val_type::cluster_server ) {
        p_db( "bad add server request as we are expecting one log entry "
              "with value type of ClusterServer" );
        resp->set_result_code(cmd_result_code::BAD_REQUEST);
        return resp;
    }

    if (role_ != srv_role::leader || write_paused_) {
        p_er("this is not a leader, cannot handle AddServerRequest");
        resp->set_result_code(cmd_result_code::NOT_LEADER);
        return resp;
    }

    // Before checking duplicate ID, confirm srv_to_leave_ is gone.
    check_srv_to_leave_timeout();
    ptr<srv_config> srv_conf =
        srv_config::deserialize( entries[0]->get_buf() );
    if ( peers_.find( srv_conf->get_id() ) != peers_.end() ||
         id_ == srv_conf->get_id() ) {
        p_wn( "the server to be added has a duplicated "
              "id with existing server %d",
              srv_conf->get_id() );
        resp->set_result_code(cmd_result_code::SERVER_ALREADY_EXISTS);
        return resp;
    }

    if (config_changing_) {
        // the previous config has not committed yet
        p_wn("previous config has not committed yet");
        resp->set_result_code(cmd_result_code::CONFIG_CHANGING);
        return resp;
    }

    if (srv_to_join_) {
        // Adding server is already in progress.

        // Check the last active time of that server.
        ulong last_active_ms = srv_to_join_->get_active_timer_us() / 1000;
        p_wn("previous adding server (%d) is in progress, "
             "last activity: %zu ms ago",
             srv_to_join_->get_id(),
             last_active_ms);

        if ( last_active_ms <=
                 (ulong)raft_server::raft_limits_.response_limit_ *
                 ctx_->get_params()->heart_beat_interval_ ) {
            resp->set_result_code(cmd_result_code::SERVER_IS_JOINING);
            return resp;
        }
        // Otherwise: activity timeout, reset the server.
        p_wn("activity timeout, start over");
        reset_srv_to_join();
    }

    conf_to_add_ = std::move(srv_conf);
    timer_task<int32>::executor exec =
        (timer_task<int32>::executor)
        std::bind( &raft_server::handle_hb_timeout,
                   this,
                   std::placeholders::_1 );
    srv_to_join_ = cs_new< peer,
                           ptr<srv_config>&,
                           context&,
                           timer_task<int32>::executor&,
                           ptr<logger>& >
                         ( conf_to_add_, *ctx_, exec, l_ );
    invite_srv_to_join_cluster();
    resp->accept(log_store_->next_slot());
    return resp;
}

void raft_server::invite_srv_to_join_cluster() {
    ptr<req_msg> req = cs_new<req_msg>
                       ( state_->get_term(),
                         msg_type::join_cluster_request,
                         id_,
                         srv_to_join_->get_id(),
                         0L,
                         log_store_->next_slot() - 1,
                         quick_commit_index_.load() );

    ptr<cluster_config> c_conf = get_config();
    req->log_entries().push_back
        ( cs_new<log_entry>
          ( state_->get_term(), c_conf->serialize(), log_val_type::conf ) );
    srv_to_join_->send_req(srv_to_join_, req, ex_resp_handler_);
    p_in("sent join request to peer %d, %s",
         srv_to_join_->get_id(),
         srv_to_join_->get_endpoint().c_str());
}

ptr<resp_msg> raft_server::handle_join_cluster_req(req_msg& req) {
    std::vector<ptr<log_entry>>& entries = req.log_entries();
    ptr<resp_msg> resp = cs_new<resp_msg>
                         ( state_->get_term(),
                           msg_type::join_cluster_response,
                           id_,
                           req.get_src() );
    if ( entries.size() != 1 ||
         entries[0]->get_val_type() != log_val_type::conf ) {
        p_in("receive an invalid JoinClusterRequest as the log entry value "
             "doesn't meet the requirements");
        return resp;
    }

    // MONSTOR-8244:
    //   Adding server may be called multiple times while previous process is
    //   in progress. It should gracefully handle the new request and should
    //   not ruin the current request.
    bool reset_commit_idx = true;
    if (catching_up_) {
        p_wn("this server is already in log syncing mode, "
             "but let's do it again: sm idx %zu, quick commit idx %zu, "
             "will not reset commit index",
             sm_commit_index_.load(),
             quick_commit_index_.load());
        reset_commit_idx = false;
    }

    p_in("got join cluster req from leader %d", req.get_src());
    catching_up_ = true;
    role_ = srv_role::follower;
    leader_ = req.get_src();

    cb_func::Param follower_param(id_, leader_);
    (void) ctx_->cb_func_.call(cb_func::BecomeFollower, &follower_param);

    if (reset_commit_idx) {
        // MONSTOR-7503: We should not reset it to 0.
        sm_commit_index_.store( initial_commit_index_ );
        quick_commit_index_.store( initial_commit_index_ );
    }

    state_->set_voted_for(-1);
    state_->set_term(req.get_term());
    ctx_->state_mgr_->save_state(*state_);
    reconfigure(cluster_config::deserialize(entries[0]->get_buf()));

    resp->accept( quick_commit_index_.load() + 1 );
    return resp;
}

void raft_server::handle_join_cluster_resp(resp_msg& resp) {
    if (srv_to_join_ && srv_to_join_ == resp.get_peer()) {
        if (resp.get_accepted()) {
            p_in("new server (%d) confirms it will join, "
                 "start syncing logs to it", srv_to_join_->get_id());
            sync_log_to_new_srv(resp.get_next_idx());
        } else {
            p_wn("new server (%d) cannot accept the invitation, give up",
                 srv_to_join_->get_id());
        }
    } else {
        p_wn("no server to join, drop the message");
    }
}

void raft_server::sync_log_to_new_srv(ulong start_idx) {
    p_db("[SYNC LOG] peer %d start idx %llu, my log start idx %llu\n",
         srv_to_join_->get_id(), start_idx, log_store_->start_index());
    // only sync committed logs
    ulong gap = ( quick_commit_index_ > start_idx )
                ? ( quick_commit_index_ - start_idx )
                : 0;
    ptr<raft_params> params = ctx_->get_params();
    if ( ( params->log_sync_stop_gap_ > 0 &&
           gap < (ulong)params->log_sync_stop_gap_ ) ||
         params->log_sync_stop_gap_ == 0 ) {
        p_in( "[SYNC LOG] LogSync is done for server %d "
              "with log gap %zu (%zu - %zu, limit %d), "
              "now put the server into cluster",
              srv_to_join_->get_id(),
              gap, quick_commit_index_.load(), start_idx,
              params->log_sync_stop_gap_ );

        ptr<cluster_config> cur_conf = get_config();

        // WARNING:
        //   If there is any uncommitted changed config,
        //   new config should be generated on top of it.
        if (uncommitted_config_) {
            p_in("uncommitted config exists at log %zu, prev log %zu",
                 uncommitted_config_->get_log_idx(),
                 uncommitted_config_->get_prev_log_idx());
            cur_conf = uncommitted_config_;
        }

        ptr<cluster_config> new_conf = cs_new<cluster_config>
                                       ( log_store_->next_slot(),
                                         cur_conf->get_log_idx() );
        new_conf->get_servers().insert( new_conf->get_servers().end(),
                                        cur_conf->get_servers().begin(),
                                        cur_conf->get_servers().end() );
        new_conf->get_servers().push_back(conf_to_add_);
        new_conf->set_user_ctx( cur_conf->get_user_ctx() );
        new_conf->set_async_replication
                  ( cur_conf->is_async_replication() );

        ptr<buffer> new_conf_buf(new_conf->serialize());
        ptr<log_entry> entry( cs_new<log_entry>( state_->get_term(),
                                                 new_conf_buf,
                                                 log_val_type::conf ) );
        store_log_entry(entry);
        config_changing_ = true;
        uncommitted_config_ = new_conf;
        request_append_entries();
        return;
    }

    ptr<req_msg> req;

    // Modified by Jung-Sang Ahn, 12/22, 2017.
    // When snapshot transmission is still in progress, start_idx can be 0.
    // We should tolerate this.
    if (/* start_idx > 0 && */ start_idx < log_store_->start_index()) {
        srv_to_join_snp_retry_required_ = false;
        req = create_sync_snapshot_req( *srv_to_join_,
                                        start_idx,
                                        state_->get_term(),
                                        quick_commit_index_);
        if (req == nullptr) {
            // If reading snapshot fails, enable HB temporarily to retry it.
            srv_to_join_snp_retry_required_ = true;
            enable_hb_for_peer(*srv_to_join_);
            return;
        }

    } else {
        int32 size_to_sync = std::min(gap, (ulong)params->log_sync_batch_size_);
        ptr<buffer> log_pack = log_store_->pack(start_idx, size_to_sync);
        p_db( "size to sync: %d, log_pack size %zu\n",
              size_to_sync, log_pack->size() );
        req = cs_new<req_msg>( state_->get_term(),
                               msg_type::sync_log_request,
                               id_,
                               srv_to_join_->get_id(),
                               0L,
                               start_idx - 1,
                               quick_commit_index_.load() );
        req->log_entries().push_back
            ( cs_new<log_entry>
              ( state_->get_term(), log_pack, log_val_type::log_pack) );
    }

    srv_to_join_->send_req(srv_to_join_, req, ex_resp_handler_);
}

ptr<resp_msg> raft_server::handle_log_sync_req(req_msg& req) {
    std::vector<ptr<log_entry>>& entries = req.log_entries();
    ptr<resp_msg> resp
        ( cs_new<resp_msg>
          ( state_->get_term(), msg_type::sync_log_response, id_,
            req.get_src(), log_store_->next_slot() ) );

    p_db("entries size %d, type %d, catching_up %s\n",
         (int)entries.size(), (int)entries[0]->get_val_type(),
         (catching_up_)?"true":"false");
    if ( entries.size() != 1 ||
         entries[0]->get_val_type() != log_val_type::log_pack ) {
        p_wn("receive an invalid LogSyncRequest as the log entry value "
             "doesn't meet the requirements: entries size %zu",
             entries.size() );
        return resp;
    }

    if (!catching_up_) {
        p_wn("This server is ready for cluster, ignore the request, "
             "my next log idx %llu", resp->get_next_idx());
        return resp;
    }

    log_store_->apply_pack(req.get_last_log_idx() + 1, entries[0]->get_buf());
    p_db("last log %ld\n", log_store_->next_slot() - 1);
    precommit_index_ = log_store_->next_slot() - 1;
    commit(log_store_->next_slot() - 1);
    resp->accept(log_store_->next_slot());
    return resp;
}

void raft_server::handle_log_sync_resp(resp_msg& resp) {
    if (srv_to_join_) {
        p_db("srv_to_join: %d\n", srv_to_join_->get_id());
        // we are reusing heartbeat interval value to indicate when to stop retry
        srv_to_join_->resume_hb_speed();
        srv_to_join_->set_next_log_idx(resp.get_next_idx());
        srv_to_join_->set_matched_idx(resp.get_next_idx() - 1);
        sync_log_to_new_srv(resp.get_next_idx());
    } else {
        p_wn("got log sync resp while srv_to_join is null");
    }
}

ptr<resp_msg> raft_server::handle_rm_srv_req(req_msg& req) {
    std::vector<ptr<log_entry>>& entries = req.log_entries();
    ptr<resp_msg> resp = cs_new<resp_msg>
                         ( state_->get_term(),
                           msg_type::remove_server_response,
                           id_,
                           leader_ );

    if (entries.size() != 1 || entries[0]->get_buf().size() != sz_int) {
        p_wn("bad remove server request as we are expecting "
             "one log entry with value type of int");
        resp->set_result_code(cmd_result_code::BAD_REQUEST);
        return resp;
    }

    if (role_ != srv_role::leader || write_paused_) {
        p_wn("this is not a leader, cannot handle RemoveServerRequest");
        resp->set_result_code(cmd_result_code::NOT_LEADER);
        return resp;
    }

    check_srv_to_leave_timeout();
    if (srv_to_leave_) {
        p_wn("previous to-be-removed server %d has not left yet",
             srv_to_leave_->get_id());
        resp->set_result_code(cmd_result_code::SERVER_IS_LEAVING);
        return resp;
    }
    // NOTE:
    //   Although `srv_to_leave_` is not set, we should check if
    //   there is any peer whose leave flag is set.
    for (auto& entry: peers_) {
        ptr<peer> pp = entry.second;
        if (pp->is_leave_flag_set()) {
            p_wn("leave flag of server %d is set, but the server "
                 "has not left yet",
                 pp->get_id());
            resp->set_result_code(cmd_result_code::SERVER_IS_LEAVING);
            return resp;
        }
    }

    if (config_changing_) {
        // the previous config has not committed yet
        p_wn("previous config has not committed yet");
        resp->set_result_code(cmd_result_code::CONFIG_CHANGING);
        return resp;
    }

    int32 srv_id = entries[0]->get_buf().get_int();
    if (srv_id == id_) {
        p_wn("cannot request to remove leader");
        resp->set_result_code(cmd_result_code::CANNOT_REMOVE_LEADER);
        return resp;
    }

    peer_itor pit = peers_.find(srv_id);
    if (pit == peers_.end()) {
        p_wn("server %d does not exist", srv_id);
        resp->set_result_code(cmd_result_code::SERVER_NOT_FOUND);
        return resp;
    }

    ptr<peer> p = pit->second;
    ptr<req_msg> leave_req( cs_new<req_msg>
                            ( state_->get_term(),
                              msg_type::leave_cluster_request,
                              id_, srv_id, 0,
                              log_store_->next_slot() - 1,
                              quick_commit_index_.load() ) );
    // WARNING:
    //   DO NOT reset HB counter to 0 as removing server
    //   may be requested multiple times, and anyway we should
    //   remove that server.
    p->set_leave_flag();

    if (p->make_busy()) {
        p->send_req(p, leave_req, ex_resp_handler_);
        p_in("sent leave request to peer %d", p->get_id());
    } else {
        p->set_rsv_msg(leave_req, ex_resp_handler_);
        p_in("peer %d is currently busy, keep the message", p->get_id());
    }

    resp->accept(log_store_->next_slot());
    return resp;
}

ptr<resp_msg> raft_server::handle_leave_cluster_req(req_msg& req) {
    ptr<resp_msg> resp
        ( cs_new<resp_msg>( state_->get_term(),
                            msg_type::leave_cluster_response,
                            id_,
                            req.get_src() ) );
    if (!config_changing_) {
        p_db("leave cluster, set steps to down to 2");
        steps_to_down_ = 2;
        resp->accept(log_store_->next_slot());
    }

    return resp;
}

void raft_server::handle_leave_cluster_resp(resp_msg& resp) {
    if (!resp.get_accepted()) {
        p_db("peer doesn't accept to stepping down, stop proceeding");
        return;
    }

    p_db("peer accepted to stepping down, removing this server from cluster");
    rm_srv_from_cluster(resp.get_src());
}

void raft_server::rm_srv_from_cluster(int32 srv_id) {
    if (srv_to_leave_) {
        p_wn("to-be-removed server %d already exists, "
             "cannot remove server %d for now",
             srv_to_leave_->get_id(), srv_id);
        return;
    }

    ptr<cluster_config> cur_conf = get_config();

    // NOTE: Need to honor uncommitted config,
    //       refer to comment in `sync_log_to_new_srv()`
    if (uncommitted_config_) {
        p_in("uncommitted config exists at log %zu, prev log %zu",
             uncommitted_config_->get_log_idx(),
             uncommitted_config_->get_prev_log_idx());
        cur_conf = uncommitted_config_;
    }

    ptr<cluster_config> new_conf = cs_new<cluster_config>
                                   ( log_store_->next_slot(),
                                     cur_conf->get_log_idx() );
    for ( cluster_config::const_srv_itor it = cur_conf->get_servers().begin();
          it != cur_conf->get_servers().end();
          ++it ) {
        if ((*it)->get_id() != srv_id) {
            new_conf->get_servers().push_back(*it);
        }
    }
    new_conf->set_user_ctx( cur_conf->get_user_ctx() );
    new_conf->set_async_replication
              ( cur_conf->is_async_replication() );

    p_in( "removed server %d from configuration and "
          "save the configuration to log store at %llu",
          srv_id,
          new_conf->get_log_idx() );

    config_changing_ = true;
    uncommitted_config_ = new_conf;
    ptr<buffer> new_conf_buf( new_conf->serialize() );
    ptr<log_entry> entry( cs_new<log_entry>( state_->get_term(),
                                             new_conf_buf,
                                             log_val_type::conf ) );
    store_log_entry(entry);

    auto p_entry = peers_.find(srv_id);
    if (p_entry != peers_.end()) {
        ptr<peer> pp = p_entry->second;
        srv_to_leave_ = pp;
        srv_to_leave_target_idx_ = new_conf->get_log_idx();
        p_in("set srv_to_leave_, "
             "server %d will be removed from cluster, config %zu",
             srv_id, srv_to_leave_target_idx_);
    }

    request_append_entries();
}

void raft_server::handle_join_leave_rpc_err(msg_type t_msg, ptr<peer> p) {
    if (t_msg == msg_type::leave_cluster_request) {
        p_in( "rpc failed for removing server (%d), "
              "will remove this server directly",
              p->get_id() );

        /**
         * In case of there are only two servers in the cluster,
         * it will be safe to remove the server directly from peers
         * as at most one config change could happen at a time
         *   prove:
         *     assume there could be two config changes at a time
         *     this means there must be a leader after previous leader
         *     offline, which is impossible (no leader could be elected
         *     after one server goes offline in case of only two servers
         *     in a cluster)
         * so the bug
         *   https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
         * does not apply to cluster which only has two members
         */
        if (peers_.size() == 1) {
            peer_itor pit = peers_.find(p->get_id());
            if (pit != peers_.end()) {
                pit->second->enable_hb(false);
                peers_.erase(pit);
                p_in("server %d is removed from cluster", p->get_id());
            } else {
                p_in("peer %d cannot be found, no action for removing",
                     p->get_id());
            }

            if (srv_to_leave_) {
                reset_srv_to_leave();
            }
        }

        if (srv_to_leave_) {
            // WARNING:
            //   If `srv_to_leave_` is already set, this function is probably
            //   invoked by `handle_hb_timeout`. In such a case, the server
            //   to be removed does not respond while the leader already
            //   generated the log for the configuration change. We should
            //   abandon the peer entry from `peers_`.
            p_wn("srv_to_leave_ is already set to %d, will remove it from "
                 "peer list", srv_to_leave_->get_id());
            remove_peer_from_peers(srv_to_leave_);
            reset_srv_to_leave();

        } else {
            // Set `srv_to_leave_` and generate a log for configuration change.
            rm_srv_from_cluster(p->get_id());
        }

    } else {
        p_in( "rpc failed again for the new coming server (%d), "
              "will stop retry for this server",
              p->get_id() );
        config_changing_ = false;
        reset_srv_to_join();
    }
}

void raft_server::reset_srv_to_join() {
    clear_snapshot_sync_ctx(*srv_to_join_);
    srv_to_join_->shutdown();
    srv_to_join_.reset();
}

void raft_server::reset_srv_to_leave() {
    srv_to_leave_->shutdown();
    srv_to_leave_.reset();
    srv_to_leave_target_idx_ = 0;
    p_in("clearing srv_to_leave_");
}

} // namespace nuraft;

