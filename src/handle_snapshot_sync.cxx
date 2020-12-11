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

#include "context.hxx"
#include "error_code.hxx"
#include "event_awaiter.h"
#include "peer.hxx"
#include "snapshot.hxx"
#include "snapshot_sync_ctx.hxx"
#include "state_machine.hxx"
#include "state_mgr.hxx"
#include "tracer.hxx"

#include <cassert>
#include <sstream>

namespace nuraft {

int32 raft_server::get_snapshot_sync_block_size() const {
    int32 block_size = ctx_->get_params()->snapshot_block_size_;
    return block_size == 0 ? default_snapshot_sync_block_size : block_size;
}

ptr<req_msg> raft_server::create_sync_snapshot_req(peer& p,
                                                   ulong last_log_idx,
                                                   ulong term,
                                                   ulong commit_idx) {
    std::lock_guard<std::mutex> guard(p.get_lock());
    ptr<snapshot_sync_ctx> sync_ctx = p.get_snapshot_sync_ctx();
    ptr<snapshot> snp = nullptr;
    ulong prev_sync_snp_log_idx = 0;
    if (sync_ctx) {
        snp = sync_ctx->get_snapshot();
        p_db( "previous sync_ctx exists %p, offset %zu, snp idx %zu, user_ctx %p",
              sync_ctx.get(),
              sync_ctx->offset_,
              snp->get_last_log_idx(),
              sync_ctx->get_user_snp_ctx() );
        prev_sync_snp_log_idx = snp->get_last_log_idx();

        if (sync_ctx->get_timer().timeout()) {
            p_in("previous sync_ctx %p timed out, reset it", sync_ctx.get());
            void*& user_ctx = sync_ctx->get_user_snp_ctx();
            state_machine_->free_user_snp_ctx(user_ctx);
            sync_ctx->set_offset(0);
        }
    }

    // Modified by Jung-Sang Ahn, May 15 2018:
    //   Even though new snapshot has been created,
    //   keep using existing snapshot, as new snapshot will reset
    //   previous catching-up.
    //
    // if ( !snp /*||
    //      ( last_snapshot_ &&
    //        last_snapshot_->get_last_log_idx() > snp->get_last_log_idx() )*/ ) {
    if ( !snp || sync_ctx->get_offset() == 0 ) {
        snp = get_last_snapshot();
        if ( snp == nilptr ||
             last_log_idx > snp->get_last_log_idx() ) {
            // LCOV_EXCL_START
            p_er( "system is running into fatal errors, failed to find a "
                  "snapshot for peer %d (snapshot null: %d, snapshot "
                  "doesn't contais lastLogIndex: %d)",
                  p.get_id(), snp == nilptr ? 1 : 0,
                  last_log_idx > snp->get_last_log_idx() ? 1 : 0 );
            if (snp) {
                p_er("last log idx %zu, snp last log idx %zu",
                     last_log_idx, snp->get_last_log_idx());
            }
            ctx_->state_mgr_->system_exit(raft_err::N16_snapshot_for_peer_not_found);
            ::exit(-1);
            return ptr<req_msg>();
            // LCOV_EXCL_STOP
        }

        if ( snp->get_type() == snapshot::raw_binary &&
             snp->size() < 1L ) {
            // LCOV_EXCL_START
            p_er("invalid snapshot, this usually means a bug from state "
                 "machine implementation, stop the system to prevent "
                 "further errors");
            ctx_->state_mgr_->system_exit(raft_err::N17_empty_snapshot);
            ::exit(-1);
            return ptr<req_msg>();
            // LCOV_EXCL_STOP
        }

        if (snp->get_last_log_idx() != prev_sync_snp_log_idx) {
            p_in( "trying to sync snapshot with last index %llu to peer %d, "
                  "its last log idx %llu",
                  snp->get_last_log_idx(), p.get_id(), last_log_idx );
        }
        if (sync_ctx) {
            // If previous user context exists, should free it
            // as it causes memory leak.
            void*& user_ctx = sync_ctx->get_user_snp_ctx();
            state_machine_->free_user_snp_ctx(user_ctx);
        }
        p.set_snapshot_in_sync(snp);
    }

    bool last_request = false;
    ptr<buffer> data = nullptr;
    ulong data_idx = 0;
    if (snp->get_type() == snapshot::raw_binary) {
        // LCOV_EXCL_START
        // Raw binary snapshot (original)
        ulong offset = p.get_snapshot_sync_ctx()->get_offset();
        ulong sz_left = ( snp->size() > offset ) ? ( snp->size() - offset ) : 0;
        int32 blk_sz = get_snapshot_sync_block_size();
        data = buffer::alloc((size_t)(std::min((ulong)blk_sz, sz_left)));
        int32 sz_rd = state_machine_->read_snapshot_data(*snp, offset, *data);
        if ((size_t)sz_rd < data->size()) {
            // LCOV_EXCL_START
            p_er( "only %d bytes could be read from snapshot while %d "
                  "bytes are expected, must be something wrong, exit.",
                  sz_rd, data->size() );
            ctx_->state_mgr_->system_exit(raft_err::N18_partial_snapshot_block);
            ::exit(-1);
            return ptr<req_msg>();
            // LCOV_EXCL_STOP
        }
        last_request = (offset + (ulong)data->size()) >= snp->size();
        data_idx = offset;
        // LCOV_EXCL_STOP

    } else {
        // Logical object type snapshot
        ulong obj_idx = p.get_snapshot_sync_ctx()->get_offset();
        void*& user_snp_ctx = p.get_snapshot_sync_ctx()->get_user_snp_ctx();
        p_dv("peer: %d, obj_idx: %ld, user_snp_ctx %p\n",
             (int)p.get_id(), obj_idx, user_snp_ctx);
        state_machine_->read_logical_snp_obj( *snp, user_snp_ctx, obj_idx,
                                              data, last_request );
        if (data) data->pos(0);
        data_idx = obj_idx;
    }

    std::unique_ptr<snapshot_sync_req> sync_req
        ( new snapshot_sync_req(snp, data_idx, data, last_request) );
    ptr<req_msg> req( cs_new<req_msg>
                      ( term,
                        msg_type::install_snapshot_request,
                        id_,
                        p.get_id(),
                        snp->get_last_log_term(),
                        snp->get_last_log_idx(),
                        commit_idx ) );
    req->log_entries().push_back( cs_new<log_entry>
                                  ( term,
                                    sync_req->serialize(),
                                    log_val_type::snp_sync_req ) );
    return req;
}

ptr<resp_msg> raft_server::handle_install_snapshot_req(req_msg& req) {
    if (req.get_term() == state_->get_term() && !catching_up_) {
        if (role_ == srv_role::candidate) {
            become_follower();

        } else if (role_ == srv_role::leader) {
            // LCOV_EXCL_START
            p_er( "Receive InstallSnapshotRequest from another leader(%d) "
                  "with same term, there must be a bug, server exits",
                  req.get_src() );
            ctx_->state_mgr_->system_exit
                ( raft_err::N10_leader_receive_InstallSnapshotRequest );
            ::exit(-1);
            return ptr<resp_msg>();
            // LCOV_EXCL_STOP

        } else {
            restart_election_timer();
        }
    }

    ptr<resp_msg> resp = cs_new<resp_msg>
                         ( state_->get_term(),
                           msg_type::install_snapshot_response,
                           id_,
                           req.get_src(),
                           log_store_->next_slot() );

    if (!catching_up_ && req.get_term() < state_->get_term()) {
        p_wn("received an install snapshot request (%zu) which has lower term "
             "than this server (%zu), decline the request",
             req.get_term(), state_->get_term());
        return resp;
    }

    std::vector<ptr<log_entry>>& entries(req.log_entries());
    if ( entries.size() != 1 ||
         entries[0]->get_val_type() != log_val_type::snp_sync_req ) {
        p_wn("Receive an invalid InstallSnapshotRequest due to "
             "bad log entries or bad log entry value");
        return resp;
    }

    ptr<snapshot_sync_req> sync_req =
        snapshot_sync_req::deserialize(entries[0]->get_buf());
    if (sync_req->get_snapshot().get_last_log_idx() <= sm_commit_index_) {
        p_wn( "received a snapshot (%zu) that is older than "
              "current commit idx (%zu), last log idx %zu",
              sync_req->get_snapshot().get_last_log_idx(),
              sm_commit_index_.load(),
              log_store_->next_slot() - 1);
        // Put dummy CTX to end the snapshot sync.
        ptr<buffer> done_ctx = buffer::alloc(1);
        done_ctx->pos(0);
        done_ctx->put((byte)0);
        done_ctx->pos(0);
        resp->set_ctx(done_ctx);
        return resp;
    }

    if (handle_snapshot_sync_req(*sync_req)) {
        if (sync_req->get_snapshot().get_type() == snapshot::raw_binary) {
            // LCOV_EXCL_START
            // Raw binary: add received byte to offset.
            resp->accept(sync_req->get_offset() + sync_req->get_data().size());
            // LCOV_EXCL_STOP

        } else {
            // Object type: add one (next object index).
            resp->accept(sync_req->get_offset());
            if (sync_req->is_done()) {
                // TODO: check if there is missing object.
                // Add a context buffer to inform installation is done.
                ptr<buffer> done_ctx = buffer::alloc(1);
                done_ctx->pos(0);
                done_ctx->put((byte)0);
                done_ctx->pos(0);
                resp->set_ctx(done_ctx);
            }
        }
    }

    return resp;
}

void raft_server::handle_install_snapshot_resp(resp_msg& resp) {
    p_db("%s\n", resp.get_accepted() ? "accepted" : "not accepted");
    peer_itor it = peers_.find(resp.get_src());
    if (it == peers_.end()) {
        p_in("the response is from an unknown peer %d", resp.get_src());
        return;
    }

    // if there are pending logs to be synced or commit index need to be advanced,
    // continue to send appendEntries to this peer
    bool need_to_catchup = true;
    ptr<peer> p = it->second;
    if (resp.get_accepted()) {
        std::lock_guard<std::mutex> guard(p->get_lock());
        ptr<snapshot_sync_ctx> sync_ctx = p->get_snapshot_sync_ctx();
        if (sync_ctx == nilptr) {
            p_in("no snapshot sync context for this peer, drop the response");
            need_to_catchup = false;

        } else {
            ptr<snapshot> snp = sync_ctx->get_snapshot();
            if (snp->get_type() == snapshot::raw_binary) {
                // LCOV_EXCL_START
                p_db("resp.get_next_idx(): %zu, snp->size(): %zu\n",
                     resp.get_next_idx(), snp->size());
                // LCOV_EXCL_STOP
            }

            bool snp_install_done =
                 ( snp->get_type() == snapshot::raw_binary &&
                   resp.get_next_idx() >= snp->size() )           ||
                 ( snp->get_type() == snapshot::logical_object &&
                   resp.get_ctx() );

            if (snp_install_done) {
                p_db("snapshot sync is done (raw type)");
                ptr<snapshot> nil_snp = nullptr;
                p->set_next_log_idx(sync_ctx->get_snapshot()->get_last_log_idx() + 1);
                p->set_matched_idx(sync_ctx->get_snapshot()->get_last_log_idx());
                void*& user_ctx = sync_ctx->get_user_snp_ctx();
                state_machine_->free_user_snp_ctx(user_ctx);
                p->set_snapshot_in_sync(nil_snp);

                need_to_catchup = p->clear_pending_commit() ||
                                  p->get_next_log_idx() < log_store_->next_slot();
                p_in("snapshot done %zu, %zu, %d\n",
                     p->get_next_log_idx(), p->get_matched_idx(), need_to_catchup);
            } else {
                p_db("continue to sync snapshot at offset %zu", resp.get_next_idx());
                sync_ctx->set_offset(resp.get_next_idx());
            }
        }

    } else {
        p_wn( "peer %d declined snapshot: p->get_next_log_idx(): %zu, "
              "log_store_->next_slot(): %zu\n",
              p->get_id(), p->get_next_log_idx(), log_store_->next_slot() );
        p->set_next_log_idx(resp.get_next_idx());

        // Added by Jung-Sang Ahn (Oct 11 2017)
        // Declining snapshot implies that the peer already has the up-to-date snapshot.
        need_to_catchup = p->get_next_log_idx() < log_store_->next_slot();

        // Should reset current snapshot context,
        // to continue with more recent snapshot.
        std::lock_guard<std::mutex> guard(p->get_lock());
        ptr<snapshot_sync_ctx> sync_ctx = p->get_snapshot_sync_ctx();
        if (sync_ctx) {
            void*& user_ctx = sync_ctx->get_user_snp_ctx();
            state_machine_->free_user_snp_ctx(user_ctx);
            p->set_snapshot_in_sync(nilptr);
        }
    }

    // This may not be a leader anymore, such as
    // the response was sent out long time ago
    // and the role was updated by UpdateTerm call
    // Try to match up the logs for this peer
    if (role_ == srv_role::leader && need_to_catchup) {
        request_append_entries(p);
    }
}

void raft_server::handle_install_snapshot_resp_new_member(resp_msg& resp) {
    if (!srv_to_join_) {
        p_in("no server to join, the response must be very old.");
        return;
    }

    if (!resp.get_accepted()) {
        p_wn("peer doesn't accept the snapshot installation request, "
             "next log idx %llu, "
             "but we can move forward",
             resp.get_next_idx());
        srv_to_join_->set_next_log_idx(resp.get_next_idx());
    }

    ptr<snapshot_sync_ctx> sync_ctx = srv_to_join_->get_snapshot_sync_ctx();
    if (sync_ctx == nilptr) {
        p_ft("SnapshotSyncContext must not be null: "
             "src %d dst %d my id %d leader id %d, "
             "maybe leader election happened in the meantime. "
             "next heartbeat or append request will cover it up.",
             resp.get_src(), resp.get_dst(), id_, leader_.load());
        return;
    }

    ptr<snapshot> snp = sync_ctx->get_snapshot();
    bool snp_install_done =
        ( snp->get_type() == snapshot::raw_binary &&
          resp.get_next_idx() >= snp->size() )         ||
        ( snp->get_type() == snapshot::logical_object &&
          resp.get_ctx() );

    if (snp_install_done) {
        // snapshot is done
        p_in("snapshot install is done\n");
        ptr<snapshot> nil_snap;
        srv_to_join_->set_snapshot_in_sync(nil_snap);
        srv_to_join_->set_next_log_idx
            ( sync_ctx->get_snapshot()->get_last_log_idx() + 1 );
        srv_to_join_->set_matched_idx
            ( sync_ctx->get_snapshot()->get_last_log_idx() );
        void*& user_ctx = sync_ctx->get_user_snp_ctx();
        state_machine_->free_user_snp_ctx(user_ctx);
        p_in( "snapshot has been copied and applied to new server, "
              "continue to sync logs after snapshot, "
              "next log idx %llu, matched idx %llu",
              srv_to_join_->get_next_log_idx(),
              srv_to_join_->get_matched_idx() );
    } else {
        sync_ctx->set_offset(resp.get_next_idx());
        p_db( "continue to send snapshot to new server at offset %llu",
              resp.get_next_idx() );
    }

    sync_log_to_new_srv(srv_to_join_->get_next_log_idx());
}

bool raft_server::handle_snapshot_sync_req(snapshot_sync_req& req) {
 try {
    // if offset == 0, it is the first object.
    bool is_first_obj = (req.get_offset()) ? false : true;
    bool is_last_obj = req.is_done();
    if (is_first_obj || is_last_obj) {
        // INFO level: log only first and last object.
        p_in("save snapshot (idx %zu, term %zu) offset 0x%zx, %s %s\n",
             req.get_snapshot().get_last_log_idx(),
             req.get_snapshot().get_last_log_term(),
             req.get_offset(),
             (is_first_obj) ? "first obj" : "",
             (is_last_obj)  ? "last obj"  : "" );
    } else {
        // above DEBUG: log all.
        p_db("save snapshot (idx %zu, term %zu) offset 0x%zx, %s %s\n",
             req.get_snapshot().get_last_log_idx(),
             req.get_snapshot().get_last_log_term(),
             req.get_offset(),
             (is_first_obj) ? "first obj" : "",
             (is_last_obj)  ? "last obj"  : "" );
    }

    cb_func::Param param(id_, leader_);
    param.ctx = &req;
    CbReturnCode rc = ctx_->cb_func_.call(cb_func::SaveSnapshot, &param);
    if (rc == CbReturnCode::ReturnNull) {
        p_wn("by callback, return false");
        return false;
    }

    // Set flag to avoid initiating election by this node.
    receiving_snapshot_ = true;
    et_cnt_receiving_snapshot_ = 0;

    // Set initialized flag
    if (!initialized_) initialized_ = true;

    if (req.get_snapshot().get_type() == snapshot::raw_binary) {
        // LCOV_EXCL_START
        // Raw binary type (original).
        state_machine_->save_snapshot_data(req.get_snapshot(),
                                           req.get_offset(),
                                           req.get_data());
        // LCOV_EXCL_STOP

    } else {
        // Logical object type.
        ulong obj_id = req.get_offset();
        buffer& buf = req.get_data();
        buf.pos(0);
        state_machine_->save_logical_snp_obj(req.get_snapshot(),
                                             obj_id,
                                             buf,
                                             is_first_obj,
                                             is_last_obj);
        req.set_offset(obj_id);
    }

    if (is_last_obj) {
        receiving_snapshot_ = false;

        // Only follower will run this piece of code, but let's check it again
        if (role_ != srv_role::follower) {
            // LCOV_EXCL_START
            p_er("bad server role for applying a snapshot, exit for debugging");
            ctx_->state_mgr_->system_exit(raft_err::N11_not_follower_for_snapshot);
            ::exit(-1);
            // LCOV_EXCL_STOP
        }

        p_in( "successfully receive a snapshot (idx %zu term %zu) from leader",
              req.get_snapshot().get_last_log_idx(),
              req.get_snapshot().get_last_log_term() );
        if (log_store_->compact(req.get_snapshot().get_last_log_idx())) {
            // The state machine will not be able to commit anything before the
            // snapshot is applied, so make this synchronously with election
            // timer stopped as usually applying a snapshot may take a very
            // long time
            stop_election_timer();
            p_in("successfully compact the log store, will now ask the "
                 "statemachine to apply the snapshot");
            if (!state_machine_->apply_snapshot(req.get_snapshot())) {
                // LCOV_EXCL_START
                p_er("failed to apply the snapshot after log compacted, "
                     "to ensure the safety, will shutdown the system");
                ctx_->state_mgr_->system_exit(raft_err::N12_apply_snapshot_failed);
                ::exit(-1);
                return false;
                // LCOV_EXCL_STOP
            }

            reconfigure(req.get_snapshot().get_last_config());

            ptr<cluster_config> c_conf = get_config();
            ctx_->state_mgr_->save_config(*c_conf);

            precommit_index_ = req.get_snapshot().get_last_log_idx();
            sm_commit_index_ = req.get_snapshot().get_last_log_idx();
            quick_commit_index_ = req.get_snapshot().get_last_log_idx();

            ctx_->state_mgr_->save_state(*state_);

            ptr<snapshot> new_snp = cs_new<snapshot>
                                    ( req.get_snapshot().get_last_log_idx(),
                                      req.get_snapshot().get_last_log_term(),
                                      c_conf,
                                      req.get_snapshot().size(),
                                      req.get_snapshot().get_type() );
            set_last_snapshot(new_snp);

            restart_election_timer();
            p_in("snapshot idx %zu term %zu is successfully applied, "
                 "log start %zu last idx %zu",
                 new_snp->get_last_log_idx(),
                 new_snp->get_last_log_term(),
                 log_store_->start_index(),
                 log_store_->next_slot() - 1);

        } else {
            p_er("failed to compact the log store after a snapshot is received, "
                 "will ask the leader to retry");
            return false;
        }
    }

 } catch (...) {
    // LCOV_EXCL_START
    p_er("failed to handle snapshot installation due to system errors");
    ctx_->state_mgr_->system_exit(raft_err::N13_snapshot_install_failed);
    ::exit(-1);
    return false;
    // LCOV_EXCL_STOP
 }

    return true;
}

} // namespace nuraft;

