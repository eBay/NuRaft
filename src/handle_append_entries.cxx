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

#include "pp_util.hxx"
#include "raft_params.hxx"
#include "raft_server.hxx"

#include "cluster_config.hxx"
#include "error_code.hxx"
#include "event_awaiter.hxx"
#include "handle_custom_notification.hxx"
#include "peer.hxx"
#include "snapshot.hxx"
#include "state_machine.hxx"
#include "state_mgr.hxx"
#include "tracer.hxx"

#include <algorithm>
#include <cassert>
#include <sstream>

namespace nuraft {

/**
 * Additional information in addition to `append_entries_response`.
 */
struct resp_appendix {
    enum extra_order : uint8_t {
        NONE = 0,
        DO_NOT_REWIND = 1,
    };

    resp_appendix() : extra_order_(NONE) {}

    ptr<buffer> serialize() const {
        const static uint8_t CUR_VERSION = 0;
        size_t buf_len = sizeof(CUR_VERSION) + sizeof(extra_order_);

        //  << Format >>
        // Format version       1 byte
        // Extra order          1 byte

        ptr<buffer> result = buffer::alloc(buf_len);
        buffer_serializer bs(*result);
        bs.put_u8(CUR_VERSION);
        bs.put_u8(extra_order_);

        return result;
    }

    static ptr<resp_appendix> deserialize(buffer& buf) {
        buffer_serializer bs(buf);
        ptr<resp_appendix> res = cs_new<resp_appendix>();

        uint8_t cur_ver = bs.get_u8();
        if (cur_ver != 0) {
            // Not supported version.
            return res;
        }

        res->extra_order_ = static_cast<extra_order>(bs.get_u8());
        return res;
    }

    static const char* extra_order_msg(extra_order v) {
        switch (v) {
        case NONE:
            return "NONE";
        case DO_NOT_REWIND:
            return "DO_NOT_REWIND";
        default:
            return "UNKNOWN";
        }
    };

    extra_order extra_order_;
};

void raft_server::append_entries_in_bg() {
    std::string thread_name = "nuraft_append";
#ifdef __linux__
    pthread_setname_np(pthread_self(), thread_name.c_str());
#elif __APPLE__
    pthread_setname_np(thread_name.c_str());
#endif

    p_in("bg append_entries thread initiated");
    do {
        bg_append_ea_->wait();
        bg_append_ea_->reset();
        if (stopping_) break;

        append_entries_in_bg_exec();
    } while (!stopping_);
    append_bg_stopped_ = true;
    p_in("bg append_entries thread terminated");
}

void raft_server::append_entries_in_bg_exec() {
    recur_lock(lock_);
    request_append_entries();
}

void raft_server::request_append_entries() {
    // Special case:
    //   1) one-node cluster, OR
    //   2) quorum size == 1 (including leader).
    //
    // In those cases, we may not enter `handle_append_entries_resp`,
    // which calls `commit()` function.
    // We should call it here.
    if ( peers_.size() == 0 ||
         get_quorum_for_commit() == 0 ) {
        uint64_t leader_index = get_current_leader_index();
        commit(leader_index);
        return;
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        request_append_entries(it->second);
    }
}

bool raft_server::request_append_entries(ptr<peer> p) {
    static timer_helper chk_timer(1000*1000);

    // Checking the validity of role first.
    if (role_ != srv_role::leader) {
        // WARNING: We should allow `write_paused_` state for
        //          graceful resignation.
        return false;
    }

    cb_func::Param cb_param(id_, leader_, p->get_id());
    CbReturnCode rc = ctx_->cb_func_.call(cb_func::RequestAppendEntries, &cb_param);
    if (rc == CbReturnCode::ReturnNull) {
        p_wn("by callback, abort request_append_entries");
        return true;
    }

    ptr<raft_params> params = ctx_->get_params();

    if ( params->auto_adjust_quorum_for_small_cluster_ &&
         get_num_voting_members() == 2 &&
         chk_timer.timeout_and_reset() ) {
        // If auto adjust mode is on for 2-node cluster, and
        // the follower is not responding, adjust the quorum.
        size_t num_not_responding_peers = get_not_responding_peers_count();
        size_t cur_quorum_size = get_quorum_for_commit();
        size_t num_stale_peers = get_num_stale_peers();
        if (cur_quorum_size >= 1) {
            bool do_adjustment = false;
            if (num_not_responding_peers) {
                p_wn("2-node cluster's follower is not responding long time, "
                     "adjust quorum to 1");
                do_adjustment = true;
            } else if (num_stale_peers) {
                p_wn("2-node cluster's follower is lagging behind, "
                     "adjust quorum to 1");
                do_adjustment = true;
            }
            if (do_adjustment) {
                cb_func::Param cb_param(id_, leader_, p->get_id());
                CbReturnCode rc =
                    ctx_->cb_func_.call(cb_func::AutoAdjustQuorum, &cb_param);
                if (rc == CbReturnCode::ReturnNull) {
                    // Callback function rejected the adjustment.
                    p_wn("quorum size adjustment was declined by callback");
                } else {
                    ptr<raft_params> clone = cs_new<raft_params>(*params);
                    clone->custom_commit_quorum_size_ = 1;
                    clone->custom_election_quorum_size_ = 1;
                    ctx_->set_params(clone);
                    // When the quorum size is 1 and the server is idle, there is no
                    // opportunity to commit pending logs
                    uint64_t leader_index = get_current_leader_index();
                    commit(leader_index);
                }
            }

        } else if ( num_not_responding_peers == 0 &&
                    num_stale_peers == 0 &&
                    params->custom_commit_quorum_size_ == 1 ) {
            // Recovered, both cases should be clear.
            p_wn("2-node cluster's follower is responding now, "
                 "restore quorum with default value");
            ptr<raft_params> clone = cs_new<raft_params>(*params);
            clone->custom_commit_quorum_size_ = 0;
            clone->custom_election_quorum_size_ = 0;
            ctx_->set_params(clone);
        }
    }

    bool need_to_reconnect = p->need_to_reconnect();
    int32 last_active_time_ms = p->get_active_timer_us() / 1000;
    if ( last_active_time_ms >
             params->heart_beat_interval_ *
                 raft_server::raft_limits_.reconnect_limit_ ) {
        if (srv_to_leave_ && srv_to_leave_->get_id() == p->get_id()) {
            // We should not re-establish the connection to
            // to-be-removed server, as it will block removing it
            // from `peers_` list.
            p_wn( "connection to peer %d is not active long time: %d ms, "
                  "but this peer should be removed. do nothing",
                  p->get_id(),
                  last_active_time_ms );
        } else {
            p_wn( "connection to peer %d is not active long time: %d ms, "
                  "force re-connect",
                  p->get_id(),
                  last_active_time_ms );
            need_to_reconnect = true;
            p->reset_active_timer();
        }
    }
    if (need_to_reconnect) {
        bool reconnected = reconnect_client(*p);
        uint64_t p_next_log_idx = p->get_next_log_idx();
        if (reconnected && p_next_log_idx) {
            // NOTE:
            //   Discussions in https://github.com/eBay/NuRaft/issues/181
            //
            //   The leader keeps the last sent log info for each peer
            //   and sends the next `append_entries` request based on
            //   that data. This process disrupts offline data change,
            //   such as snapshot install, as the leader's peer info
            //   remains unchanged and results in wrong decisions. The
            //   right way to do this is to remove the node, install
            //   the snapshot, and then add the node back.
            //
            //   To avoid those hassles, we can reset the peer info that
            //   the leader has. Once any disconnection happens to a
            //   peer, we reset the last sent log info. The log info will
            //   be re-adjusted by the first `append_entries`
            //   communication between the leader and the peer. It will
            //   eventually have the same impact of removing and then
            //   re-adding the peer.
            //
            p_tr("new rpc for peer %d is created, "
                 "reset next log idx (%" PRIu64 ") and matched log idx (%" PRIu64 ")",
                 p->get_id(), p_next_log_idx, p->get_matched_idx());
            p->set_next_log_idx(0);
            p->set_matched_idx(0);
        }
        p->clear_reconnection();
    }

    if (params->use_bg_thread_for_snapshot_io_) {
        // Check the current queue if previous request exists.
        if (snapshot_io_mgr::instance().has_pending_request(this, p->get_id())) {
            p_tr( "previous snapshot request for peer %d already exists",
                  p->get_id() );
            return true;
        }
    }

    // If reserved message exists, process it first.
    ptr<req_msg> msg = p->get_rsv_msg();
    rpc_handler m_handler = p->get_rsv_msg_handler();
    if (msg) {
        if (p->make_busy()) {
            // Clear the reserved message.
            p->set_rsv_msg(nullptr, nullptr);
            p_in("found reserved message to peer %d, type %d",
                    p->get_id(), msg->get_type());
            return send_request(p, msg, m_handler);
        }
    } else {
        ulong last_streamed_log_idx = p->get_last_streamed_log_idx();
        int32 max_gap_in_stream = ctx_->get_params()->max_log_gap_in_stream_;
        if (last_streamed_log_idx > 0 && max_gap_in_stream == 0) {
            p_in("disable stream mode at runtime");
            last_streamed_log_idx = 0;
            p->reset_stream();
        }
        bool streaming = last_streamed_log_idx > 0;

        if (streaming || p->make_busy()) {
            p_tr("send request to %d, streaming: %d, is_busy: %d\n", (int)p->get_id(),
                 streaming, p->is_busy());
            msg = create_append_entries_req(p, last_streamed_log_idx);
            m_handler = resp_handler_;

            if (msg) {
                streaming = streaming && 
                            msg->get_type() == msg_type::append_entries_request;
                bool make_busy_result = p->is_busy();
                if (streaming) {
                    // throttling
                    if (max_gap_in_stream + p->get_next_log_idx() 
                        <= (last_streamed_log_idx + 1)) {
                        p_db("flying log entry exceeds %d in stream mode, "
                             "skip this request", max_gap_in_stream);
                        streaming = false;
                    } else {
                        p_tr("send following request to %d in stream mode, " 
                             "start idx: %ld", (int)p->get_id(), 
                             msg->get_last_log_idx());
                        p->set_last_streamed_log_idx(
                            last_streamed_log_idx, 
                            last_streamed_log_idx + msg->log_entries().size());
                    }
                } else if (!make_busy_result) {
                    make_busy_result = p->make_busy();
                }

                if (streaming || make_busy_result) {
                    return send_request(p, msg, m_handler, streaming);
                }
            } else {
                if (!streaming) {
                    p->set_free();
                }

                if ( params->use_bg_thread_for_snapshot_io_ &&
                     p->get_snapshot_sync_ctx() ) {
                    // If this is an async snapshot request, invoke IO thread.
                    snapshot_io_mgr::instance().invoke();
                }
                return true;
            }
        }
    }

    p_db("Server %d is busy, skip the request", p->get_id());
    check_snapshot_timeout(p);

    int32 last_ts_ms = p->get_ls_timer_us() / 1000;
    if ( last_ts_ms > params->heart_beat_interval_ ) {
        // Waiting time becomes longer than HB interval, warning.
        p->inc_long_pause_warnings();
        if (p->get_long_puase_warnings() < raft_server::raft_limits_.warning_limit_) {
            p_wn("skipped sending msg to %d too long time, "
                 "last msg sent %d ms ago",
                 p->get_id(), last_ts_ms);

        } else if ( p->get_long_puase_warnings() ==
                        raft_server::raft_limits_.warning_limit_ ) {
            p_wn("long pause warning to %d is too verbose, "
                 "will suppress it from now", p->get_id());
        }
    }
    return false;
}



bool raft_server::send_request(ptr<peer>& p, 
                               ptr<req_msg>& msg, 
                               rpc_handler& m_handler,
                               bool streaming) {
    if (!p->is_manual_free()) {
        // Actual recovery.
        if ( p->get_long_puase_warnings() >=
                    raft_server::raft_limits_.warning_limit_ ) {
            int32 last_ts_ms = p->get_ls_timer_us() / 1000;
            p->inc_recovery_cnt();
            p_wn( "recovered from long pause to peer %d, %d warnings, "
                    "%d ms, %d times",
                    p->get_id(),
                    p->get_long_puase_warnings(),
                    last_ts_ms,
                    p->get_recovery_cnt() );

            if (p->get_recovery_cnt() >= 10) {
                // Re-connect client, just in case.
                //reconnect_client(*p);
                p->reset_recovery_cnt();
            }
        }
        p->reset_long_pause_warnings();

    } else {
        // FIXME: `manual_free` is deprecated, need to get rid of it.

        // It means that this is not an actual recovery,
        // but just temporarily freed busy flag.
        p->reset_manual_free();
    }

    p->send_req(p, msg, m_handler, streaming);
    p->reset_ls_timer();

    cb_func::Param param(id_, leader_, p->get_id(), msg.get());
    ctx_->cb_func_.call(cb_func::SentAppendEntriesReq, &param);

    if ( srv_to_leave_ &&
            srv_to_leave_->get_id() == p->get_id() &&
            msg->get_commit_idx() >= srv_to_leave_target_idx_ &&
            !srv_to_leave_->is_stepping_down() ) {
        // If this is the server to leave, AND
        // current request's commit index includes
        // the target log index number, step down and remove it
        // as soon as we get the corresponding response.
        srv_to_leave_->step_down();
        p_in("srv_to_leave_ %d is safe to be erased from peer list, "
                "log idx %" PRIu64 " commit idx %" PRIu64 ", set flag",
                srv_to_leave_->get_id(),
                msg->get_last_log_idx(),
                msg->get_commit_idx());
    }

    p_tr("sent\n");
    return true;
}

ptr<req_msg> raft_server::create_append_entries_req(ptr<peer>& pp ,
                                                    ulong custom_last_log_idx) {
    peer& p = *pp;
    ulong cur_nxt_idx(0L);
    ulong commit_idx(0L);
    ulong last_log_idx(0L);
    ulong term(0L);
    ulong starting_idx(1L);

    {
        recur_lock(lock_);
        starting_idx = log_store_->start_index();
        cur_nxt_idx = precommit_index_ + 1;
        commit_idx = quick_commit_index_;
        term = state_->get_term();
    }

    {
        std::lock_guard<std::mutex> guard(p.get_lock());
        if (p.get_next_log_idx() == 0L) {
            p.set_next_log_idx(cur_nxt_idx);
        }

        if (custom_last_log_idx > 0) {
            last_log_idx = custom_last_log_idx;
        } else {
            last_log_idx = p.get_next_log_idx() - 1;
        }
    }

    if (last_log_idx >= cur_nxt_idx) {
        // LCOV_EXCL_START
        p_er( "Peer's lastLogIndex is too large %" PRIu64 " v.s. %" PRIu64 ", ",
              last_log_idx, cur_nxt_idx );
        ctx_->state_mgr_->system_exit(raft_err::N8_peer_last_log_idx_too_large);
        ::exit(-1);
        return ptr<req_msg>();
        // LCOV_EXCL_STOP
    }

    // cur_nxt_idx: last log index of myself (leader).
    // starting_idx: start log index of myself (leader).
    // last_log_idx: last log index of replica (follower).
    // end_idx: if (cur_nxt_idx - last_log_idx) > max_append_size, limit it.

    p_tr("last_log_idx: %" PRIu64 ", starting_idx: %" PRIu64
         ", cur_nxt_idx: %" PRIu64 "\n",
         last_log_idx, starting_idx, cur_nxt_idx);

    // Verify log index range.
    bool entries_valid = (last_log_idx + 1 >= starting_idx);

    // Read log entries. The underlying log store may have removed some log entries
    // causing some of the requested entries to be unavailable. The log store should
    // return nullptr to indicate such errors.
    ulong end_idx = std::min( cur_nxt_idx,
                              last_log_idx + 1 + ctx_->get_params()->max_append_size_ );

    // NOTE: If this is a retry, probably the follower is down.
    //       Send just one log until it comes back
    //       (i.e., max_append_size_ = 1).
    //       Only when end_idx - start_idx > 1, and 5th try.
    ulong peer_last_sent_idx = p.get_last_sent_idx();
    if ( last_log_idx + 1 == peer_last_sent_idx &&
         last_log_idx + 2 < end_idx ) {
        int32 cur_cnt = p.inc_cnt_not_applied();
        p_db("last sent log (%" PRIu64 ") to peer %d is not applied, cnt %d",
             peer_last_sent_idx, p.get_id(), cur_cnt);
        if (cur_cnt >= 5) {
            ulong prev_end_idx = end_idx;
            end_idx = std::min( cur_nxt_idx, last_log_idx + 1 + 1 );
            p_db("reduce end_idx %" PRIu64 " -> %" PRIu64, prev_end_idx, end_idx);
        }
    } else {
        p.reset_cnt_not_applied();
    }

    ptr<std::vector<ptr<log_entry>>> log_entries;
    if ((last_log_idx + 1) >= cur_nxt_idx) {
        log_entries = ptr<std::vector<ptr<log_entry>>>();
    } else if (entries_valid) {
        log_entries = log_store_->log_entries_ext(last_log_idx + 1, end_idx,
                                                  p.get_next_batch_size_hint_in_bytes());
        if (log_entries == nullptr) {
            p_wn("failed to retrieve log entries: %" PRIu64 " - %" PRIu64,
                 last_log_idx + 1, end_idx);
            entries_valid = false;
        }
    }

    if (!entries_valid) {
        // Required log entries are missing. First, we try to use snapshot to recover.
        // To avoid inconsistency due to smart pointer, should have local varaible
        // to increase its ref count.
        ptr<snapshot> snp_local = get_last_snapshot();

        // Modified by Jung-Sang Ahn (Oct 11 2017):
        // As `reserved_log` has been newly added, need to check snapshot
        // in addition to `starting_idx`.
        if ( snp_local &&
             last_log_idx < starting_idx &&
             last_log_idx < snp_local->get_last_log_idx() ) {
            p_db( "send snapshot peer %d, peer log idx: %" PRIu64
                  ", my starting idx: %" PRIu64 ", "
                  "my log idx: %" PRIu64 ", last_snapshot_log_idx: %" PRIu64,
                  p.get_id(),
                  last_log_idx, starting_idx, cur_nxt_idx,
                  snp_local->get_last_log_idx() );

            bool succeeded_out = false;
            return create_sync_snapshot_req( pp, last_log_idx, term,
                                             commit_idx, succeeded_out );
        }

        // Cannot recover using snapshot. Return here to protect the leader.
        static timer_helper msg_timer(5000000);
        int log_lv = msg_timer.timeout_and_reset() ? L_ERROR : L_TRACE;
        p_lv(log_lv,
             "neither snapshot nor log exists, peer %d, last log %" PRIu64 ", "
             "leader's start log %" PRIu64,
             p.get_id(), last_log_idx, starting_idx);

        // Send out-of-log-range notification to this follower.
        ptr<req_msg> req = cs_new<req_msg>
                           ( term, msg_type::custom_notification_request,
                             id_, p.get_id(), 0, last_log_idx, commit_idx );

        // Out-of-log message.
        ptr<out_of_log_msg> ool_msg = cs_new<out_of_log_msg>();
        ool_msg->start_idx_of_leader_ = starting_idx;

        // Create a notification containing OOL message.
        ptr<custom_notification_msg> custom_noti =
            cs_new<custom_notification_msg>
            ( custom_notification_msg::out_of_log_range_warning );
        custom_noti->ctx_ = ool_msg->serialize();

        // Wrap it using log_entry.
        ptr<log_entry> custom_noti_le =
            cs_new<log_entry>(0, custom_noti->serialize(), log_val_type::custom);

        req->log_entries().push_back(custom_noti_le);
        return req;
    }

    ulong last_log_term = term_for_log(last_log_idx);
    ulong adjusted_end_idx = end_idx;
    if (log_entries) adjusted_end_idx = last_log_idx + 1 + log_entries->size();
    if (adjusted_end_idx != end_idx) {
        p_tr("adjusted end_idx due to batch size hint: %" PRIu64 " -> %" PRIu64,
             end_idx, adjusted_end_idx);
    }

    p_db( "append_entries for %d with LastLogIndex=%" PRIu64 ", "
          "LastLogTerm=%" PRIu64 ", EntriesLength=%zu, CommitIndex=%" PRIu64 ", "
          "Term=%" PRIu64 ", peer_last_sent_idx %" PRIu64,
          p.get_id(), last_log_idx, last_log_term,
          ( log_entries ? log_entries->size() : 0 ), commit_idx, term,
          peer_last_sent_idx );
    if (last_log_idx+1 == adjusted_end_idx) {
        p_tr( "EMPTY PAYLOAD" );
    } else if (last_log_idx+1 + 1 == adjusted_end_idx) {
        p_db( "idx: %" PRIu64, last_log_idx+1 );
    } else {
        p_db( "idx range: %" PRIu64 "-%" PRIu64, last_log_idx+1, adjusted_end_idx-1 );
    }

    ptr<req_msg> req
        ( cs_new<req_msg>
          ( term, msg_type::append_entries_request, id_, p.get_id(),
            last_log_term, last_log_idx, commit_idx ) );
    std::vector<ptr<log_entry>>& v = req->log_entries();
    if (log_entries) {
        v.insert(v.end(), log_entries->begin(), log_entries->end());
    }
    p.set_last_sent_idx(last_log_idx + 1);

    return req;
}

ptr<resp_msg> raft_server::handle_append_entries(req_msg& req)
{
    bool supp_exp_warning = false;
    if (catching_up_) {
        // WARNING:
        //   We should clear the `catching_up_` flag only after this node's
        //   config has been added to the cluster config. Otherwise, if we
        //   clear it before that, any membership change configs (which is
        //   already outdated but committed after the received snapshot)
        //   may cause stepping down of this node.
        ptr<cluster_config> cur_config = get_config();
        ptr<srv_config> my_config = cur_config->get_server(id_);
        if (my_config) {
            p_in("catch-up process is done, clearing the flag");
            catching_up_ = false;
        }
        supp_exp_warning = true;
    }

    // To avoid election timer wakes up while we are in the middle
    // of this function, this structure sets the flag and automatically
    // clear it when we return from this function.
    struct ServingReq {
        ServingReq(std::atomic<bool>* _val) : val(_val) { val->store(true); }
        ~ServingReq() { val->store(false); }
        std::atomic<bool>* val;
    } _s_req(&serving_req_);
    timer_helper tt;

    p_tr("from peer %d, req type: %d, req term: %" PRIu64 ", "
         "req l idx: %" PRIu64 " (%zu), req c idx: %" PRIu64 ", "
         "my term: %" PRIu64 ", my role: %d\n",
         req.get_src(), (int)req.get_type(), req.get_term(),
         req.get_last_log_idx(), req.log_entries().size(), req.get_commit_idx(),
         state_->get_term(), (int)role_);

    if (req.get_term() == state_->get_term()) {
        if (role_ == srv_role::candidate) {
            become_follower();
        } else if (role_ == srv_role::leader) {
            p_wn( "Receive AppendEntriesRequest from another leader (%d) "
                  "with same term, there must be a bug. Ignore it instead of exit.",
                  req.get_src() );
            return nullptr;
        } else {
            update_target_priority();
            // Modified by JungSang Ahn, Mar 28 2018:
            //   As we have `serving_req_` flag, restarting election timer
            //   should be move to the end of this function.
            // restart_election_timer();
        }
    }

    // After a snapshot the req.get_last_log_idx() may less than
    // log_store_->next_slot() but equals to log_store_->next_slot() -1
    //
    // In this case, log is Okay if
    //   req.get_last_log_idx() == lastSnapshot.get_last_log_idx() &&
    //   req.get_last_log_term() == lastSnapshot.get_last_log_term()
    //
    // In not accepted case, we will return log_store_->next_slot() for
    // the leader to quick jump to the index that might aligned.
    ptr<resp_msg> resp = cs_new<resp_msg>( state_->get_term(),
                                           msg_type::append_entries_response,
                                           id_,
                                           req.get_src(),
                                           log_store_->next_slot() );

    ptr<snapshot> local_snp = get_last_snapshot();
    ulong log_term = 0;
    if (req.get_last_log_idx() < log_store_->next_slot()) {
        log_term = term_for_log( req.get_last_log_idx() );
    }
    bool log_okay =
            req.get_last_log_idx() == 0 ||
            ( log_term &&
              req.get_last_log_term() == log_term ) ||
            ( local_snp &&
              local_snp->get_last_log_idx() == req.get_last_log_idx() &&
              local_snp->get_last_log_term() == req.get_last_log_term() );

    int log_lv = log_okay ? L_TRACE : (supp_exp_warning ? L_INFO : L_WARN);
    static timer_helper log_timer(500*1000, true);
    if (log_lv == L_WARN) {
        // To avoid verbose logs.
        if (!log_timer.timeout_and_reset()) {
            log_lv = L_TRACE;
        }
    }
    p_lv( log_lv,
          "[LOG %s] req log idx: %" PRIu64 ", req log term: %" PRIu64
          ", my last log idx: %" PRIu64 ", "
          "my log (%" PRIu64 ") term: %" PRIu64,
          (log_okay ? "OK" : "XX"),
          req.get_last_log_idx(),
          req.get_last_log_term(),
          log_store_->next_slot() - 1,
          req.get_last_log_idx(),
          log_term );

    if ( req.get_term() < state_->get_term() ||
         log_okay == false ) {
        p_lv( log_lv,
              "deny, req term %" PRIu64 ", my term %" PRIu64
              ", req log idx %" PRIu64 ", my log idx %" PRIu64,
              req.get_term(), state_->get_term(),
              req.get_last_log_idx(), log_store_->next_slot() - 1 );
        if (local_snp) {
            p_lv( log_lv, "snp idx %" PRIu64 " term %" PRIu64,
                  local_snp->get_last_log_idx(),
                  local_snp->get_last_log_term() );
        }
        resp->set_next_batch_size_hint_in_bytes(
                state_machine_->get_next_batch_size_hint_in_bytes() );
        return resp;
    }

    // --- Now this node is a follower, and given log is okay. ---

    // set initialized flag
    if (!initialized_) initialized_ = true;

    // Callback if necessary.
    cb_func::Param param(id_, leader_, -1, &req);
    cb_func::ReturnCode cb_ret =
        ctx_->cb_func_.call(cb_func::GotAppendEntryReqFromLeader, &param);
    // If callback function decided to refuse this request, return here.
    if (cb_ret != cb_func::Ok) {
        // If this request is declined by the application, not because of
        // term mismatch, we should request leader not to rewind the log.
        resp_appendix appendix;
        appendix.extra_order_ = resp_appendix::DO_NOT_REWIND;
        resp->set_ctx( appendix.serialize() );

        // Also we should set the hint to a negative number,
        // to slow down the leader.
        resp->set_next_batch_size_hint_in_bytes(-1);

        static timer_helper log_timer(1000 * 1000);
        int log_lv = log_timer.timeout_and_reset() ? L_INFO : L_TRACE;
        p_lv(log_lv, "appended extra order %s",
             resp_appendix::extra_order_msg(appendix.extra_order_));

        // Since this decline is on purpose, we should not let this server
        // initiaite leader election.
        restart_election_timer();

        return resp;
    }

    if (req.log_entries().size() > 0) {
        // Write logs to store, start from overlapped logs

        // Actual log number.
        ulong log_idx = req.get_last_log_idx() + 1;
        // Local counter for iterating req.log_entries().
        size_t cnt = 0;

        p_db("[INIT] log_idx: %" PRIu64 ", count: %zu, "
             "log_store_->next_slot(): %" PRIu64 ", "
             "req.log_entries().size(): %zu",
             log_idx, cnt, log_store_->next_slot(), req.log_entries().size());

        // Skipping already existing (with the same term) logs.
        while ( log_idx < log_store_->next_slot() &&
                cnt < req.log_entries().size() )
        {
            if ( log_store_->term_at(log_idx) ==
                     req.log_entries().at(cnt)->get_term() ) {
                log_idx++;
                cnt++;
            } else {
                break;
            }
        }
        p_db("[after SKIP] log_idx: %" PRIu64 ", count: %zu", log_idx, cnt);

        // Rollback (only if necessary).
        // WARNING:
        //   1) Rollback should be done separately before overwriting,
        //      and MUST BE in backward direction.
        //   2) Should do rollback ONLY WHEN we have at least one log
        //      to overwrite.
        ulong my_last_log_idx = log_store_->next_slot() - 1;
        bool rollback_in_progress = false;
        if ( my_last_log_idx >= log_idx &&
             cnt < req.log_entries().size() ) {
            p_in( "rollback logs: %" PRIu64 " - %" PRIu64
                  ", commit idx req %" PRIu64 ", quick %" PRIu64 ", sm %" PRIu64 ", "
                  "num log entries %zu, current count %zu",
                  log_idx,
                  my_last_log_idx,
                  req.get_commit_idx(),
                  quick_commit_index_.load(),
                  sm_commit_index_.load(),
                  req.log_entries().size(),
                  cnt );
            rollback_in_progress = true;
            // If rollback point is smaller than commit index,
            // should rollback commit index as well
            // (should not happen in Raft though).
            if ( quick_commit_index_ >= log_idx ) {
                p_wn( "rollback quick commit index from %" PRIu64 " to %" PRIu64,
                      quick_commit_index_.load(),
                      log_idx - 1 );
                quick_commit_index_ = log_idx - 1;
            }
            if ( sm_commit_index_ >= log_idx ) {
                p_er( "rollback sm commit index from %" PRIu64 " to %" PRIu64 ", "
                      "it shouldn't happen and may indicate data loss",
                      sm_commit_index_.load(),
                      log_idx - 1 );
                sm_commit_index_ = log_idx - 1;
            }

            for ( uint64_t ii = 0; ii < my_last_log_idx - log_idx + 1; ++ii ) {
                uint64_t idx = my_last_log_idx - ii;
                ptr<log_entry> old_entry = log_store_->entry_at(idx);
                ptr<buffer> buf = old_entry->get_buf_ptr();
                if (old_entry->get_val_type() == log_val_type::app_log) {
                    buf->pos(0);
                    state_machine_->rollback_ext
                        ( state_machine::ext_op_params( idx, buf ) );
                    p_in( "rollback log %" PRIu64 ", term %" PRIu64,
                          idx, old_entry->get_term() );

                } else if (old_entry->get_val_type() == log_val_type::conf) {
                    ptr<cluster_config> conf_to_rollback =
                        cluster_config::deserialize(*buf);
                    state_machine_->rollback_config(idx, conf_to_rollback);
                    p_in( "revert from a prev config change to config at %" PRIu64,
                          get_config()->get_log_idx() );
                    config_changing_ = false;
                }
            }
        }

        // Dealing with overwrites (logs with different term).
        while ( log_idx < log_store_->next_slot() &&
                cnt < req.log_entries().size() )
        {
            ptr<log_entry> entry = req.log_entries().at(cnt);
            p_in("overwrite at %" PRIu64 ", term %" PRIu64 ", timestamp %" PRIu64 "\n",
                 log_idx, entry->get_term(), entry->get_timestamp());
            store_log_entry(entry, log_idx);

            if (entry->get_val_type() == log_val_type::app_log) {
                ptr<buffer> buf = entry->get_buf_ptr();
                buf->pos(0);
                state_machine_->pre_commit_ext
                    ( state_machine::ext_op_params( log_idx, buf ) );

            } else if(entry->get_val_type() == log_val_type::conf) {
                p_in("receive a config change from leader at %" PRIu64, log_idx);
                config_changing_ = true;
            }

            log_idx += 1;
            cnt += 1;

            if (stopping_) return resp;
        }
        p_db("[after OVWR] log_idx: %" PRIu64 ", count: %zu", log_idx, cnt);

        if (rollback_in_progress) {
            p_in("last log index after rollback and overwrite: %" PRIu64,
                 log_store_->next_slot() - 1);
        }

        // Append new log entries
        while (cnt < req.log_entries().size()) {
            ptr<log_entry> entry = req.log_entries().at( cnt++ );
            p_tr("append at %" PRIu64 ", term %" PRIu64 ", timestamp %" PRIu64 "\n",
                 log_store_->next_slot(), entry->get_term(), entry->get_timestamp());
            ulong idx_for_entry = store_log_entry(entry);
            if (entry->get_val_type() == log_val_type::conf) {
                p_in( "receive a config change from leader at %" PRIu64,
                      idx_for_entry );
                config_changing_ = true;

            } else if(entry->get_val_type() == log_val_type::app_log) {
                ptr<buffer> buf = entry->get_buf_ptr();
                buf->pos(0);
                state_machine_->pre_commit_ext
                    ( state_machine::ext_op_params( idx_for_entry, buf ) );
            }

            if (stopping_) return resp;
        }

        // End of batch.
        log_store_->end_of_append_batch( req.get_last_log_idx() + 1,
                                         req.log_entries().size() );

        ptr<raft_params> params = ctx_->get_params();
        if (params->parallel_log_appending_) {
            uint64_t last_durable_index = log_store_->last_durable_index();
            while ( last_durable_index <
                    req.get_last_log_idx() + req.log_entries().size() ) {
                // Some logs are not durable yet, wait here and block the thread.
                p_tr( "durable index %" PRIu64
                      ", sleep and wait for log appending completion",
                      last_durable_index );
                ea_follower_log_append_->wait_ms(params->heart_beat_interval_);

                // --- `notify_log_append_completion` API will wake it up. ---

                ea_follower_log_append_->reset();
                last_durable_index = log_store_->last_durable_index();
                p_tr( "wake up, durable index %" PRIu64, last_durable_index );
            }
        }
    }

    leader_ = req.get_src();

    // WARNING:
    //   If this node was leader but now follower, and right after
    //   leader election, new leader's committed index can be
    //   smaller than this node's quick/sm commit index.
    //   But that doesn't mean that rollback can happen on
    //   already committed index. Committed log index should never go back,
    //   in the state machine's point of view.
    //
    // e.g.)
    //   1) All replicas have log 1, and also committed up to 1.
    //   2) Leader appends log 2 and 3, replicates them, reaches consensus,
    //      so that commits up to 3.
    //   3) Leader appends a new log 4, but before replicating log 4 with
    //      committed log index 3, leader election happens.
    //   4) New leader has logs up to 3, but its last committed index is still 1.
    //   5) In such case, the old leader's log 4 should be rolled back,
    //      and new leader's commit index (1) can be temporarily smaller
    //      than old leader's commit index (3), but that doesn't mean
    //      old leader's commit index is wrong. New leader will soon commit
    //      logs up to 3 that is identical to what old leader has, and make
    //      progress after that. Logs already reached consensus (1, 2, and 3)
    //      will remain unchanged.
    leader_commit_index_.store(req.get_commit_idx());

    // WARNING:
    //   If `commit_idx > next_slot()`, it may cause problem
    //   on next `append_entries()` call, due to racing
    //   between BG commit thread and appending logs.
    //   Hence, we always should take smaller one.
    ulong target_precommit_index = req.get_last_log_idx() + req.log_entries().size();

    // WARNING:
    //   Since `peer::set_free()` is called prior than response handler
    //   without acquiring `raft_server::lock_`, there can be an edge case
    //   that leader may send duplicate logs, and their last log index may not
    //   be greater than the last log index this server already has. We should
    //   always compare the target index with current precommit index, and take
    //   it only when it is greater than the previous one.
    bool pc_updated = try_update_precommit_index(target_precommit_index);
    if (!pc_updated) {
        // If updating `precommit_index_` failed, we SHOULD NOT update
        // commit index as well.
    } else {
        commit( std::min( req.get_commit_idx(), target_precommit_index ) );
    }

    resp->accept(target_precommit_index + 1);

    int32 time_ms = tt.get_us() / 1000;
    if (time_ms >= ctx_->get_params()->heart_beat_interval_) {
        // Append entries took longer than HB interval. Warning.
        p_wn("appending entries from peer %d took long time (%d ms)\n"
             "req type: %d, req term: %" PRIu64 ", "
             "req l idx: %" PRIu64 " (%zu), req c idx: %" PRIu64 ", "
             "my term: %" PRIu64 ", my role: %d",
             req.get_src(), time_ms, (int)req.get_type(), req.get_term(),
             req.get_last_log_idx(), req.log_entries().size(), req.get_commit_idx(),
             state_->get_term(), (int)role_);
    }

    // Modified by Jung-Sang Ahn, Mar 28 2018.
    // Restart election timer here, as this function may take long time.
    if ( req.get_term() == state_->get_term() &&
         role_ == srv_role::follower ) {
        restart_election_timer();
    }

    int64 bs_hint = state_machine_->get_next_batch_size_hint_in_bytes();
    resp->set_next_batch_size_hint_in_bytes(bs_hint);
    p_tr("batch size hint: %" PRId64 " bytes", bs_hint);

    out_of_log_range_ = false;

    return resp;
}

bool raft_server::try_update_precommit_index(ulong desired, const size_t MAX_ATTEMPTS) {
    // If `MAX_ATTEMPTS == 0`, try forever.
    size_t num_attempts = 0;
    ulong prev_precommit_index = precommit_index_;
    while ( prev_precommit_index < desired &&
            (num_attempts < MAX_ATTEMPTS || MAX_ATTEMPTS == 0) ) {
        if ( precommit_index_.compare_exchange_strong( prev_precommit_index,
                                                       desired ) ) {
            return true;
        }
        // Otherwise: retry until `precommit_index_` is equal to or greater than
        //            `desired`.
        num_attempts++;
    }
    if (precommit_index_ >= desired) {
        return true;
    }
    p_er("updating precommit_index_ failed after %zu/%zu attempts, "
         "last seen precommit_index_ %" PRIu64 ", target %" PRIu64,
         num_attempts, MAX_ATTEMPTS, prev_precommit_index, desired);
    return false;
}

void raft_server::handle_append_entries_resp(resp_msg& resp) {
    peer_itor it = peers_.find(resp.get_src());
    if (it == peers_.end()) {
        p_in("the response is from an unknown peer %d", resp.get_src());
        return;
    }

    check_srv_to_leave_timeout();
    if ( srv_to_leave_ &&
         srv_to_leave_->get_id() == resp.get_src() &&
         srv_to_leave_->is_stepping_down() &&
         resp.get_next_idx() > srv_to_leave_target_idx_ ) {
        // Catch-up is done.
        p_in("server to be removed %d fully caught up the "
             "target config log %" PRIu64,
             srv_to_leave_->get_id(),
             srv_to_leave_target_idx_);
        remove_peer_from_peers(srv_to_leave_);
        reset_srv_to_leave();
        return;
    }

    // If there are pending logs to be synced or commit index need to be advanced,
    // continue to send appendEntries to this peer
    bool need_to_catchup = true;

    ptr<peer> p = it->second;
    p_tr("handle append entries resp (from %d), resp.get_next_idx(): %" PRIu64,
         (int)p->get_id(), resp.get_next_idx());

    int64 bs_hint = resp.get_next_batch_size_hint_in_bytes();
    p_tr("peer %d batch size hint: %" PRId64 " bytes", p->get_id(), bs_hint);
    p->set_next_batch_size_hint_in_bytes(bs_hint);

    if (resp.get_accepted()) {
        uint64_t prev_matched_idx = 0;
        uint64_t new_matched_idx = 0;
        {
            std::lock_guard<std::mutex> l(p->get_lock());
            p->set_next_log_idx(resp.get_next_idx());
            prev_matched_idx = p->get_matched_idx();
            new_matched_idx = resp.get_next_idx() - 1;
            p_tr("peer %d, prev matched idx: %" PRIu64 ", new matched idx: %" PRIu64,
                 p->get_id(), prev_matched_idx, new_matched_idx);
            p->set_matched_idx(new_matched_idx);
            p->set_last_accepted_log_idx(new_matched_idx);
        }
        cb_func::Param param(id_, leader_, p->get_id());
        param.ctx = &new_matched_idx;
        CbReturnCode rc = ctx_->cb_func_.call
                          ( cb_func::GotAppendEntryRespFromPeer, &param );
        (void)rc;

        // Try to enable stream
        int32 max_gap_in_stream = ctx_->get_params()->max_log_gap_in_stream_;
        ulong acceptable_precommit_idx = resp.get_next_idx() +
                                         max_gap_in_stream;
        ulong last_streamed_log_idx = p->get_last_streamed_log_idx();
        p_tr("max gap: %d, acceptable_precommit_idx: %ld, last_streamed_log_idx: %ld, "
             "last_sent: %ld, next_idx: %ld", max_gap_in_stream, 
             acceptable_precommit_idx, last_streamed_log_idx, p->get_last_sent_idx(), 
             resp.get_next_idx());
        if (max_gap_in_stream > 0 &&
            last_streamed_log_idx == 0 && 
            resp.get_next_idx() > 0 &&
            p->get_last_sent_idx() < resp.get_next_idx() && 
            precommit_index_ < acceptable_precommit_idx) {
            p_in("start stream mode at idx: %ld", resp.get_next_idx() - 1);
            p->set_last_streamed_log_idx(0, resp.get_next_idx() - 1);
        }

        // Try to commit with this response.
        ulong committed_index = get_expected_committed_log_idx();
        commit( committed_index );

        ulong next_idx_to_send = last_streamed_log_idx 
                                 ? last_streamed_log_idx + 1 
                                 : resp.get_next_idx();
        need_to_catchup = p->clear_pending_commit() ||
                          next_idx_to_send < log_store_->next_slot();

    } else {
        std::lock_guard<std::mutex> guard(p->get_lock());
        ulong prev_next_log = p->get_next_log_idx();
        if (resp.get_next_idx() > 0 && prev_next_log > resp.get_next_idx()) {
            // fast move for the peer to catch up
            p->set_next_log_idx(resp.get_next_idx());
        } else {
            bool do_log_rewind = true;
            // If not, check an extra order exists.
            if (resp.get_ctx()) {
                ptr<resp_appendix> appendix = resp_appendix::deserialize(*resp.get_ctx());
                if (appendix->extra_order_ == resp_appendix::DO_NOT_REWIND) {
                    do_log_rewind = false;
                }

                static timer_helper extra_order_timer(1000 * 1000, true);
                int log_lv = extra_order_timer.timeout_and_reset() ? L_INFO : L_TRACE;
                p_lv(log_lv, "received extra order: %s",
                     resp_appendix::extra_order_msg(appendix->extra_order_));
            }
            // if not, move one log backward.
            // WARNING: Make sure that `next_log_idx_` shouldn't be smaller than 0.
            if (do_log_rewind && prev_next_log) {
                p->set_next_log_idx(prev_next_log - 1);
            }
        }
        bool suppress = p->need_to_suppress_error();

        // To avoid verbose logs here.
        static timer_helper log_timer(500 * 1000, true);
        int log_lv = suppress ? L_INFO : L_WARN;
        if (log_lv == L_WARN) {
            if (!log_timer.timeout_and_reset()) {
                log_lv = L_TRACE;
            }
        }
        p_lv( log_lv,
              "declined append: peer %d, prev next log idx %" PRIu64 ", "
              "resp next %" PRIu64 ", new next log idx %" PRIu64,
              p->get_id(), prev_next_log,
              resp.get_next_idx(), p->get_next_log_idx() );
        
        // disable stream
        p->reset_stream();
    }

    // NOTE:
    //   If all other followers are not responding, we may not make
    //   below condition true. In that case, we check the timeout of
    //   re-election timer in heartbeat handler, and do force resign.
    ulong p_matched_idx = p->get_matched_idx();
    if ( write_paused_ &&
         p->get_id() == next_leader_candidate_ &&
         p_matched_idx &&
         p_matched_idx == log_store_->next_slot() - 1 &&
         p->make_busy() ) {
        // NOTE:
        //   If `make_busy` fails (very unlikely to happen), next
        //   response handler (of heartbeat, append_entries ..) will
        //   retry this.
        p_in("ready to resign, server id %d, "
             "latest log index %" PRIu64 ", "
             "%" PRIu64 " us elapsed, resign now",
             next_leader_candidate_.load(),
             p_matched_idx,
             reelection_timer_.get_us());
        leader_ = -1;

        // To avoid this node becomes next leader again, set timeout
        // value bigger than any others, just once at this time.
        rand_timeout_ = [this]() -> int32 {
            return this->ctx_->get_params()->election_timeout_upper_bound_ +
                   this->ctx_->get_params()->election_timeout_lower_bound_;
        };
        become_follower();
        update_rand_timeout();

        // Clear live flag to avoid pre-vote rejection.
        hb_alive_ = false;

        // Send leadership takeover request to this follower.
        ptr<req_msg> req = cs_new<req_msg>
                           ( state_->get_term(),
                             msg_type::custom_notification_request,
                             id_, p->get_id(),
                             term_for_log(log_store_->next_slot() - 1),
                             log_store_->next_slot() - 1,
                             quick_commit_index_.load() );

        // Create a notification.
        ptr<custom_notification_msg> custom_noti =
            cs_new<custom_notification_msg>
            ( custom_notification_msg::leadership_takeover );

        // Wrap it using log_entry.
        ptr<log_entry> custom_noti_le =
            cs_new<log_entry>(0, custom_noti->serialize(), log_val_type::custom);

        req->log_entries().push_back(custom_noti_le);
        p->send_req(p, req, resp_handler_);
        return;
    }

    if (bs_hint < 0) {
        // If hint is a negative number, we should set `need_to_catchup`
        // to `false` to avoid sending meaningless messages continuously
        // which eats up CPU. Then the leader will send heartbeats only.
        need_to_catchup = false;
    }

    // This may not be a leader anymore,
    // such as the response was sent out long time ago
    // and the role was updated by UpdateTerm call
    // Try to match up the logs for this peer
    if (role_ == srv_role::leader) {
        if (need_to_catchup) {
            p_db("reqeust append entries need to catchup, p %d\n",
                 (int)p->get_id());
            request_append_entries(p);
        }
        if (status_check_timer_.timeout_and_reset()) {
            check_overall_status();
        }
    }
}

uint64_t raft_server::get_current_leader_index() {
    uint64_t leader_index = precommit_index_;
    ptr<raft_params> params = ctx_->get_params();
    if (params->parallel_log_appending_) {
        // For parallel appending, take the smaller one.
        uint64_t durable_index = log_store_->last_durable_index();
        p_tr("last durable index %" PRIu64 ", precommit index %" PRIu64,
             durable_index, precommit_index_.load());
        leader_index = std::min(precommit_index_.load(), durable_index);
    }
    return leader_index;
}

ulong raft_server::get_expected_committed_log_idx() {
    std::vector<ulong> matched_indexes;
    state_machine::adjust_commit_index_params aci_params;
    matched_indexes.reserve(16);
    aci_params.peer_index_map_.reserve(16);

    // Put the index of leader itself.
    uint64_t leader_index = get_current_leader_index();
    matched_indexes.push_back( leader_index );
    aci_params.peer_index_map_[id_] = leader_index;

    for (auto& entry: peers_) {
        ptr<peer>& p = entry.second;
        aci_params.peer_index_map_[p->get_id()] = p->get_matched_idx();

        if (!is_regular_member(p)) continue;
        matched_indexes.push_back( p->get_matched_idx() );
    }
    int voting_members = get_num_voting_members();
    assert((int32)matched_indexes.size() == voting_members);

    // NOTE: Descending order.
    //       e.g.) 100 100 99 95 92
    //             => commit on 99 if `quorum_idx == 2`.
    std::sort( matched_indexes.begin(),
               matched_indexes.end(),
               std::greater<ulong>() );

    size_t quorum_idx = get_quorum_for_commit();
    if (ctx_->get_params()->use_full_consensus_among_healthy_members_) {
        size_t not_responding_peers = get_not_responding_peers_count();
        if (not_responding_peers < voting_members - quorum_idx) {
            // If full consensus option is on, commit should be
            // agreed by all healthy members, and the number of
            // aggreed members should be bigger than regular quorum size.
            size_t prev_quorum_idx = quorum_idx;
            quorum_idx = voting_members - not_responding_peers - 1;
            p_tr( "full consensus mode: %zu peers are not responding out of %d, "
                  "adjust quorum %zu -> %zu",
                  not_responding_peers, voting_members,
                  prev_quorum_idx, quorum_idx );
        } else {
            p_tr( "full consensus mode, but %zu peers are not responding, "
                  "required quorum size %zu/%d",
                  not_responding_peers, quorum_idx + 1, voting_members );
        }
    }

    if (l_ && l_->get_level() >= 6) {
        std::string tmp_str;
        for (ulong m_idx: matched_indexes) {
            tmp_str += std::to_string(m_idx) + " ";
        }
        p_tr("quorum idx %zu, %s", quorum_idx, tmp_str.c_str());
    }

    aci_params.current_commit_index_ = quick_commit_index_;
    aci_params.expected_commit_index_ = matched_indexes[quorum_idx];
    uint64_t adjusted_commit_index = state_machine_->adjust_commit_index(aci_params);
    if (aci_params.expected_commit_index_ != adjusted_commit_index) {
        p_tr( "commit index adjusted: %" PRIu64 " -> %" PRIu64,
              aci_params.expected_commit_index_, adjusted_commit_index );
    }
    return adjusted_commit_index;
}

void raft_server::notify_log_append_completion(bool ok) {
    p_tr("got log append completion notification: %s", ok ? "OK" : "FAILED");

    if (role_ == srv_role::leader) {
        recur_lock(lock_);
        if (!ok) {
            // If log appending fails, leader should resign immediately.
            p_er("log appending failed, resign immediately");
            leader_ = -1;
            become_follower();

            // Clear this flag to avoid pre-vote rejection.
            hb_alive_ = false;
            return;
        }

        // Leader: commit the log and send append_entries request, if needed.
        uint64_t prev_committed_index = quick_commit_index_.load();
        uint64_t committed_index = get_expected_committed_log_idx();
        commit( committed_index );

        if (quick_commit_index_ > prev_committed_index) {
            // Commit index has been changed as a result of log appending.
            // Send replication messages.
            request_append_entries_for_all();
        }
    } else {
        if (!ok) {
            // If log appending fails for follower, there is no way to proceed it.
            // We should stop the server immediately.
            recur_lock(lock_);
            p_ft("log appending failed, stop this server");
            ctx_->state_mgr_->system_exit(N21_log_flush_failed);
            return;
        }

        // Follower: wake up the waiting thread.
        ea_follower_log_append_->invoke();
    }
}

} // namespace nuraft;

