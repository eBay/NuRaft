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
#include "event_awaiter.h"
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
        commit(precommit_index_.load());
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
        size_t num_not_responding_peers = get_not_responding_peers();
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
                ptr<raft_params> clone = cs_new<raft_params>(*params);
                clone->custom_commit_quorum_size_ = 1;
                clone->custom_election_quorum_size_ = 1;
                ctx_->set_params(clone);
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
            p_wn( "connection to peer %d is not active long time: %zu ms, "
                  "but this peer should be removed. do nothing",
                  p->get_id(),
                  last_active_time_ms );
        } else {
            p_wn( "connection to peer %d is not active long time: %zu ms, "
                  "force re-connect",
                  p->get_id(),
                  last_active_time_ms );
            need_to_reconnect = true;
            p->reset_active_timer();
        }
    }
    if (need_to_reconnect) {
        reconnect_client(*p);
        p->clear_reconnection();
    }

    if (p->make_busy()) {
        p_tr("send request to %d\n", (int)p->get_id());

        // If reserved message exists, process it first.
        ptr<req_msg> msg = p->get_rsv_msg();
        rpc_handler m_handler = p->get_rsv_msg_handler();
        if (msg) {
            // Clear the reserved message.
            p->set_rsv_msg(nullptr, nullptr);
            p_in("found reserved message to peer %d, type %d",
                 p->get_id(), msg->get_type());

        } else {
            // Normal message.
            msg = create_append_entries_req(*p);
            m_handler = resp_handler_;
        }
        if (!msg) {
            // Even normal message doesn't exist.
            p->set_free();
            return true;
        }

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

        p->send_req(p, msg, m_handler);
        p->reset_ls_timer();

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
                 "log idx %zu commit idx %zu, set flag",
                 srv_to_leave_->get_id(),
                 msg->get_last_log_idx(),
                 msg->get_commit_idx());
        }

        p_tr("sent\n");
        return true;
    }

    p_db("Server %d is busy, skip the request", p->get_id());

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

ptr<req_msg> raft_server::create_append_entries_req(peer& p) {
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

        last_log_idx = p.get_next_log_idx() - 1;
    }

    if (last_log_idx >= cur_nxt_idx) {
        // LCOV_EXCL_START
        p_er( "Peer's lastLogIndex is too large %llu v.s. %llu, ",
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

    p_tr("last_log_idx: %d, starting_idx: %d, cur_nxt_idx: %d\n",
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
        p_db("last sent log (%zu) to peer %d is not applied, cnt %d",
             peer_last_sent_idx, p.get_id(), cur_cnt);
        if (cur_cnt >= 5) {
            ulong prev_end_idx = end_idx;
            end_idx = std::min( cur_nxt_idx, last_log_idx + 1 + 1 );
            p_db("reduce end_idx %zu -> %zu", prev_end_idx, end_idx);
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
            p_wn("failed to retrieve log entries: %zu - %zu", last_log_idx + 1, end_idx);
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
            p_db( "send snapshot peer %d, peer log idx: %zu, my starting idx: %zu, "
                  "my log idx: %zu, last_snapshot_log_idx: %zu\n",
                  p.get_id(),
                  last_log_idx, starting_idx, cur_nxt_idx,
                  snp_local->get_last_log_idx() );
            return create_sync_snapshot_req(p, last_log_idx, term, commit_idx);
        }

        // Cannot recover using snapshot. Return here to protect the leader.
        static timer_helper msg_timer(5000000);
        int log_lv = msg_timer.timeout_and_reset() ? L_ERROR : L_TRACE;
        p_lv(log_lv,
             "neither snapshot nor log exists, peer %d, last log %zu, "
             "leader's start log %zu",
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
        p_tr("adjusted end_idx due to batch size hint: %zu -> %zu",
             end_idx, adjusted_end_idx);
    }

    p_db( "append_entries for %d with LastLogIndex=%llu, "
          "LastLogTerm=%llu, EntriesLength=%d, CommitIndex=%llu, "
          "Term=%llu, peer_last_sent_idx %zu",
          p.get_id(), last_log_idx, last_log_term,
          ( log_entries ? log_entries->size() : 0 ), commit_idx, term,
          peer_last_sent_idx );
    if (last_log_idx+1 == adjusted_end_idx) {
        p_tr( "EMPTY PAYLOAD" );
    } else if (last_log_idx+1 + 1 == adjusted_end_idx) {
        p_db( "idx: %zu", last_log_idx+1 );
    } else {
        p_db( "idx range: %zu-%zu", last_log_idx+1, adjusted_end_idx-1 );
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

    p_tr("from peer %d, req type: %d, req term: %ld, "
         "req l idx: %ld (%zu), req c idx: %ld, "
         "my term: %ld, my role: %d\n",
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
          "[LOG %s] req log idx: %zu, req log term: %zu, my last log idx: %zu, "
          "my log (%zu) term: %zu",
          (log_okay ? "OK" : "XX"),
          req.get_last_log_idx(),
          req.get_last_log_term(),
          log_store_->next_slot() - 1,
          req.get_last_log_idx(),
          log_term );

    if ( req.get_term() < state_->get_term() ||
         log_okay == false ) {
        p_lv( log_lv,
              "deny, req term %zu, my term %zu, req log idx %zu, my log idx %zu",
              req.get_term(), state_->get_term(),
              req.get_last_log_idx(), log_store_->next_slot() - 1 );
        if (local_snp) {
            p_lv( log_lv, "snp idx %zu term %zu",
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
    if (cb_ret != cb_func::Ok) return resp;

    if (req.log_entries().size() > 0) {
        // Write logs to store, start from overlapped logs

        // Actual log number.
        ulong log_idx = req.get_last_log_idx() + 1;
        // Local counter for iterating req.log_entries().
        size_t cnt = 0;

        p_db("[INIT] log_idx: %ld, count: %ld, log_store_->next_slot(): %ld, "
             "req.log_entries().size(): %ld\n",
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
        p_db("[after SKIP] log_idx: %ld, count: %ld\n", log_idx, cnt);

        // Rollback (only if necessary).
        // WARNING:
        //   1) Rollback should be done separately before overwriting,
        //      and MUST BE in backward direction.
        //   2) Should do rollback ONLY WHEN we have more than one log
        //      to overwrite.
        ulong my_last_log_idx = log_store_->next_slot() - 1;
        bool rollback_in_progress = false;
        if ( my_last_log_idx >= log_idx &&
             cnt < req.log_entries().size() ) {
            p_in( "rollback logs: %zu - %zu, commit idx req %zu, quick %zu, sm %zu, "
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
                p_wn( "rollback quick commit index from %zu to %zu",
                      quick_commit_index_.load(),
                      log_idx - 1 );
                quick_commit_index_ = log_idx - 1;
            }
            if ( sm_commit_index_ >= log_idx ) {
                p_wn( "rollback sm commit index from %zu to %zu",
                      sm_commit_index_.load(),
                      log_idx - 1 );
                sm_commit_index_ = log_idx - 1;
            }

            for ( uint64_t ii = 0; ii < my_last_log_idx - log_idx + 1; ++ii ) {
                uint64_t idx = my_last_log_idx - ii;
                ptr<log_entry> old_entry = log_store_->entry_at(idx);
                if (old_entry->get_val_type() == log_val_type::app_log) {
                    ptr<buffer> buf = old_entry->get_buf_ptr();
                    buf->pos(0);
                    state_machine_->rollback_ext
                        ( state_machine::ext_op_params( idx, buf ) );
                    p_in( "rollback log %zu", idx );

                } else if (old_entry->get_val_type() == log_val_type::conf) {
                    p_in( "revert from a prev config change to config at %llu",
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
            p_in("overwrite at %zu\n", log_idx);
            store_log_entry(entry, log_idx);

            if (entry->get_val_type() == log_val_type::app_log) {
                ptr<buffer> buf = entry->get_buf_ptr();
                buf->pos(0);
                state_machine_->pre_commit_ext
                    ( state_machine::ext_op_params( log_idx, buf ) );

            } else if(entry->get_val_type() == log_val_type::conf) {
                p_in("receive a config change from leader at %llu", log_idx);
                config_changing_ = true;
            }

            log_idx += 1;
            cnt += 1;

            if (stopping_) return resp;
        }
        p_db("[after OVWR] log_idx: %ld, count: %ld\n", log_idx, cnt);

        if (rollback_in_progress) {
            p_in("last log index after rollback and overwrite: %zu",
                 log_store_->next_slot() - 1);
        }

        // Append new log entries
        while (cnt < req.log_entries().size()) {
            p_tr("append at %zu\n", log_store_->next_slot());
            ptr<log_entry> entry = req.log_entries().at( cnt++ );
            ulong idx_for_entry = store_log_entry(entry);
            if (entry->get_val_type() == log_val_type::conf) {
                p_in( "receive a config change from leader at %llu",
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
             "req type: %d, req term: %ld, "
             "req l idx: %ld (%zu), req c idx: %ld, "
             "my term: %ld, my role: %d\n",
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
    p_tr("batch size hint: %ld bytes", bs_hint);

    out_of_log_range_ = false;

    return resp;
}

bool raft_server::try_update_precommit_index(ulong desired, const size_t MAX_ATTEMPTS) {
    // If `MAX_ATTEMPTS == 0`, try forever.
    size_t num_attempts = 0;
    ulong prev_precommit_index = precommit_index_;
    while ( prev_precommit_index < desired &&
            num_attempts < MAX_ATTEMPTS ) {
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
         "last seen precommit_index_ %zu, target %zu",
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
             "target config log %zu",
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
    p_tr("handle append entries resp (from %d), resp.get_next_idx(): %zu\n",
         (int)p->get_id(), resp.get_next_idx());

    int64 bs_hint = resp.get_next_batch_size_hint_in_bytes();
    p_tr("peer %d batch size hint: %ld bytes", p->get_id(), bs_hint);
    p->set_next_batch_size_hint_in_bytes(bs_hint);

    if (resp.get_accepted()) {
        uint64_t prev_matched_idx = 0;
        uint64_t new_matched_idx = 0;
        {
            std::lock_guard<std::mutex>(p->get_lock());
            p->set_next_log_idx(resp.get_next_idx());
            prev_matched_idx = p->get_matched_idx();
            new_matched_idx = resp.get_next_idx() - 1;
            p_tr("peer %d, prev matched idx: %ld, new matched idx: %ld",
                 p->get_id(), prev_matched_idx, new_matched_idx);
            p->set_matched_idx(new_matched_idx);
        }
        cb_func::Param param(id_, leader_, p->get_id());
        param.ctx = &new_matched_idx;
        CbReturnCode rc = ctx_->cb_func_.call
                          ( cb_func::GotAppendEntryRespFromPeer, &param );
        (void)rc;

        // Try to commit with this response.
        ulong committed_index = get_expected_committed_log_idx();
        commit( committed_index );
        need_to_catchup = p->clear_pending_commit() ||
                          resp.get_next_idx() < log_store_->next_slot();

    } else {
        ulong prev_next_log = p->get_next_log_idx();
        std::lock_guard<std::mutex> guard(p->get_lock());
        if (resp.get_next_idx() > 0 && p->get_next_log_idx() > resp.get_next_idx()) {
            // fast move for the peer to catch up
            p->set_next_log_idx(resp.get_next_idx());
        } else {
            // if not, move one log backward.
            p->set_next_log_idx(p->get_next_log_idx() - 1);
        }
        bool suppress = p->need_to_suppress_error();

        // To avoid verbose logs here.
        static timer_helper log_timer(500*1000, true);
        int log_lv = suppress ? L_INFO : L_WARN;
        if (log_lv == L_WARN) {
            if (!log_timer.timeout_and_reset()) {
                log_lv = L_TRACE;
            }
        }
        p_lv( log_lv,
              "declined append: peer %d, prev next log idx %zu, "
              "resp next %zu, new next log idx %zu",
              p->get_id(), prev_next_log,
              resp.get_next_idx(), p->get_next_log_idx() );
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
             "latest log index %zu, "
             "%zu us elapsed, resign now",
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

ulong raft_server::get_expected_committed_log_idx() {
    std::vector<ulong> matched_indexes;
    matched_indexes.reserve(16);

    // Leader itself.
    matched_indexes.push_back( precommit_index_ );
    for (auto& entry: peers_) {
        ptr<peer>& p = entry.second;
        if (!is_regular_member(p)) continue;

        matched_indexes.push_back( p->get_matched_idx() );
    }
    assert((int32)matched_indexes.size() == get_num_voting_members());

    // NOTE: Descending order.
    //       e.g.) 100 100 99 95 92
    //             => commit on 99 if `quorum_idx == 2`.
    std::sort( matched_indexes.begin(),
               matched_indexes.end(),
               std::greater<ulong>() );

    size_t quorum_idx = get_quorum_for_commit();
    if (l_->get_level() >= 6) {
        std::string tmp_str;
        for (ulong m_idx: matched_indexes) {
            tmp_str += std::to_string(m_idx) + " ";
        }
        p_tr("quorum idx %zu, %s", quorum_idx, tmp_str.c_str());
    }

    return matched_indexes[ quorum_idx ];
}

} // namespace nuraft;

