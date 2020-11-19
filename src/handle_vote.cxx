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
#include "handle_custom_notification.hxx"
#include "peer.hxx"
#include "state_mgr.hxx"
#include "tracer.hxx"

#include <cassert>
#include <sstream>

namespace nuraft {

bool raft_server::check_cond_for_zp_election() {
    ptr<raft_params> params = ctx_->get_params();
    if ( params->allow_temporary_zero_priority_leader_ &&
         target_priority_ == 1 &&
         my_priority_ == 0 &&
         priority_change_timer_.get_ms() >
            (uint64_t)params->heart_beat_interval_ * 20 ) {
        return true;
    }
    return false;
}

void raft_server::request_prevote() {
    ptr<raft_params> params = ctx_->get_params();
    ptr<cluster_config> c_config = get_config();
    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        ptr<peer> pp = it->second;
        if (!is_regular_member(pp)) continue;
        ptr<srv_config> s_config = c_config->get_server( pp->get_id() );

        if (s_config) {
            bool recreate = false;
            if (hb_alive_) {
                // First pre-vote request: reset RPC client for all peers.
                recreate = true;

            } else {
                // Since second time: reset only if `rpc_` is null.
                recreate = pp->need_to_reconnect();

                // Or if it is not active long time, reconnect as well.
                int32 last_active_time_ms = pp->get_active_timer_us() / 1000;
                if ( last_active_time_ms >
                         params->heart_beat_interval_ *
                             raft_server::raft_limits_.reconnect_limit_ ) {
                    p_wn( "connection to peer %d is not active long time: %zu ms, "
                          "need reconnection for prevote",
                          pp->get_id(),
                          last_active_time_ms );
                    recreate = true;
                }
            }

            if (recreate) {
                p_in("reset RPC client for peer %d", s_config->get_id());
                pp->recreate_rpc(s_config, *ctx_);
                pp->set_free();
                pp->set_manual_free();
            }
        }
    }

    int quorum_size = get_quorum_for_election();
    if (pre_vote_.live_ + pre_vote_.dead_ > 0) {
        if (pre_vote_.live_ + pre_vote_.dead_ < quorum_size + 1) {
            // Pre-vote failed due to non-responding voters.
            pre_vote_.failure_count_++;
            p_wn("total %zu nodes (including this node) responded for pre-vote "
                 "(term %zu, live %zu, dead %zu), at least %zu nodes should "
                 "respond. failure count %zu",
                 pre_vote_.live_.load() + pre_vote_.dead_.load(),
                 pre_vote_.term_,
                 pre_vote_.live_.load(),
                 pre_vote_.dead_.load(),
                 quorum_size + 1,
                 pre_vote_.failure_count_.load());
        } else {
            pre_vote_.failure_count_ = 0;
        }
    }
    int num_voting_members = get_num_voting_members();
    if ( params->auto_adjust_quorum_for_small_cluster_ &&
         num_voting_members == 2 &&
         pre_vote_.failure_count_ > raft_server::raft_limits_.vote_limit_ ) {
        // 2-node cluster's pre-vote failed due to offline node.
        p_wn("2-node cluster's pre-vote is failing long time, "
             "adjust quorum to 1");
        ptr<raft_params> clone = cs_new<raft_params>(*params);
        clone->custom_commit_quorum_size_ = 1;
        clone->custom_election_quorum_size_ = 1;
        ctx_->set_params(clone);
    }

    hb_alive_ = false;
    leader_ = -1;
    pre_vote_.reset(state_->get_term());
    // Count for myself.
    pre_vote_.dead_++;

    if ( my_priority_ < target_priority_ ) {
        if ( check_cond_for_zp_election() ) {
            p_in("[PRIORITY] temporarily allow election for zero-priority member");
        } else {
            p_in("[PRIORITY] will not initiate pre-vote due to priority: "
                 "target %d, mine %d", target_priority_, my_priority_);
            restart_election_timer();
            return;
        }
    }

    p_in("[PRE-VOTE INIT] my id %d, my role %s, term %ld, log idx %ld, "
         "log term %ld, priority (target %d / mine %d)\n",
         id_, srv_role_to_string(role_).c_str(),
         state_->get_term(), log_store_->next_slot() - 1,
         term_for_log(log_store_->next_slot() - 1),
         target_priority_, my_priority_);

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        ptr<peer> pp = it->second;
        if (!is_regular_member(pp)) {
            // Do not send voting request to learner.
            continue;
        }

        ptr<req_msg> req( cs_new<req_msg>
                          ( state_->get_term(),
                            msg_type::pre_vote_request,
                            id_,
                            pp->get_id(),
                            term_for_log(log_store_->next_slot() - 1),
                            log_store_->next_slot() - 1,
                            quick_commit_index_.load() ) );
        if (pp->make_busy()) {
            pp->send_req(pp, req, resp_handler_);
        } else {
            p_wn("failed to send prevote request: peer %d (%s) is busy",
                 pp->get_id(), pp->get_endpoint().c_str());
        }
    }
}

void raft_server::initiate_vote(bool ignore_priority) {
    if ( my_priority_ >= target_priority_ ||
         ignore_priority ||
         check_cond_for_zp_election() ||
         get_quorum_for_election() == 0 ) {
        // Request vote when
        //  1) my priority satisfies the target, OR
        //  2) I'm the only node in the group.
        state_->inc_term();
        state_->set_voted_for(-1);
        role_ = srv_role::candidate;
        votes_granted_ = 0;
        votes_responded_ = 0;
        election_completed_ = false;
        ctx_->state_mgr_->save_state(*state_);
        request_vote(ignore_priority);
    }

    if (role_ != srv_role::leader) {
        hb_alive_ = false;
        leader_ = -1;
    }
}

void raft_server::request_vote(bool ignore_priority) {
    state_->set_voted_for(id_);
    ctx_->state_mgr_->save_state(*state_);
    votes_granted_ += 1;
    votes_responded_ += 1;
    p_in("[VOTE INIT] my id %d, my role %s, term %ld, log idx %ld, "
         "log term %ld, priority (target %d / mine %d)\n",
         id_, srv_role_to_string(role_).c_str(),
         state_->get_term(), log_store_->next_slot() - 1,
         term_for_log(log_store_->next_slot() - 1),
         target_priority_, my_priority_);

    // is this the only server?
    if (votes_granted_ > get_quorum_for_election()) {
        election_completed_ = true;
        become_leader();
        return;
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        ptr<peer> pp = it->second;
        if (!is_regular_member(pp)) {
            // Do not send voting request to learner.
            continue;
        }
        ptr<req_msg> req = cs_new<req_msg>
                           ( state_->get_term(),
                             msg_type::request_vote_request,
                             id_,
                             pp->get_id(),
                             term_for_log(log_store_->next_slot() - 1),
                             log_store_->next_slot() - 1,
                             quick_commit_index_.load() );
        if (ignore_priority) {
            // Add a special log entry to let receivers ignore the priority.

            // Force vote message, and wrap it using log_entry.
            ptr<force_vote_msg> fv_msg = cs_new<force_vote_msg>();
            ptr<log_entry> fv_msg_le =
                cs_new<log_entry>(0, fv_msg->serialize(), log_val_type::custom);

            // Ship it.
            req->log_entries().push_back(fv_msg_le);
        }
        p_db( "send %s to server %d with term %llu",
              msg_type_to_string(req->get_type()).c_str(),
              it->second->get_id(),
              state_->get_term() );
        if (pp->make_busy()) {
            pp->send_req(pp, req, resp_handler_);
        } else {
            p_wn("failed to send vote request: peer %d (%s) is busy",
                 pp->get_id(), pp->get_endpoint().c_str());
        }
    }
}

ptr<resp_msg> raft_server::handle_vote_req(req_msg& req) {
    p_in("[VOTE REQ] my role %s, from peer %d, log term: req %ld / mine %ld\n"
         "last idx: req %ld / mine %ld, term: req %ld / mine %ld\n"
         "priority: target %d / mine %d, voted_for %d",
         srv_role_to_string(role_).c_str(),
         req.get_src(), req.get_last_log_term(), log_store_->last_entry()->get_term(),
         req.get_last_log_idx(), log_store_->next_slot()-1,
         req.get_term(), state_->get_term(),
         target_priority_, my_priority_, state_->get_voted_for());

    ptr<resp_msg> resp( cs_new<resp_msg>
                        ( state_->get_term(),
                          msg_type::request_vote_response,
                          id_,
                          req.get_src() ) );

    bool log_okay =
        req.get_last_log_term() > log_store_->last_entry()->get_term() ||
        ( req.get_last_log_term() == log_store_->last_entry()->get_term() &&
          log_store_->next_slot() - 1 <= req.get_last_log_idx() );

    bool grant =
        req.get_term() == state_->get_term() &&
        log_okay &&
        ( state_->get_voted_for() == req.get_src() ||
          state_->get_voted_for() == -1 );

    bool force_vote = (req.log_entries().size() > 0);
    if (force_vote) {
        p_in("[VOTE REQ] force vote request, will ignore priority");
    }

    if (grant) {
        ptr<cluster_config> c_conf = get_config();
        for (auto& entry: c_conf->get_servers()) {
            srv_config* s_conf = entry.get();
            if ( !force_vote &&
                 s_conf->get_id() == req.get_src() &&
                 s_conf->get_priority() &&
                 s_conf->get_priority() < target_priority_ ) {
                // NOTE:
                //   If zero-priority member initiates leader election,
                //   that is intentionally triggered by the flag in
                //   `raft_params`. In such case, we don't check the
                //   priority.
                p_in("I (%d) could vote for peer %d, "
                     "but priority %d is lower than %d",
                     id_, s_conf->get_id(),
                     s_conf->get_priority(), target_priority_);
                p_in("decision: X (deny)\n");
                return resp;
            }
        }

        p_in("decision: O (grant), voted_for %d, term %zu",
             req.get_src(), resp->get_term());
        resp->accept(log_store_->next_slot());
        state_->set_voted_for(req.get_src());
        ctx_->state_mgr_->save_state(*state_);
    } else {
        p_in("decision: X (deny), term %zu", resp->get_term());
    }

    return resp;
}

void raft_server::handle_vote_resp(resp_msg& resp) {
    if (election_completed_) {
        p_in("Election completed, will ignore the voting result from this server");
        return;
    }

    if (resp.get_term() != state_->get_term()) {
        // Vote response for other term. Should ignore it.
        p_in("[VOTE RESP] from peer %d, my role %s, "
             "but different resp term %zu. ignore it.",
             resp.get_src(), srv_role_to_string(role_).c_str(), resp.get_term());
        return;
    }
    votes_responded_ += 1;

    if (resp.get_accepted()) {
        votes_granted_ += 1;
    }

    if (votes_responded_ >= get_num_voting_members()) {
        election_completed_ = true;
    }

    int32 election_quorum_size = get_quorum_for_election() + 1;

    p_in("[VOTE RESP] peer %d (%s), resp term %zu, my role %s, "
         "granted %d, responded %d, "
         "num voting members %d, quorum %d\n",
         resp.get_src(), (resp.get_accepted()) ? "O" : "X", resp.get_term(),
         srv_role_to_string(role_).c_str(),
         (int)votes_granted_, (int)votes_responded_,
         get_num_voting_members(), election_quorum_size);

    if (votes_granted_ >= election_quorum_size) {
        p_in("Server is elected as leader for term %zu", state_->get_term());
        election_completed_ = true;
        become_leader();
        p_in("  === LEADER (term %zu) ===\n", state_->get_term());
    }
}

ptr<resp_msg> raft_server::handle_prevote_req(req_msg& req) {
    ulong next_idx_for_resp = 0;
    auto entry = peers_.find(req.get_src());
    if (entry == peers_.end()) {
        // This node already has been removed, set a special value.
        next_idx_for_resp = std::numeric_limits<ulong>::max();
    }

    p_in("[PRE-VOTE REQ] my role %s, from peer %d, log term: req %ld / mine %ld\n"
         "last idx: req %ld / mine %ld, term: req %ld / mine %ld\n"
         "%s",
         srv_role_to_string(role_).c_str(),
         req.get_src(), req.get_last_log_term(),
         log_store_->last_entry()->get_term(),
         req.get_last_log_idx(), log_store_->next_slot()-1,
         req.get_term(), state_->get_term(),
         (hb_alive_) ? "HB alive" : "HB dead");

    ptr<resp_msg> resp
        ( cs_new<resp_msg>
          ( req.get_term(),
            msg_type::pre_vote_response,
            id_,
            req.get_src(),
            next_idx_for_resp ) );

    // NOTE:
    //   While `catching_up_` flag is on, this server does not get
    //   normal append_entries request so that `hb_alive_` may not
    //   be cleared properly. Hence, it should accept any pre-vote
    //   requests.
    if (catching_up_) {
        p_in("this server is catching up, always accept pre-vote");
    }
    if (!hb_alive_ || catching_up_) {
        p_in("pre-vote decision: O (grant)");
        resp->accept(log_store_->next_slot());
    } else {
        if (next_idx_for_resp != std::numeric_limits<ulong>::max()) {
            p_in("pre-vote decision: X (deny)");
        } else {
            p_in("pre-vote decision: XX (strong deny, non-existing node)");
        }
    }

    return resp;
}

void raft_server::handle_prevote_resp(resp_msg& resp) {
    if (resp.get_term() != pre_vote_.term_) {
        // Vote response for other term. Should ignore it.
        p_in("[PRE-VOTE RESP] from peer %d, my role %s, "
             "but different resp term %zu (pre-vote term %zu). "
             "ignore it.",
             resp.get_src(), srv_role_to_string(role_).c_str(),
             resp.get_term(), pre_vote_.term_);
        return;
    }

    if (resp.get_accepted()) {
        // Accept: means that this peer is not receiving HB.
        pre_vote_.dead_++;
    } else {
        if (resp.get_next_idx() != std::numeric_limits<ulong>::max()) {
            // Deny: means that this peer still sees leader.
            pre_vote_.live_++;
        } else {
            // `next_idx_for_resp == MAX`, it is a special signal
            // indicating that this node has been already removed.
            pre_vote_.abandoned_++;
        }
    }

    int32 election_quorum_size = get_quorum_for_election() + 1;

    p_in("[PRE-VOTE RESP] peer %d (%s), term %zu, resp term %zu, "
         "my role %s, dead %d, live %d, "
         "num voting members %d, quorum %d\n",
         resp.get_src(), (resp.get_accepted())?"O":"X",
         pre_vote_.term_, resp.get_term(),
         srv_role_to_string(role_).c_str(),
         pre_vote_.dead_.load(), pre_vote_.live_.load(),
         get_num_voting_members(), election_quorum_size);

    if (pre_vote_.dead_ >= election_quorum_size) {
        p_in("[PRE-VOTE DONE] SUCCESS, term %zu", pre_vote_.term_);

        bool exp = false;
        bool val = true;
        if (pre_vote_.done_.compare_exchange_strong(exp, val)) {
            p_in("[PRE-VOTE DONE] initiate actual vote");

            // Immediately initiate actual vote.
            initiate_vote();

            // restart the election timer if this is not yet a leader
            if (role_ != srv_role::leader) {
                restart_election_timer();
            }

        } else {
            p_in("[PRE-VOTE DONE] actual vote is already initiated, do nothing");
        }
    }

    if (pre_vote_.live_ >= election_quorum_size) {
        pre_vote_.quorum_reject_count_.fetch_add(1);
        p_wn("[PRE-VOTE] rejected by quorum, count %zu",
             pre_vote_.quorum_reject_count_.load());
        if ( pre_vote_.quorum_reject_count_ >=
                 raft_server::raft_limits_.pre_vote_rejection_limit_ ) {
            p_ft("too many pre-vote rejections, probably this node is not "
                 "receiving heartbeat from leader. "
                 "we should re-establish the network connection");
            send_reconnect_request();
        }
    }

    if (pre_vote_.abandoned_ >= election_quorum_size) {
        p_er("[PRE-VOTE DONE] this node has been removed, stepping down");
        steps_to_down_ = 2;
    }
}

} // namespace nuraft;

