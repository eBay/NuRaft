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
#include "context.hxx"
#include "error_code.hxx"
#include "event_awaiter.hxx"
#include "exit_handler.hxx"
#include "global_mgr.hxx"
#include "handle_client_request.hxx"
#include "handle_custom_notification.hxx"
#include "internal_timer.hxx"
#include "peer.hxx"
#include "snapshot.hxx"
#include "snapshot_sync_ctx.hxx"
#include "stat_mgr.hxx"
#include "state_machine.hxx"
#include "state_mgr.hxx"
#include "tracer.hxx"

#include <cassert>
#include <random>
#include <sstream>
#include <thread>

namespace nuraft {

const int raft_server::default_snapshot_sync_block_size = 4 * 1024;

raft_server::limits raft_server::raft_limits_;

raft_server::raft_server(context* ctx, const init_options& opt)
    : bg_append_ea_(nullptr)
    , initialized_(false)
    , leader_(-1)
    , id_(ctx->state_mgr_->server_id())
    , target_priority_(srv_config::INIT_PRIORITY)
    , votes_responded_(0)
    , votes_granted_(0)
    , leader_commit_index_(0)
    , quick_commit_index_(ctx->state_machine_->last_commit_index())
    , sm_commit_index_(ctx->state_machine_->last_commit_index())
    , index_at_becoming_leader_(0)
    , initial_commit_index_(ctx->state_machine_->last_commit_index())
    , hb_alive_(false)
    , election_completed_(true)
    , config_changing_(false)
    , out_of_log_range_(false)
    , data_fresh_(false)
    , stopping_(false)
    , commit_bg_stopped_(false)
    , append_bg_stopped_(false)
    , write_paused_(false)
    , sm_commit_paused_(false)
    , sm_commit_exec_in_progress_(false)
    , ea_sm_commit_exec_in_progress_(new EventAwaiter())
    , next_leader_candidate_(-1)
    , im_learner_(false)
    , serving_req_(false)
    , steps_to_down_(0)
    , snp_in_progress_(false)
    , snp_creation_scheduled_(false)
    , sched_snp_creation_result_(nullptr)
    , ctx_(ctx)
    , scheduler_(ctx->scheduler_)
    , election_exec_(std::bind(&raft_server::handle_election_timeout, this))
    , election_task_(nullptr)
    , role_(srv_role::follower)
    , state_(ctx->state_mgr_->read_state())
    , log_store_(ctx->state_mgr_->load_log_store())
    , state_machine_(ctx->state_machine_)
    , et_cnt_receiving_snapshot_(0)
    , first_snapshot_distance_(0)
    , l_(ctx->logger_)
    , stale_config_(nullptr)
    , config_(ctx->state_mgr_->load_config())
    , uncommitted_config_(nullptr)
    , srv_to_join_(nullptr)
    , srv_to_join_snp_retry_required_(false)
    , srv_to_leave_(nullptr)
    , srv_to_leave_target_idx_(0)
    , conf_to_add_(nullptr)
    , resp_handler_( (rpc_handler)std::bind( &raft_server::handle_peer_resp,
                                             this,
                                             std::placeholders::_1,
                                             std::placeholders::_2 ) )
    , ex_resp_handler_( (rpc_handler)std::bind( &raft_server::handle_ext_resp,
                                                this,
                                                std::placeholders::_1,
                                                std::placeholders::_2 ) )
    , last_snapshot_(ctx->state_machine_->last_snapshot())
    , ea_follower_log_append_(new EventAwaiter())
    , test_mode_flag_(opt.test_mode_flag_)
    , self_mark_down_(false)
    , excluded_from_the_quorum_(false)
{
    if (opt.raft_callback_) {
        ctx->set_cb_func(opt.raft_callback_);
    }

    ptr<raft_params> params = ctx_->get_params();
    if (params->stale_log_gap_ < params->fresh_log_gap_) {
        params->stale_log_gap_ = params->fresh_log_gap_;
    }
    if (params->enable_randomized_snapshot_creation_ &&
        !get_last_snapshot() &&
        params->snapshot_distance_ > 1) {
        uint64_t seed = timer_helper::get_timeofday_us() * id_;

        // Flip the integer.
        auto* first = reinterpret_cast<uint8_t*>(&seed);
        auto* last = reinterpret_cast<uint8_t*>(&seed) + sizeof(seed);
        while ((first != last) && (first != -- last)) std::swap(*first++, *last);

        std::default_random_engine engine(seed);
        std::uniform_int_distribution<int32>
            distribution( params->snapshot_distance_ / 2,
                          std::max( params->snapshot_distance_ / 2,
                                    params->snapshot_distance_ - 1 ) );

        first_snapshot_distance_ = distribution(engine);
        p_in("First snapshot creation log distance %u", first_snapshot_distance_);
    }

    apply_and_log_current_params();
    update_rand_timeout();
    precommit_index_ = log_store_->next_slot() - 1;
    lagging_sm_target_index_ = log_store_->next_slot() - 1;

    if (!state_) {
        state_ = cs_new<srv_state>();
        state_->set_term(0);
        state_->set_voted_for(-1);
    }
    vote_init_timer_term_ = state_->get_term();

    ptr<cluster_config> c_conf = get_config();
    std::stringstream init_msg;
    init_msg << "   === INIT RAFT SERVER ===\n"
             << "commit index " << sm_commit_index_ << "\n"
             << "term " << state_->get_term() << "\n"
             << "election timer " << ( state_->is_election_timer_allowed()
                                       ? "allowed" : "not allowed" ) << "\n"
             << "catching-up " << ( state_->is_catching_up()
                                    ? "yes" : "no" ) << "\n"
             << "log store start " << log_store_->start_index()
             << ", end " << log_store_->next_slot() - 1 << "\n"
             << "config log idx " << c_conf->get_log_idx()
             << ", prev log idx " << c_conf->get_prev_log_idx() << "\n";
    if (c_conf->is_async_replication()) {
        init_msg << " -- ASYNC REPLICATION --\n";
    }
    p_in("%s", init_msg.str().c_str());

    /**
     * I found this implementation is also a victim of bug
     *     https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
     * As the implementation is based on Diego's thesis
     *
     * Fix:
     * We should never load configurations that is not committed,
     * this prevents an old server from replicating an obsoleted config to
     * other servers.
     *
     * The prove is as below:
     * Assume S0 is the last committed server set for the old server A
     * |- EXITS Log l which has been committed but l !BELONGS TO A.logs
     *     =>  Vote(A) < Majority(S0)
     * In other words, we need to prove that A cannot be elected to leader
     * if any logs/configs has been committed.
     *
     * Case #1, There is no configuration change since S0, then it's
     *          obvious that
     *          Vote(A) < Majority(S0), see the core Algorithm.
     * Case #2, There are one or more configuration changes since S0,
     *          then at the time of first configuration change was
     *          committed, there are at least Majority(S0 - 1) servers
     *          committed the configuration change
     *          Majority(S0 - 1) + Majority(S0) > S0 => Vote(A) < Majority(S0)
     * -|
     */
    for ( ulong i = std::max( sm_commit_index_ + 1,
                              log_store_->start_index() );
          i < log_store_->next_slot();
          ++i ) {
        auto const entry = log_store_->entry_at(i);
        if (entry->get_val_type() == log_val_type::conf) {
            p_in( "detect a configuration change "
                  "that is not committed yet at index %" PRIu64 "", i );
            config_changing_ = true;
            break;
        }
    }

    std::stringstream peer_info_msg;
    auto srvs_end = c_conf->get_servers().end();
    for (auto it = c_conf->get_servers().begin(); it != srvs_end; ++it) {
        ptr<srv_config> cur_srv = *it;
        if (cur_srv->get_id() != id_) {
            timer_task<int32>::executor exec =
                (timer_task<int32>::executor)
                std::bind( &raft_server::handle_hb_timeout,
                           this,
                           std::placeholders::_1 );
            peers_.insert( std::make_pair
                           ( cur_srv->get_id(),
                             cs_new< peer,
                                     ptr<srv_config>&,
                                     context&,
                                     timer_task<int32>::executor&,
                                     ptr<logger>& >
                                   ( cur_srv, *ctx_, exec, l_ ) ) );
        } else {
            // Myself.
            im_learner_ = cur_srv->is_learner();
            my_priority_ = cur_srv->get_priority();
        }

        // WARNING:
        //   We should set the initial target priority to 1.
        //   When multiple nodes are gone at the same time and
        //   then re-started, and if some nodes cannot decay the
        //   target priority, we can never elect new leader.
        //
        //   (The reason why some nodes cannot decay is because
        //    request vote message resets election timer.)
        //
        // target_priority_ = std::max( target_priority_,
        //                              cur_srv->get_priority() );

        peer_info_msg
            << "peer " << cur_srv->get_id()
            << ": DC ID " << cur_srv->get_dc_id()
            << ", " << cur_srv->get_endpoint()
            << ", " << (cur_srv->is_learner() ? "learner" : "voting member")
            << ", " << cur_srv->get_priority()
            << std::endl;
    }

    peer_info_msg << "my id: " << id_
                  << ", " << ((im_learner_) ? "learner" : "voting_member")
                  << std::endl;
    peer_info_msg << "num peers: " << peers_.size() << std::endl;
    p_in("%s", peer_info_msg.str().c_str());

    if (opt.start_server_in_constructor_) {
        start_server(opt.skip_initial_election_timeout_);
    }
}

void raft_server::start_server(bool skip_initial_election_timeout)
{
    ptr<raft_params> params = ctx_->get_params();
    global_mgr* mgr = get_global_mgr();
    if (mgr) {
        p_in("global manager is detected. will use shared thread pool");
        commit_bg_stopped_ = true;
        append_bg_stopped_ = true;
        mgr->init_raft_server(this);

    } else {
        p_in("global manager does not exist. "
             "will use local thread for commit and append");
        bg_commit_thread_ = std::thread(std::bind(&raft_server::commit_in_bg, this));

        bg_append_ea_ = new EventAwaiter();
        bg_append_thread_ = std::thread(std::bind(&raft_server::append_entries_in_bg, this));
    }

    if (skip_initial_election_timeout) {
        // Issue #23:
        //   During remediation, the node (to be added) shouldn't be
        //   even a temp leader (to avoid local commit). We provide
        //   this option for that purpose.
        p_in("skip initialization of election timer by given parameter, "
             "waiting for the first heartbeat");
        // Make this status persistent, so as to make it not
        // trigger any election even after process restart.
        state_->allow_election_timer(false);
        ctx_->state_mgr_->save_state(*state_);

    } else if (!state_->is_election_timer_allowed()) {
        p_in("skip initialization of election timer by previously saved state, "
             "waiting for the first heartbeat");

    } else {
        p_in("wait for HB, for %d + [%d, %d] ms",
             params->rpc_failure_backoff_,
             params->election_timeout_lower_bound_,
             params->election_timeout_upper_bound_);
        std::this_thread::sleep_for( std::chrono::milliseconds
                                     (params->rpc_failure_backoff_) );
        restart_election_timer();
    }
    priority_change_timer_.reset();
    vote_init_timer_.set_duration_ms(params->grace_period_of_lagging_state_machine_);
    vote_init_timer_.reset();
    self_mark_down_ = excluded_from_the_quorum_ = stopping_ = false;
    p_db("server %d started", id_);
}

raft_server::~raft_server() {
    // For the case that user does not call shutdown() and directly
    // destroy the current `raft_server` instance.
    cancel_global_requests();

    recur_lock(lock_);
    stopping_ = true;
    std::unique_lock<std::mutex> commit_lock(commit_cv_lock_);
    commit_cv_.notify_all();
    std::unique_lock<std::mutex> lock(ready_to_stop_cv_lock_);
    commit_lock.unlock();
    commit_lock.release();
    ready_to_stop_cv_.wait_for( lock,
                                std::chrono::milliseconds(10),
                                [this](){ return commit_bg_stopped_.load(); } );
    cancel_schedulers();
    delete bg_append_ea_;
    delete ea_sm_commit_exec_in_progress_;
    delete ea_follower_log_append_;
}

void raft_server::update_rand_timeout() {
    ptr<raft_params> params = ctx_->get_params();
    uint seed = (uint)( std::chrono::system_clock::now()
                           .time_since_epoch().count() * id_ );
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<int32>
        distribution( params->election_timeout_lower_bound_,
                      params->election_timeout_upper_bound_ );
    rand_timeout_ = std::bind(distribution, engine);
    p_in("new timeout range: %d -- %d",
         params->election_timeout_lower_bound_,
         params->election_timeout_upper_bound_);
}

void raft_server::update_params(const raft_params& new_params) {
    recur_lock(lock_);

    ptr<raft_params> clone = cs_new<raft_params>(new_params);
    ctx_->set_params(clone);
    apply_and_log_current_params();

    update_rand_timeout();
    if (role_ != srv_role::leader) {
        restart_election_timer();
    }
    for (auto& entry: peers_) {
        peer* p = entry.second.get();
        auto_lock(p->get_lock());
        p->set_hb_interval(clone->heart_beat_interval_);
        p->resume_hb_speed();
    }
}

void raft_server::apply_and_log_current_params() {
    ptr<raft_params> params = ctx_->get_params();

    if (!test_mode_flag_) {
        if (params->heart_beat_interval_ >= params->election_timeout_lower_bound_) {
            params->election_timeout_lower_bound_ = params->heart_beat_interval_ * 2;
            p_wn("invalid election timeout lower bound detected, adjusted to %d",
                 params->election_timeout_lower_bound_);
        }
        if (params->election_timeout_lower_bound_
            >= params->election_timeout_upper_bound_) {
            params->election_timeout_upper_bound_ =
                params->election_timeout_lower_bound_ * 2;
            p_wn("invalid election timeout upper bound detected, adjusted to %d",
                 params->election_timeout_upper_bound_);
        }
    }

    p_in( "parameters: "
          "timeout %d - %d, heartbeat %d, "
          "leadership expiry %d, "
          "max batch %d, backoff %d, snapshot distance %d, "
          "enable randomized snapshot creation %s, "
          "log sync stop gap %d, "
          "use new joiner type %s, "
          "reserved logs %d, client timeout %d, "
          "auto forwarding %s, API call type %s, "
          "custom commit quorum size %d, "
          "custom election quorum size %d, "
          "snapshot receiver %s, "
          "leadership transfer wait time %d, "
          "grace period of lagging state machine %d, "
          "snapshot IO: %s, "
          "parallel log appending: %s, "
          "streaming mode max log gap %d, max bytes %" PRIu64 ", "
          "full consensus mode: %s",
          params->election_timeout_lower_bound_,
          params->election_timeout_upper_bound_,
          params->heart_beat_interval_,
          get_leadership_expiry(),
          params->max_append_size_,
          params->rpc_failure_backoff_,
          params->snapshot_distance_,
          params->enable_randomized_snapshot_creation_ ? "YES" : "NO",
          params->log_sync_stop_gap_,
          params->use_new_joiner_type_ ? "YES" : "NO",
          params->reserved_log_items_,
          params->client_req_timeout_,
          ( params->auto_forwarding_ ? "ON" : "OFF" ),
          ( params->return_method_ == raft_params::blocking
            ? "BLOCKING" : "ASYNC" ),
          params->custom_commit_quorum_size_,
          params->custom_election_quorum_size_,
          params->exclude_snp_receiver_from_quorum_ ? "EXCLUDED" : "INCLUDED",
          params->leadership_transfer_min_wait_time_,
          params->grace_period_of_lagging_state_machine_,
          params->use_bg_thread_for_snapshot_io_ ? "ASYNC" : "BLOCKING",
          params->parallel_log_appending_ ? "ON" : "OFF",
          params->max_log_gap_in_stream_,
          params->max_bytes_in_flight_in_stream_,
          params->use_full_consensus_among_healthy_members_ ? "ON" : "OFF" );

    status_check_timer_.set_duration_ms(params->heart_beat_interval_);
    status_check_timer_.reset();

    leadership_transfer_timer_.set_duration_ms
        (params->leadership_transfer_min_wait_time_);
}

raft_params raft_server::get_current_params() const {
    return *ctx_->get_params();
}

void raft_server::stop_server() {
    stopping_ = true;

    // Cancel all awaiting client requests.
    drop_all_pending_commit_elems();

    // Cancel all sm watchers.
    drop_all_sm_watcher_elems();
}

void raft_server::cancel_global_requests() {
    global_mgr* mgr = get_global_mgr();
    if (mgr) {
        mgr->close_raft_server(this);
    }
}

void raft_server::shutdown() {
    p_in("shutting down raft core");

    // If the global manager exists, cancel all pending requests.
    cancel_global_requests();

    // Cancel snapshot requests if exist.
    ptr<raft_params> params = ctx_->get_params();
    if (params->use_bg_thread_for_snapshot_io_) {
        snapshot_io_mgr::instance().drop_reqs(this);
    }

    // Terminate background commit thread.
    {   recur_lock(lock_);
        stopping_ = true;
        {   std::unique_lock<std::mutex> commit_lock(commit_cv_lock_);
            commit_cv_.notify_all();
        }
    }

    p_in("sent stop signal to the commit thread.");

    // Cancel all scheduler tasks.
    // TODO: how do we guarantee all tasks are done?
    cancel_schedulers();

    p_in("cancelled all schedulers.");

    // Wait until background commit thread terminates.
    while (!commit_bg_stopped_) {
        {   std::unique_lock<std::mutex> commit_lock(commit_cv_lock_);
            commit_cv_.notify_all();
        }
        std::this_thread::yield();
    }

    p_in("commit thread stopped.");

    drop_all_pending_commit_elems();

    p_in("all pending commit elements dropped.");

    drop_all_sm_watcher_elems();

    p_in("all state machine watchers dropped.");

    // Clear shared_ptrs that the current server is holding.
    {   std::lock_guard<std::mutex> l(ctx_->ctx_lock_);
        ctx_->logger_.reset();
        ctx_->rpc_listener_.reset();
        ctx_->rpc_cli_factory_.reset();
        ctx_->scheduler_.reset();
    }

    p_in("reset all pointers.");

    // Server to join/leave.
    if (srv_to_join_) {
        reset_srv_to_join();
    }
    if (srv_to_leave_) {
        reset_srv_to_leave();
    }

    // Wait for BG commit thread.
    if (bg_commit_thread_.joinable()) {
        bg_commit_thread_.join();
    }

    p_in("joined terminated commit thread.");

    while (bg_append_ea_ && !append_bg_stopped_) {
        bg_append_ea_->invoke();
        std::this_thread::yield();
    }

    p_in("sent stop signal to background append thread.");

    if (bg_append_thread_.joinable()) {
        bg_append_thread_.join();
    }

    {
        auto_lock(auto_fwd_reqs_lock_);
        p_in("clean up auto-forwarding queue: %zu elems", auto_fwd_reqs_.size());
        auto_fwd_reqs_.clear();
    }

    p_in("clean up auto-forwarding clients");
    cleanup_auto_fwd_pkgs();

    p_in("raft_server shutdown completed.");
}

bool raft_server::is_regular_member(const ptr<peer>& p) {
    // Skip to-be-removed server.
    if (srv_to_leave_ && srv_to_leave_->get_id() == p->get_id()) return false;

    // Skip learner.
    if (p->is_learner()) return false;

    // Skip new joiner.
    if (p->is_new_joiner()) return false;

    return true;
}

// Number of nodes that are able to vote, including leader itself.
int32 raft_server::get_num_voting_members() {
    int32 count = 0;
    for (auto& entry: peers_) {
        ptr<peer>& p = entry.second;
        auto_lock(p->get_lock());
        if (!is_regular_member(p)) continue;
        count++;
    }
    if (!im_learner_) count++;
    return count;
}

// NOTE: Below two functions return the size of quorum
//       EXCLUDING the leader.
//       e.g.) 7 nodes, quorum 4: return 3.
int32 raft_server::get_quorum_for_election() {
    ptr<raft_params> params = ctx_->get_params();
    int32 num_voting_members = get_num_voting_members();
    if ( params->custom_election_quorum_size_ <= 0 ||
         params->custom_election_quorum_size_ > num_voting_members ) {
        return num_voting_members / 2;
    }
    return params->custom_election_quorum_size_ - 1;
}

int32 raft_server::get_quorum_for_commit() {
    ptr<raft_params> params = ctx_->get_params();
    int32 num_voting_members = get_num_voting_members();

    if (params->exclude_snp_receiver_from_quorum_){
        // If the option is on, exclude any peer who is
        // receiving snapshot.
        for (auto& entry: peers_) {
            ptr<peer>& p = entry.second;
            if ( num_voting_members &&
                 p->get_snapshot_sync_ctx() ) {
                num_voting_members--;
            }
        }
    }

    if ( params->custom_commit_quorum_size_ <= 0 ||
         params->custom_commit_quorum_size_ > num_voting_members ) {
        return num_voting_members / 2;
    }
    return params->custom_commit_quorum_size_ - 1;
}

int32 raft_server::get_leadership_expiry() {
    ptr<raft_params> params = ctx_->get_params();
    int expiry = params->leadership_expiry_;
    if (expiry == 0) {
        // If 0, default expiry: 20x of heartbeat.
        return params->heart_beat_interval_ *
                 raft_server::raft_limits_.leadership_limit_;
    }
    return expiry;
}

std::list<ptr<peer>> raft_server::get_not_responding_peers(int expiry) {
    // Check if quorum nodes are not responding
    // (i.e., don't respond 20x heartbeat time long or expiry if sent as argument).
    // default argument for expiry is used in case user defines leadership_expiry_.
    ptr<raft_params> params = ctx_->get_params();
    if (expiry == 0) {
        expiry = params->heart_beat_interval_ * raft_server::raft_limits_.response_limit_;
    }

    std::list<ptr<peer>> rs;
    auto cb = [&rs, expiry](const ptr<peer>& peer_ptr, int32_t resp_elapsed_ms) {
        if (resp_elapsed_ms <= expiry) {
            // Response time is within the expiry time.
            return;
        }
        rs.push_back(peer_ptr);
    };
    for_each_voting_members(cb);
    return rs;
}

size_t raft_server::get_not_responding_peers_count(
    int expiry, uint64_t required_log_idx)
{
    // Check if quorum nodes are not responding
    // (i.e., don't respond 20x heartbeat time long or expiry if sent as argument).
    // default argument for expiry is used in case user defines leadership_expiry_.
    ptr<raft_params> params = ctx_->get_params();
    if (expiry == 0) {
        expiry = params->heart_beat_interval_ * raft_server::raft_limits_.response_limit_;
    }

    size_t num_not_resp_nodes = 0;
    auto cb = [&num_not_resp_nodes, required_log_idx, expiry]
        (const ptr<peer>& pp, int32_t resp_elapsed_ms)
    {
        bool non_responding_peer = false;
        if (resp_elapsed_ms <= expiry) {
            // Response time is within the expiry time.

            if (required_log_idx &&
                pp->get_matched_idx() &&
                pp->get_matched_idx() < required_log_idx) {
                // If the peer's matched index is less than the required log index,
                // it is considered as not responding for full consensus.
                //
                // WARNING: Should exclude matched_idx = 0,
                //          which means the peer has not responded yet right after
                //          the new connection.
                non_responding_peer = true;
            }

            if (pp->is_self_mark_down()) {
                // If the peer marks itself down, count it too.
                non_responding_peer = true;
            }

        } else {
            non_responding_peer = true;
        }

        if (non_responding_peer) {
            ++num_not_resp_nodes;
        }
    };
    for_each_voting_members(cb);
    return num_not_resp_nodes;
}

void raft_server::for_each_voting_members(
    const std::function<void(const ptr<peer>&, int32_t)>& callback) {

    // Check not responding peers.
    for (auto& entry: peers_) {
        const auto& peer_ptr = entry.second;

        if (!is_regular_member(peer_ptr)) continue;

        const auto resp_elapsed_ms =
            static_cast<int32>(peer_ptr->get_resp_timer_us() / 1000);
        callback(peer_ptr, resp_elapsed_ms);
    }
}

size_t raft_server::get_num_stale_peers() {
    // Check the number of peers lagging more than `stale_log_gap_`.
    if (leader_ != id_) return 0;

    size_t count = 0;
    for (auto& entry: peers_) {
        ptr<peer>& pp = entry.second;
        if ( get_last_log_idx() > pp->get_last_accepted_log_idx() +
                                  ctx_->get_params()->stale_log_gap_ ) {
            count++;
        }
    }
    return count;
}

ptr<resp_msg> raft_server::process_req(req_msg& req,
                                       const req_ext_params& ext_params) {
    cb_func::Param param(id_, leader_);
    param.ctx = &req;
    CbReturnCode rc = ctx_->cb_func_.call(cb_func::ProcessReq, &param);
    if (rc == CbReturnCode::ReturnNull) {
        p_wn("by callback, return null");
        return nullptr;
    }

    p_db( "Receive a %s message from %d with LastLogIndex=%" PRIu64 ", "
          "LastLogTerm %" PRIu64 ", EntriesLength=%zu, CommitIndex=%" PRIu64
          ", Term=%" PRIu64 ", flags=%" PRIx64 "",
          msg_type_to_string(req.get_type()).c_str(),
          req.get_src(),
          req.get_last_log_idx(),
          req.get_last_log_term(),
          req.log_entries().size(),
          req.get_commit_idx(),
          req.get_term(),
          req.get_extra_flags() );

    if (stopping_) {
        // Shutting down, ignore all incoming messages.
        p_wn("stopping, return null");
        return nullptr;
    }

    if ( req.get_type() == msg_type::client_request ) {
        // Client request doesn't need to go through below process.
        return handle_cli_req_prelock(req, ext_params);
    }

    recur_lock(lock_);
    if ( req.get_type() == msg_type::append_entries_request ||
         req.get_type() == msg_type::request_vote_request ||
         req.get_type() == msg_type::install_snapshot_request ) {
        // we allow the server to be continue after term updated to save a round message
        bool term_updated = update_term(req.get_term());

        if ( !im_learner_ &&
             !hb_alive_ &&
             term_updated &&
             req.get_type() == msg_type::request_vote_request ) {
            // If someone has newer term, that means leader has not been
            // elected in the current term, and this node's election timer
            // has been reset by this request.
            // We should decay the target priority here.
            decay_target_priority();
        }

        // Reset stepping down value to prevent this server goes down when leader
        // crashes after sending a LeaveClusterRequest
        if (steps_to_down_ > 0) {
            steps_to_down_ = 2;
        }
    }

    ptr<resp_msg> resp;
    if (req.get_type() == msg_type::append_entries_request) {
        {
            cb_func::Param param(id_, leader_, req.get_src(), &req);
            cb_func::ReturnCode rc =
                ctx_->cb_func_.call(cb_func::ReceivedAppendEntriesReq, &param);
            if (rc != cb_func::ReturnCode::Ok) {
                return nullptr;
            }
        }
        resp = handle_append_entries(req);
        {
            cb_func::Param param(id_, leader_, req.get_src(), resp.get());
            ctx_->cb_func_.call(cb_func::SentAppendEntriesResp, &param);
        }

    } else if (req.get_type() == msg_type::request_vote_request) {
        resp = handle_vote_req(req);

    } else if (req.get_type() == msg_type::pre_vote_request) {
        resp = handle_prevote_req(req);

    } else if (req.get_type() == msg_type::ping_request) {
        p_in("got ping from %d", req.get_src());
        resp = cs_new<resp_msg>( state_->get_term(),
                                 msg_type::ping_response,
                                 id_,
                                 req.get_src() );

    } else if (req.get_type() == msg_type::priority_change_request) {
        resp = handle_priority_change_req(req);
    } else {
        // extended requests
        resp = handle_ext_msg(req, guard);
    }

    if (resp) {
        p_db( "Response back a %s message to %d with Accepted=%d, "
              "Term=%" PRIu64 ", NextIndex=%" PRIu64 "",
              msg_type_to_string(resp->get_type()).c_str(),
              resp->get_dst(),
              resp->get_accepted() ? 1 : 0,
              resp->get_term(),
              resp->get_next_idx() );
    }

    return resp;
}

void raft_server::reset_peer_info() {
    ptr<cluster_config> c_config = get_config();
    auto const srv_cnt = c_config->get_servers().size();
    p_db("servers: %zu\n", srv_cnt);
    if (srv_cnt > 1) {
        ptr<srv_config> my_srv_config = c_config->get_server(id_);
        if (!my_srv_config) {
            // It means that this node was removed, and then
            // added again (it shouldn't happen though).
            // Just return.
            p_wn("my_srv_config is NULL");
            return;
        }

        ptr<cluster_config> my_next_config = cs_new<cluster_config>
            ( c_config->get_log_idx(), c_config->get_prev_log_idx() );
        my_next_config->get_servers().push_back(my_srv_config);
        my_next_config->set_user_ctx( c_config->get_user_ctx() );
        my_next_config->set_async_replication
                        ( c_config->is_async_replication() );

        set_config(my_next_config);
        ctx_->state_mgr_->save_config(*my_next_config);

        // Make its local temporary log for configuration.
        // It will be rolled back and overwritten if this node
        // re-joins the cluster.
        ptr<buffer> new_conf_buf(my_next_config->serialize());
        ptr<log_entry> entry(cs_new<log_entry>(
            state_->get_term(), new_conf_buf, log_val_type::conf,
            timer_helper::get_timeofday_us()));
        store_log_entry(entry, log_store_->next_slot()-1);
    }
}

void raft_server::handle_peer_resp(ptr<resp_msg>& resp, ptr<rpc_exception>& err) {
    recur_lock(lock_);
    if (err) {
        int32 peer_id = err->req()->get_dst();
        ptr<peer> pp = nullptr;
        auto entry = peers_.find(peer_id);
        if (entry != peers_.end()) pp = entry->second;

        int rpc_errs = 0;
        if (pp) {
            pp->inc_rpc_errs();
            rpc_errs = pp->get_rpc_errs();

            check_snapshot_timeout(pp);
        }

        if (rpc_errs < raft_server::raft_limits_.warning_limit_) {
            p_wn("peer (%d) response error: %s", peer_id, err->what());
        } else if (rpc_errs == raft_server::raft_limits_.warning_limit_) {
            p_wn("too verbose RPC error on peer (%d), "
                 "will suppress it from now", peer_id);
            if (!pp || !pp->is_lost()) {
                if (pp) {
                    pp->set_lost();
                }
                cb_func::Param param(id_, leader_, peer_id);
                const auto rc = ctx_->cb_func_.call(cb_func::FollowerLost, &param);
                (void)rc;
                assert(rc == cb_func::ReturnCode::Ok);
            }
        }

        if (pp && pp->is_leave_flag_set()) {
            // If this is to-be-removed server, proceed it without
            // waiting for the response.
            handle_join_leave_rpc_err(msg_type::leave_cluster_request, pp);
        }
        return;
    }

    if (!resp.get()) {
        p_wn("empty peer response");
        return;
    }

    p_db( "Receive a %s message from peer %d with "
          "Result=%d, Term=%" PRIu64 ", NextIndex=%" PRIu64 ", "
          "flags=%" PRIx64 "",
          msg_type_to_string(resp->get_type()).c_str(),
          resp->get_src(),
          resp->get_accepted() ? 1 : 0,
          resp->get_term(),
          resp->get_next_idx(),
          resp->get_extra_flags() );

    p_tr("src: %d, dst: %d, resp->get_term(): %d\n",
         (int)resp->get_src(), (int)resp->get_dst(), (int)resp->get_term());

    if (resp->get_accepted()) {
        // On accepted response, reset response timer.
        auto entry = peers_.find(resp->get_src());
        if (entry != peers_.end()) {
            peer* pp = entry->second.get();
            int rpc_errs = pp->get_rpc_errs();
            if (rpc_errs >= raft_server::raft_limits_.warning_limit_) {
                p_wn("recovered from RPC failure from peer %d, %d errors",
                     resp->get_src(), rpc_errs);
            }
            pp->set_recovered();
            pp->reset_rpc_errs();
            pp->reset_resp_timer();
        }
    }

    if ( is_valid_msg(resp->get_type()) ) {
        bool update_term_succ = update_term(resp->get_term());

        // if term is updated, no more action is required
        if (update_term_succ) return;
    }

    // ignore the response that with lower term for safety
    switch (resp->get_type())
    {
    case msg_type::pre_vote_response:
        handle_prevote_resp(*resp);
        break;

    case msg_type::request_vote_response:
        handle_vote_resp(*resp);
        break;

    case msg_type::append_entries_response:
        {
            cb_func::Param param(id_, leader_, resp->get_src(), resp.get());
            ctx_->cb_func_.call(cb_func::ReceivedAppendEntriesResp, &param);
        }
        handle_append_entries_resp(*resp);
        break;

    case msg_type::install_snapshot_response:
        handle_install_snapshot_resp(*resp);
        break;

    case msg_type::priority_change_response:
        handle_priority_change_resp(*resp);
        break;

    case msg_type::ping_response:
        p_in("got ping response from %d", resp->get_src());
        break;

    case msg_type::custom_notification_response:
        handle_custom_notification_resp(*resp);
        break;

    default:
        p_er( "received an unexpected response: %s, ignore it",
              msg_type_to_string(resp->get_type()).c_str() );
        break;
    }
}

void raft_server::send_reconnect_request() {
    recur_lock(lock_);

    if (leader_ == id_) {
        p_er("this node %d is leader, "
             "cannot send reconnect request",
             id_);
        return;
    }

    // Find leader object.
    auto entry = peers_.find(leader_);
    if (entry != peers_.end()) {
        ptr<peer> p_leader = entry->second;
        ptr<req_msg> req = cs_new<req_msg>( state_->get_term(),
                                            msg_type::reconnect_request,
                                            id_,
                                            leader_,
                                            0, 0, 0 );

        if (p_leader->make_busy()) {
            p_leader->send_req(p_leader, req, ex_resp_handler_);
        } else {
            p_er("previous message to leader %d hasn't been responded yet",
                 p_leader->get_id());
        }

    } else {
        // LCOV_EXCL_START
        p_ft("cannot find leader!");
        ctx_->state_mgr_->system_exit(N22_unrecoverable_isolation);
        // LCOV_EXCL_STOP
    }
}

ptr<resp_msg> raft_server::handle_reconnect_req(req_msg& req) {
    int32 srv_id = req.get_src();
    ptr<resp_msg> resp( cs_new<resp_msg>
                        ( state_->get_term(),
                          msg_type::reconnect_response,
                          id_,
                          srv_id ) );
    if (role_ != srv_role::leader) {
        p_er("this node is not a leader "
             "(upon re-connect req from peer %d)",
             srv_id);
        return resp;
    }

    auto entry = peers_.find(srv_id);
    if (entry == peers_.end()) {
        p_er("cannot find peer %d to re-connect", srv_id);
        return resp;
    }

    // Schedule re-connection.
    ptr<peer> pp = entry->second;
    pp->schedule_reconnection();
    resp->accept(log_store_->next_slot());
    p_in("re-connection to peer %d scheduled", srv_id);

    return resp;
}

void raft_server::handle_reconnect_resp(resp_msg& resp) {
    p_in("got re-connection scheduling response "
         "from leader %d to my id %d, result %s",
         resp.get_src(), resp.get_dst(),
         resp.get_accepted() ? "accepted" : "rejected");
}

bool raft_server::reconnect_client(peer& p) {
    if (stopping_) return false;

    ptr<cluster_config> c_config = get_config();
    ptr<srv_config> s_config = c_config->get_server(p.get_id());

    // NOTE: To-be-removed server will not exist in config,
    //       but we still need to reconnect to it if we can
    //       send the latest config to the server.
    if (!s_config && p.is_leave_flag_set()) {
        s_config = srv_config::deserialize( *p.get_config().serialize() );
    }

    if (s_config) {
        p_db( "reset RPC client for peer %d",
              p.get_id() );
        return p.recreate_rpc(s_config, *ctx_);
    }
    return false;
}

void raft_server::become_leader() {
    stop_election_timer();

    {   auto_lock(commit_ret_elems_lock_);
        p_in("number of pending commit elements: %zu",
             commit_ret_elems_.size());
    }

    ptr<raft_params> params = ctx_->get_params();
    {   recur_lock(cli_lock_);
        role_ = srv_role::leader;
        leader_ = id_;
        self_mark_down_ = false;
        srv_to_join_.reset();
        leadership_transfer_timer_.set_duration_ms
            (params->leadership_transfer_min_wait_time_);
        leadership_transfer_timer_.reset();
        precommit_index_ = log_store_->next_slot() - 1;
        p_in("state machine commit index %" PRIu64 ", "
             "precommit index %" PRIu64 ", last log index %" PRIu64,
             sm_commit_index_.load(),
             precommit_index_.load(),
             log_store_->next_slot() - 1);
        ptr<snapshot> nil_snp;
        for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
            ptr<peer> pp = it->second;
            clear_snapshot_sync_ctx(*pp);
            // Reset RPC client for all peers.
            // NOTE: Now we don't reset client, as we already did it
            //       during pre-vote phase.
            // NOTE: In the case that this peer takeover the leadership,
            //       connection will be re-used
            // reconnect_client(*pp);

            pp->set_next_log_idx(log_store_->next_slot());
            pp->reset_stream();
            enable_hb_for_peer(*pp);
            pp->set_recovered();
            pp->set_snapshot_sync_is_needed(false);
            if (params->use_full_consensus_among_healthy_members_) {
                // We should reset response timer here
                // so as not to disrupt full consensus.
                pp->reset_resp_timer();
            }
        }

        // If there are uncommitted logs, search if conf log exists.
        ptr<cluster_config> last_config = get_config();

        ulong s_idx = sm_commit_index_ + 1;
        ulong e_idx = log_store_->next_slot();
        for (ulong ii = s_idx; ii < e_idx; ++ii) {
            ptr<log_entry> le = log_store_->entry_at(ii);
            if (le->get_val_type() != log_val_type::conf) continue;

            p_in("found uncommitted config at %" PRIu64 ", size %zu",
                 ii, le->get_buf().size());
            last_config = cluster_config::deserialize(le->get_buf());
        }

        // WARNING: WE SHOULD NOT CHANGE THE ORIGINAL CONTENTS DIRECTLY!
        ptr<cluster_config> last_config_cloned =
            cluster_config::deserialize( *last_config->serialize() );
        last_config_cloned->set_log_idx(log_store_->next_slot());
        ptr<buffer> conf_buf = last_config_cloned->serialize();
        ptr<log_entry> entry
            ( cs_new<log_entry>
              ( state_->get_term(),
                conf_buf,
                log_val_type::conf,
                timer_helper::get_timeofday_us() ) );
        index_at_becoming_leader_ = store_log_entry(entry);
        p_in("[BECOME LEADER] appended new config at %" PRIu64,
             index_at_becoming_leader_.load());
        config_changing_ = true;
    }

    cb_func::Param param(id_, leader_);
    ulong my_term = state_->get_term();
    param.ctx = &my_term;
    CbReturnCode rc = ctx_->cb_func_.call(cb_func::BecomeLeader, &param);
    (void)rc; // nothing to do in this callback.

    write_paused_ = false;
    next_leader_candidate_ = -1;
    initialized_ = true;
    pre_vote_.quorum_reject_count_ = 0;
    pre_vote_.no_response_failure_count_ = 0;
    data_fresh_ = true;

    request_append_entries();

    if (my_priority_ == 0 && get_num_voting_members() > 1) {
        // If this member's priority is zero, this node owns a temporary
        // leadership. Let other node takeover shortly.
        p_in("[BECOME LEADER] my priority is 0, will resign shortly");
        yield_leadership();
    }
}

bool raft_server::check_leadership_validity() {
    recur_lock(lock_);

    if (role_ != leader)
        return false;

    // Check if quorum is not responding.
    int32 num_voting_members = get_num_voting_members();


    int leadership_expiry = get_leadership_expiry();

    int32 nr_peers{0};

    // Negative expiry: leadership will never expire.
    if (leadership_expiry >= 0) {
        nr_peers = (int32)get_not_responding_peers_count(leadership_expiry);
    }

    int32 min_quorum_size = get_quorum_for_commit() + 1;
    if ( (num_voting_members - nr_peers) < min_quorum_size ) {
        p_er("%d nodes (out of %d, %zu including learners) are not "
             "responding longer than %d ms, "
             "at least %d nodes (including leader) should be alive "
             "to proceed commit",
             nr_peers,
             num_voting_members,
             peers_.size() + 1,
             get_leadership_expiry(),
             min_quorum_size);

        const auto nr_peers_list = get_not_responding_peers();

        // NOTE:
        //   `nr_peers` and `nr_peers_list.size()` may not be the same,
        //   since it is based on timer.
        // assert(nr_peers_list.size() == static_cast<std::size_t>(nr_peers));
        for (auto& peer : nr_peers_list) {
            if (peer->is_lost()) {
                continue;
            }
            peer->set_lost();
            cb_func::Param param(id_, leader_, peer->get_id());
            const auto rc = ctx_->cb_func_.call(cb_func::FollowerLost, &param);
            (void)rc;
            assert(rc == cb_func::ReturnCode::Ok);
        }

        // NOTE:
        //   For a cluster where the number of members is the same
        //   as the size of quorum, we should not expire leadership,
        //   since it will block the cluster doing any further actions.
        if (num_voting_members <= min_quorum_size) {
            p_wn("we cannot yield the leadership of this small cluster");
            return true;
        }

        p_er("will yield the leadership of this node");
        yield_leadership(true);
        return false;
    }
    return true;
}

void raft_server::check_leadership_transfer() {
    ptr<raft_params> params = ctx_->get_params();
    if (!params->leadership_transfer_min_wait_time_) {
        // Transferring leadership is disabled.
        p_tr("leadership transfer is disabled");
        return;
    }
    if (!leadership_transfer_timer_.timeout()) {
        // Leadership period is too short.
        p_tr("leadership period is too short: %zu ms",
             leadership_transfer_timer_.get_duration_us() / 1000);
        return;
    }

    size_t election_lower = ctx_->get_params()->election_timeout_lower_bound_;

    recur_lock(lock_);

    int32 successor_id = -1;
    int32 max_priority = my_priority_;
    ulong cur_commit_idx = quick_commit_index_;
    for (auto& entry: peers_) {
        ptr<peer> peer_elem = entry.second;
        const srv_config& s_conf = peer_elem->get_config();
        int32 cur_priority = s_conf.get_priority();
        if (cur_priority > max_priority) {
            max_priority = cur_priority;
            successor_id = s_conf.get_id();
        }

        if (peer_elem->get_matched_idx() + params->stale_log_gap_ <
                cur_commit_idx) {
            // This peer is lagging behind.
            p_tr("peer %d is lagging behind, %" PRIu64 " < %" PRIu64,
                 s_conf.get_id(), peer_elem->get_matched_idx(),
                 cur_commit_idx);
            return;
        }

        uint64_t last_resp_ms = peer_elem->get_resp_timer_us() / 1000;
        if (last_resp_ms > election_lower) {
            // This replica is not responding.
            p_tr("peer %d is not responding, %" PRIu64 " ms ago",
                 s_conf.get_id(), last_resp_ms);
            return;
        }
    }

    if (my_priority_ >= max_priority || successor_id == -1) {
        // This leader already has the highest priority.
        p_tr("my priority %d is already the highest", my_priority_);
        return;
    }

    if (!state_machine_->allow_leadership_transfer()) {
        // Although all conditions are met,
        // user does not want to transfer the leadership.
        p_tr("state machine does not allow leadership transfer");
        return;
    }

    p_in( "going to transfer leadership to %d, "
          "my priority %d, max priority %d, "
          "has been leader for %" PRIu64 " sec",
          successor_id, my_priority_, max_priority,
          leadership_transfer_timer_.get_sec() );
    yield_leadership(false, successor_id);
}

void raft_server::yield_leadership(bool immediate_yield,
                                   int successor_id)
{
    // Leader reelection is already happening.
    if (write_paused_) return;

    // Not a leader, do nothing.
    if (id_ != leader_) return;

    // This node is the only node, do nothing.
    if (get_num_voting_members() <= 1) {
        p_er("this node is the only node in the cluster, will do nothing");
        return;
    }

    // Callback if necessary.
    cb_func::Param param(id_, leader_, successor_id, nullptr);
    cb_func::ReturnCode cb_ret =
        ctx_->cb_func_.call(cb_func::ResignationFromLeader, &param);

    // If callback function decided to refuse this request, return here.
    if (cb_ret != cb_func::Ok) {
        p_in("[RESIGNATION REQUEST] refused by callback function");
        return;
    }

    recur_lock(lock_);

    if (immediate_yield) {
        p_in("got immediate re-elect request, resign now");
        leader_ = -1;
        become_follower();
        // Clear live flag to avoid pre-vote rejection.
        hb_alive_ = false;
        return;
    }

    // Not immediate yield, nominate the successor.
    int max_priority = 0;
    int candidate_id = -1;
    uint64_t last_resp_ms = 0;
    std::string candidate_endpoint;
    size_t hb_interval_ms = ctx_->get_params()->heart_beat_interval_;
    if (successor_id >= 0) {
        // If successor is given, find that one.
        auto entry = peers_.find(successor_id);
        if (entry != peers_.end()) {
            int32 srv_id = entry->first;
            ptr<peer>& pp = entry->second;
            max_priority = pp->get_config().get_priority();
            candidate_id = srv_id;
            candidate_endpoint = pp->get_config().get_endpoint();
            last_resp_ms = pp->get_resp_timer_us() / 1000;
        }
    }

    // Successor is not given or the given successor is incorrect,
    // find the highest priority node whose response time is not expired.
    if (candidate_id == -1) {
        for (auto& entry: peers_) {
            int32 srv_id = entry.first;
            ptr<peer>& pp = entry.second;
            uint64_t pp_last_resp_ms = pp->get_resp_timer_us() / 1000;

            if ( srv_id != id_ &&
                 pp_last_resp_ms <= hb_interval_ms &&
                 pp->get_config().get_priority() > max_priority ) {
                max_priority = pp->get_config().get_priority();
                candidate_id = srv_id;
                candidate_endpoint = pp->get_config().get_endpoint();
                last_resp_ms = pp_last_resp_ms;
            }
        }
    }

    if (successor_id >= 0) {
        p_in("got graceful re-elect request (designated successor %d), "
             "pause write from now",
             successor_id);

        if ( successor_id >= 0 &&
             successor_id != candidate_id ) {
            p_wn("could not find given successor %d", successor_id);
        }

    } else {
        p_in("got graceful re-elect request, pause write from now");
    }

    if (candidate_id > -1) {
        p_in("next leader candidate: id %d endpoint %s priority %d "
             "last response %" PRIu64 " ms ago",
             candidate_id, candidate_endpoint.c_str(), max_priority,
             last_resp_ms);
        next_leader_candidate_ = candidate_id;;
    } else {
        p_wn("cannot find valid candidate for next leader, will proceed anyway");
    }

    // Reset reelection timer, and pause write.
    write_paused_ = true;

    // Wait until election timeout upper bound.
    reelection_timer_.set_duration_ms
                      ( ctx_->get_params()->election_timeout_upper_bound_ );
    reelection_timer_.reset();
}

bool raft_server::request_leadership() {
    // If this server is already a leader, do nothing.
    if (id_ == leader_ || is_leader()) {
        p_er("cannot request leadership: this server is already a leader");
        return false;
    }
    if (leader_ == -1) {
        p_er("cannot request leadership: cannot find leader");
        return false;
    }

    recur_lock(lock_);
    auto entry = peers_.find(leader_);
    if (entry == peers_.end()) {
        p_er("cannot request leadership: cannot find peer for "
             "leader id %d", leader_.load());
        return false;
    }
    ptr<peer> pp = entry->second;

    // Send resignation message to the follower.
    ptr<req_msg> req = cs_new<req_msg>
                       ( state_->get_term(),
                         msg_type::custom_notification_request,
                         id_, leader_,
                         term_for_log(log_store_->next_slot() - 1),
                         log_store_->next_slot() - 1,
                         quick_commit_index_.load() );

    // Create a notification.
    ptr<custom_notification_msg> custom_noti =
        cs_new<custom_notification_msg>
        ( custom_notification_msg::request_resignation );

    // Wrap it using log_entry.
    ptr<log_entry> custom_noti_le =
        cs_new<log_entry>(0, custom_noti->serialize(), log_val_type::custom);

    req->log_entries().push_back(custom_noti_le);
    pp->send_req(pp, req, resp_handler_);
    p_in("sent leadership request to leader %d", leader_.load());
    return true;
}

void raft_server::become_follower() {
    // stop hb for all peers
    p_in("[BECOME FOLLOWER] term %" PRIu64 "", state_->get_term());
    {   std::lock_guard<std::recursive_mutex> ll(cli_lock_);
        for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
            it->second->enable_hb(false);
            it->second->reset_stream();
        }

        srv_to_join_.reset();
        role_ = srv_role::follower;
        index_at_becoming_leader_ = 0;

        cb_func::Param param(id_, leader_);
        uint64_t my_term = state_->get_term();
        param.ctx = &my_term;
        (void) ctx_->cb_func_.call(cb_func::BecomeFollower, &param);

        write_paused_ = false;
        next_leader_candidate_ = -1;
        initialized_ = true;
        uncommitted_config_.reset();
        pre_vote_.quorum_reject_count_ = 0;
        pre_vote_.no_response_failure_count_ = 0;

        ptr<raft_params> params = ctx_->get_params();
        if ( params->auto_adjust_quorum_for_small_cluster_ &&
             peers_.size() == 1 &&
             params->custom_commit_quorum_size_ == 1 ) {
            p_wn("became 2-node cluster's follower, "
                 "restore quorum with default value");
            ptr<raft_params> clone = cs_new<raft_params>(*params);
            clone->custom_commit_quorum_size_ = 0;
            clone->custom_election_quorum_size_ = 0;
            ctx_->set_params(clone);
        }

        // Drain all pending callback functions.
        drop_all_pending_commit_elems();

        // NOTE: sm watchers are not reset here, as state machine commit can be
        //       executed regardless of the role.
    }

    restart_election_timer();
}

bool raft_server::update_term(ulong term) {
    if (term > state_->get_term()) {
        {
            // NOTE:
            //   There could be a race between `update_term` (let's say T1) and
            //   `handle_cli_req` (let's say T2) as follows:
            //
            //     * The server was a leader at term X
            //     [T1] call `state_->set_term(Y)`
            //     [T2] call `state_->get_term()`
            //     [T2] write a log with term Y (which should be X).
            //     [T1] call `become_follower()`
            //          => now this server becomes a follower at term Y,
            //             but it still has the incorrect log with term Y.
            //
            //   To avoid this issue, we acquire `cli_lock_`,
            //   and change `role_` first before setting the term.
            std::lock_guard<std::recursive_mutex> ll(cli_lock_);
            role_ = srv_role::follower;
            state_->set_term(term);
        }
        state_->set_voted_for(-1);
        state_->allow_election_timer(true);
        election_completed_ = false;
        votes_granted_ = 0;
        votes_responded_ = 0;
        ctx_->state_mgr_->save_state(*state_);
        become_follower();
        return true;
    }
    return false;
}

ptr<resp_msg> raft_server::handle_ext_msg(req_msg& req, std::unique_lock<std::recursive_mutex>& guard) {
    switch (req.get_type()) {
    case msg_type::add_server_request:
        return handle_add_srv_req(req);

    case msg_type::remove_server_request:
        return handle_rm_srv_req(req);

    case msg_type::sync_log_request:
        return handle_log_sync_req(req);

    case msg_type::join_cluster_request:
        return handle_join_cluster_req(req);

    case msg_type::leave_cluster_request:
        return handle_leave_cluster_req(req);

    case msg_type::install_snapshot_request:
        return handle_install_snapshot_req(req, guard);

    case msg_type::reconnect_request:
        return handle_reconnect_req(req);

    case msg_type::custom_notification_request:
        return handle_custom_notification_req(req);

    default:
        p_er( "received request: %s, ignore it",
              msg_type_to_string(req.get_type()).c_str() );
        break;
    }

    return ptr<resp_msg>();
}

void raft_server::handle_ext_resp(ptr<resp_msg>& resp, ptr<rpc_exception>& err) {
    recur_lock(lock_);
    if (err) {
        handle_ext_resp_err(*err);
        return;
    }
    p_db("type: %d, err %p\n", (int)resp->get_type(), err.get());

    p_db( "Receive an extended %s message from peer %d with Result=%d, "
          "Term=%" PRIu64 ", NextIndex=%" PRIu64 "",
          msg_type_to_string(resp->get_type()).c_str(),
          resp->get_src(),
          resp->get_accepted() ? 1 : 0,
          resp->get_term(),
          resp->get_next_idx() );

    switch (resp->get_type())
    {
    case msg_type::sync_log_response:
        handle_log_sync_resp(*resp);
        break;

    case msg_type::join_cluster_response:
        handle_join_cluster_resp(*resp);
        break;

    case msg_type::leave_cluster_response:
        handle_leave_cluster_resp(*resp);
        break;

    case msg_type::install_snapshot_response:
        handle_install_snapshot_resp_new_member(*resp);
        break;

    case msg_type::reconnect_response:
        handle_reconnect_resp(*resp);
        break;

    default:
        p_er( "received an unexpected response message type %s",
              msg_type_to_string(resp->get_type()).c_str() );
        break;
    }
}

void raft_server::handle_ext_resp_err(rpc_exception& err) {
    ptr<req_msg> req = err.req();
    p_in( "receive an rpc error response from peer server, %s %d",
          err.what(), req->get_type() );

    if ( req->get_type() == msg_type::install_snapshot_request ) {
        if (srv_to_join_ && srv_to_join_->get_id() == req->get_dst()) {
            bool timed_out = check_snapshot_timeout(srv_to_join_);
            if (!timed_out) {
                // Enable temp HB to retry snapshot.
                p_wn("sending snapshot to joining server %d failed, "
                     "retry with temp heartbeat", srv_to_join_->get_id());
                srv_to_join_snp_retry_required_ = true;
                enable_hb_for_peer(*srv_to_join_);
            }
        }
    }

    if ( req->get_type() != msg_type::sync_log_request     &&
         req->get_type() != msg_type::join_cluster_request &&
         req->get_type() != msg_type::leave_cluster_request ) {
        return;
    }

    ptr<peer> p;
    msg_type t_msg = req->get_type();
    int32 peer_id = req->get_dst();
    if (t_msg == msg_type::leave_cluster_request) {
        peer_itor pit = peers_.find(peer_id);
        if (pit != peers_.end()) {
            p = pit->second;
        }
    } else {
        p = srv_to_join_;
    }
    if (!p) return;

    if (p->get_current_hb_interval() >= ctx_->get_params()->max_hb_interval()) {
        handle_join_leave_rpc_err(t_msg, p);

    } else {
        // reuse the heartbeat interval value to indicate
        // when to stop retrying, as rpc backoff is the same.
        p_db("retry the request");
        p->slow_down_hb();
        timer_task<void>::executor exec =
            (timer_task<void>::executor)
            std::bind( &raft_server::on_retryable_req_err, this, p, req );
        ptr<delayed_task> task(cs_new<timer_task<void>>(exec));
        schedule_task(task, p->get_current_hb_interval());
    }
}

void raft_server::on_retryable_req_err(ptr<peer>& p, ptr<req_msg>& req) {
    p_db( "retry the request %s for %d",
          msg_type_to_string(req->get_type()).c_str(), p->get_id() );
    if (p->make_busy()) {
        p->send_req(p, req, ex_resp_handler_);
    } else {
        p_er("retry request %d failed: peer %d is busy",
             req->get_type(), p->get_id());
    }
}

ulong raft_server::term_for_log(ulong log_idx) {
    if (log_idx == 0) {
        return 0L;
    }

    if (log_idx >= log_store_->start_index()) {
        return log_store_->term_at(log_idx);
    }

    ptr<snapshot> last_snapshot(state_machine_->last_snapshot());
    if ( !last_snapshot || log_idx != last_snapshot->get_last_log_idx() ) {
        static timer_helper bad_log_timer(1000000, true);
        int log_lv = bad_log_timer.timeout_and_reset() ? L_ERROR : L_TRACE;

        p_lv(log_lv, "bad log_idx %" PRIu64 " for retrieving the term value, "
             "will ignore this log req", log_idx);
        if (last_snapshot) {
            p_lv(log_lv, "last snapshot %p, log_idx %" PRIu64
                 ", snapshot last_log_idx %" PRIu64 "\n",
                 last_snapshot.get(), log_idx, last_snapshot->get_last_log_idx());
        }
        p_lv(log_lv, "log_store_->start_index() %" PRIu64, log_store_->start_index());
        //ctx_->state_mgr_->system_exit(raft_err::N19_bad_log_idx_for_term);
        //_sys_exit(-1);
        return 0L;
    }

    return last_snapshot->get_last_log_term();
}

void raft_server::set_user_ctx(const std::string& ctx) {
    // Clone current cluster config.
    ptr<cluster_config> c_conf = get_config();
    ptr<buffer> enc_conf = c_conf->serialize();
    ptr<cluster_config> cloned_config = cluster_config::deserialize(*enc_conf);

    // Create a log for new configuration, it should be replicated.
    cloned_config->set_log_idx(log_store_->next_slot());
    cloned_config->set_user_ctx(ctx);

    ptr<buffer> new_conf_buf = cloned_config->serialize();
    ptr<log_entry> entry = cs_new<log_entry>
                           ( state_->get_term(),
                             new_conf_buf,
                             log_val_type::conf,
                             timer_helper::get_timeofday_us() );
    store_log_entry(entry);
    request_append_entries();
}

std::string raft_server::get_user_ctx() const {
    ptr<cluster_config> c_conf = get_config();
    return c_conf->get_user_ctx();
}

int32 raft_server::get_snapshot_sync_ctx_timeout() const {
    if (ctx_->get_params()->snapshot_sync_ctx_timeout_ == 0) {
        return raft_limits_.response_limit_ * ctx_->get_params()->heart_beat_interval_;
    }
    return ctx_->get_params()->snapshot_sync_ctx_timeout_;
}

int32 raft_server::get_dc_id(int32 srv_id) const {
    ptr<cluster_config> c_conf = get_config();
    ptr<srv_config> s_conf = c_conf->get_server(srv_id);
    if (!s_conf) return -1; // Not found.

    return s_conf->get_dc_id();
}

std::string raft_server::get_aux(int32 srv_id) const {
    ptr<cluster_config> c_conf = get_config();
    ptr<srv_config> s_conf = c_conf->get_server(srv_id);
    if (!s_conf) return std::string();

    return s_conf->get_aux();
}

ptr<srv_config> raft_server::get_srv_config(int32 srv_id) const {
    ptr<cluster_config> c_conf = get_config();
    return c_conf->get_server(srv_id);
}

void raft_server::get_srv_config_all
     ( std::vector< ptr<srv_config> >& configs_out ) const
{
    ptr<cluster_config> c_conf = get_config();
    auto& servers = c_conf->get_servers();
    for (auto& entry: servers) configs_out.push_back(entry);
}

bool raft_server::update_srv_config(const srv_config& new_config) {
    if (!is_leader()) {
        p_er("cannot update server config: this node is not a leader");
        return false;
    }

    // Clone current cluster config.
    ptr<cluster_config> c_conf = get_config();
    ptr<buffer> enc_conf = c_conf->serialize();
    ptr<cluster_config> cloned_config = cluster_config::deserialize(*enc_conf);

    // Create a log for new configuration, it should be replicated.
    cloned_config->set_log_idx(log_store_->next_slot());

    // If server ID does not exist, do nothing.
    ptr<srv_config> prev_srv_config = cloned_config->get_server(new_config.get_id());

    if (!prev_srv_config) {
        p_er("cannot update server config: server %d does not exist",
             new_config.get_id());
        return false;
    }

    if (prev_srv_config->is_new_joiner()) {
        // If this server is a new joiner, we cannot update it.
        p_er("cannot update server config: server %d is a new joiner",
             new_config.get_id());
        return false;
    }

    if (prev_srv_config->get_endpoint() != new_config.get_endpoint()) {
        // Endpoint change will not be accepted.
        p_er("cannot update server config: endpoint change is not allowed, "
             "server %d endpoint %s -> %s",
             new_config.get_id(),
             prev_srv_config->get_endpoint().c_str(),
             new_config.get_endpoint().c_str());
        return false;
    }

    auto enc_new_config = new_config.serialize();
    auto& servers = cloned_config->get_servers();
    for (auto& entry: servers) {
        if (entry->get_id() == new_config.get_id()) {
            // Update the server config.
            entry = srv_config::deserialize(*enc_new_config);
            break;
        }
    }

    ptr<buffer> new_conf_buf = cloned_config->serialize();
    ptr<log_entry> entry = cs_new<log_entry>
                           ( state_->get_term(),
                             new_conf_buf,
                             log_val_type::conf,
                             timer_helper::get_timeofday_us() );
    store_log_entry(entry);
    request_append_entries();

    p_in("appended new server config for server %d",
         new_config.get_id());

    return true;
}

raft_server::peer_info raft_server::get_peer_info(int32 srv_id) const {
    if (!is_leader()) return peer_info();

    recur_lock(lock_);
    auto entry = peers_.find(srv_id);
    if (entry == peers_.end()) return peer_info();

    peer_info ret;
    ptr<peer> pp = entry->second;
    ret.id_ = pp->get_id();
    ret.last_log_idx_ = pp->get_last_accepted_log_idx();
    ret.last_succ_resp_us_ = pp->get_resp_timer_us();
    return ret;
}

std::vector<raft_server::peer_info> raft_server::get_peer_info_all() const {
    std::vector<raft_server::peer_info> ret;
    if (!is_leader()) return ret;

    recur_lock(lock_);
    for (auto entry: peers_) {
        peer_info pi;
        ptr<peer> pp = entry.second;
        pi.id_ = pp->get_id();
        pi.last_log_idx_ = pp->get_last_accepted_log_idx();
        pi.last_succ_resp_us_ = pp->get_resp_timer_us();
        ret.push_back(pi);
    }
    return ret;
}

ptr<cluster_config> raft_server::get_config() const {
    std::lock_guard<std::mutex> l(config_lock_);
    ptr<cluster_config> ret = config_;
    return ret;
}

void raft_server::set_config(const ptr<cluster_config>& new_config) {
    std::lock_guard<std::mutex> l(config_lock_);
    stale_config_ = config_;
    config_ = new_config;
}

ptr<snapshot> raft_server::get_last_snapshot() const {
    std::lock_guard<std::mutex> l(last_snapshot_lock_);
    ptr<snapshot> ret = last_snapshot_;
    return ret;
}

void raft_server::set_last_snapshot(const ptr<snapshot>& new_snapshot) {
    std::lock_guard<std::mutex> l(last_snapshot_lock_);
    last_snapshot_ = new_snapshot;
}

ulong raft_server::store_log_entry(ptr<log_entry>& entry, ulong index) {
    ulong log_index = index;
    if (index == 0) {
        log_index = log_store_->append(entry);
    } else {
        log_store_->write_at(log_index, entry);
    }

    if ( entry->get_val_type() == log_val_type::conf ) {
        // Force persistence of config_change logs to guarantee the durability of
        // cluster membership change log entries.  Losing cluster membership log
        // entries may lead to split brain.
        if ( !log_store_->flush() ) {
            // LCOV_EXCL_START
            p_ft("log store flush failed");
            ctx_->state_mgr_->system_exit(N21_log_flush_failed);
            // LCOV_EXCL_STOP
        }

        if ( role_ == srv_role::leader ) {
            // WARNING:
            // Configuration changes, such as adding or removing a member,
            // can run concurrently with normal log appending operations. This
            // concurrency can lead to an inversion issue between precommit and
            // commit orders, particularly when there is only one member in the
            // cluster.
            //
            // Let's consider the following scenario: T1 is the thread handling
            // log appending, T2 is the thread processing configuration changes,
            // and T3 is the commit thread.
            // The initial precommit and commit index is 10.
            //
            // [T1] Acquires `cli_lock_` and enters `handle_cli_req()`.
            // [T1] Appends a log at index 11 by calling `store_log_entry()`.
            // [T2] Appends a log at index 12 by calling `store_log_entry()`.
            // [T2] Calls `try_update_precommit_index()`,
            //      updating the precommit index to 12.
            // [T2] Calls `request_append_entries()` and `commit()`,
            //      updating the commit index to 12.
            // [T3] Calls `state_machine::commit()` for logs 11 and 12.
            // [T1] Calls `state_machine::pre_commit()` for log 11.
            //      => order inversion happens here.
            //
            // To prevent this inversion, T2 should acquire the same `cli_lock_`
            // before calling `try_update_precommit_index()`. This ensures that T2
            // cannot update the precommit index between T1's `store_log_entry()`
            // and `state_machine::pre_commit()` calls, maintaining the correct
            // order of operations.
            recur_lock(cli_lock_);

            // Need to progress precommit index for config.
            try_update_precommit_index(log_index);
        }
    }

    return log_index;
}

CbReturnCode raft_server::invoke_callback( cb_func::Type type,
                                           cb_func::Param* param )
{
    CbReturnCode rc = ctx_->cb_func_.call(type, param);
    return rc;
}

void raft_server::set_inc_term_func(srv_state::inc_term_func func) {
    recur_lock(lock_);
    if (!state_) return;
    state_->set_inc_term_func(func);
}

raft_server::limits raft_server::get_raft_limits() {
    return raft_limits_;
}

void raft_server::set_raft_limits(const raft_server::limits& new_limits) {
    raft_limits_ = new_limits;
}

void raft_server::check_overall_status() {
    check_leadership_transfer();
}

global_mgr* raft_server::get_global_mgr() const {
    if (ctx_->custom_global_mgr_ != nullptr) {
        return ctx_->custom_global_mgr_;
    }
    return nuraft_global_mgr::get_instance();
}

bool raft_server::set_self_mark_down(bool to) {
    if (is_leader()) {
        p_er("cannot set self mark down to %s: "
             "this node is a leader",
             to ? "true" : "false");
        return self_mark_down_;
    }

    bool old = self_mark_down_;
    self_mark_down_ = to;
    p_in("self mark down set from %s to %s",
         old ? "true" : "false",
         self_mark_down_.load() ? "true" : "false");
    return old;
}

bool raft_server::is_part_of_full_consensus() {
    if (self_mark_down_ ||
        excluded_from_the_quorum_ ||
        !initialized_ ||
        stopping_) {
        return false;
    }
    const auto& params = get_current_params();

    if (is_leader()) {
        // If it is a leader, check if responding peers are enough
        // to form a full consensus.
        recur_lock(lock_);
        int32_t num_voting_members = get_num_voting_members();
        int32_t nr_peers = (int32_t)get_not_responding_peers_count(
            params.heart_beat_interval_ * raft_limits_.full_consensus_follower_limit_);
        int32_t min_quorum_size = get_quorum_for_commit() + 1;
        if (num_voting_members - nr_peers < min_quorum_size) {
            // It means this leader couldn't reach a majority of the cluster members
            // for the duration of follower's markdown interval.
            //
            // In that case, there can be a chance that a new leader has been elected.
            // But that new leader may not reach a full consensus yet,
            // because leader's markdown interval is longer than
            // follower's markdown interval.
            return false;
        }

        // Valid leadership, we are part of full consensus.
        return true;
    }

    // Follower.
    if (last_rcvd_append_entries_req_.get_ms() >
            (uint64_t)params.heart_beat_interval_ *
            raft_limits_.full_consensus_follower_limit_) {
        // If we have not received any append entries request for
        // the configured time, we are not part of the full consensus.
        return false;
    }
    return true;
}

} // namespace nuraft;
