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
#include "global_mgr.hxx"
#include "handle_client_request.hxx"
#include "handle_custom_notification.hxx"
#include "peer.hxx"
#include "snapshot.hxx"
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

struct auto_destroyer {
    ~auto_destroyer() {
        stat_mgr::destroy();
        nuraft_global_mgr::shutdown();
    }
};
static auto_destroyer auto_destroyer_;

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
    , initial_commit_index_(ctx->state_machine_->last_commit_index())
    , hb_alive_(false)
    , election_completed_(true)
    , config_changing_(false)
    , catching_up_(false)
    , out_of_log_range_(false)
    , data_fresh_(false)
    , stopping_(false)
    , commit_bg_stopped_(false)
    , append_bg_stopped_(false)
    , write_paused_(false)
    , next_leader_candidate_(-1)
    , im_learner_(false)
    , serving_req_(false)
    , steps_to_down_(0)
    , snp_in_progress_(false)
    , ctx_(ctx)
    , scheduler_(ctx->scheduler_)
    , election_exec_(std::bind(&raft_server::handle_election_timeout, this))
    , election_task_(nullptr)
    , role_(srv_role::follower)
    , state_(ctx->state_mgr_->read_state())
    , log_store_(ctx->state_mgr_->load_log_store())
    , state_machine_(ctx->state_machine_)
    , receiving_snapshot_(false)
    , et_cnt_receiving_snapshot_(0)
    , l_(ctx->logger_)
    , stale_config_(nullptr)
    , config_(ctx->state_mgr_->load_config())
    , uncommitted_config_(nullptr)
    , srv_to_join_(nullptr)
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
{
    char temp_buf[4096];
    std::string print_msg;

    if (opt.raft_callback_) {
        ctx->set_cb_func(opt.raft_callback_);
    }

    ptr<raft_params> params = ctx_->get_params();
    if (params->stale_log_gap_ < params->fresh_log_gap_) {
        params->stale_log_gap_ = params->fresh_log_gap_;
    }

    apply_and_log_current_params();
    update_rand_timeout();
    precommit_index_ = log_store_->next_slot() - 1;

    if (!state_) {
        state_ = cs_new<srv_state>();
        state_->set_term(0);
        state_->set_voted_for(-1);
    }

    print_msg.clear();

    ptr<cluster_config> c_conf = get_config();
    std::stringstream ss;
    ss << "   === INIT RAFT SERVER ===\n"
       << "commit index " << sm_commit_index_ << "\n"
       << "term " << state_->get_term() << "\n"
       << "election timer " << ( state_->is_election_timer_allowed()
                                 ? "allowed" : "not allowed" ) << "\n"
       << "log store start " << log_store_->start_index()
       << ", end " << log_store_->next_slot() - 1 << "\n"
       << "config log idx " << c_conf->get_log_idx()
       << ", prev log idx " << c_conf->get_prev_log_idx() << "\n";
    if (c_conf->is_async_replication()) {
        ss << " -- ASYNC REPLICATION --\n";
    }
    print_msg = ss.str();
    p_in("%s", print_msg.c_str());

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
        ptr<log_entry> entry(log_store_->entry_at(i));
        if (entry->get_val_type() == log_val_type::conf) {
            p_in( "detect a configuration change "
                  "that is not committed yet at index %llu", i );
            config_changing_ = true;
            break;
        }
    }

    print_msg.clear();
    std::list< ptr<srv_config> >& srvs = c_conf->get_servers();
    for (cluster_config::srv_itor it = srvs.begin(); it != srvs.end(); ++it) {
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

        sprintf( temp_buf,
                 "peer %d: DC ID %d, %s, %s, %d\n",
                 (int)cur_srv->get_id(),
                 (int)cur_srv->get_dc_id(),
                 cur_srv->get_endpoint().c_str(),
                 cur_srv->is_learner() ? "learner" : "voting member",
                 cur_srv->get_priority() );
        print_msg += temp_buf;
    }

    sprintf(temp_buf, "my id: %d, %s\n",
            id_, (im_learner_)?"learner":"voting_member");
    print_msg += temp_buf;
    sprintf(temp_buf, "num peers: %d\n", (int)peers_.size());
    print_msg += temp_buf;
    p_in(print_msg.c_str());

    nuraft_global_mgr* mgr = nuraft_global_mgr::get_instance();
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

    if (opt.skip_initial_election_timeout_) {
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
    ready_to_stop_cv_.wait_for(lock, std::chrono::milliseconds(10));
    cancel_schedulers();
    delete bg_append_ea_;
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
        p->set_hb_interval(clone->heart_beat_interval_);
        p->resume_hb_speed();
    }
}

void raft_server::apply_and_log_current_params() {
    ptr<raft_params> params = ctx_->get_params();
    p_in( "parameters: "
          "timeout %d - %d, heartbeat %d, "
          "leadership expiry %d, "
          "max batch %d, backoff %d, snapshot distance %d, "
          "log sync stop gap %d, "
          "reserved logs %d, client timeout %d, "
          "auto forwarding %s, API call type %s, "
          "custom commit quorum size %d, "
          "custom election quorum size %d, "
          "snapshot receiver %s",
          params->election_timeout_lower_bound_,
          params->election_timeout_upper_bound_,
          params->heart_beat_interval_,
          get_leadership_expiry(),
          params->max_append_size_,
          params->rpc_failure_backoff_,
          params->snapshot_distance_,
          params->log_sync_stop_gap_,
          params->reserved_log_items_,
          params->client_req_timeout_,
          ( params->auto_forwarding_ ? "ON" : "OFF" ),
          ( params->return_method_ == raft_params::blocking
            ? "BLOCKING" : "ASYNC" ),
          params->custom_commit_quorum_size_,
          params->custom_election_quorum_size_,
          params->exclude_snp_receiver_from_quorum_ ? "EXCLUDED" : "INCLUDED");

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
}

void raft_server::cancel_global_requests() {
    nuraft_global_mgr* mgr = nuraft_global_mgr::get_instance();
    if (mgr) {
        mgr->close_raft_server(this);
    }
}

void raft_server::shutdown() {
    p_in("shutting down raft core");

    // If the global manager exists, cancel all pending requests.
    cancel_global_requests();

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

    p_in("raft_server shutdown completed.");
}

bool raft_server::is_regular_member(const ptr<peer>& p) {
    // Skip to-be-removed server.
    if (srv_to_leave_ && srv_to_leave_->get_id() == p->get_id()) return false;

    // Skip learner.
    if (p->is_learner()) return false;

    return true;
}

// Number of nodes that are able to vote, including leader itself.
int32 raft_server::get_num_voting_members() {
    int32 count = 0;
    for (auto& entry: peers_) {
        ptr<peer>& p = entry.second;
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
        expiry = params->heart_beat_interval_ *
                     raft_server::raft_limits_.leadership_limit_;
    }
    return expiry;
}

size_t raft_server::get_not_responding_peers() {
    // Check if quorum nodes are not responding
    // (i.e., don't respond 20x heartbeat time long).
    size_t num_not_resp_nodes = 0;

    int expiry = get_leadership_expiry();
    if (expiry < 0) {
        // Negative expiry, leadership will never be expired.
        return 0;
    }

    // Check the number of not responding peers.
    for (auto& entry: peers_) {
        ptr<peer> p = entry.second;

        if (!is_regular_member(p)) continue;

        int32 resp_elapsed_ms = (int32)(p->get_resp_timer_us() / 1000);
        if ( resp_elapsed_ms > expiry ) {
            num_not_resp_nodes++;
        }
    }
    return num_not_resp_nodes;
}

size_t raft_server::get_num_stale_peers() {
    // Check the number of peers lagging more than `stale_log_gap_`.
    if (leader_ != id_) return 0;

    size_t count = 0;
    for (auto& entry: peers_) {
        ptr<peer>& pp = entry.second;
        if ( get_last_log_idx() > pp->get_matched_idx() +
                                  ctx_->get_params()->stale_log_gap_ ) {
            count++;
        }
    }
    return count;
}

ptr<resp_msg> raft_server::process_req(req_msg& req) {
    cb_func::Param param(id_, leader_);
    param.ctx = &req;
    CbReturnCode rc = ctx_->cb_func_.call(cb_func::ProcessReq, &param);
    if (rc == CbReturnCode::ReturnNull) {
        p_wn("by callback, return null");
        return nullptr;
    }

    p_db( "Receive a %s message from %d with LastLogIndex=%llu, "
          "LastLogTerm=%llu, EntriesLength=%d, CommitIndex=%llu and Term=%llu",
          msg_type_to_string(req.get_type()).c_str(),
          req.get_src(),
          req.get_last_log_idx(),
          req.get_last_log_term(),
          req.log_entries().size(),
          req.get_commit_idx(),
          req.get_term() );

    if (stopping_) {
        // Shutting down, ignore all incoming messages.
        p_wn("stopping, return null");
        return nullptr;
    }

    if ( req.get_type() == msg_type::client_request ) {
        // Client request doesn't need to go through below process.
        return handle_cli_req_prelock(req);
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
        resp = handle_append_entries(req);

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
        resp = handle_ext_msg(req);
    }

    if (resp) {
        p_db( "Response back a %s message to %d with Accepted=%d, "
              "Term=%llu, NextIndex=%llu",
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
    p_db("servers: %zu\n", c_config->get_servers().size());
    if (c_config->get_servers().size() > 1) {
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
                state_->get_term(), new_conf_buf, log_val_type::conf));
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
        }

        if (rpc_errs < raft_server::raft_limits_.warning_limit_) {
            p_wn("peer (%d) response error: %s", peer_id, err->what());
        } else if (rpc_errs == raft_server::raft_limits_.warning_limit_) {
            p_wn("too verbose RPC error on peer (%d), "
                 "will suppress it from now", peer_id);
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
          "Result=%d, Term=%llu, NextIndex=%llu",
          msg_type_to_string(resp->get_type()).c_str(),
          resp->get_src(),
          resp->get_accepted() ? 1 : 0,
          resp->get_term(),
          resp->get_next_idx() );

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

void raft_server::reconnect_client(peer& p) {
    if (stopping_) return;

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
        p.recreate_rpc(s_config, *ctx_);
        p.set_free();
        p.set_manual_free();
    }
}

void raft_server::become_leader() {
    stop_election_timer();

    {   auto_lock(commit_ret_elems_lock_);
        p_in("number of pending commit elements: %zu",
             commit_ret_elems_.size());
    }

    ptr<raft_params> params = ctx_->get_params();
    {   auto_lock(cli_lock_);
        role_ = srv_role::leader;
        leader_ = id_;
        srv_to_join_.reset();
        leadership_transfer_timer_.set_duration_ms
            (params->leadership_transfer_min_wait_time_);
        leadership_transfer_timer_.reset();
        precommit_index_ = log_store_->next_slot() - 1;
        p_in("state machine commit index %zu, "
             "precommit index %zu, last log index %zu",
             sm_commit_index_.load(),
             precommit_index_.load(),
             log_store_->next_slot() - 1);
        ptr<snapshot> nil_snp;
        for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
            ptr<peer> pp = it->second;
            ptr<snapshot_sync_ctx> sync_ctx = pp->get_snapshot_sync_ctx();
            if (sync_ctx) {
                void*& user_ctx = sync_ctx->get_user_snp_ctx();
                state_machine_->free_user_snp_ctx(user_ctx);
                pp->set_snapshot_in_sync(nil_snp);
            }
            // Reset RPC client for all peers.
            // NOTE: Now we don't reset client, as we already did it
            //       during pre-vote phase.
            // reconnect_client(*pp);

            pp->set_next_log_idx(log_store_->next_slot());
            enable_hb_for_peer(*pp);
        }

        // If there are uncommitted logs, search if conf log exists.
        ptr<cluster_config> last_config = get_config();

        ulong s_idx = sm_commit_index_ + 1;
        ulong e_idx = log_store_->next_slot();
        for (ulong ii = s_idx; ii < e_idx; ++ii) {
            ptr<log_entry> le = log_store_->entry_at(ii);
            if (le->get_val_type() != log_val_type::conf) continue;

            p_in("found uncommitted config at %zu, size %zu",
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
              ( state_->get_term(), conf_buf, log_val_type::conf ) );
        p_in("[BECOME LEADER] appended new config at %d\n", log_store_->next_slot());
        store_log_entry(entry);
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
    pre_vote_.failure_count_ = 0;
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

    // Check if quorum is not responding.
    int32 num_voting_members = get_num_voting_members();
    int32 nr_peers = (int32)get_not_responding_peers();
    int32 min_quorum_size = get_quorum_for_commit() + 1;
    if ( (num_voting_members - nr_peers) < min_quorum_size ) {
        p_er("%zu nodes (out of %zu, %zu including learners) are not "
             "responding longer than %zu ms, "
             "at least %zu nodes (including leader) should be alive "
             "to proceed commit",
             nr_peers,
             num_voting_members,
             peers_.size() + 1,
             get_leadership_expiry(),
             min_quorum_size);

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
        return;
    }
    if (!leadership_transfer_timer_.timeout()) {
        // Leadership period is too short.
        return;
    }

    size_t hb_interval_ms = ctx_->get_params()->heart_beat_interval_;

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
            return;
        }

        uint64_t last_resp_ms = peer_elem->get_resp_timer_us() / 1000;
        if (last_resp_ms > hb_interval_ms) {
            // This replica is not responding.
            return;
        }
    }

    if (my_priority_ >= max_priority || successor_id == -1) {
        // This leader already has the highest priority.
        return;
    }

    if (!state_machine_->allow_leadership_transfer()) {
        // Although all conditions are met,
        // user does not want to transfer the leadership.
        return;
    }

    p_in( "going to transfer leadership to %d, "
          "my priority %d, max priority %d, "
          "has been leader for %zu sec",
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
             "last response %zu ms ago",
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
    p_tr("  FOLLOWER\n");
    {   std::lock_guard<std::mutex> ll(cli_lock_);
        for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
            it->second->enable_hb(false);
        }

        srv_to_join_.reset();
        role_ = srv_role::follower;

        cb_func::Param param(id_, leader_);
        (void) ctx_->cb_func_.call(cb_func::BecomeFollower, &param);

        write_paused_ = false;
        next_leader_candidate_ = -1;
        initialized_ = true;
        uncommitted_config_.reset();
        pre_vote_.quorum_reject_count_ = 0;
        pre_vote_.failure_count_ = 0;

        // Drain all pending callback functions.
        drop_all_pending_commit_elems();
    }

    restart_election_timer();
}

bool raft_server::update_term(ulong term) {
    if (term > state_->get_term()) {
        state_->set_term(term);
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

ptr<resp_msg> raft_server::handle_ext_msg(req_msg& req) {
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
        return handle_install_snapshot_req(req);

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
          "Term=%llu, NextIndex=%llu",
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
        p_er("bad log_idx %llu for retrieving the term value, "
             "will ignore this log req", log_idx);
        if (last_snapshot) {
            p_er("last snapshot %p, log_idx %llu, snapshot last_log_idx %llu\n",
                 last_snapshot.get(), log_idx, last_snapshot->get_last_log_idx());
        }
        p_er("log_store_->start_index() %ld\n", log_store_->start_index());
        //ctx_->state_mgr_->system_exit(raft_err::N19_bad_log_idx_for_term);
        //::exit(-1);
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
                             log_val_type::conf);
    store_log_entry(entry);
    request_append_entries();
}

std::string raft_server::get_user_ctx() const {
    ptr<cluster_config> c_conf = get_config();
    return c_conf->get_user_ctx();
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
    std::list< ptr<srv_config> >& servers = c_conf->get_servers();
    for (auto& entry: servers) configs_out.push_back(entry);
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

} // namespace nuraft;

