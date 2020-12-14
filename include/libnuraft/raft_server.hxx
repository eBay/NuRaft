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

#ifndef _RAFT_SERVER_HXX_
#define _RAFT_SERVER_HXX_

#include "async.hxx"
#include "callback.hxx"
#include "internal_timer.hxx"
#include "log_store.hxx"
#include "snapshot_sync_req.hxx"
#include "rpc_cli.hxx"
#include "srv_config.hxx"
#include "srv_role.hxx"
#include "srv_state.hxx"
#include "timer_task.hxx"

#include <map>
#include <string>
#include <unordered_map>

class EventAwaiter;

namespace nuraft {

using CbReturnCode = cb_func::ReturnCode;

class cluster_config;
class custom_notification_msg;
class delayed_task_scheduler;
class logger;
class peer;
class rpc_client;
class req_msg;
class resp_msg;
class rpc_exception;
class state_machine;
class state_mgr;
struct context;
struct raft_params;
class raft_server : public std::enable_shared_from_this<raft_server> {
    friend class nuraft_global_mgr;
public:
    struct init_options {
        init_options()
            : skip_initial_election_timeout_(false)
            {}

        /**
         * If `true`, the election timer will not be initiated
         * automatically, so that this node will never trigger
         * leader election until it gets the first heartbeat
         * from any valid leader.
         *
         * Purpose: to avoid becoming leader when there is only one
         *          node in the cluster.
         */
        bool skip_initial_election_timeout_;

        /**
         * Callback function for hooking the operation.
         */
        cb_func::func_type raft_callback_;
    };

    struct limits {
        limits()
            : pre_vote_rejection_limit_(20)
            , warning_limit_(20)
            , response_limit_(20)
            , leadership_limit_(20)
            , reconnect_limit_(50)
            , leave_limit_(5)
            , vote_limit_(5)
            {}

        limits(const limits& src) {
            *this = src;
        }

        limits& operator=(const limits& src) {
            pre_vote_rejection_limit_ = src.pre_vote_rejection_limit_.load();
            warning_limit_ = src.warning_limit_.load();
            response_limit_ = src.response_limit_.load();
            leadership_limit_  = src.leadership_limit_.load();
            reconnect_limit_ = src.reconnect_limit_.load();
            leave_limit_ = src.leave_limit_.load();
            vote_limit_ = src.vote_limit_.load();
            return *this;
        }

        /**
         * If pre-vote rejection count is greater than this limit,
         * Raft will re-establish the network connection;
         */
        std::atomic<int32> pre_vote_rejection_limit_;

        /**
         * Max number of warnings before suppressing it.
         */
        std::atomic<int32> warning_limit_;

        /**
         * If a node is not responding more than this limit,
         * we treat that node as dead.
         */
        std::atomic<int32> response_limit_;

        /**
         * Default value of leadership expiration
         * (multiplied by heartbeat interval).
         */
        std::atomic<int32> leadership_limit_;

        /**
         * If connection is silent longer than this limit
         * (multiplied by heartbeat interval), we re-establish
         * the connection.
         */
        std::atomic<int32> reconnect_limit_;

        /**
         * If removed node is not responding more than this limit,
         * just force remove it from server list.
         */
        std::atomic<int32> leave_limit_;

        /**
         * For 2-node cluster, if the other peer is not responding for
         * pre-vote more than this limit, adjust quorum size.
         * Active only when `auto_adjust_quorum_for_small_cluster_` is enabled.
         */
        std::atomic<int32> vote_limit_;
    };

    raft_server(context* ctx, const init_options& opt = init_options());

    virtual ~raft_server();

    __nocopy__(raft_server);

public:
    /**
     * Process Raft request.
     *
     * @param req Request.
     * @return Response.
     */
    virtual ptr<resp_msg> process_req(req_msg& req);

    /**
     * Check if this server is ready to serve operation.
     *
     * @return `true` if it is ready.
     */
    bool is_initialized() const { return initialized_; }

    /**
     * Check if this server is catching up the current leader
     * to join the cluster.
     *
     * @return `true` if it is in catch-up mode.
     */
    bool is_catching_up() const { return catching_up_; }

    /**
     * Check if this server is receiving snapshot from leader.
     *
     * @return `true` if it is receiving snapshot.
     */
    bool is_receiving_snapshot() const { return receiving_snapshot_; }

    /**
     * Add a new server to the current cluster.
     * Only leader will accept this operation.
     * Note that this is an asynchronous task so that needs more network
     * communications. Returning this function does not guarantee
     * adding the server.
     *
     * @param srv Configuration of server to add.
     * @return `get_accepted()` will be true on success.
     */
    ptr< cmd_result< ptr<buffer> > >
        add_srv(const srv_config& srv);

    /**
     * Remove a server from the current cluster.
     * Only leader will accept this operation.
     * The same as `add_srv`, this is also an asynchronous task.
     *
     * @param srv_id ID of server to remove.
     * @return `get_accepted()` will be true on success.
     */
    ptr< cmd_result< ptr<buffer> > >
        remove_srv(const int srv_id);

    /**
     * Append and replicate the given logs.
     * Only leader will accept this operation.
     *
     * @param logs Set of logs to replicate.
     * @return
     *     In blocking mode, it will be blocked during replication, and
     *     return `cmd_result` instance which contains the commit results from
     *     the state machine.
     *     In async mode, this function will return immediately, and the
     *     commit results will be set to returned `cmd_result` instance later.
     */
    ptr< cmd_result< ptr<buffer> > >
        append_entries(const std::vector< ptr<buffer> >& logs);

    /**
     * Update the priority of given server.
     * Only leader will accept this operation.
     *
     * @param srv_id ID of server to update priority.
     * @param new_priority
     *     Priority value, greater than or equal to 0.
     *     If priority is set to 0, this server will never be a leader.
     */
    void set_priority(const int srv_id, const int new_priority);

    /**
     * Broadcast the priority change of given server to all peers.
     * This function should be used only when there is no live leader
     * and leader election is blocked by priorities of live followers.
     * In that case, we are not able to change priority by using
     * normal `set_priority` operation.
     *
     * @param srv_id ID of server to update priority.
     * @param new_priority New priority.
     */
    void broadcast_priority_change(const int srv_id,
                                   const int new_priority);

    /**
     * Yield current leadership and becomes a follower. Only a leader
     * will accept this operation.
     *
     * If given `immediate_yield` flag is `true`, it will become a
     * follower immediately. The subsequent leader election will be
     * totally random so that there is always a chance that this
     * server becomes the next leader again.
     *
     * Otherwise, this server will pause write operations first, wait
     * until the successor (except for this server) finishes the
     * catch-up of the latest log, and then resign. In such a case,
     * the next leader will be much more predictable.
     *
     * Users can designate the successor. If not given, this API will
     * automatically choose the highest priority server as a successor.
     *
     * @param immediate_yield If `true`, yield immediately.
     * @param successor_id The server ID of the successor.
     *                     If `-1`, the successor will be chosen
     *                     automatically.
     */
    void yield_leadership(bool immediate_yield = false,
                          int successor_id = -1);

    /**
     * Send a request to the current leader to yield its leadership,
     * and become the next leader.
     *
     * @return `true` on success. But it does not guarantee to become
     *         the next leader due to various failures.
     */
    bool request_leadership();

    /**
     * Start the election timer on this server, if this server is a follower.
     * It will allow the election timer permanently, if it was disabled
     * by state manager.
     */
    void restart_election_timer();

    /**
     * Set custom context to Raft cluster config.
     *
     * @param ctx Custom context.
     */
    void set_user_ctx(const std::string& ctx);

    /**
     * Get custom context from the current cluster config.
     *
     * @return Custom context.
     */
    std::string get_user_ctx() const;

    /**
     * Get ID of this server.
     *
     * @return Server ID.
     */
    int32 get_id() const
    { return id_; }

    /**
     * Get the current term of this server.
     *
     * @return Term.
     */
    ulong get_term() const
    { return state_->get_term(); }

    /**
     * Get the term of given log index number.
     *
     * @param log_idx Log index number
     * @return Term of given log.
     */
    ulong get_log_term(ulong log_idx) const
    { return log_store_->term_at(log_idx); }

    /**
     * Get the term of the last log.
     *
     * @return Term of the last log.
     */
    ulong get_last_log_term() const
    { return log_store_->term_at(get_last_log_idx()); }

    /**
     * Get the last log index number.
     *
     * @return Last log index number.
     */
    ulong get_last_log_idx() const
    { return log_store_->next_slot() - 1; }

    /**
     * Get the last committed log index number of state machine.
     *
     * @return Last committed log index number of state machine.
     */
    ulong get_committed_log_idx() const
    { return sm_commit_index_.load(); }

    /**
     * Get the target log index number we are required to commit.
     *
     * @return Target committed log index number.
     */
    ulong get_target_committed_log_idx() const
    { return quick_commit_index_.load(); }

    /**
     * Get the leader's last committed log index number.
     *
     * @return The leader's last committed log index number.
     */
    ulong get_leader_committed_log_idx() const
    { return is_leader() ? get_committed_log_idx() : leader_commit_index_.load(); }

    /**
     * Calculate the log index to be committed
     * from current peers' matched indexes.
     *
     * @return Expected committed log index.
     */
    ulong get_expected_committed_log_idx();

    /**
     * Get the current Raft cluster config.
     *
     * @return Cluster config.
     */
    ptr<cluster_config> get_config() const;

    /**
     * Get log store instance.
     *
     * @return Log store instance.
     */
    ptr<log_store> get_log_store() const { return log_store_; }

    /**
     * Get data center ID of the given server.
     *
     * @param srv_id Server ID.
     * @return -1 if given server ID does not exist.
     *          0 if data center ID was not assigned.
     */
    int32 get_dc_id(int32 srv_id) const;

    /**
     * Get auxiliary context stored in the server config.
     *
     * @param srv_id Server ID.
     * @return Auxiliary context.
     */
    std::string get_aux(int32 srv_id) const ;

    /**
     * Get the ID of current leader.
     *
     * @return Leader ID
     *         -1 if there is no live leader.
     */
    int32 get_leader() const {
        // We should handle the case when `role_` is already
        // updated, but `leader_` value is stale.
        if ( leader_ == id_ &&
             role_ != srv_role::leader ) return -1;
        return leader_;
    }

    /**
     * Check if this server is leader.
     *
     * @return `true` if it is leader.
     */
    bool is_leader() const {
        if ( leader_ == id_ &&
             role_ == srv_role::leader ) return true;
        return false;
    }

    /**
     * Check if there is live leader in the current cluster.
     *
     * @return `true` if live leader exists.
     */
    bool is_leader_alive() const {
        if ( leader_ == -1 || !hb_alive_ ) return false;
        return true;
    }

    /**
     * Get the configuration of given server.
     *
     * @param srv_id Server ID.
     * @return Server configuration.
     */
    ptr<srv_config> get_srv_config(int32 srv_id) const;

    /**
     * Get the configuration of all servers.
     *
     * @param[out] configs_out Set of server configurations.
     */
    void get_srv_config_all(std::vector< ptr<srv_config> >& configs_out) const;

    /**
     * Shut down server instance.
     */
    void shutdown();

    /**
     * Stop background commit thread.
     */
    void stop_server();

    /**
     * Send reconnect request to leader.
     * Leader will re-establish the connection to this server in a few seconds.
     * Only follower will accept this operation.
     */
    void send_reconnect_request();

    /**
     * Update Raft parameters.
     *
     * @param new_params Parameters to set.
     */
    void update_params(const raft_params& new_params);

    /**
     * Get the current Raft parameters.
     * Returned instance is the clone of the original one,
     * so that user can modify its contents.
     *
     * @return Clone of Raft parameters.
     */
    raft_params get_current_params() const;

    /**
     * Get the counter number of given stat name.
     *
     * @param name Stat name to retrieve.
     * @return Counter value.
     */
    static uint64_t get_stat_counter(const std::string& name);

    /**
     * Get the gauge number of given stat name.
     *
     * @param name Stat name to retrieve.
     * @return Gauge value.
     */
    static int64_t get_stat_gauge(const std::string& name);

    /**
     * Get the histogram of given stat name.
     *
     * @param name Stat name to retrieve.
     * @param[out] histogram_out
     *     Histogram as a map. Key is the upper bound of a bucket, and
     *     value is the counter of that bucket.
     * @return `true` on success.
     *         `false` if stat does not exist, or is not histogram type.
     */
    static bool get_stat_histogram(const std::string& name,
                                   std::map<double, uint64_t>& histogram_out);

    /**
     * Reset given stat to zero.
     *
     * @param name Stat name to reset.
     */
    static void reset_stat(const std::string& name);

    /**
     * Reset all existing stats to zero.
     */
    static void reset_all_stats();

    /**
     * Apply a log entry containing configuration change, while Raft
     * server is not running.
     * This API is only for recovery purpose, and user should
     * make sure that when Raft server starts, the last committed
     * index should be equal to or bigger than the index number of
     * the last configuration log entry applied.
     *
     * @param le Log entry containing configuration change.
     * @param s_mgr State manager instance.
     * @param err_msg Will contain a message if error happens.
     * @return `true` on success.
     */
    static bool apply_config_log_entry(ptr<log_entry>& le,
                                       ptr<state_mgr>& s_mgr,
                                       std::string& err_msg);

    /**
     * Get the current Raft limit values.
     *
     * @return Raft limit values.
     */
    static limits get_raft_limits();

    /**
     * Update the Raft limits with given values.
     *
     * @param new_limits New values to set.
     */
    static void set_raft_limits(const limits& new_limits);

    /**
     * Invoke internal callback function given by user,
     * with given type and parameters.
     *
     * @param type Callback event type.
     * @param param Parameters.
     * @return cb_func::ReturnCode.
     */
    CbReturnCode invoke_callback(cb_func::Type type,
                                 cb_func::Param* param);

    /**
     * Set a custom callback function for increasing term.
     */
    void set_inc_term_func(srv_state::inc_term_func func);

protected:
    typedef std::unordered_map<int32, ptr<peer>>::const_iterator peer_itor;

    struct commit_ret_elem;

    struct pre_vote_status_t {
        pre_vote_status_t()
            : quorum_reject_count_(0)
            , failure_count_(0)
            { reset(0); }
        void reset(ulong _term) {
            term_ = _term;
            done_ = false;
            live_ = dead_ = abandoned_ = 0;
        }
        ulong term_;
        std::atomic<bool> done_;
        std::atomic<int32> live_;
        std::atomic<int32> dead_;
        std::atomic<int32> abandoned_;

        /**
         * Number of pre-vote rejections by quorum.
         */
        std::atomic<int32> quorum_reject_count_;

        /**
         * Number of pre-vote failures due to not-responding peers.
         */
        std::atomic<int32> failure_count_;
    };

protected:
    void apply_and_log_current_params();
    void cancel_schedulers();
    void schedule_task(ptr<delayed_task>& task, int32 milliseconds);
    void cancel_task(ptr<delayed_task>& task);
    bool check_leadership_validity();
    void check_leadership_transfer();
    void update_rand_timeout();
    void cancel_global_requests();

    bool is_regular_member(const ptr<peer>& p);
    int32 get_num_voting_members();
    int32 get_quorum_for_election();
    int32 get_quorum_for_commit();
    int32 get_leadership_expiry();
    size_t get_not_responding_peers();
    size_t get_num_stale_peers();

    ptr<resp_msg> handle_append_entries(req_msg& req);
    ptr<resp_msg> handle_prevote_req(req_msg& req);
    ptr<resp_msg> handle_vote_req(req_msg& req);
    ptr<resp_msg> handle_cli_req_prelock(req_msg& req);
    ptr<resp_msg> handle_cli_req(req_msg& req);
    ptr<resp_msg> handle_cli_req_callback(ptr<commit_ret_elem> elem,
                                          ptr<resp_msg> resp);
    ptr< cmd_result< ptr<buffer> > >
        handle_cli_req_callback_async(ptr< cmd_result< ptr<buffer> > > async_res);

    void drop_all_pending_commit_elems();

    ptr<resp_msg> handle_ext_msg(req_msg& req);
    ptr<resp_msg> handle_install_snapshot_req(req_msg& req);
    ptr<resp_msg> handle_rm_srv_req(req_msg& req);
    ptr<resp_msg> handle_add_srv_req(req_msg& req);
    ptr<resp_msg> handle_log_sync_req(req_msg& req);
    ptr<resp_msg> handle_join_cluster_req(req_msg& req);
    ptr<resp_msg> handle_leave_cluster_req(req_msg& req);
    ptr<resp_msg> handle_priority_change_req(req_msg& req);
    ptr<resp_msg> handle_reconnect_req(req_msg& req);
    ptr<resp_msg> handle_custom_notification_req(req_msg& req);

    void handle_join_cluster_resp(resp_msg& resp);
    void handle_log_sync_resp(resp_msg& resp);
    void handle_leave_cluster_resp(resp_msg& resp);

    bool handle_snapshot_sync_req(snapshot_sync_req& req);

    bool check_cond_for_zp_election();
    void request_prevote();
    void initiate_vote(bool ignore_priority = false);
    void request_vote(bool ignore_priority);
    void request_append_entries();
    bool request_append_entries(ptr<peer> p);
    void handle_peer_resp(ptr<resp_msg>& resp, ptr<rpc_exception>& err);
    void handle_append_entries_resp(resp_msg& resp);
    void handle_install_snapshot_resp(resp_msg& resp);
    void handle_install_snapshot_resp_new_member(resp_msg& resp);
    void handle_prevote_resp(resp_msg& resp);
    void handle_vote_resp(resp_msg& resp);
    void handle_priority_change_resp(resp_msg& resp);
    void handle_reconnect_resp(resp_msg& resp);
    void handle_custom_notification_resp(resp_msg& resp);

    bool try_update_precommit_index(ulong desired, const size_t MAX_ATTEMPTS = 10);

    void handle_ext_resp(ptr<resp_msg>& resp, ptr<rpc_exception>& err);
    void handle_ext_resp_err(rpc_exception& err);
    void handle_join_leave_rpc_err(msg_type t_msg, ptr<peer> p);
    void reset_srv_to_join();
    void reset_srv_to_leave();
    ptr<req_msg> create_append_entries_req(peer& p);
    ptr<req_msg> create_sync_snapshot_req(peer& p,
                                          ulong last_log_idx,
                                          ulong term,
                                          ulong commit_idx);
    void commit(ulong target_idx);
    void snapshot_and_compact(ulong committed_idx);
    bool update_term(ulong term);
    void reconfigure(const ptr<cluster_config>& new_config);
    void update_target_priority();
    void decay_target_priority();
    void reconnect_client(peer& p);
    void become_leader();
    void become_follower();
    void check_srv_to_leave_timeout();
    void enable_hb_for_peer(peer& p);
    void stop_election_timer();
    void handle_hb_timeout(int32 srv_id);
    void reset_peer_info();
    void handle_election_timeout();
    void sync_log_to_new_srv(ulong start_idx);
    void invite_srv_to_join_cluster();
    void rm_srv_from_cluster(int32 srv_id);
    int get_snapshot_sync_block_size() const;
    void on_snapshot_completed(ptr<snapshot>& s,
                               bool result,
                               ptr<std::exception>& err);
    void on_retryable_req_err(ptr<peer>& p, ptr<req_msg>& req);
    ulong term_for_log(ulong log_idx);

    void commit_in_bg();
    bool commit_in_bg_exec(size_t timeout_ms = 0);

    void append_entries_in_bg();
    void append_entries_in_bg_exec();

    void commit_app_log(ulong idx_to_commit,
                        ptr<log_entry>& le,
                        bool need_to_handle_commit_elem);
    void commit_conf(ulong idx_to_commit, ptr<log_entry>& le);

    ptr< cmd_result< ptr<buffer> > > send_msg_to_leader(ptr<req_msg>& req);

    void set_config(const ptr<cluster_config>& new_config);
    ptr<snapshot> get_last_snapshot() const;
    void set_last_snapshot(const ptr<snapshot>& new_snapshot);

    ulong store_log_entry(ptr<log_entry>& entry, ulong index = 0);

    ptr<resp_msg> handle_out_of_log_msg(req_msg& req,
                                        ptr<custom_notification_msg> msg,
                                        ptr<resp_msg> resp);

    ptr<resp_msg> handle_leadership_takeover(req_msg& req,
                                             ptr<custom_notification_msg> msg,
                                             ptr<resp_msg> resp);

    ptr<resp_msg> handle_resignation_request(req_msg& req,
                                             ptr<custom_notification_msg> msg,
                                             ptr<resp_msg> resp);

    void remove_peer_from_peers(const ptr<peer>& pp);

    void check_overall_status();

protected:
    static const int default_snapshot_sync_block_size;

    /**
     * Current limit values.
     */
    static limits raft_limits_;

    /**
     * (Read-only)
     * Background thread for commit and snapshot.
     */
    std::thread bg_commit_thread_;

    /**
     * (Read-only)
     * Background thread for sending quick append entry request.
     */
    std::thread bg_append_thread_;

    /**
     * Condition variable to invoke append thread.
     */
    EventAwaiter* bg_append_ea_;

    /**
     * `true` if this server is ready to serve operation.
     */
    std::atomic<bool> initialized_;

    /**
     * Current leader ID.
     * If leader currently does not exist, it will be -1.
     */
    std::atomic<int32> leader_;

    /**
     * (Read-only)
     * ID of this server.
     */
    int32 id_;

    /**
     * Current priority of this server, protected by `lock_`.
     */
    int32 my_priority_;

    /**
     * Current target priority for vote, protected by `lock_`.
     */
    int32 target_priority_;

    /**
     * Timer that will be reset on `target_priority_` change.
     */
    timer_helper priority_change_timer_;

    /**
     * Number of servers responded my vote request, protected by `lock_`.
     */
    int32 votes_responded_;

    /**
     * Number of servers voted for me, protected by `lock_`.
     */
    int32 votes_granted_;

    /**
     * Last pre-committed index.
     */
    std::atomic<ulong> precommit_index_;

    /**
     * Leader commit index, seen by this node last time.
     * Only valid when the current role is `follower`.
     */
    std::atomic<ulong> leader_commit_index_;

    /**
     * Target commit index.
     * This value will be basically the same as `leader_commit_index_`.
     * However, if the current role is `follower` and this node's log
     * is far behind leader so that requires further catch-up, this
     * value can be adjusted to the last log index number of the current
     * node, which might be smaller than `leader_commit_index_` value.
     */
    std::atomic<ulong> quick_commit_index_;

    /**
     * Actual commit index of state machine.
     */
    std::atomic<ulong> sm_commit_index_;

    /**
     * (Read-only)
     * Initial commit index when this server started.
     */
    ulong initial_commit_index_;

    /**
     * `true` if this server is seeing alive leader.
     */
    std::atomic<bool> hb_alive_;

    /**
     * Current status of pre-vote, protected by `lock_`.
     */
    pre_vote_status_t pre_vote_;

    /**
     * `false` if currently leader election is not in progress,
     * protected by `lock_`.
     */
    bool election_completed_;

    /**
     * `true` if there is uncommitted config, which will
     * reject the configuration change.
     * Protected by `lock_`.
     */
    bool config_changing_;

    /**
     * `true` if this server falls behind leader so that
     * catching up the latest log. It will not receive
     * normal `append_entries` request while in catch-up status.
     */
    std::atomic<bool> catching_up_;

    /**
     * `true` if this server receives out of log range message
     * from leader. Once this flag is set, this server will not
     * initiate leader election.
     */
    std::atomic<bool> out_of_log_range_;

    /**
     * `true` if this is a follower and its committed log index is close enough
     * to the leader's committed log index, so the data is fresh enough.
     */
    std::atomic<bool> data_fresh_;

    /**
     * `true` if this server is terminating.
     * Will not accept any request.
     */
    std::atomic<bool> stopping_;

    /**
     * `true` if background commit thread has been terminated.
     */
    std::atomic<bool> commit_bg_stopped_;

    /**
     * `true` if background append thread has been terminated.
     */
    std::atomic<bool> append_bg_stopped_;

    /**
     * `true` if write operation is paused, as the first phase of
     * leader re-election.
     */
    std::atomic<bool> write_paused_;

    /**
     * Server ID indicates the candidate for the next leader,
     * as a part of leadership takeover task.
     */
    std::atomic<int32> next_leader_candidate_;

    /**
     * Timer that will start at pausing write.
     */
    timer_helper reelection_timer_;

    /**
     * (Read-only)
     * `true` if this server is a learner. Will not participate
     * leader election.
     */
    bool im_learner_;

    /**
     * `true` if this server is in the middle of
     * `append_entries` handler.
     */
    std::atomic<bool> serving_req_;

    /**
     * Number of steps remaining to turn off this server.
     * Will be triggered once this server is removed from the cluster.
     * Protected by `lock_`.
     */
    int32 steps_to_down_;

    /**
     * `true` if this server is creating a snapshot.
     */
    std::atomic<bool> snp_in_progress_;

    /**
     * (Read-only, but its contents will change)
     * Server context.
     */
    std::unique_ptr<context> ctx_;

    /**
     * Scheduler.
     */
    ptr<delayed_task_scheduler> scheduler_;

    /**
     * Election timeout handler.
     */
    timer_task<void>::executor election_exec_;

    /**
     * Election timer.
     */
    ptr<delayed_task> election_task_;

    /**
     * The time when the election timer was reset last time.
     */
    timer_helper last_election_timer_reset_;

    /**
     * Map of {Server ID, `peer` instance},
     * protected by `lock_`.
     */
    std::unordered_map<int32, ptr<peer>> peers_;

    /**
     * Map of {server ID, connection to corresponding server},
     * protected by `lock_`.
     */
    std::unordered_map<int32, ptr<rpc_client>> rpc_clients_;

    /**
     * Current role of this server.
     */
    std::atomic<srv_role> role_;

    /**
     * (Read-only, but its contents will change)
     * Server status (term and vote).
     */
    ptr<srv_state> state_;

    /**
     * (Read-only)
     * Log store instance.
     */
    ptr<log_store> log_store_;

    /**
     * (Read-only)
     * State machine instance.
     */
    ptr<state_machine> state_machine_;

    /**
     * `true` if this server is receiving a snapshot.
     */
    std::atomic<bool> receiving_snapshot_;

    /**
     * Election timeout count while receiving snapshot.
     * This happens when the sender (i.e., leader) is too slow
     * so that cannot send message before election timeout.
     */
    std::atomic<ulong> et_cnt_receiving_snapshot_;

    /**
     * (Read-only)
     * Logger instance.
     */
    ptr<logger> l_;

    /**
     * (Read-only)
     * Random generator for timeout.
     */
    std::function<int32()> rand_timeout_;

    /**
     * Previous config for debugging purpose, protected by `config_lock_`.
     */
    ptr<cluster_config> stale_config_;

    /**
     * Current (committed) cluster config, protected by `config_lock_`.
     */
    ptr<cluster_config> config_;

    /**
     * Lock for cluster config.
     */
    mutable std::mutex config_lock_;

    /**
     * Latest uncommitted cluster config changed from `config_`,
     * protected by `lock_`. `nullptr` if `config_` is the latest one.
     */
    ptr<cluster_config> uncommitted_config_;

    /**
     * Server that is preparing to join,
     * protected by `lock_`.
     */
    ptr<peer> srv_to_join_;

    /**
     * Server that is agreed to leave,
     * protected by `lock_`.
     */
    ptr<peer> srv_to_leave_;

    /**
     * Target log index number containing the config that
     * this server is actually removed.
     * Connection to `srv_to_leave_` should be kept until this log.
     */
    ulong srv_to_leave_target_idx_;

    /**
     * Config of the server preparing to join,
     * protected by `lock_`.
     */
    ptr<srv_config> conf_to_add_;

    /**
     * Lock of entire Raft operation.
     */
    std::recursive_mutex lock_;

    /**
     * Lock of handling client request and role change.
     */
    std::mutex cli_lock_;

    /**
     * Condition variable to invoke BG commit thread.
     */
    std::condition_variable commit_cv_;

    /**
     * Lock for `commit_cv_`.
     */
    std::mutex commit_cv_lock_;

    /**
     * Lock to allow only one thread for commit.
     */
    std::mutex commit_lock_;

    /**
     * Lock for auto forwarding.
     */
    std::mutex rpc_clients_lock_;

    /**
     * Client requests waiting for replication.
     * Only used in blocking mode.
     */
    std::map<ulong, ptr<commit_ret_elem>> commit_ret_elems_;

    /**
     * Lock for `commit_ret_elems_`.
     */
    std::mutex commit_ret_elems_lock_;

    /**
     * Condition variable to invoke Raft server for
     * notifying the termination of BG commit thread.
     */
    std::condition_variable ready_to_stop_cv_;

    /**
     * Lock for `read_to_stop_cv_`.
     */
    std::mutex ready_to_stop_cv_lock_;

    /**
     * (Read-only)
     * Response handler.
     */
    rpc_handler resp_handler_;

    /**
     * (Read-only)
     * Extended response handler.
     */
    rpc_handler ex_resp_handler_;

    /**
     * Last snapshot instance.
     */
    ptr<snapshot> last_snapshot_;

    /**
     * Lock for `last_snapshot_`.
     */
    mutable std::mutex last_snapshot_lock_;

    /**
     * Timer that will be reset on becoming a leader.
     */
    timer_helper leadership_transfer_timer_;

    /**
     * Timer that will be used for status checking
     * for each heartbeat period.
     */
    timer_helper status_check_timer_;
};

} // namespace nuraft;

#endif //_RAFT_SERVER_HXX_
