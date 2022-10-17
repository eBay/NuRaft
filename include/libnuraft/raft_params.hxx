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

#ifndef _RAFT_PARAMS_HXX_
#define _RAFT_PARAMS_HXX_

#include "basic_types.hxx"
#include "pp_util.hxx"

#include <algorithm>

namespace nuraft {

struct raft_params {
    enum return_method_type {
        /**
         * `append_entries()` will be a blocking call,
         * and will return after it is committed in leader node.
         */
        blocking = 0x0,

        /**
         * `append_entries()` will return immediately,
         * and callback function (i.e., handler) will be
         * invoked after it is committed in leader node.
         */
        async_handler = 0x1,
    };

    enum locking_method_type {
        /**
         * `append_entries()` will share the same mutex with
         * background worker threads.
         */
        single_mutex = 0x0,

        /**
         * `append_entries()` and background worker threads will
         * use separate mutexes.
         */
        dual_mutex = 0x1,

        /**
         * (Not supported yet)
         * `append_entries()` will use RW-lock, which is separate to
         * the mutex used by background worker threads.
         */
        dual_rw_lock = 0x2,
    };

    raft_params()
        : election_timeout_upper_bound_(500)
        , election_timeout_lower_bound_(250)
        , heart_beat_interval_(125)
        , rpc_failure_backoff_(50)
        , log_sync_batch_size_(1000)
        , log_sync_stop_gap_(99999)
        , snapshot_distance_(0)
        , snapshot_block_size_(0)
        , enable_randomized_snapshot_creation_(false)
        , max_append_size_(100)
        , reserved_log_items_(100000)
        , client_req_timeout_(3000)
        , fresh_log_gap_(200)
        , stale_log_gap_(2000)
        , custom_commit_quorum_size_(0)
        , custom_election_quorum_size_(0)
        , leadership_expiry_(0)
        , leadership_transfer_min_wait_time_(0)
        , allow_temporary_zero_priority_leader_(true)
        , auto_forwarding_(false)
        , auto_forwarding_max_connections_(10)
        , use_bg_thread_for_urgent_commit_(true)
        , exclude_snp_receiver_from_quorum_(false)
        , auto_adjust_quorum_for_small_cluster_(false)
        , locking_method_type_(dual_mutex)
        , return_method_(blocking)
        , auto_forwarding_req_timeout_(0)
        , grace_period_of_lagging_state_machine_(0)
        , use_bg_thread_for_snapshot_io_(false)
        , use_full_consensus_among_healthy_members_(false)
        , parallel_log_appending_(false)
        {}

    /**
     * Election timeout upper bound in milliseconds
     *
     * @param timeout
     * @return self
     */
    raft_params& with_election_timeout_upper(int32 timeout) {
        election_timeout_upper_bound_ = timeout;
        return *this;
    }

    /**
     * Election timeout lower bound in milliseconds
     *
     * @param timeout
     * @return self
     */
    raft_params& with_election_timeout_lower(int32 timeout) {
        election_timeout_lower_bound_ = timeout;
        return *this;
    }

    /**
     * heartbeat interval in milliseconds
     *
     * @param hb_interval
     * @return self
     */
    raft_params& with_hb_interval(int32 hb_interval) {
        heart_beat_interval_ = hb_interval;
        return *this;
    }

    /**
     * Rpc failure backoff in milliseconds
     *
     * @param backoff
     * @return self
     */
    raft_params& with_rpc_failure_backoff(int32 backoff) {
        rpc_failure_backoff_ = backoff;
        return *this;
    }

    /**
     * The maximum log entries could be attached to an appendEntries call
     *
     * @param size
     * @return self
     */
    raft_params& with_max_append_size(int32 size) {
        max_append_size_ = size;
        return *this;
    }

    /**
     * For new member that just joined the cluster, we will use
     * log sync to ask it to catch up, and this parameter is to
     * specify how many log entries to pack for each sync request.
     *
     * @param batch_size
     * @return self
     */
    raft_params& with_log_sync_batch_size(int32 batch_size) {
        log_sync_batch_size_ = batch_size;
        return *this;
    }

    /**
     * For new member that just joined the cluster, we will use
     * log sync to ask it to catch up, and this parameter is to
     * tell when to stop using log sync but appendEntries for the
     * new server.
     * When `leaderCommitIndex - indexCaughtUp < logSyncStopGap`,
     * then appendEntries will be used.
     *
     * @param gap
     * @return self
     */
    raft_params& with_log_sync_stopping_gap(int32 gap) {
        log_sync_stop_gap_ = gap;
        return *this;
    }

    /**
     * Enable log compact and snapshot with the commit distance
     *
     * @param commit_distance
     *     Log distance to compact between two snapshots.
     * @return self
     */
    raft_params& with_snapshot_enabled(int32 commit_distance) {
        snapshot_distance_ = commit_distance;
        return *this;
    }

    /**
     * Enable randomized snapshot creation which will avoid simultaneous
     * snapshot creation among cluster members.
     *
     * @param enabled
     * @return self
     */
    raft_params& with_randomized_snapshot_creation_enabled(bool enabled) {
        enable_randomized_snapshot_creation_ = enabled;
        return *this;
    }

    /**
     * The TCP block size for syncing the snapshots.
     *
     * @param size
     * @return self
     */
    raft_params& with_snapshot_sync_block_size(int32 size) {
        snapshot_block_size_ = size;
        return *this;
    }

    /**
     * The number of reserved log items when doing log compaction.
     *
     * @param number_of_logs Number of log items.
     * @return self
     */
    raft_params& with_reserved_log_items(int32 number_of_logs) {
        reserved_log_items_ = number_of_logs;
        return *this;
    }

    /**
     * Timeout of the execution of client request (in ms).
     *
     * @param timeout
     * @return self
     */
    raft_params& with_client_req_timeout(int timeout) {
        client_req_timeout_ = timeout;
        return *this;
    }

    /**
     * Enable auto-forwarding, so that non-leader node re-directs client
     * request to the current leader.
     *
     * @param enable
     * @return self
     */
    raft_params& with_auto_forwarding(bool enable) {
        auto_forwarding_ = enable;
        return *this;
    }

    /**
     * If this node is considered as stale and the gap between this node's committed
     * log index and the leader's committed log index is smaller than this threshold,
     * this node becomes fresh.
     *
     * @param new_gap New threshold.
     * @return self
     */
    raft_params& with_fresh_log_gap(int32 new_gap) {
        fresh_log_gap_ = new_gap;
        return *this;
    }

    /**
     * If this node is considered as fresh and the gap between this node's committed
     * log index and the leader's committed log index is larger than this threshold,
     * this node becomes stale.
     *
     * @param new_gap New threshold.
     * @return self
     */
    raft_params& with_stale_log_gap(int32 new_gap) {
        stale_log_gap_ = new_gap;
        return *this;
    }

    /**
     * If this is set to positive non-zero value, commiting
     * a log will be based on this quorum size. Leader election
     * will not be affected.
     *
     * If set to zero, the default quorum size will be used:
     * `ceil{ (N+1) / 2 }`, where N is the number of nodes including
     * the leader.
     *
     * If this is set to wrong value, Raft will use the default
     * quorum size.
     *
     * @param new_size New custom commit quorum size.
     * @return self
     */
    raft_params& with_custom_commit_quorum_size(int32 new_size) {
        custom_commit_quorum_size_ = new_size;
        return *this;
    }

    /**
     * If this is set to positive non-zero value, electing a
     * new leader will be based on this quorum size. Committing
     * a log will not be affected.
     *
     * If set to zero, the default quorum size will be used:
     * `ceil{ (N+1) / 2 }`, where N is the number of nodes including
     * the leader.
     *
     * If this is set to wrong value, Raft will use the default
     * quorum size.
     *
     * @param new_size New custom election quorum size.
     * @return self
     */
    raft_params& with_custom_election_quorum_size(int32 new_size) {
        custom_election_quorum_size_ = new_size;
        return *this;
    }

    /**
     * Set the expiration time of leadership.
     *
     * @param expiry_ms New leadership expiration in millisecond.
     * @return self
     */
    raft_params& with_leadership_expiry(int32 expiry_ms) {
        leadership_expiry_ = expiry_ms;
        return *this;
    }

    /**
     * Set the auto-forwarding request timeout
     *
     * @param timeout_ms New timeout in millisecond.
     * @return self
     */
    raft_params& with_auto_forwarding_req_timeout(int32 timeout_ms) {
        auto_forwarding_req_timeout_ = timeout_ms;
        return *this;
    }


    /**
     * Return heartbeat interval.
     * If given heartbeat interval is smaller than a specific value
     * based on election timeout, return it instead.
     *
     * @return Heartbeat interval in millisecond.
     */
    int max_hb_interval() const {
        return std::max
               ( heart_beat_interval_,
                 election_timeout_lower_bound_ - (heart_beat_interval_ / 2) );
    }

public:
    /**
     * Upper bound of election timer, in millisecond.
     */
    int32 election_timeout_upper_bound_;

    /**
     * Lower bound of election timer, in millisecond.
     */
    int32 election_timeout_lower_bound_;

    /**
     * Heartbeat interval, in millisecond.
     */
    int32 heart_beat_interval_;

    /**
     * Backoff time when RPC failure happens, in millisecond.
     */
    int32 rpc_failure_backoff_;

    /**
     * Max number of logs that can be packed in a RPC
     * for catch-up of joining an empty node.
     */
    int32 log_sync_batch_size_;

    /**
     * Log gap (the number of logs) to stop catch-up of
     * joining a new node. Once this condition meets,
     * that newly joined node is added to peer list
     * and starts to receive heartbeat from leader.
     *
     * If zero, the new node will be added to the peer list
     * immediately.
     */
    int32 log_sync_stop_gap_;

    /**
     * Log gap (the number of logs) to create a Raft snapshot.
     */
    int32 snapshot_distance_;

    /**
     * (Deprecated).
     */
    int32 snapshot_block_size_;

    /**
     * Enable randomized snapshot creation which will avoid
     * simultaneous snapshot creation among cluster members.
     * It is achieved by randomizing the distance of the
     * first snapshot. From the second snapshot, the fixed
     * distance given by snapshot_distance_ will be used.
     */
    bool enable_randomized_snapshot_creation_;

    /**
     * Max number of logs that can be packed in a RPC
     * for append entry request.
     */
    int32 max_append_size_;

    /**
     * Minimum number of logs that will be preserved
     * (i.e., protected from log compaction) since the
     * last Raft snapshot.
     */
    int32 reserved_log_items_;

    /**
     * Client request timeout in millisecond.
     */
    int32 client_req_timeout_;

    /**
     * Log gap (compared to the leader's latest log)
     * for treating this node as fresh.
     */
    int32 fresh_log_gap_;

    /**
     * Log gap (compared to the leader's latest log)
     * for treating this node as stale.
     */
    int32 stale_log_gap_;

    /**
     * Custom quorum size for commit.
     * If set to zero, the default quorum size will be used.
     */
    int32 custom_commit_quorum_size_;

    /**
     * Custom quorum size for leader election.
     * If set to zero, the default quorum size will be used.
     */
    int32 custom_election_quorum_size_;

    /**
     * Expiration time of leadership in millisecond.
     * If more than quorum nodes do not respond within
     * this time, the current leader will immediately
     * yield its leadership and become follower.
     * If 0, it is automatically set to `heartbeat * 20`.
     * If negative number, leadership will never be expired
     * (the same as the original Raft logic).
     */
    int32 leadership_expiry_;

    /**
     * Minimum wait time required for transferring the leadership
     * in millisecond. If this value is non-zero, and the below
     * conditions are met together,
     *   - the elapsed time since this server became a leader
     *     is longer than this number, and
     *   - the current leader's priority is not the highest one, and
     *   - all peers are responding, and
     *   - the log gaps of all peers are smaller than `stale_log_gap_`, and
     *   - `allow_leadership_transfer` of the state machine returns true,
     * then the current leader will transfer its leadership to the peer
     * with the highest priority.
     */
    int32 leadership_transfer_min_wait_time_;

    /**
     * If true, zero-priority member can initiate vote
     * when leader is not elected long time (that can happen
     * only the zero-priority member has the latest log).
     * Once the zero-priority member becomes a leader,
     * it will immediately yield leadership so that other
     * higher priority node can takeover.
     */
    bool allow_temporary_zero_priority_leader_;

    /**
     * If true, follower node will forward client request
     * to the current leader.
     * Otherwise, it will return error to client immediately.
     */
    bool auto_forwarding_;

    /**
     * The maximum number of connections for auto forwarding (if enabled).
     */
    int32 auto_forwarding_max_connections_;

    /**
     * If true, creating replication (append_entries) requests will be
     * done by a background thread, instead of doing it in user threads.
     * There can be some delay a little bit, but it improves reducing
     * the lock contention.
     */
    bool use_bg_thread_for_urgent_commit_;

    /**
     * If true, a server who is currently receiving snapshot will not be
     * counted in quorum. It is useful when there are only two servers
     * in the cluster. Once the follower is receiving snapshot, the
     * leader cannot make any progress.
     */
    bool exclude_snp_receiver_from_quorum_;

    /**
     * If `true` and the size of the cluster is 2, the quorum size
     * will be adjusted to 1 automatically, once one of two nodes
     * becomes offline.
     */
    bool auto_adjust_quorum_for_small_cluster_;

    /**
     * Choose the type of lock that will be used by user threads.
     */
    locking_method_type locking_method_type_;

    /**
     * To choose blocking call or asynchronous call.
     */
    return_method_type return_method_;

    /**
     * Wait ms for response after forwarding request to leader.
     * must be larger than client_req_timeout_.
     * If 0, there will be no timeout for auto forwarding.
     */
    int32 auto_forwarding_req_timeout_;

    /**
     * If non-zero, any server whose state machine's commit index is
     * lagging behind the last committed log index will not
     * initiate vote requests for the given amount of time
     * in milliseconds.
     *
     * The purpose of this option is to avoid a server (whose state
     * machine is still catching up with the committed logs and does
     * not contain the latest data yet) being a leader.
     */
    int32 grace_period_of_lagging_state_machine_;

    /**
     * (Experimental)
     * If `true`, reading snapshot objects will be done by a background thread
     * asynchronously instead of synchronous read by Raft worker threads.
     * Asynchronous IO will reduce the overall latency of the leader's operations.
     */
    bool use_bg_thread_for_snapshot_io_;

    /**
     * (Experimental)
     * If `true`, it will commit a log upon the agreement of all healthy members.
     * In other words, with this option, all healthy members have the log at the
     * moment the leader commits the log. If the number of healthy members is
     * smaller than the regular (or configured custom) quorum size, the leader
     * cannot commit the log.
     *
     * A member becomes "unhealthy" if it does not respond to the leader's
     * request for a configured time (`response_limit_`).
     */
    bool use_full_consensus_among_healthy_members_;

    /**
     * (Experimental)
     * If `true`, users can let the leader append logs parallel with their
     * replication. To implement parallel log appending, users need to make
     * `log_store::append`, `log_store::write_at`, or
     * `log_store::end_of_append_batch` API triggers asynchronous disk writes
     * without blocking the thread. Even while the disk write is in progress,
     * the other read APIs of log store should be able to read the log.
     *
     * The replication and the disk write will be executed in parallel,
     * and users need to call `raft_server::notify_log_append_completion`
     * when the asynchronous disk write is done. Also, users need to properly
     * implement `log_store::last_durable_index` API to return the most recent
     * durable log index. The leader will commit the log based on the
     * result of this API.
     *
     *   - If the disk write is done earlier than the replication,
     *     the commit behavior is the same as the original protocol.
     *
     *   - If the replication is done earlier than the disk write,
     *     the leader will commit the log based on the quorum except
     *     for the leader itself. The leader can apply the log to
     *     the state machine even before completing the disk write
     *     of the log.
     *
     * Note that parallel log appending is available for the leader only,
     * and followers will wait for `notify_log_append_completion` call
     * before returning the response.
     */
    bool parallel_log_appending_;
};

}

#endif //_RAFT_PARAMS_HXX_
