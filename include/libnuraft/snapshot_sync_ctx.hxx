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

#ifndef _SNAPSHOT_SYNC_CTX_HXX_
#define _SNAPSHOT_SYNC_CTX_HXX_

#include "event_awaiter.h"
#include "internal_timer.hxx"
#include "pp_util.hxx"
#include "ptr.hxx"

#include <functional>
#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>

class EventAwaiter;

namespace nuraft {

class peer;
class raft_server;
class resp_msg;
class rpc_exception;
class snapshot;
class snapshot_sync_ctx {
public:
    snapshot_sync_ctx(const ptr<snapshot>& s,
                      int peer_id,
                      ulong timeout_ms,
                      ulong offset = 0L);

    __nocopy__(snapshot_sync_ctx);

public:
    const ptr<snapshot>& get_snapshot() const { return snapshot_; }
    ulong get_offset() const { return offset_; }
    ulong get_obj_idx() const { return obj_idx_; }
    void*& get_user_snp_ctx() { return user_snp_ctx_; }

    void set_offset(ulong offset);
    void set_obj_idx(ulong obj_idx) { obj_idx_ = obj_idx; }
    void set_user_snp_ctx(void* _user_snp_ctx) { user_snp_ctx_ = _user_snp_ctx; }

    timer_helper& get_timer() { return timer_; }

private:
    void io_thread_loop();

    /**
     * Destination peer ID.
     */
    int32_t peer_id_;

    /**
     * Pointer to snapshot.
     */
    ptr<snapshot> snapshot_;

    /**
     * Current cursor of snapshot.
     * Can be used for either byte offset or object index,
     * but the legacy raw snapshot (offset_) is deprecated.
     */
    union {
        ulong offset_;
        ulong obj_idx_;
    };

    /**
     * User-defined snapshot context, given by the state machine.
     */
    void* user_snp_ctx_;

    /**
     * Timer to check snapshot transfer timeout.
     */
    timer_helper timer_;
};

// Singleton class.
class snapshot_io_mgr {
public:
    static snapshot_io_mgr& instance() {
        static snapshot_io_mgr mgr;
        return mgr;
    };

    /**
     * Push a snapshot read request to the queue.
     *
     * @param r Raft server instance.
     * @param p Peer instance.
     * @param h Response handler.
     * @return `true` if succeeds (when there is no pending request for the same peer).
     */
    bool push(ptr<raft_server> r,
              ptr<peer> p,
              std::function< void(ptr<resp_msg>&, ptr<rpc_exception>&) >& h);

    /**
     * Invoke IO thread.
     */
    void invoke();

    /**
     * Drop all pending requests belonging to the given Raft instance.
     *
     * @param r Raft server instance.
     */
    void drop_reqs(raft_server* r);

    /**
     * Check if there is pending request for the given peer.
     *
     * @param r Raft server instance.
     * @param srv_id Server ID to check.
     * @return `true` if pending request exists.
     */
    bool has_pending_request(raft_server* r, int srv_id);

    /**
     * Shutdown the global snapshot IO manager.
     */
    void shutdown();

private:
    struct io_queue_elem;

    snapshot_io_mgr();

    ~snapshot_io_mgr();

    void async_io_loop();

    bool push(ptr<io_queue_elem>& elem);

    /**
     * A dedicated thread for reading snapshot object.
     */
    std::thread io_thread_;

    /**
     * Event awaiter for `io_thread_`.
     */
    ptr<EventAwaiter> io_thread_ea_;

    /**
     * `true` if we are closing this context.
     */
    std::atomic<bool> terminating_;

    /**
     * Request queue. Allow only one request per peer at a time.
     */
    std::list< ptr<io_queue_elem> > queue_;

    /**
     * Lock for `queue_`.
     */
    std::mutex queue_lock_;
};

}

#endif //_SNAPSHOT_SYNC_CTX_HXX_
