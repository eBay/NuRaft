/************************************************************************
Modifications Copyright 2017-present eBay Inc.
Author/Developer(s): Jung-Sang Ahn

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

#include "snapshot_sync_ctx.hxx"

#include "event_awaiter.hxx"
#include "peer.hxx"
#include "raft_server.hxx"
#include "state_machine.hxx"
#include "tracer.hxx"

namespace nuraft {

class raft_server;

snapshot_sync_ctx::snapshot_sync_ctx(const std::shared_ptr< snapshot >& s, int peer_id, uint64_t timeout_ms,
                                     uint64_t offset) :
        peer_id_(peer_id), snapshot_(s), offset_(offset), user_snp_ctx_(nullptr) {
    // 10 seconds by default.
    timer_.set_duration_ms(timeout_ms);
}

void snapshot_sync_ctx::set_offset(uint64_t offset) {
    if (offset_ != offset) timer_.reset();
    offset_ = offset;
}

struct snapshot_io_mgr::io_queue_elem {
    io_queue_elem(std::shared_ptr< raft_server > r, std::shared_ptr< snapshot > s,
                  std::shared_ptr< snapshot_sync_ctx > c, std::shared_ptr< peer > p,
                  std::function< void(std::shared_ptr< resp_msg >&, std::shared_ptr< rpc_exception >&) >& h) :
            raft_(r), snapshot_(s), sync_ctx_(c), dst_(p), handler_(h) {}
    std::shared_ptr< raft_server > raft_;
    std::shared_ptr< snapshot > snapshot_;
    std::shared_ptr< snapshot_sync_ctx > sync_ctx_;
    std::shared_ptr< peer > dst_;
    std::function< void(std::shared_ptr< resp_msg >&, std::shared_ptr< rpc_exception >&) > handler_;
};

snapshot_io_mgr::snapshot_io_mgr() : io_thread_ea_(new EventAwaiter()), terminating_(false) {
    io_thread_ = std::thread(&snapshot_io_mgr::async_io_loop, this);
}

snapshot_io_mgr::~snapshot_io_mgr() { shutdown(); }

bool snapshot_io_mgr::push(std::shared_ptr< snapshot_io_mgr::io_queue_elem >& elem) {
    auto guard = auto_lock(queue_lock_);
    logger* l_ = elem->raft_->l_.get();

    // If there is existing one for the same peer, ignore it.
    for (auto& entry : queue_) {
        if (entry->raft_ == elem->raft_ && entry->dst_->get_id() == elem->dst_->get_id()) {
            p_tr("snapshot request for peer %d already exists, do nothing", elem->dst_->get_id());
            return false;
        }
    }
    queue_.push_back(elem);
    p_tr("added snapshot request for peer %d", elem->dst_->get_id());

    return true;
}

bool snapshot_io_mgr::push(std::shared_ptr< raft_server > r, std::shared_ptr< peer > p,
                           std::function< void(std::shared_ptr< resp_msg >&, std::shared_ptr< rpc_exception >&) >& h) {
    std::shared_ptr< io_queue_elem > elem = std::make_shared< io_queue_elem >(
        r, p->get_snapshot_sync_ctx()->get_snapshot(), p->get_snapshot_sync_ctx(), p, h);
    return push(elem);
}

void snapshot_io_mgr::invoke() { io_thread_ea_->invoke(); }

void snapshot_io_mgr::drop_reqs(raft_server* r) {
    auto guard = auto_lock(queue_lock_);
    logger* l_ = r->l_.get();
    auto entry = queue_.begin();
    while (entry != queue_.end()) {
        if ((*entry)->raft_.get() == r) {
            p_tr("drop snapshot request for peer %d, raft server %p", (*entry)->dst_->get_id(), r);
            entry = queue_.erase(entry);
        } else {
            entry++;
        }
    }
}

bool snapshot_io_mgr::has_pending_request(raft_server* r, int srv_id) {
    auto guard = auto_lock(queue_lock_);
    for (auto& entry : queue_) {
        if (entry->raft_.get() == r && entry->dst_->get_id() == srv_id) { return true; }
    }
    return false;
}

void snapshot_io_mgr::shutdown() {
    terminating_ = true;
    if (io_thread_.joinable()) {
        io_thread_ea_->invoke();
        io_thread_.join();
    }
}

void snapshot_io_mgr::async_io_loop() {
    std::string thread_name = "nuraft_snp_io";
#ifdef __linux__
    pthread_setname_np(pthread_self(), thread_name.c_str());
#elif __APPLE__
    pthread_setname_np(thread_name.c_str());
#endif

    do {
        io_thread_ea_->wait_ms(1000);
        io_thread_ea_->reset();

        std::list< std::shared_ptr< io_queue_elem > > reqs;
        std::list< std::shared_ptr< io_queue_elem > > reqs_to_return;
        if (!terminating_) {
            auto guard = auto_lock(queue_lock_);
            reqs = queue_;
        }

        for (std::shared_ptr< io_queue_elem >& elem : reqs) {
            if (terminating_) { break; }
            if (!elem->raft_->is_leader()) { break; }

            int dst_id = elem->dst_->get_id();

            std::unique_lock< std::mutex > lock(elem->dst_->get_lock());
            // ---- lock acquired
            logger* l_ = elem->raft_->l_.get();
            uint64_t obj_idx = elem->sync_ctx_->get_offset();
            void*& user_snp_ctx = elem->sync_ctx_->get_user_snp_ctx();
            p_db("peer: %d, obj_idx: %" PRIu64 ", user_snp_ctx %p", dst_id, obj_idx, user_snp_ctx);

            uint64_t snp_log_idx = elem->snapshot_->get_last_log_idx();
            uint64_t snp_log_term = elem->snapshot_->get_last_log_term();
            // ---- lock released
            lock.unlock();

            std::shared_ptr< buffer > data = nullptr;
            bool is_last_request = false;

            int rc = elem->raft_->state_machine_->read_logical_snp_obj(*elem->snapshot_, user_snp_ctx, obj_idx, data,
                                                                       is_last_request);
            if (rc < 0) {
                // Snapshot read failed.
                p_wn("reading snapshot (idx %" PRIu64 ", term %" PRIu64 ", object %" PRIu64 ") "
                     "for peer %d failed: %d",
                     snp_log_idx, snp_log_term, obj_idx, dst_id, rc);

                auto guard = recur_lock(elem->raft_->lock_);
                auto entry = elem->raft_->peers_.find(dst_id);
                if (entry != elem->raft_->peers_.end()) {
                    // If normal member (already in the peer list):
                    //   reset the `sync_ctx` so as to retry with the newer version.
                    elem->raft_->clear_snapshot_sync_ctx(*elem->dst_);
                } else {
                    // If it is joing the server (not in the peer list),
                    // enable HB temporarily to retry the request.
                    elem->raft_->srv_to_join_snp_retry_required_ = true;
                    elem->raft_->enable_hb_for_peer(*elem->raft_->srv_to_join_);
                }

                continue;
            }
            if (data) data->pos(0);

            // Send snapshot message with the given response handler.
            auto guard = recur_lock(elem->raft_->lock_);
            uint64_t term = elem->raft_->state_->get_term();
            uint64_t commit_idx = elem->raft_->quick_commit_index_;

            std::unique_ptr< snapshot_sync_req > sync_req(
                new snapshot_sync_req(elem->snapshot_, obj_idx, data, is_last_request));
            std::shared_ptr< req_msg > req(std::make_shared< req_msg >(
                term, msg_type::install_snapshot_request, elem->raft_->id_, dst_id,
                elem->snapshot_->get_last_log_term(), elem->snapshot_->get_last_log_idx(), commit_idx));
            req->log_entries().push_back(
                std::make_shared< log_entry >(term, sync_req->serialize(), log_val_type::snp_sync_req));
            if (elem->dst_->make_busy()) {
                elem->dst_->set_rsv_msg(nullptr, nullptr);
                elem->dst_->send_req(elem->dst_, req, elem->handler_);
                elem->dst_->reset_ls_timer();
                p_tr("bg thread sent message to peer %d", dst_id);

            } else {
                p_db("peer %d is busy, push the request back to queue", dst_id);
                reqs_to_return.push_back(elem);
            }
        }

        {
            auto guard = auto_lock(queue_lock_);
            // Remove elements in `reqs` from `queue_`.
            for (auto& entry : reqs) {
                auto e2 = queue_.begin();
                while (e2 != queue_.end()) {
                    if (*e2 == entry) {
                        e2 = queue_.erase(e2);
                        break;
                    } else {
                        e2++;
                    }
                }
            }
            // Return elements in `reqs_to_return` to `queue_` for retrying.
            for (auto& entry : reqs_to_return) {
                queue_.push_back(entry);
            }
        }

    } while (!terminating_);
}

} // namespace nuraft
