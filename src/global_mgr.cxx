/************************************************************************
Modifications Copyright 2017-present eBay Inc.

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

#include "global_mgr.hxx"

#include "event_awaiter.h"
#include "logger.hxx"
#include "raft_server.hxx"
#include "tracer.hxx"

namespace nuraft {

std::atomic<nuraft_global_mgr*> nuraft_global_mgr::instance_(nullptr);
std::mutex nuraft_global_mgr::instance_lock_;

struct nuraft_global_mgr::worker_handle {
    worker_handle(size_t id = 0)
        : id_(id)
        , thread_(nullptr)
        , stopping_(false)
        , status_(SLEEPING)
        {}

    ~worker_handle() {
        shutdown();
    }

    void shutdown() {
        stopping_ = true;
        if (thread_) {
            if (thread_->joinable()) {
                ea_.invoke();
                thread_->join();
            }
            thread_.reset();
        }
    }

    enum status {
        SLEEPING = 0,
        WORKING = 1,
    };

    size_t id_;
    EventAwaiter ea_;
    ptr<std::thread> thread_;
    bool stopping_;
    std::atomic<status> status_;
};

nuraft_global_mgr::nuraft_global_mgr()
    : asio_service_(nullptr)
    , thread_id_counter_(0)
    {}

nuraft_global_mgr::~nuraft_global_mgr() {
    for (auto& entry: append_workers_) {
        ptr<worker_handle>& wh = entry;
        wh->shutdown();
    }
    append_workers_.clear();

    for (auto& entry: commit_workers_) {
        ptr<worker_handle>& wh = entry;
        wh->shutdown();
    }
    commit_workers_.clear();
}

nuraft_global_mgr* nuraft_global_mgr::init(const nuraft_global_config& config) {
    nuraft_global_mgr* mgr = instance_.load();
    if (!mgr) {
        std::lock_guard<std::mutex> l(instance_lock_);
        mgr = instance_.load();
        if (!mgr) {
            mgr = new nuraft_global_mgr();
            instance_.store(mgr);
            mgr->config_ = config;
            mgr->init_thread_pool();
        }
    }
    return mgr;
}

void nuraft_global_mgr::shutdown() {
    std::lock_guard<std::mutex> l(instance_lock_);
    nuraft_global_mgr* mgr = instance_.load();
    if (mgr) {
        delete mgr;
        instance_.store(nullptr);
    }
}

nuraft_global_mgr* nuraft_global_mgr::get_instance() {
    return instance_.load();
}

void nuraft_global_mgr::init_thread_pool() {
    for (size_t ii = 0; ii < config_.num_commit_threads_; ++ii) {
        ptr<worker_handle> w_hdl =
            cs_new<worker_handle>( thread_id_counter_.fetch_add(1) );
        w_hdl->thread_ = cs_new<std::thread>( &nuraft_global_mgr::commit_worker_loop,
                                              this,
                                              w_hdl );
        commit_workers_.push_back(w_hdl);
    }

    for (size_t ii = 0; ii < config_.num_append_threads_; ++ii) {
        ptr<worker_handle> w_hdl =
            cs_new<worker_handle>( thread_id_counter_.fetch_add(1) );
        w_hdl->thread_ = cs_new<std::thread>( &nuraft_global_mgr::append_worker_loop,
                                              this,
                                              w_hdl );
        append_workers_.push_back(w_hdl);
    }
}

void nuraft_global_mgr::init_raft_server(raft_server* server) {
    ptr<logger>& l_ = server->l_;
    p_in("global manager detected, %zu commit workers, %zu append workers",
         config_.num_commit_threads_,
         config_.num_append_threads_);
}

void nuraft_global_mgr::close_raft_server(raft_server* server) {
    // Cancel all requests for this raft server.
    size_t num_aborted_append = 0;
    {
        std::lock_guard<std::mutex> l(append_queue_lock_);
        auto entry = append_queue_.begin();
        while (entry != append_queue_.end()) {
            if (entry->get() == server) {
                append_server_set_.erase(*entry);
                entry = append_queue_.erase(entry);
                num_aborted_append++;
                break;
            } else {
                entry++;
            }
        }
    }

    size_t num_aborted_commit = 0;
    {
        std::lock_guard<std::mutex> l(commit_queue_lock_);
        auto entry = commit_queue_.begin();
        while (entry != commit_queue_.end()) {
            if (entry->get() == server) {
                commit_server_set_.erase(*entry);
                entry = commit_queue_.erase(entry);
                num_aborted_commit++;
                break;
            } else {
                entry++;
            }
        }
    }

    ptr<logger>& l_ = server->l_;
    p_in("global manager detected, %zu appends %zu commits are aborted",
         num_aborted_append,
         num_aborted_commit);
}

void nuraft_global_mgr::request_append(ptr<raft_server> server) {
    {
        std::lock_guard<std::mutex> l(append_queue_lock_);
        // First search the set if the server is duplicate.
        auto entry = append_server_set_.find(server);
        if (entry != append_server_set_.end()) {
            // `server` is already in the queue. Ignore it.
            return;
        }

        // Put into queue.
        append_queue_.push_back(server);
        append_server_set_.insert(server);

        ptr<logger>& l_ = server->l_;
        p_tr("added append request to global queue, "
             "server %p, queue length %zu",
             server.get(),
             append_queue_.size());
    }

    // Find a sleeping worker and invoke.
    for (auto& entry: append_workers_) {
        ptr<worker_handle>& wh = entry;
        if (wh->status_ == worker_handle::SLEEPING) {
            wh->ea_.invoke();
            break;
        }
    }
    // If all workers are working, nothing to do for now.
}

void nuraft_global_mgr::request_commit(ptr<raft_server> server) {
    {
        std::lock_guard<std::mutex> l(commit_queue_lock_);
        // First search the set if the server is duplicate.
        auto entry = commit_server_set_.find(server);
        if (entry != commit_server_set_.end()) {
            // `server` is already in the queue. Ignore it.
            return;
        }

        // Put into queue.
        commit_queue_.push_back(server);
        commit_server_set_.insert(server);

        ptr<logger>& l_ = server->l_;
        p_tr("added commit request to global queue, "
             "server %p, queue length %zu",
             server.get(),
             commit_queue_.size());
    }

    // Find a sleeping worker and invoke.
    for (auto& entry: commit_workers_) {
        ptr<worker_handle>& wh = entry;
        if (wh->status_ == worker_handle::SLEEPING) {
            wh->ea_.invoke();
            break;
        }
    }
    // If all workers are working, nothing to do for now.
}

void nuraft_global_mgr::commit_worker_loop(ptr<worker_handle> handle) {
    std::string thread_name = "nuraft_g_c" + std::to_string(handle->id_);
#ifdef __linux__
    pthread_setname_np(pthread_self(), thread_name.c_str());
#elif __APPLE__
    pthread_setname_np(thread_name.c_str());
#endif

    bool skip_sleeping = false;
    while (!handle->stopping_) {
        if (!skip_sleeping) {
            handle->status_ = worker_handle::SLEEPING;
            // Wake up for every 1 second even without invoke, just in case.
            handle->ea_.wait_ms(1000);
            handle->ea_.reset();
            handle->status_ = worker_handle::WORKING;
        }
        if (handle->stopping_) break;

        skip_sleeping = false;
        size_t queue_length = 0;
        ptr<raft_server> target = nullptr;
        {
            std::lock_guard<std::mutex> l(commit_queue_lock_);
            auto entry = commit_queue_.begin();
            if (entry != commit_queue_.end()) {
                target = *entry;
                commit_server_set_.erase(target);
                commit_queue_.pop_front();
                queue_length = commit_queue_.size();
                if (!commit_queue_.empty()) {
                    // Other requests are waiting in the queue,
                    // skip sleeping next time.
                    skip_sleeping = true;
                }
            }
        }
        if (!target) continue;

        if ( target->quick_commit_index_ <= target->sm_commit_index_ ||
             target->log_store_->next_slot() - 1 <= target->sm_commit_index_ ) {
            // State machine's commit index is large enough not to execute commit
            // (see the comment in `commit_in_bg()`).
            continue;
        }

        ptr<logger>& l_ = target->l_;
        p_tr("executed commit by global worker, queue length %zu", queue_length);
        bool finished_in_time =
            target->commit_in_bg_exec(config_.max_scheduling_unit_ms_);
        if (!finished_in_time) {
            // Commit took too long time and aborted in the middle.
            // Put this server to queue again.
            p_tr("couldn't finish in time (%zu ms), re-push to queue",
                 config_.max_scheduling_unit_ms_);
            request_commit(target);
            skip_sleeping = true;
        }
    }
}

void nuraft_global_mgr::append_worker_loop(ptr<worker_handle> handle) {
    std::string thread_name = "nuraft_g_a" + std::to_string(handle->id_);
#ifdef __linux__
    pthread_setname_np(pthread_self(), thread_name.c_str());
#elif __APPLE__
    pthread_setname_np(thread_name.c_str());
#endif

    bool skip_sleeping = false;
    while (!handle->stopping_) {
        if (!skip_sleeping) {
            handle->status_ = worker_handle::SLEEPING;
            // Ditto, just in case.
            handle->ea_.wait_ms(1000);
            handle->ea_.reset();
            handle->status_ = worker_handle::WORKING;
        }
        if (handle->stopping_) break;

        skip_sleeping = false;
        size_t queue_length = 0;
        ptr<raft_server> target = nullptr;
        {
            std::lock_guard<std::mutex> l(append_queue_lock_);
            auto entry = append_queue_.begin();
            if (entry != append_queue_.end()) {
                target = *entry;
                append_server_set_.erase(target);
                append_queue_.pop_front();
                queue_length = append_queue_.size();
                if (!append_queue_.empty()) {
                    // Other requests are waiting in the queue,
                    // skip sleeping next time.
                    skip_sleeping = true;
                }
            }
        }
        if (!target) continue;

        ptr<logger>& l_ = target->l_;
        p_tr("executed append_entries by global worker, queue length %zu",
             queue_length);
        target->append_entries_in_bg_exec();
    }
}

} // namespace nuraft;

