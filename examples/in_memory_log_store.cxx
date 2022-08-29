/************************************************************************
Copyright 2017-2019 eBay Inc.
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

#include "in_memory_log_store.hxx"

#include "nuraft.hxx"

#include <cassert>

namespace nuraft {

inmem_log_store::inmem_log_store()
    : start_idx_(1)
    , raft_server_bwd_pointer_(nullptr)
    , disk_emul_delay(0)
    , disk_emul_thread_(nullptr)
    , disk_emul_thread_stop_signal_(false)
    , disk_emul_last_durable_index_(0)
{
    // Dummy entry for index 0.
    ptr<buffer> buf = buffer::alloc(sz_ulong);
    logs_[0] = cs_new<log_entry>(0, buf);
}

inmem_log_store::~inmem_log_store() {
    if (disk_emul_thread_) {
        disk_emul_thread_stop_signal_ = true;
        disk_emul_ea_.invoke();
        if (disk_emul_thread_->joinable()) {
            disk_emul_thread_->join();
        }
    }
}

ptr<log_entry> inmem_log_store::make_clone(const ptr<log_entry>& entry) {
    // NOTE:
    //   Timestamp is used only when `replicate_log_timestamp_` option is on.
    //   Otherwise, log store does not need to store or load it.
    ptr<log_entry> clone = cs_new<log_entry>
                           ( entry->get_term(),
                             buffer::clone( entry->get_buf() ),
                             entry->get_val_type(),
                             entry->get_timestamp() );
    return clone;
}

ulong inmem_log_store::next_slot() const {
    std::lock_guard<std::mutex> l(logs_lock_);
    // Exclude the dummy entry.
    return start_idx_ + logs_.size() - 1;
}

ulong inmem_log_store::start_index() const {
    return start_idx_;
}

ptr<log_entry> inmem_log_store::last_entry() const {
    ulong next_idx = next_slot();
    std::lock_guard<std::mutex> l(logs_lock_);
    auto entry = logs_.find( next_idx - 1 );
    if (entry == logs_.end()) {
        entry = logs_.find(0);
    }

    return make_clone(entry->second);
}

ulong inmem_log_store::append(ptr<log_entry>& entry) {
    ptr<log_entry> clone = make_clone(entry);

    std::lock_guard<std::mutex> l(logs_lock_);
    size_t idx = start_idx_ + logs_.size() - 1;
    logs_[idx] = clone;

    if (disk_emul_delay) {
        uint64_t cur_time = timer_helper::get_timeofday_us();
        disk_emul_logs_being_written_[cur_time + disk_emul_delay * 1000] = idx;
        disk_emul_ea_.invoke();
    }

    return idx;
}

void inmem_log_store::write_at(ulong index, ptr<log_entry>& entry) {
    ptr<log_entry> clone = make_clone(entry);

    // Discard all logs equal to or greater than `index.
    std::lock_guard<std::mutex> l(logs_lock_);
    auto itr = logs_.lower_bound(index);
    while (itr != logs_.end()) {
        itr = logs_.erase(itr);
    }
    logs_[index] = clone;

    if (disk_emul_delay) {
        uint64_t cur_time = timer_helper::get_timeofday_us();
        disk_emul_logs_being_written_[cur_time + disk_emul_delay * 1000] = index;

        // Remove entries greater than `index`.
        auto entry = disk_emul_logs_being_written_.begin();
        while (entry != disk_emul_logs_being_written_.end()) {
            if (entry->second > index) {
                entry = disk_emul_logs_being_written_.erase(entry);
            } else {
                entry++;
            }
        }
        disk_emul_ea_.invoke();
    }
}

ptr< std::vector< ptr<log_entry> > >
    inmem_log_store::log_entries(ulong start, ulong end)
{
    ptr< std::vector< ptr<log_entry> > > ret =
        cs_new< std::vector< ptr<log_entry> > >();

    ret->resize(end - start);
    ulong cc=0;
    for (ulong ii = start ; ii < end ; ++ii) {
        ptr<log_entry> src = nullptr;
        {   std::lock_guard<std::mutex> l(logs_lock_);
            auto entry = logs_.find(ii);
            if (entry == logs_.end()) {
                entry = logs_.find(0);
                assert(0);
            }
            src = entry->second;
        }
        (*ret)[cc++] = make_clone(src);
    }
    return ret;
}

ptr<std::vector<ptr<log_entry>>>
    inmem_log_store::log_entries_ext(ulong start,
                                     ulong end,
                                     int64 batch_size_hint_in_bytes)
{
    ptr< std::vector< ptr<log_entry> > > ret =
        cs_new< std::vector< ptr<log_entry> > >();

    if (batch_size_hint_in_bytes < 0) {
        return ret;
    }

    size_t accum_size = 0;
    for (ulong ii = start ; ii < end ; ++ii) {
        ptr<log_entry> src = nullptr;
        {   std::lock_guard<std::mutex> l(logs_lock_);
            auto entry = logs_.find(ii);
            if (entry == logs_.end()) {
                entry = logs_.find(0);
                assert(0);
            }
            src = entry->second;
        }
        ret->push_back(make_clone(src));
        accum_size += src->get_buf().size();
        if (batch_size_hint_in_bytes &&
            accum_size >= (ulong)batch_size_hint_in_bytes) break;
    }
    return ret;
}

ptr<log_entry> inmem_log_store::entry_at(ulong index) {
    ptr<log_entry> src = nullptr;
    {   std::lock_guard<std::mutex> l(logs_lock_);
        auto entry = logs_.find(index);
        if (entry == logs_.end()) {
            entry = logs_.find(0);
        }
        src = entry->second;
    }
    return make_clone(src);
}

ulong inmem_log_store::term_at(ulong index) {
    ulong term = 0;
    {   std::lock_guard<std::mutex> l(logs_lock_);
        auto entry = logs_.find(index);
        if (entry == logs_.end()) {
            entry = logs_.find(0);
        }
        term = entry->second->get_term();
    }
    return term;
}

ptr<buffer> inmem_log_store::pack(ulong index, int32 cnt) {
    std::vector< ptr<buffer> > logs;

    size_t size_total = 0;
    for (ulong ii=index; ii<index+cnt; ++ii) {
        ptr<log_entry> le = nullptr;
        {   std::lock_guard<std::mutex> l(logs_lock_);
            le = logs_[ii];
        }
        assert(le.get());
        ptr<buffer> buf = le->serialize();
        size_total += buf->size();
        logs.push_back( buf );
    }

    ptr<buffer> buf_out = buffer::alloc
                          ( sizeof(int32) +
                            cnt * sizeof(int32) +
                            size_total );
    buf_out->pos(0);
    buf_out->put((int32)cnt);

    for (auto& entry: logs) {
        ptr<buffer>& bb = entry;
        buf_out->put((int32)bb->size());
        buf_out->put(*bb);
    }
    return buf_out;
}

void inmem_log_store::apply_pack(ulong index, buffer& pack) {
    pack.pos(0);
    int32 num_logs = pack.get_int();

    for (int32 ii=0; ii<num_logs; ++ii) {
        ulong cur_idx = index + ii;
        int32 buf_size = pack.get_int();

        ptr<buffer> buf_local = buffer::alloc(buf_size);
        pack.get(buf_local);

        ptr<log_entry> le = log_entry::deserialize(*buf_local);
        {   std::lock_guard<std::mutex> l(logs_lock_);
            logs_[cur_idx] = le;
        }
    }

    {   std::lock_guard<std::mutex> l(logs_lock_);
        auto entry = logs_.upper_bound(0);
        if (entry != logs_.end()) {
            start_idx_ = entry->first;
        } else {
            start_idx_ = 1;
        }
    }
}

bool inmem_log_store::compact(ulong last_log_index) {
    std::lock_guard<std::mutex> l(logs_lock_);
    for (ulong ii = start_idx_; ii <= last_log_index; ++ii) {
        auto entry = logs_.find(ii);
        if (entry != logs_.end()) {
            logs_.erase(entry);
        }
    }

    // WARNING:
    //   Even though nothing has been erased,
    //   we should set `start_idx_` to new index.
    if (start_idx_ <= last_log_index) {
        start_idx_ = last_log_index + 1;
    }
    return true;
}

bool inmem_log_store::flush() {
    disk_emul_last_durable_index_ = next_slot() - 1;
    return true;
}

void inmem_log_store::close() {}

void inmem_log_store::set_disk_delay(raft_server* raft, size_t delay_ms) {
    disk_emul_delay = delay_ms;
    raft_server_bwd_pointer_ = raft;

    if (!disk_emul_thread_) {
        disk_emul_thread_ =
            std::unique_ptr<std::thread>(
                new std::thread(&inmem_log_store::disk_emul_loop, this) );
    }
}

ulong inmem_log_store::last_durable_index() {
    uint64_t last_log = next_slot() - 1;
    if (!disk_emul_delay) {
        return last_log;
    }

    return disk_emul_last_durable_index_;
}

void inmem_log_store::disk_emul_loop() {
    // This thread mimics async disk writes.

    size_t next_sleep_us = 100 * 1000;
    while (!disk_emul_thread_stop_signal_) {
        disk_emul_ea_.wait_us(next_sleep_us);
        disk_emul_ea_.reset();
        if (disk_emul_thread_stop_signal_) break;

        uint64_t cur_time = timer_helper::get_timeofday_us();
        next_sleep_us = 100 * 1000;

        bool call_notification = false;
        {
            std::lock_guard<std::mutex> l(logs_lock_);
            // Remove all timestamps equal to or smaller than `cur_time`,
            // and pick the greatest one among them.
            auto entry = disk_emul_logs_being_written_.begin();
            while (entry != disk_emul_logs_being_written_.end()) {
                if (entry->first <= cur_time) {
                    disk_emul_last_durable_index_ = entry->second;
                    entry = disk_emul_logs_being_written_.erase(entry);
                    call_notification = true;
                } else {
                    break;
                }
            }

            entry = disk_emul_logs_being_written_.begin();
            if (entry != disk_emul_logs_being_written_.end()) {
                next_sleep_us = entry->first - cur_time;
            }
        }

        if (call_notification) {
            raft_server_bwd_pointer_->notify_log_append_completion(true);
        }
    }
}

}
