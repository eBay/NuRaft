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

#pragma once

#include "event_awaiter.hxx"
#include "internal_timer.hxx"
#include "log_store.hxx"

#include <atomic>
#include <map>
#include <mutex>

namespace nuraft {

class raft_server;

class inmem_log_store : public log_store {
public:
    inmem_log_store();

    ~inmem_log_store();

    __nocopy__(inmem_log_store);

public:
    ulong next_slot() const;

    ulong start_index() const;

    ptr<log_entry> last_entry() const;

    ulong append(ptr<log_entry>& entry);

    void write_at(ulong index, ptr<log_entry>& entry);

    ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end);

    ptr<std::vector<ptr<log_entry>>> log_entries_ext(
            ulong start, ulong end, int64 batch_size_hint_in_bytes = 0);

    ptr<log_entry> entry_at(ulong index);

    ulong term_at(ulong index);

    ptr<buffer> pack(ulong index, int32 cnt);

    void apply_pack(ulong index, buffer& pack);

    bool compact(ulong last_log_index);

    bool is_conf(ulong index);

    bool flush();

    void close();

    ulong last_durable_index();

    void set_disk_delay(raft_server* raft, size_t delay_ms);

private:
    static ptr<log_entry> make_clone(const ptr<log_entry>& entry);

    void disk_emul_loop();

    /**
     * Map of <log index, log data>.
     */
    std::map<ulong, ptr<log_entry>> logs_;

    /**
     * Lock for `logs_`.
     */
    mutable std::mutex logs_lock_;

    /**
     * The index of the first log.
     */
    std::atomic<ulong> start_idx_;

    /**
     * Backward pointer to Raft server.
     */
    raft_server* raft_server_bwd_pointer_;

    // Testing purpose --------------- BEGIN

    /**
     * If non-zero, this log store will emulate the disk write delay.
     */
    std::atomic<size_t> disk_emul_delay;

    /**
     * Map of <timestamp, log index>, emulating logs that is being written to disk.
     * Log index will be regarded as "durable" after the corresponding timestamp.
     */
    std::map<uint64_t, uint64_t> disk_emul_logs_being_written_;

    /**
     * Thread that will update `last_durable_index_` and call
     * `notify_log_append_completion` at proper time.
     */
    std::unique_ptr<std::thread> disk_emul_thread_;

    /**
     * Flag to terminate the thread.
     */
    std::atomic<bool> disk_emul_thread_stop_signal_;

    /**
     * Event awaiter that emulates disk delay.
     */
    EventAwaiter disk_emul_ea_;

    /**
     * Last written log index.
     */
    std::atomic<uint64_t> disk_emul_last_durable_index_;

    // Testing purpose --------------- END
};

}

