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
#include <fstream>
#include <map>
#include <mutex>
#include <string>

namespace nuraft {

class raft_server;

/**
 * Simple file-based persistent log store implementation.
 *
 * This is a naive implementation that persists log entries to individual files
 * in a specified directory. Each log entry is stored as a separate file named
 * by its index (e.g., "1.log", "2.log", etc.).
 *
 * This is intended as a minimal example to demonstrate how to make a log store
 * persistent. For production use, consider using more sophisticated storage
 * engines (e.g., RocksDB, LevelDB) or implementing WAL (Write-Ahead Log) with
 * compaction.
 */
class file_based_log_store : public log_store {
public:
    /**
     * Constructor.
     *
     * @param log_dir Directory where log files will be stored.
     * @param server_id Server ID (used to create unique subdirectory).
     */
    file_based_log_store(const std::string& log_dir, int server_id);

    ~file_based_log_store();

    __nocopy__(file_based_log_store);

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

    bool flush();

    void close();

    ulong last_durable_index();

private:
    /**
     * Load log entries from disk on startup.
     * Rebuilds the in-memory index and determines the start index.
     */
    void load_from_disk();

    /**
     * Write a log entry to disk.
     * @param index Log index.
     * @param entry Log entry to write.
     * @return true if successful, false otherwise.
     */
    bool write_to_disk(ulong index, ptr<log_entry>& entry);

    /**
     * Delete a log entry file from disk.
     * @param index Log index.
     */
    void delete_from_disk(ulong index);

    /**
     * Read a log entry from disk.
     * @param index Log index.
     * @return Log entry, or nullptr if not found.
     */
    ptr<log_entry> read_from_disk(ulong index);

    /**
     * Get the file path for a given log index.
     * @param index Log index.
     * @return Full file path.
     */
    std::string get_file_path(ulong index) const;

    /**
     * Make a clone of a log entry (same as inmem version).
     */
    static ptr<log_entry> make_clone(const ptr<log_entry>& entry);

private:
    /**
     * Directory where log files are stored.
     */
    std::string log_dir_;

    /**
     * Server ID.
     */
    int server_id_;

    /**
     * In-memory cache of log entries for faster access.
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
     * Last flushed index to disk.
     */
    std::atomic<ulong> last_durable_idx_;
};

}
