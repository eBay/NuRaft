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

#include "file_based_log_store.hxx"

#include "nuraft.hxx"

#include <cassert>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace nuraft {

file_based_log_store::file_based_log_store(const std::string& log_dir, int server_id)
    : log_dir_(log_dir)
    , server_id_(server_id)
    , start_idx_(1)
    , last_durable_idx_(0)
{
    // Create server-specific subdirectory
    std::string server_dir = log_dir_ + "/server_" + std::to_string(server_id_);
    mkdir(server_dir.c_str(), 0755);

    // Load existing log entries from disk
    load_from_disk();

    // Ensure we have at least the dummy entry at index 0
    {
        std::lock_guard<std::mutex> l(logs_lock_);
        if (logs_.find(0) == logs_.end()) {
            ptr<buffer> buf = buffer::alloc(sz_ulong);
            logs_[0] = cs_new<log_entry>(0, buf);
        }
    }
}

file_based_log_store::~file_based_log_store() {
    close();
}

ptr<log_entry> file_based_log_store::make_clone(const ptr<log_entry>& entry) {
    // NOTE:
    //   Timestamp is used only when `replicate_log_timestamp_` option is on.
    //   Otherwise, log store does not need to store or load it.
    ptr<log_entry> clone = cs_new<log_entry>
                           ( entry->get_term(),
                             buffer::clone( entry->get_buf() ),
                             entry->get_val_type(),
                             entry->get_timestamp(),
                             entry->has_crc32(),
                             entry->get_crc32(),
                             false );
    return clone;
}

std::string file_based_log_store::get_file_path(ulong index) const {
    // Format: <log_dir>/server_<server_id>/<index>.log
    return log_dir_ + "/server_" + std::to_string(server_id_) + "/" +
           std::to_string(index) + ".log";
}

void file_based_log_store::load_from_disk() {
    std::string server_dir = log_dir_ + "/server_" + std::to_string(server_id_);
    DIR* dir = opendir(server_dir.c_str());
    if (!dir) {
        // Directory doesn't exist yet, nothing to load
        return;
    }

    struct dirent* entry;
    std::vector<ulong> indices;

    // Scan directory for log files
    while ((entry = readdir(dir)) != nullptr) {
        std::string name(entry->d_name);
        if (name.length() > 4 && name.substr(name.length() - 4) == ".log") {
            try {
                ulong index = std::stoull(name.substr(0, name.length() - 4));
                indices.push_back(index);
            } catch (...) {
                // Skip invalid file names
                continue;
            }
        }
    }
    closedir(dir);

    // Sort indices
    std::sort(indices.begin(), indices.end());

    // Load each log entry
    for (ulong index : indices) {
        ptr<log_entry> entry = read_from_disk(index);
        if (entry) {
            std::lock_guard<std::mutex> l(logs_lock_);
            logs_[index] = entry;
        }
    }

    // Set start_idx_ to the first non-zero index
    {
        std::lock_guard<std::mutex> l(logs_lock_);
        if (logs_.size() > 1) {  // More than just dummy entry
            auto entry = logs_.upper_bound(0);
            if (entry != logs_.end()) {
                start_idx_ = entry->first;
            }
        }
        last_durable_idx_ = logs_.size() > 0 ? logs_.rbegin()->first : 0;
    }
}

bool file_based_log_store::write_to_disk(ulong index, ptr<log_entry>& entry) {
    std::string file_path = get_file_path(index);
    std::ofstream outfile(file_path, std::ios::binary);
    if (!outfile.is_open()) {
        return false;
    }

    try {
        ptr<buffer> buf = entry->serialize();
        outfile.write(reinterpret_cast<const char*>(buf->data_begin()), buf->size());
        outfile.close();
        return true;
    } catch (...) {
        return false;
    }
}

void file_based_log_store::delete_from_disk(ulong index) {
    std::string file_path = get_file_path(index);
    unlink(file_path.c_str());
}

ptr<log_entry> file_based_log_store::read_from_disk(ulong index) {
    std::string file_path = get_file_path(index);
    std::ifstream infile(file_path, std::ios::binary | std::ios::ate);
    if (!infile.is_open()) {
        return nullptr;
    }

    try {
        std::streamsize size = infile.tellg();
        infile.seekg(0, std::ios::beg);

        ptr<buffer> buf = buffer::alloc(size);
        infile.read(reinterpret_cast<char*>(buf->data_begin()), size);
        infile.close();

        return log_entry::deserialize(*buf);
    } catch (...) {
        return nullptr;
    }
}

ulong file_based_log_store::next_slot() const {
    std::lock_guard<std::mutex> l(logs_lock_);
    // Exclude the dummy entry.
    return start_idx_ + logs_.size() - 1;
}

ulong file_based_log_store::start_index() const {
    return start_idx_;
}

ptr<log_entry> file_based_log_store::last_entry() const {
    ulong next_idx = next_slot();
    std::lock_guard<std::mutex> l(logs_lock_);
    auto entry = logs_.find( next_idx - 1 );
    if (entry == logs_.end()) {
        entry = logs_.find(0);
    }

    return make_clone(entry->second);
}

ulong file_based_log_store::append(ptr<log_entry>& entry) {
    ptr<log_entry> clone = make_clone(entry);

    std::lock_guard<std::mutex> l(logs_lock_);
    size_t idx = start_idx_ + logs_.size() - 1;
    logs_[idx] = clone;

    // Write to disk
    write_to_disk(idx, clone);
    last_durable_idx_ = idx;

    return idx;
}

void file_based_log_store::write_at(ulong index, ptr<log_entry>& entry) {
    ptr<log_entry> clone = make_clone(entry);

    // Discard all logs equal to or greater than `index`.
    std::lock_guard<std::mutex> l(logs_lock_);
    auto itr = logs_.lower_bound(index);
    std::vector<ulong> to_delete;
    while (itr != logs_.end()) {
        to_delete.push_back(itr->first);
        itr = logs_.erase(itr);
    }
    logs_[index] = clone;

    // Update disk
    write_to_disk(index, clone);
    for (ulong idx : to_delete) {
        delete_from_disk(idx);
    }
    last_durable_idx_ = index;
}

ptr< std::vector< ptr<log_entry> > >
    file_based_log_store::log_entries(ulong start, ulong end)
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
    file_based_log_store::log_entries_ext(ulong start,
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

ptr<log_entry> file_based_log_store::entry_at(ulong index) {
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

ulong file_based_log_store::term_at(ulong index) {
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

ptr<buffer> file_based_log_store::pack(ulong index, int32 cnt) {
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

void file_based_log_store::apply_pack(ulong index, buffer& pack) {
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
        // Write to disk
        write_to_disk(cur_idx, le);
    }

    {   std::lock_guard<std::mutex> l(logs_lock_);
        auto entry = logs_.upper_bound(0);
        if (entry != logs_.end()) {
            start_idx_ = entry->first;
        } else {
            start_idx_ = 1;
        }
        // Update last durable index
        if (!logs_.empty()) {
            last_durable_idx_ = logs_.rbegin()->first;
        }
    }
}

bool file_based_log_store::compact(ulong last_log_index) {
    std::lock_guard<std::mutex> l(logs_lock_);
    for (ulong ii = start_idx_; ii <= last_log_index; ++ii) {
        auto entry = logs_.find(ii);
        if (entry != logs_.end()) {
            logs_.erase(entry);
            // Delete from disk
            delete_from_disk(ii);
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

bool file_based_log_store::flush() {
    // Already flushed on each write in this simple implementation
    last_durable_idx_ = next_slot() - 1;
    return true;
}

void file_based_log_store::close() {
    flush();
}

ulong file_based_log_store::last_durable_index() {
    return last_durable_idx_.load();
}

}
