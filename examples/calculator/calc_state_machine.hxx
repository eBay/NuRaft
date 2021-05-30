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

#include "nuraft.hxx"

#include <atomic>
#include <cassert>
#include <mutex>

#include <string.h>

using namespace nuraft;

namespace calc_server {

class calc_state_machine : public state_machine {
public:
    calc_state_machine()
        : cur_value_(0)
        , last_committed_idx_(0)
        {}

    ~calc_state_machine() {}

    enum op_type : int {
        ADD = 0x0,
        SUB = 0x1,
        MUL = 0x2,
        DIV = 0x3,
        SET = 0x4
    };

    struct op_payload {
        op_type type_;
        int oprnd_;
    };

    static ptr<buffer> enc_log(const op_payload& payload) {
        // Encode from {operator, operand} to Raft log.
        ptr<buffer> ret = buffer::alloc(sizeof(op_payload));
        buffer_serializer bs(ret);

        // WARNING: We don't consider endian-safety in this example.
        bs.put_raw(&payload, sizeof(op_payload));

        return ret;
    }

    static void dec_log(buffer& log, op_payload& payload_out) {
        // Decode from Raft log to {operator, operand} pair.
        assert(log.size() == sizeof(op_payload));

        buffer_serializer bs(log);
        memcpy(&payload_out, bs.get_raw(log.size()), sizeof(op_payload));
    }

    ptr<buffer> pre_commit(const ulong log_idx, buffer& data) {
        // Nothing to do with pre-commit in this example.
        return nullptr;
    }

    ptr<buffer> commit(const ulong log_idx, buffer& data) {
        op_payload payload;
        dec_log(data, payload);

        int64_t prev_value = cur_value_;
        switch (payload.type_) {
        case ADD:   prev_value += payload.oprnd_;   break;
        case SUB:   prev_value -= payload.oprnd_;   break;
        case MUL:   prev_value *= payload.oprnd_;   break;
        case DIV:   prev_value /= payload.oprnd_;   break;
        default:
        case SET:   prev_value  = payload.oprnd_;   break;
        }
        cur_value_ = prev_value;

        last_committed_idx_ = log_idx;

        // Return Raft log number as a return result.
        ptr<buffer> ret = buffer::alloc( sizeof(log_idx) );
        buffer_serializer bs(ret);
        bs.put_u64(log_idx);
        return ret;
    }

    void commit_config(const ulong log_idx, ptr<cluster_config>& new_conf) {
        // Nothing to do with configuration change. Just update committed index.
        last_committed_idx_ = log_idx;
    }

    void rollback(const ulong log_idx, buffer& data) {
        // Nothing to do with rollback,
        // as this example doesn't do anything on pre-commit.
    }

    int read_logical_snp_obj(snapshot& s,
                             void*& user_snp_ctx,
                             ulong obj_id,
                             ptr<buffer>& data_out,
                             bool& is_last_obj)
    {
        ptr<snapshot_ctx> ctx = nullptr;
        {   std::lock_guard<std::mutex> ll(snapshots_lock_);
            auto entry = snapshots_.find(s.get_last_log_idx());
            if (entry == snapshots_.end()) {
                // Snapshot doesn't exist.
                data_out = nullptr;
                is_last_obj = true;
                return 0;
            }
            ctx = entry->second;
        }

        if (obj_id == 0) {
            // Object ID == 0: first object, put dummy data.
            data_out = buffer::alloc( sizeof(int32) );
            buffer_serializer bs(data_out);
            bs.put_i32(0);
            is_last_obj = false;

        } else {
            // Object ID > 0: second object, put actual value.
            data_out = buffer::alloc( sizeof(ulong) );
            buffer_serializer bs(data_out);
            bs.put_u64( ctx->value_ );
            is_last_obj = true;
        }
        return 0;
    }

    void save_logical_snp_obj(snapshot& s,
                              ulong& obj_id,
                              buffer& data,
                              bool is_first_obj,
                              bool is_last_obj)
    {
        if (obj_id == 0) {
            // Object ID == 0: it contains dummy value, create snapshot context.
            create_snapshot_internal(s);

        } else {
            // Object ID > 0: actual snapshot value.
            buffer_serializer bs(data);
            int64_t local_value = (int64_t)bs.get_u64();

            std::lock_guard<std::mutex> ll(snapshots_lock_);
            auto entry = snapshots_.find(s.get_last_log_idx());
            assert(entry != snapshots_.end());
            entry->second->value_ = local_value;
        }
        // Request next object.
        obj_id++;
    }

    bool apply_snapshot(snapshot& s) {
        std::lock_guard<std::mutex> ll(snapshots_lock_);
        auto entry = snapshots_.find(s.get_last_log_idx());
        if (entry == snapshots_.end()) return false;

        ptr<snapshot_ctx> ctx = entry->second;
        cur_value_ = ctx->value_;
        return true;
    }

    void free_user_snp_ctx(void*& user_snp_ctx) {
        // In this example, `read_logical_snp_obj` doesn't create
        // `user_snp_ctx`. Nothing to do in this function.
    }

    ptr<snapshot> last_snapshot() {
        // Just return the latest snapshot.
        std::lock_guard<std::mutex> ll(snapshots_lock_);
        auto entry = snapshots_.rbegin();
        if (entry == snapshots_.rend()) return nullptr;

        ptr<snapshot_ctx> ctx = entry->second;
        return ctx->snapshot_;
    }

    ulong last_commit_index() {
        return last_committed_idx_;
    }

    void create_snapshot(snapshot& s,
                         async_result<bool>::handler_type& when_done)
    {
        {   std::lock_guard<std::mutex> ll(snapshots_lock_);
            create_snapshot_internal(s);
        }
        ptr<std::exception> except(nullptr);
        bool ret = true;
        when_done(ret, except);
    }

    int64_t get_current_value() const { return cur_value_; }

private:
    struct snapshot_ctx {
        snapshot_ctx( ptr<snapshot>& s, int64_t v )
            : snapshot_(s), value_(v) {}
        ptr<snapshot> snapshot_;
        int64_t value_;
    };

    void create_snapshot_internal(snapshot& s) {
        // Clone snapshot from `s`.
        ptr<buffer> snp_buf = s.serialize();
        ptr<snapshot> ss = snapshot::deserialize(*snp_buf);

        // Put into snapshot map.
        ptr<snapshot_ctx> ctx = cs_new<snapshot_ctx>(ss, cur_value_);
        snapshots_[s.get_last_log_idx()] = ctx;

        // Maintain last 3 snapshots only.
        const int MAX_SNAPSHOTS = 3;
        int num = snapshots_.size();
        auto entry = snapshots_.begin();
        for (int ii = 0; ii < num - MAX_SNAPSHOTS; ++ii) {
            if (entry == snapshots_.end()) break;
            entry = snapshots_.erase(entry);
        }
    }

    // State machine's current value.
    std::atomic<int64_t> cur_value_;

    // Last committed Raft log number.
    std::atomic<uint64_t> last_committed_idx_;

    // Keeps the last 3 snapshots, by their Raft log numbers.
    std::map< uint64_t, ptr<snapshot_ctx> > snapshots_;

    // Mutex for `snapshots_`.
    std::mutex snapshots_lock_;
};

}; // namespace calc_server

