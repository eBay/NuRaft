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

#include "include/scheduler.hxx"
#include "include/task.hxx"
#include "nuraft.hxx"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>

#include <string.h>
#include <cstring>

using namespace nuraft;

namespace task_scheduler {

class task_scheduler_state_machine : public state_machine {
public:
    task_scheduler_state_machine(bool async_snapshot = false)
        : scheduler_(Scheduler::create_fairness_scheduler(2.0))
        , last_committed_idx_(0)
        , async_snapshot_(async_snapshot)
        , last_snapshot_(nullptr) {}

    ~task_scheduler_state_machine() = default;

    enum cmd_type: std::uint8_t{

        TASK_ADD = 0x0,
        TASK_FETCH = 0x1,
        TASK_COMPLETE=0x2,
    };
struct task_add_cmd {
    std::string task_name;
      double arrival_time;
      double expected_duration;
      double cpu_cores_usage;
      double memory_req;
      double deadline;
      int priority;
  };

    static ptr<buffer> enc_task_add(const std::string& task_name,  double arrival_time, double expected_duration,
                                   double cpu_cores_usage, double memory_req,
                                   double deadline, int priority) {
        // Use dynamic sizing - let buffer_serializer handle the size
        ptr<buffer> ret = buffer::alloc(4096); // Start with a reasonable size
        buffer_serializer bs(ret);

        bs.put_i8(TASK_ADD);
        bs.put_str(task_name);

        // Use safe double serialization - copy bytes directly
        uint64_t arrival_bits, duration_bits, cpu_bits, memory_bits, deadline_bits;
        std::memcpy(&arrival_bits, &arrival_time, sizeof(double));
        std::memcpy(&duration_bits, &expected_duration, sizeof(double));
        std::memcpy(&cpu_bits, &cpu_cores_usage, sizeof(double));
        std::memcpy(&memory_bits, &memory_req, sizeof(double));
        std::memcpy(&deadline_bits, &deadline, sizeof(double));

        bs.put_u64(arrival_bits);
        bs.put_u64(duration_bits);
        bs.put_u64(cpu_bits);
        bs.put_u64(memory_bits);
        bs.put_u64(deadline_bits);
        bs.put_i32(priority);

        return ret;
    }

    static void dec_task_add(buffer& log, task_add_cmd& cmd_out) {
        buffer_serializer bs(log);
        bs.get_i8(); // Skip command type

        // Extract task name
        cmd_out.task_name = bs.get_str();

        // Use safe double deserialization - copy bytes back
        uint64_t arrival_bits, duration_bits, cpu_bits, memory_bits, deadline_bits;
        arrival_bits = bs.get_u64();
        duration_bits = bs.get_u64();
        cpu_bits = bs.get_u64();
        memory_bits = bs.get_u64();
        deadline_bits = bs.get_u64();

        std::memcpy(&cmd_out.arrival_time, &arrival_bits, sizeof(double));
        std::memcpy(&cmd_out.expected_duration, &duration_bits, sizeof(double));
        std::memcpy(&cmd_out.cpu_cores_usage, &cpu_bits, sizeof(double));
        std::memcpy(&cmd_out.memory_req, &memory_bits, sizeof(double));
        std::memcpy(&cmd_out.deadline, &deadline_bits, sizeof(double));

        cmd_out.priority = bs.get_i32();
    }

    static ptr<buffer> enc_task_fetch( ) {
        size_t size = sizeof(cmd_type);
        ptr<buffer> ret = buffer::alloc(size);
        buffer_serializer bs(ret);

        bs.put_i8(TASK_FETCH);
        return ret;
    }



    static ptr<buffer> enc_task_complete( std::string task_id) {
        // Allocate reasonable size - let buffer_serializer handle the actual sizing
        size_t size = 4096;  // Same as other encoders
        ptr<buffer> ret = buffer::alloc(size);
        buffer_serializer bs(ret);

        bs.put_i8(TASK_COMPLETE);
        bs.put_str(task_id);

        return ret;
    }

    static void dec_task_complete(buffer& log, std::string& cmd_out) {
        buffer_serializer bs(log);
        bs.get_i8(); // Skip command type

        cmd_out = bs.get_str();
    }


    ptr<buffer> pre_commit(const ulong log_idx, buffer& data) {
        // Simplified pre-commit - just approve all operations for now
        // Avoid reading buffer here to prevent position conflicts with commit()
        return nullptr;
    }

    ptr<buffer> commit(const ulong log_idx, buffer& data) {
        std::lock_guard<std::mutex> lock(lock_);

        buffer_serializer bs(data);
        cmd_type cmd = static_cast<cmd_type>(bs.get_i8());

        switch (cmd) {
        case TASK_ADD: {
            task_add_cmd task_cmd;
            dec_task_add(data, task_cmd);
            scheduler_->add_new_task(task_cmd.task_name, task_cmd.arrival_time,
                                     task_cmd.expected_duration,
                                     task_cmd.cpu_cores_usage,
                                     task_cmd.memory_req,
                                     task_cmd.deadline,
                                     task_cmd.priority);
            break;
        }
        case TASK_FETCH: {
            // Get the next task and safely access its properties
            auto task = scheduler_->schedule_next();
            if (task.has_value()) {
                // Get task properties before potentially modifying the task
                std::string task_name = task.value().get_task_name();
                std::string task_uuid = task.value().get_id().to_string();

                std::cout << "Task '" << task_name << "' assigned, UUID: " << task_uuid << std::endl;
            } else {
                std::cout << "No tasks available to fetch" << std::endl;
            }
            break;
        }
        case TASK_COMPLETE:{
            std::string uuid;
            dec_task_complete(data, uuid);
            scheduler_->mark_complete(uuid);
            break;
        }
        default:
            // Unknown command
            break;
        }

        last_committed_idx_ = log_idx;

        // Return Raft log number as a return result.
        ptr<buffer> ret = buffer::alloc(sizeof(log_idx));
        buffer_serializer bs_ret(ret);
        bs_ret.put_u64(log_idx);
        return ret;
    }

    void commit_config(const ulong log_idx, ptr<cluster_config>& new_conf) {
        // Nothing to do with configuration change. Just update committed index.
        last_committed_idx_ = log_idx;
    }

    void rollback(const ulong log_idx, buffer& data) {
        // Nothing to do with rollback in this example.
    }

    ptr<snapshot> last_snapshot() {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        return last_snapshot_;
    }

    void save_snapshot_data(snapshot& snapshot, const ulong offset, buffer& data) {
        // In this example, we'll store snapshot data in memory
        // For a production system, you'd save to disk here
        // For now, this is a no-op as we handle the data in create_snapshot
    }

    bool apply_snapshot(snapshot& snapshot) {
        std::lock_guard<std::mutex> lock(lock_);

        try {
            // Get snapshot data size
            ulong snapshot_size = snapshot.size();
            if (snapshot_size == 0) {
                return false;
            }

            // Create buffer for snapshot data
            ptr<buffer> snp_buf = buffer::alloc(snapshot_size);
            buffer_serializer bs(snp_buf);

            // Read header
            ulong last_committed = bs.get_u64();
            uint32_t pending_tasks_count = bs.get_u32();

            // Clear current state
            scheduler_->clear_all_tasks();

            // Read and restore pending tasks
            std::unordered_map<std::string, Task> restored_tasks;
            for (uint32_t i = 0; i < pending_tasks_count; ++i) {
                uint32_t task_size = bs.get_u32();
                if (task_size > 0) {
                    // Create a buffer for just this task
                    ptr<buffer> task_buf = buffer::alloc(task_size);
                    // Read raw task data
                    void* task_data = bs.get_raw(task_size);
                    std::memcpy(task_buf->data_begin(), task_data, task_size);

                    // Deserialize task
                    Task task = deserialize_task(*task_buf);
                    restored_tasks.emplace(task.get_id().to_string(), std::move(task));
                }
            }

            // Restore tasks to scheduler
            scheduler_->restore_tasks(restored_tasks);

            // Read completed tasks count
            uint32_t completed_tasks_count = bs.get_u32();

            // Read and restore completed tasks
            std::vector<CompletedTaskInfo> completed_tasks;
            for (uint32_t i = 0; i < completed_tasks_count; ++i) {
                uint32_t task_size = bs.get_u32();
                if (task_size > 0) {
                    // Create a buffer for just this task
                    ptr<buffer> task_buf = buffer::alloc(task_size);
                    // Read raw task data
                    void* task_data = bs.get_raw(task_size);
                    std::memcpy(task_buf->data_begin(), task_data, task_size);

                    // Deserialize completed task
                    CompletedTaskInfo task_info = deserialize_completed_task(*task_buf);
                    completed_tasks.push_back(task_info);
                }
            }

            // Restore completed tasks to scheduler
            scheduler_->restore_completed_tasks(completed_tasks);

            // Update state
            last_committed_idx_ = last_committed;

            // Store snapshot reference
            {
                std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
                // For this example, we don't need to store the actual snapshot object
                // just clear the reference to indicate we have a snapshot
                last_snapshot_ = nullptr;
            }

            return true;

        } catch (const std::exception& ex) {
            std::cerr << "Failed to apply snapshot: " << ex.what() << std::endl;
            return false;
        }
    }

    ulong last_commit_index() { return last_committed_idx_; }

    void create_snapshot(
        snapshot& s,
        cmd_result<bool, std::shared_ptr<std::exception>>::handler_type& when_done) {
        std::lock_guard<std::mutex> lock(lock_);

        try {
            // Serialize all pending tasks
            auto all_tasks = scheduler_->get_all_tasks();

            // Calculate snapshot size
            size_t snapshot_size = sizeof(ulong) + sizeof(uint32_t) + sizeof(uint32_t);

            // Add size for each task
            for (const auto& pair : all_tasks) {
                auto task_buf = serialize_task(pair.second);
                snapshot_size += sizeof(uint32_t) + task_buf->size();  // Size prefix + task data
            }

            // Add size for completed tasks
            auto completed_tasks = scheduler_->get_completed_tasks();
            snapshot_size += sizeof(uint32_t);  // Number of completed tasks
            for (const auto& task_info : completed_tasks) {
                auto task_buf = serialize_completed_task(task_info);
                snapshot_size += sizeof(uint32_t) + task_buf->size();  // Size prefix + task data
            }

            // Create snapshot buffer
            ptr<buffer> snp_buf = buffer::alloc(snapshot_size);
            buffer_serializer bs(snp_buf);

            // Write header
            bs.put_u64(last_committed_idx_);
            bs.put_u32(static_cast<uint32_t>(all_tasks.size()));

            // Write pending tasks
            for (const auto& pair : all_tasks) {
                auto task_buf = serialize_task(pair.second);
                bs.put_u32(static_cast<uint32_t>(task_buf->size()));
                bs.put_raw(task_buf->data_begin(), task_buf->size());
            }

            // Write completed tasks count
            bs.put_u32(static_cast<uint32_t>(completed_tasks.size()));

            // Write completed tasks
            for (const auto& task_info : completed_tasks) {
                auto task_buf = serialize_completed_task(task_info);
                bs.put_u32(static_cast<uint32_t>(task_buf->size()));
                bs.put_raw(task_buf->data_begin(), task_buf->size());
            }

            // Create snapshot object (need cluster_config parameter)
            // For this example, we'll use a nullptr for config (NuRaft will handle it)
            ptr<cluster_config> empty_config;
            ptr<snapshot> new_snapshot = cs_new<snapshot>(last_committed_idx_, 0, empty_config, snp_buf->size());

            // Store snapshot reference
            {
                std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
                last_snapshot_ = new_snapshot;
            }

            // Set snapshot data on the passed-in snapshot reference
            // Note: NuRaft will handle setting the snapshot metadata

            // Call completion handler
            bool result = true;
            ptr<std::exception> e = nullptr;
            when_done(result, e);

        } catch (const std::exception& ex) {
            // Don't call handler for simplicity in this example
        }
    }

    void read_logical_snp_obj(snapshot& s,
                              void*& user_snp_ctx,
                              ulong obj_id,
                              std::function<void(ptr<buffer>&)> done) {
        // For this example, we handle all snapshot data in create_snapshot/apply_snapshot
        // This method is used for reading individual objects from a snapshot
        ptr<buffer> ret = nullptr;
        done(ret);
    }

    void save_logical_snp_obj(snapshot& s,
                              ulong& obj_id,
                              std::function<void(ptr<buffer>&)> done) {
        // For this example, we handle all snapshot data in create_snapshot/apply_snapshot
        // This method is used for saving individual objects to a snapshot
        ptr<buffer> ret = nullptr;
        done(ret);
    }

    void free_snp_ctx(void*& user_snp_ctx) {
        // Nothing to do.
    }

    // Accessor for scheduler
    Scheduler& get_scheduler() { return *scheduler_; }

    // Snapshot serialization helpers
    static ptr<buffer> serialize_task(const Task& task);
    static Task deserialize_task(buffer& data);
    static ptr<buffer> serialize_completed_task(const CompletedTaskInfo& info);
    static CompletedTaskInfo deserialize_completed_task(buffer& data);

private:
    std::unique_ptr<Scheduler> scheduler_;
    std::atomic<ulong> last_committed_idx_;
    bool async_snapshot_;
    std::mutex lock_;

    // Snapshot handling
    ptr<snapshot> last_snapshot_;
    std::mutex snapshot_mutex_;
};

// Inline implementation of serialization helpers
inline ptr<buffer> task_scheduler_state_machine::serialize_task(const Task& task) {
    // Start with a reasonable buffer size
    ptr<buffer> buf = buffer::alloc(1024);
    buffer_serializer bs(buf);

    // Write UUID (16 bytes)
    std::string uuid_str = task.get_id().to_string();
    bs.put_str(uuid_str);

    // Write task name
    bs.put_str(task.get_task_name());

    // Write task properties (using double serialization)
    double arrival = task.get_arrival_time();
    double duration = task.get_expected_duration();
    double cpu = task.get_cpu_cores_usage();
    double memory = task.get_memory_req();
    double deadline_val = task.get_deadline();

    uint64_t arrival_bits, duration_bits, cpu_bits, memory_bits, deadline_bits;
    std::memcpy(&arrival_bits, &arrival, sizeof(double));
    std::memcpy(&duration_bits, &duration, sizeof(double));
    std::memcpy(&cpu_bits, &cpu, sizeof(double));
    std::memcpy(&memory_bits, &memory, sizeof(double));
    std::memcpy(&deadline_bits, &deadline_val, sizeof(double));

    bs.put_u64(arrival_bits);
    bs.put_u64(duration_bits);
    bs.put_u64(cpu_bits);
    bs.put_u64(memory_bits);
    bs.put_u64(deadline_bits);
    bs.put_i32(task.get_priority());

    // Write status
    bs.put_i8(static_cast<int8_t>(task.get_status()));

    return buf;
}

inline Task task_scheduler_state_machine::deserialize_task(buffer& data) {
    buffer_serializer bs(data);

    // Read UUID (not used for reconstruction - Task will generate new UUID)
    std::string uuid_str = bs.get_str();
    // In a real implementation, you'd want proper UUID parsing
    // For simplicity, we let Task generate a new UUID

    // Read task name
    std::string task_name = bs.get_str();

    // Read task properties (using double deserialization)
    uint64_t arrival_bits, duration_bits, cpu_bits, memory_bits, deadline_bits;
    arrival_bits = bs.get_u64();
    duration_bits = bs.get_u64();
    cpu_bits = bs.get_u64();
    memory_bits = bs.get_u64();
    deadline_bits = bs.get_u64();

    double arrival_time, expected_duration, cpu_cores_usage, memory_req, deadline;
    std::memcpy(&arrival_time, &arrival_bits, sizeof(double));
    std::memcpy(&expected_duration, &duration_bits, sizeof(double));
    std::memcpy(&cpu_cores_usage, &cpu_bits, sizeof(double));
    std::memcpy(&memory_req, &memory_bits, sizeof(double));
    std::memcpy(&deadline, &deadline_bits, sizeof(double));
    int priority = bs.get_i32();

    // Read status
    TaskStatus status = static_cast<TaskStatus>(bs.get_i8());

    // Create task (will generate new UUID)
    Task task(task_name, arrival_time, expected_duration, cpu_cores_usage,
              memory_req, deadline, priority, status);

    return task;
}

inline ptr<buffer> task_scheduler_state_machine::serialize_completed_task(const CompletedTaskInfo& info) {
    ptr<buffer> buf = buffer::alloc(2048);
    buffer_serializer bs(buf);

    // Serialize the task first
    ptr<buffer> task_buf = serialize_task(info.task);
    bs.put_u32(static_cast<uint32_t>(task_buf->size()));
    bs.put_raw(task_buf->data_begin(), task_buf->size());

    // Write completion info (using double serialization)
    double completion_time = info.completion_time;
    double actual_duration = info.actual_duration;

    uint64_t completion_bits, duration_bits;
    std::memcpy(&completion_bits, &completion_time, sizeof(double));
    std::memcpy(&duration_bits, &actual_duration, sizeof(double));

    bs.put_u64(completion_bits);
    bs.put_u64(duration_bits);
    bs.put_u8(info.success ? 1 : 0);  // bool as u8
    bs.put_str(info.error_message);

    return buf;
}

inline CompletedTaskInfo task_scheduler_state_machine::deserialize_completed_task(buffer& data) {
    buffer_serializer bs(data);

    // Read task size
    uint32_t task_size = bs.get_u32();

    // Read task data
    void* task_data = bs.get_raw(task_size);
    ptr<buffer> task_buf = buffer::alloc(task_size);
    std::memcpy(task_buf->data_begin(), task_data, task_size);

    // Deserialize task
    Task task = deserialize_task(*task_buf);

    // Read completion info (using double deserialization)
    uint64_t completion_bits, duration_bits;
    completion_bits = bs.get_u64();
    duration_bits = bs.get_u64();

    double completion_time, actual_duration;
    std::memcpy(&completion_time, &completion_bits, sizeof(double));
    std::memcpy(&actual_duration, &duration_bits, sizeof(double));

    bool success = (bs.get_u8() != 0);
    std::string error_message = bs.get_str();

    return CompletedTaskInfo(task, completion_time, actual_duration, success, error_message);
}

} // namespace task_scheduler
