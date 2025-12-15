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

#include "./utils.hxx"
#include "./policy.hxx"
#include "./queue.hxx"
#include "./task.hxx"
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

class Scheduler {
private:
  std::unordered_map<std::string, Task> tasks_queue; // pending tasks queue (key: UUID string)
  CompletedTaskBuffer* completed_tasks; // completed tasks circular buffer
  SchedulingPolicy policy;
  mutable std::mutex tasks_mutex; // thread safety for tasks_queue
  static const size_t DEFAULT_COMPLETED_TASKS_CAPACITY;

  // Private helper method declaration
  auto select_task_by_criteria(Criteria criteria,
                               const std::vector<Task> &candidates) const
      -> optional<Task>;

public:
  // Constructor declaration
  explicit Scheduler(SchedulingPolicy policy,
                    size_t completed_tasks_capacity = DEFAULT_COMPLETED_TASKS_CAPACITY);

  // Destructor
  ~Scheduler();

  // Factory method declarations
  static auto create_fairness_scheduler(double fairness_bound = 2.0) -> Scheduler*;
  static auto create_edf_scheduler() -> Scheduler*;
  static auto create_cpu_aware_scheduler(double fairness_bound = 3.0) -> Scheduler*;
  static auto create_priority_scheduler(double fairness_bound = 5.0) -> Scheduler*;

private:
  // Copy constructor and assignment operator (not implemented due to mutex)
  Scheduler(const Scheduler&);
  Scheduler& operator=(const Scheduler&);

public:
  // Public method declarations
  auto add_new_task(const std::string& task_name, double arrival_time = 0.0, double expected_dur = 1.0,
                    double cpu_cores_usage = 0.1, double memory_req = 0.01,
                    double deadline = std::numeric_limits<double>::infinity(),
                    int priority = 0) -> void;

  auto schedule_next() -> optional<Task>;

  // Additional methods for completed tasks access
  auto get_completed_tasks(size_t limit = 0) const -> std::vector<CompletedTaskInfo>;
  auto get_completed_tasks_count() const -> size_t;
  auto clear_completed_tasks() -> void;
auto mark_complete(std::string uuid)->void;

  void print_summary() const;
  bool is_empty_queue();
  // Additional methods needed for integration
  auto get_pending_task_count() const -> size_t;
  auto get_policy_name() const -> std::string;

  // Snapshot support
  auto get_all_tasks() const -> std::unordered_map<std::string, Task>;
  auto clear_all_tasks() -> void;
  auto restore_tasks(const std::unordered_map<std::string, Task>& tasks) -> void;
  auto restore_completed_tasks(const std::vector<CompletedTaskInfo>& tasks) -> void;
  
};
