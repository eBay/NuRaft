#include "../include/scheduler.hxx"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

// Define static const member
const size_t Scheduler::DEFAULT_COMPLETED_TASKS_CAPACITY = 1000;

// Private method implementation
auto Scheduler::select_task_by_criteria(Criteria criteria,
                                       const std::vector<Task> &candidates) const
    -> optional<Task> {
  if (candidates.empty()) {
    return optional<Task>();
  }

  auto min_it = std::min_element(
      candidates.begin(), candidates.end(),
      [this, criteria](const Task &a, const Task &b) {
        return TaskComparators::compare_for_selection(a, b, criteria);
      });

  if (min_it != candidates.end()) {
    // Create a copy to avoid memory issues
    return optional<Task>(Task(*min_it));
  }

  return optional<Task>();
}

// Constructor implementation
Scheduler::Scheduler(SchedulingPolicy policy, size_t completed_tasks_capacity)
    : completed_tasks(new CompletedTaskBuffer(completed_tasks_capacity)),
      policy(policy) {
}

// Destructor implementation
Scheduler::~Scheduler() {
  delete completed_tasks;
}

// Factory method implementations
auto Scheduler::create_fairness_scheduler(double fairness_bound) -> Scheduler* {
  SchedulingPolicy policy(Criteria::EXPECTED_DURATION,
                          Criteria::WAIT_TIME, fairness_bound,
                          "Fairness Scheduler");
  return new Scheduler(policy);
}

auto Scheduler::create_edf_scheduler() -> Scheduler* {
  SchedulingPolicy policy(Criteria::DEADLINE, Criteria::WAIT_TIME,
                          10.0, "Earliest Deadline First");
  return new Scheduler(policy);
}

auto Scheduler::create_cpu_aware_scheduler(double fairness_bound) -> Scheduler* {
  SchedulingPolicy policy(Criteria::CPU_USAGE,
                          Criteria::PRIORITY_LEVEL, fairness_bound,
                          "CPU-Aware Scheduler");
  return new Scheduler(policy);
}

auto Scheduler::create_priority_scheduler(double fairness_bound) -> Scheduler* {
  SchedulingPolicy policy(Criteria::PRIORITY_LEVEL,
                          Criteria::WAIT_TIME, fairness_bound,
                          "Priority-Based Scheduler");
  return new Scheduler(policy);
}

// Public method implementations
auto Scheduler::add_new_task(const std::string& task_name, double arrival_time, double expected_dur,
                            double cpu_cores_usage, double memory_req,
                            double deadline, int priority) -> void {
  Task new_task(task_name, arrival_time, expected_dur, cpu_cores_usage, memory_req,
                deadline, priority);

  std::unique_lock<std::mutex> lock(tasks_mutex);
  std::string task_uuid = new_task.get_id().to_string();
  tasks_queue.emplace(task_uuid, new_task);
}

auto Scheduler::schedule_next() -> optional<Task> {
  std::unique_lock<std::mutex> lock(tasks_mutex);

  if (tasks_queue.empty()) {
    return optional<Task>();
  }

  // Create vector of PENDING tasks only
  std::vector<Task> pending_tasks;
  for (const auto& pair : tasks_queue) {
    if (pair.second.get_status() == PENDING) {
      pending_tasks.push_back(pair.second);
    }
  }

  if (pending_tasks.empty()) {
    return optional<Task>();
  }

  lock.unlock();

  double min_wait = std::numeric_limits<double>::infinity();

  for (auto &a : pending_tasks) {
    min_wait = std::min(min_wait, a.wait_time());
  }

  bool has_violated = false;
  for (const auto& task : pending_tasks) {
    if (task.wait_time() > min_wait + policy.get_fairness_bound()) {
      has_violated = true;
      break;
    }
  }

  optional<Task> selected_task;

  if (!has_violated) {
    selected_task =
        select_task_by_criteria(policy.get_primary_criteria(), pending_tasks);
  } else {
    selected_task = select_task_by_criteria(policy.get_fairness_criteria(),
                                            pending_tasks);
  }

  if (selected_task.has_value()) {
    // Mark the task as ASSIGNED in-place
    std::unique_lock<std::mutex> write_lock(tasks_mutex);
    std::string task_uuid = selected_task.value().get_id().to_string();
    auto it = tasks_queue.find(task_uuid);
    if (it != tasks_queue.end()) {
      it->second.mark_assigned();
    }
  }

  return selected_task;
}



auto Scheduler::is_empty_queue()->bool{
  return tasks_queue.empty();
}

// Additional methods for completed tasks access
auto Scheduler::get_completed_tasks(size_t limit) const
    -> std::vector<CompletedTaskInfo> {
  auto all_tasks = completed_tasks->get_snapshot();

  if (limit == 0 || limit >= all_tasks.size()) {
    return all_tasks;
  }

  // Return the most recent 'limit' tasks
  return std::vector<CompletedTaskInfo>(all_tasks.end() - limit,
                                        all_tasks.end());
}

auto Scheduler::get_completed_tasks_count() const -> size_t {
  return completed_tasks->size();
}

auto Scheduler::clear_completed_tasks() -> void {
  completed_tasks->clear();
}

void Scheduler::print_summary() const {
  auto completed = get_completed_tasks();

  std::cout << "\n=== Execution Summary ===";
  std::cout << "\nTotal completed tasks: " << completed.size();

  if (!completed.empty()) {
    size_t successful = 0;
    for (const auto &info : completed) {
      if (info.success) successful++;
    }

    double total_duration = 0;
    for (const auto &info : completed) {
      total_duration += info.actual_duration;
    }

    std::cout << "\nSuccessful tasks: " << successful;
    std::cout << "\nFailed tasks: " << (completed.size() - successful);
    std::cout << "\nAverage execution time: "
              << (total_duration / completed.size()) << "s";
  }

  std::cout << "\n========================\n";
}

auto Scheduler::get_pending_task_count() const -> size_t {
  std::unique_lock<std::mutex> lock(tasks_mutex);
  return tasks_queue.size();
}

auto Scheduler::get_policy_name() const -> std::string {
  return policy.get_name();
}

auto Scheduler::mark_complete(std::string uuid)->void{
  std::unique_lock<std::mutex> lock(tasks_mutex);

  // Find the task in the pending queue
  auto it = tasks_queue.find(uuid);
  if (it != tasks_queue.end()) {
    // Create completed task info
    Task task = it->second;
    task.mark_completed();

    double current_time = get_current_time();
    double actual_duration = current_time - task.get_arrival_time();

    CompletedTaskInfo completed_info(task, current_time, actual_duration, true);

    // Add to completed tasks buffer
    completed_tasks->push(completed_info);

    // Remove from pending queue
    tasks_queue.erase(it);
  }
}

// Snapshot support methods
auto Scheduler::get_all_tasks() const -> std::unordered_map<std::string, Task> {
  std::unique_lock<std::mutex> lock(tasks_mutex);
  return tasks_queue;  // Copy the entire map
}

auto Scheduler::clear_all_tasks() -> void {
  std::unique_lock<std::mutex> lock(tasks_mutex);
  tasks_queue.clear();
}

auto Scheduler::restore_tasks(const std::unordered_map<std::string, Task>& tasks) -> void {
  std::unique_lock<std::mutex> lock(tasks_mutex);
  tasks_queue = tasks;  // Replace entire map
}

auto Scheduler::restore_completed_tasks(const std::vector<CompletedTaskInfo>& tasks) -> void {
  for (const auto& task : tasks) {
    completed_tasks->push(task);
  }
}