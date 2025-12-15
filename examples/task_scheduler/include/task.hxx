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
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <string>

class UUID {
private:
    std::array<uint8_t, 16> bytes{};

public:
    static auto generate() -> UUID;
    auto to_string() const -> std::string;
};

enum TaskStatus: std::int8_t {
  PENDING,
  ASSIGNED,
COMPLETED,
};

class Task {
private:
    UUID id;
    double arrival_time;
    double expected_duration;
    double cpu_cores_usage;
    double memory_req;
    double deadline;
    int priority;
    std::string task_name;
    TaskStatus status;
public:
    explicit Task(const std::string& task_name,
                  double arrival_time = 0.0,
                  double expected_dur = 1.0,
                  double cpu_cores_usage = 0.1,
                  double memory_req = 0.01,
                  double deadline = std::numeric_limits<double>::infinity(),
                  int priority = 0, TaskStatus status = PENDING);

    auto wait_time() const -> double;
    auto expected_end() const -> double;
    auto slack_time() const -> double;
    auto missed_deadline() const -> bool;
auto get_task_name() const ->std::string;
    auto get_id() const -> UUID;
    auto get_status() const -> TaskStatus;
    auto get_arrival_time() const -> double;
    auto get_expected_duration() const -> double;
    auto get_cpu_cores_usage() const -> double;
    auto get_memory_req() const -> double;
    auto get_deadline() const -> double;
    auto get_priority() const -> int;
    auto mark_assigned() -> void;
    auto mark_completed() -> void;
    bool operator==(const Task& other) const {
        return id.to_string() == other.get_id().to_string();
    }
};

class TaskHash {
public:
    auto operator()(const Task& task) const -> std::size_t;
};

struct CompletedTaskInfo {
    Task task;
    double completion_time;
    double actual_duration;
    bool success;
    std::string error_message; // Empty if success

    // Default constructor for vector initialization
    CompletedTaskInfo()
        : task("", 0.0, 0.0, 0.0, 0.0, 0.0, 0)
        , completion_time(0)
        , actual_duration(0)
        , success(false)
        , error_message("") {}

    CompletedTaskInfo(const Task& t,
                      double completion,
                      double duration,
                      bool s = true,
                      const std::string& error = "")
        : task(t)
        , completion_time(completion)
        , actual_duration(duration)
        , success(s)
        , error_message(error) {}
};
