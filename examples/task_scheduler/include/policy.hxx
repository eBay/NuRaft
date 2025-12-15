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

#include <array>
#include <cstdint>
#include <string>
#include <utility>

#include "./task.hxx"

// Scheduling criteria enumeration
enum class Criteria : uint8_t {
  EXPECTED_END,      // Earliest expected end time first
  WAIT_TIME,         // Longest waiting time first
  EXPECTED_DURATION, // Shortest job first
  CPU_USAGE,         // Highest CPU requirement first
  PRIORITY_LEVEL,    // Highest priority first
  SLACK_TIME,        // Least slack first
  MEMORY_USAGE,      // Highest memory requirement first
  DEADLINE,          // Earliest deadline first
  CRITERIA_COUNT     // Total number of criteria
};

class SchedulingPolicy {
private:
  Criteria primary_criteria;
  Criteria fairness_criteria;
  double fairness_bound;
  std::string name;

public:
  SchedulingPolicy(Criteria primary, Criteria fairness, double bound,
                   std::string name)
      : primary_criteria(primary), fairness_criteria(fairness),
        fairness_bound(bound), name(std::move(name)) {}

  auto get_primary_criteria() const -> Criteria {
    return primary_criteria;
  }
  auto get_fairness_criteria() const -> Criteria {
    return fairness_criteria;
  }
  auto get_fairness_bound() const -> double {
    return fairness_bound;
  }
  auto get_name() const -> const std::string & { return name; }
};

using CriterionFn = double (*)(const Task &);

// Helper functions for criteria evaluation
static double expected_end_criteria(const Task &task) { return task.expected_end(); }
static double wait_time_criteria(const Task &task) { return task.wait_time(); }
static double expected_duration_criteria(const Task &task) { return task.get_expected_duration(); }
static double cpu_usage_criteria(const Task &task) { return task.get_cpu_cores_usage(); }
static double priority_level_criteria(const Task &task) { return -static_cast<double>(task.get_priority()); }
static double slack_time_criteria(const Task &task) { return task.slack_time(); }
static double memory_usage_criteria(const Task &task) { return task.get_memory_req(); }
static double deadline_criteria(const Task &task) { return task.get_deadline(); }

// Initialize dispatch table
static std::array<CriterionFn, static_cast<size_t>(Criteria::CRITERIA_COUNT)> init_dispatch_table() {
  std::array<CriterionFn, static_cast<size_t>(Criteria::CRITERIA_COUNT)> table;
  table[static_cast<size_t>(Criteria::EXPECTED_END)] = expected_end_criteria;
  table[static_cast<size_t>(Criteria::WAIT_TIME)] = wait_time_criteria;
  table[static_cast<size_t>(Criteria::EXPECTED_DURATION)] = expected_duration_criteria;
  table[static_cast<size_t>(Criteria::CPU_USAGE)] = cpu_usage_criteria;
  table[static_cast<size_t>(Criteria::PRIORITY_LEVEL)] = priority_level_criteria;
  table[static_cast<size_t>(Criteria::SLACK_TIME)] = slack_time_criteria;
  table[static_cast<size_t>(Criteria::MEMORY_USAGE)] = memory_usage_criteria;
  table[static_cast<size_t>(Criteria::DEADLINE)] = deadline_criteria;
  return table;
}

static const std::array<CriterionFn, static_cast<size_t>(Criteria::CRITERIA_COUNT)> dispatch_table = init_dispatch_table();

class TaskComparators {
public:
  static auto get_criteria_value(const Task &task,
                                 Criteria criteria) -> double;

  static auto compare_for_selection(const Task &a, const Task &b,
                                    Criteria criteria) -> bool;
};
