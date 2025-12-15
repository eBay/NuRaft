#include "../include/policy.hxx"

auto TaskComparators::get_criteria_value(const Task &task,
                                         Criteria criteria) -> double {
  std::size_t idx = static_cast<size_t>(criteria);
  if (idx >= dispatch_table.size()) {
    return 0.0;
  }

  double value = dispatch_table[idx](task);
  return value;
}

auto TaskComparators::compare_for_selection(const Task &a, const Task &b,
                                            Criteria criteria) -> bool {
  double value_a = get_criteria_value(a, criteria);
  double value_b = get_criteria_value(b, criteria);

  bool result;
  if (criteria == Criteria::PRIORITY_LEVEL) {
    result = value_a > value_b;
  } else {
    // For most criteria, lower values are better (earlier times, shorter
    // durations) For priority and CPU/memory usage, higher values are better
    // (handled by negative in criterion functions)
    result = value_a < value_b;
  }

  return result;
}
