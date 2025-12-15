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

#include "./policy.hxx"
#include "./utils.hxx"
#include <algorithm>
#include <stdexcept>
#include <vector>

// Flexible priority queue that can use different comparison criteria
template <Criteria Criterion> class FlexiblePriorityQueue {
private:
  std::vector<Task> heap;

  // Private helper methods
  void heapify_up(int index) {
    while (index > 0) {
      int parent = heap_parent(index);
      if (TaskComparators::compare_for_selection(heap[index], heap[parent],
                                                 Criterion)) {
        std::swap(heap[index], heap[parent]);
        index = parent;
      } else {
        break;
      }
    }
  }

  void heapify_down(int index) {
    int n = heap.size();
    while (true) {
      int left = heap_left_child(index);
      int right = heap_right_child(index);
      int smallest = index;

      if (left < n && TaskComparators::compare_for_selection(
                          heap[left], heap[smallest], Criterion)) {
        smallest = left;
      }
      if (right < n && TaskComparators::compare_for_selection(
                           heap[right], heap[smallest], Criterion)) {
        smallest = right;
      }

      if (smallest != index) {
        std::swap(heap[index], heap[smallest]);
        index = smallest;
      } else {
        break;
      }
    }
  }

  auto get_current_time() const -> double {
    return ::get_current_time(); // Use the global function from utils.h
  }

public:
  // Public interface
  void push(const Task &task) {
    heap.push_back(task);
    heapify_up(heap.size() - 1);
  }

  auto pop() -> Task {
    if (heap.empty()) {
      throw std::runtime_error("Priority queue is empty");
    }
    Task result = heap[0];
    heap[0] = heap.back();
    heap.pop_back();
    if (!heap.empty()) {
      heapify_down(0);
    }
    return result;
  }

  bool empty() const { return heap.empty(); }

  size_t size() const { return heap.size(); }

  // Peek without removing
  auto top() const -> const Task & {
    if (heap.empty()) {
      throw std::runtime_error("Priority queue is empty");
    }
    return heap[0];
  }

  // Rebuild heap with new time (for time-dependent criteria)
  void rebuild_with_time(double current_time) {
    // Simple rebuild: make heap from scratch
    std::make_heap(
        heap.begin(), heap.end(), [current_time](const Task &a, const Task &b) {
          return !TaskComparators::compare_for_selection(a, b, Criterion);
        });
  }
};
