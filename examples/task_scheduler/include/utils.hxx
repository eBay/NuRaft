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
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// C++11 compatible optional implementation
template<typename T>
class optional {
private:
    bool has_value_;
    typename std::aligned_storage<sizeof(T), alignof(T)>::type storage_;

public:
    optional() : has_value_(false) {}

    optional(const T& value) : has_value_(true) {
        new(&storage_) T(value);
    }

    optional(const optional& other) : has_value_(other.has_value_) {
        if (has_value_) {
            new(&storage_) T(*reinterpret_cast<const T*>(&other.storage_));
        }
    }

    optional(optional&& other) : has_value_(other.has_value_) {
        if (has_value_) {
            new(&storage_) T(std::move(*reinterpret_cast<T*>(&other.storage_)));
            other.has_value_ = false;
        }
    }

    optional& operator=(const optional& other) {
        if (this != &other) {
            if (has_value_) {
                reinterpret_cast<T*>(&storage_)->~T();
                has_value_ = false;
            }
            if (other.has_value_) {
                new(&storage_) T(*reinterpret_cast<const T*>(&other.storage_));
                has_value_ = true;
            }
        }
        return *this;
    }

    optional& operator=(optional&& other) {
        if (this != &other) {
            if (has_value_) {
                reinterpret_cast<T*>(&storage_)->~T();
                has_value_ = false;
            }
            if (other.has_value_) {
                new(&storage_) T(std::move(*reinterpret_cast<T*>(&other.storage_)));
                has_value_ = true;
                other.has_value_ = false;
            }
        }
        return *this;
    }

    ~optional() {
        if (has_value_) {
            reinterpret_cast<T*>(&storage_)->~T();
        }
    }

    bool has_value() const { return has_value_; }
    explicit operator bool() const { return has_value_; }

    T& value() {
        return *reinterpret_cast<T*>(&storage_);
    }

    const T& value() const {
        return *reinterpret_cast<const T*>(&storage_);
    }

    T& operator*() { return value(); }
    const T& operator*() const { return value(); }

    T* operator->() { return &value(); }
    const T* operator->() const { return &value(); }
};

// Forward declarations
struct CompletedTaskInfo;

// Time utilities
auto get_current_time() -> double;

// Heap utilities
int heap_parent(int index);
int heap_left_child(int index);
int heap_right_child(int index);

// Byte array utilities
auto bytes_to_hex_string(const std::array<uint8_t, 16>& bytes,
                        const std::array<size_t, 4>& dash_positions = {3, 5, 7, 9})
                        -> std::string;

// Random utilities
auto generate_random_bytes(size_t count) -> std::vector<uint8_t>;
auto generate_uuid_v4() -> std::array<uint8_t, 16>;

// Thread-safe circular buffer for completed tasks
template<typename T>
class CircularBuffer {
private:
  std::vector<T> buffer_;
  size_t capacity_;
  size_t head_;
  size_t tail_;
  size_t size_;
  mutable std::mutex mutex_;

public:
  explicit CircularBuffer(size_t capacity);

  bool push(const T& item);
  optional<T> pop();
  std::vector<T> get_snapshot() const;
  bool empty() const;
  size_t size() const;
  void clear();
};

using CompletedTaskBuffer = CircularBuffer<CompletedTaskInfo>;
