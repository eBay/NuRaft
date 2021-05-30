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

#include <chrono>
#include <mutex>
#include <thread>

namespace nuraft {

struct timer_helper {
    timer_helper(size_t duration_us = 0, bool fire_first_event = false)
        : duration_us_(duration_us)
        , first_event_fired_(!fire_first_event)
    {
        reset();
    }

    static void sleep_us(size_t us) {
        std::this_thread::sleep_for(std::chrono::microseconds(us));
    }

    static void sleep_ms(size_t ms) {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }

    static void sleep_sec(size_t sec) {
        std::this_thread::sleep_for(std::chrono::seconds(sec));
    }

    void reset() {
        std::lock_guard<std::mutex> l(lock_);
        t_created_ = std::chrono::system_clock::now();
    }

    size_t get_duration_us() const {
        std::lock_guard<std::mutex> l(lock_);
        return duration_us_;
    }

    void set_duration_us(size_t us) {
        std::lock_guard<std::mutex> l(lock_);
        duration_us_ = us;
    }

    void set_duration_ms(size_t ms) {
        set_duration_us(ms * 1000);
    }

    void set_duration_sec(size_t sec) {
        set_duration_us(sec * 1000000);
    }

    uint64_t get_us() {
        std::lock_guard<std::mutex> l(lock_);
        auto cur = std::chrono::system_clock::now();
        std::chrono::duration<double> elapsed = cur - t_created_;
        return (uint64_t)(1000000 * elapsed.count());
    }

    uint64_t get_ms() {
        std::lock_guard<std::mutex> l(lock_);
        auto cur = std::chrono::system_clock::now();
        std::chrono::duration<double> elapsed = cur - t_created_;
        return (uint64_t)(1000 * elapsed.count());
    }

    uint64_t get_sec() {
        std::lock_guard<std::mutex> l(lock_);
        auto cur = std::chrono::system_clock::now();
        std::chrono::duration<double> elapsed = cur - t_created_;
        return (uint64_t)elapsed.count();
    }

    bool timeout() {
        auto cur = std::chrono::system_clock::now();

        std::lock_guard<std::mutex> l(lock_);
        if (!first_event_fired_) {
            // First event, return `true` immediately.
            first_event_fired_ = true;
            return true;
        }

        std::chrono::duration<double> elapsed = cur - t_created_;
        return (duration_us_ < elapsed.count() * 1000000);
    }

    bool timeout_and_reset() {
        auto cur = std::chrono::system_clock::now();

        std::lock_guard<std::mutex> l(lock_);
        if (!first_event_fired_) {
            // First event, return `true` immediately.
            first_event_fired_ = true;
            return true;
        }

        std::chrono::duration<double> elapsed = cur - t_created_;
        if (duration_us_ < elapsed.count() * 1000000) {
            t_created_ = cur;
            return true;
        }
        return false;
    }

    std::chrono::time_point<std::chrono::system_clock> t_created_;
    size_t duration_us_;
    mutable bool first_event_fired_;
    mutable std::mutex lock_;
};

}

