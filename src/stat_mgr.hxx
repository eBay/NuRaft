/************************************************************************
Copyright 2017-2019 eBay Inc.

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

#include "histogram.h"

#include <atomic>
#include <cassert>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace nuraft {

// NOTE: Accessing a stat_elem instance using multiple threads is safe.
class stat_elem {
public:
    enum Type {
        COUNTER = 0,
        HISTOGRAM = 1,
        GAUGE = 2,
    };

    stat_elem(Type _type, const std::string& _name);

    ~stat_elem();

    inline void inc(size_t amount = 1) {
#ifndef ENABLE_RAFT_STATS
        return;
#endif
        assert(stat_type_ != HISTOGRAM);
        if (stat_type_ == COUNTER) {
            counter_.fetch_add(amount, std::memory_order_relaxed);
        } else {
            gauge_.fetch_add(amount, std::memory_order_relaxed);
        }
    }

    inline void dec(size_t amount = 1) {
#ifndef ENABLE_RAFT_STATS
        return;
#endif
        assert(stat_type_ != HISTOGRAM);
        if (stat_type_ == COUNTER) {
            counter_.fetch_sub(amount, std::memory_order_relaxed);
        } else {
            gauge_.fetch_sub(amount, std::memory_order_relaxed);
        }
    }

    inline void add_value(uint64_t val) {
#ifndef ENABLE_RAFT_STATS
        return;
#endif
        assert(stat_type_ == HISTOGRAM);
        hist_->add(val);
    }

    inline void set(int64_t value) {
#ifndef ENABLE_RAFT_STATS
        return;
#endif
        assert(stat_type_ != HISTOGRAM);
        if (stat_type_ == COUNTER) {
            counter_.store(value, std::memory_order_relaxed);
        } else {
            gauge_.store(value, std::memory_order_relaxed);
        }
    }

    stat_elem& operator+=(size_t amount) {
        switch (stat_type_) {
        case COUNTER:
        case GAUGE:
            inc(amount);
            break;
        case HISTOGRAM:
            add_value(amount);
            break;
        default: break;
        }
        return *this;
    }

    stat_elem& operator-=(size_t amount) {
        switch (stat_type_) {
        case COUNTER:
        case GAUGE:
            dec(amount);
            break;
        case HISTOGRAM:
            assert(0);
            break;
        default: break;
        }
        return *this;
    }

    stat_elem& operator++(int) {
        inc();
        return *this;
    }

    stat_elem& operator--(int) {
        dec();
        return *this;
    }

    stat_elem& operator=(size_t val) {
        set(val);
        return *this;
    }

    const std::string& get_name() const { return stat_name_; }

    Type get_type() const { return stat_type_; }

    uint64_t get_counter() const { return counter_; }

    int64_t get_gauge() const { return gauge_; }

    Histogram* get_histogram() const { return hist_; }

    void reset() {
        switch (stat_type_) {
        case COUNTER:
        case GAUGE:
            set(0);
            break;
        case HISTOGRAM: {
            Histogram empty_histogram;
            *hist_ = empty_histogram;
            break; }
        default: break;
        }
    }

private:
    Type stat_type_;
    std::string stat_name_;
    std::atomic<uint64_t> counter_;
    std::atomic<int64_t> gauge_;
    Histogram* hist_;
};

// Singleton class
class stat_mgr {
public:
    static stat_mgr* get_instance();

    stat_elem* get_stat(const std::string& stat_name);

    stat_elem* create_stat(stat_elem::Type type, const std::string& stat_name);

    void get_all_stats(std::vector<stat_elem*>& stats_out);

    void reset_stat(const std::string& stat_name);

    void reset_all_stats();

private:
    stat_mgr();
    ~stat_mgr();

    std::mutex stat_map_lock_;
    std::map<std::string, stat_elem*> stat_map_;
};

} // namespace nuraft

