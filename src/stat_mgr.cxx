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

#include "raft_server.hxx"
#include "stat_mgr.hxx"

#include <fstream>
#include <iostream>
#include <string>

namespace nuraft {

// === stat_elem ==============================================================

stat_elem::stat_elem(Type _type, const std::string& _name)
    : stat_type_(_type)
    , stat_name_(_name)
    , counter_(0)
    , gauge_(0)
    , hist_( ( _type == HISTOGRAM )
             ? ( new Histogram() )
             : nullptr )
    {}

stat_elem::~stat_elem() {
    delete hist_;
}


// === stat_mgr ===============================================================

stat_mgr::stat_mgr() {
}

stat_mgr::~stat_mgr() {
    std::unique_lock<std::mutex> l(stat_map_lock_);
    for (auto& entry: stat_map_) {
        delete entry.second;
    }
    stat_map_.clear();
}

stat_mgr* stat_mgr::get_instance() {
    static stat_mgr mgr_instance;
    return &mgr_instance;
}

stat_elem* stat_mgr::get_stat(const std::string& stat_name) {
    std::unique_lock<std::mutex> l(stat_map_lock_);
    auto entry = stat_map_.find(stat_name);
    if (entry == stat_map_.end()) {
        // Not exist.
        return nullptr;
    }
    return entry->second;
}

stat_elem* stat_mgr::create_stat(stat_elem::Type type, const std::string& stat_name) {
#ifndef ENABLE_RAFT_STATS
    static stat_elem dummy_elem(stat_elem::COUNTER, "dummy");
    (void)type;
    (void)stat_name;
    return &dummy_elem;
#endif

    stat_elem* elem = new stat_elem(type, stat_name);

    std::unique_lock<std::mutex> l(stat_map_lock_);
    auto entry = stat_map_.find(stat_name);
    if (entry != stat_map_.end()) {
        // Alraedy exist.
        delete elem;
        return entry->second;
    }
    stat_map_.insert( std::make_pair(stat_name, elem) );
    return elem;
}

void stat_mgr::get_all_stats(std::vector<stat_elem*>& stats_out) {
    std::unique_lock<std::mutex> l(stat_map_lock_);
    stats_out.resize(stat_map_.size());
    size_t idx = 0;
    for (auto& entry: stat_map_) {
        stats_out[idx++] = entry.second;
    }
}

void stat_mgr::reset_stat(const std::string& stat_name) {
    std::unique_lock<std::mutex> l(stat_map_lock_);
    auto entry = stat_map_.find(stat_name);
    if (entry != stat_map_.end()) {
        stat_elem* elem = entry->second;
        elem->reset();
    }
}

void stat_mgr::reset_all_stats() {
    std::unique_lock<std::mutex> l(stat_map_lock_);
    for (auto& entry: stat_map_) {
        stat_elem* elem = entry.second;
        elem->reset();
    }
}


// === raft_server ============================================================

uint64_t raft_server::get_stat_counter(const std::string& name) {
    stat_elem* elem = stat_mgr::get_instance()->get_stat(name);
    if (!elem) return 0;
    // Tolerate type cast between counter and gauge.
    if (elem->get_type() == stat_elem::COUNTER) {
        return elem->get_counter();
    } else if (elem->get_type() == stat_elem::GAUGE) {
        return elem->get_gauge();
    }
    return 0;
}

int64_t raft_server::get_stat_gauge(const std::string& name) {
    stat_elem* elem = stat_mgr::get_instance()->get_stat(name);
    if (!elem) return 0;
    // Tolerate type cast between counter and gauge.
    if (elem->get_type() == stat_elem::COUNTER) {
        return elem->get_counter();
    } else if (elem->get_type() == stat_elem::GAUGE) {
        return elem->get_gauge();
    }
    return 0;
}

bool raft_server::get_stat_histogram(const std::string& name,
                                     std::map<double, uint64_t>& histogram_out ) {
    stat_elem* elem = stat_mgr::get_instance()->get_stat(name);
    if (!elem) return false;
    if (elem->get_type() != stat_elem::HISTOGRAM) return false;

    for (HistItr& entry: *elem->get_histogram()) {
        uint64_t cnt = entry.getCount();
        if (cnt) {
            histogram_out.insert( std::make_pair(entry.getUpperBound(), cnt) );
        }
    }
    return true;
}

void raft_server::reset_stat(const std::string& name) {
    stat_mgr::get_instance()->reset_stat(name);
}

void raft_server::reset_all_stats() {
    stat_mgr::get_instance()->reset_all_stats();
}

} // namespace nuraft

