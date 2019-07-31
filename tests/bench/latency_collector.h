/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright 2017 Jung-Sang Ahn
See URL: https://github.com/greensky00/latency-collector
         (v0.2.2)

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

#include "ashared_ptr.h"
#include "histogram.h"

#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <assert.h>
#include <inttypes.h>
#include <stdint.h>
#include <string.h>

struct LatencyCollectorDumpOptions {
    enum SortBy {
        NAME,
        TOTAL_TIME,
        NUM_CALLS,
        AVG_LATENCY
    };

    enum ViewType {
        TREE,
        FLAT
    };

    LatencyCollectorDumpOptions()
        : sort_by(SortBy::NAME)
        , view_type(ViewType::TREE)
        {}

    SortBy sort_by;
    ViewType view_type;
};

class LatencyItem;
class MapWrapper;
class LatencyDump {
public:
    virtual std::string dump(MapWrapper* map_w,
                             const LatencyCollectorDumpOptions& opt) = 0;
    virtual std::string dumpTree(MapWrapper* map_w,
                                 const LatencyCollectorDumpOptions& opt) = 0;

    // To make child class be able to access internal map.
    std::unordered_map<std::string, LatencyItem*>& getMap(MapWrapper* map_w);
};

class LatencyItem {
public:
    LatencyItem() {}
    LatencyItem(const std::string& _name) : statName(_name) {}
    LatencyItem(const LatencyItem& src)
        : statName(src.statName)
        , hist(src.hist) {}

    // this = src
    LatencyItem& operator=(const LatencyItem& src) {
        statName = src.statName;
        hist = src.hist;
        return *this;
    }

    // this += rhs
    LatencyItem& operator+=(const LatencyItem& rhs) {
        hist += rhs.hist;
        return *this;
    }

    // returning lhs + rhs
    friend LatencyItem operator+(LatencyItem lhs,
                                 const LatencyItem& rhs)
    {
        lhs.hist += rhs.hist;
        return lhs;
    }

    std::string getName() const {
        return statName;
    }

    void addLatency(uint64_t latency) { hist.add(latency); }
    uint64_t getAvgLatency() const { return hist.getAverage(); }
    uint64_t getTotalTime() const { return hist.getSum(); }
    uint64_t getNumCalls() const { return hist.getTotal(); }
    uint64_t getMaxLatency() const { return hist.getMax(); }
    uint64_t getMinLatency() { return hist.estimate(1); }
    uint64_t getPercentile(double percentile) { return hist.estimate(percentile); }

    size_t getNumStacks() const {
        size_t pos = 0;
        size_t str_size = statName.size();
        size_t ret = 0;
        while (pos < str_size) {
            pos = statName.find(" ## ", pos);
            if (pos == std::string::npos) break;
            pos += 4;
            ret++;
        }
        return ret;
    }

    std::string getActualFunction() const {
        size_t level = getNumStacks();
        if (!level) {
            return statName;
        }

        size_t pos = statName.rfind(" ## ");
        return statName.substr(pos + 4);
    }

    std::string getStatName() const { return statName; }

    std::map<double, uint64_t> dumpHistogram() const {
        std::map<double, uint64_t> ret;
        for (auto& entry: hist) {
            HistItr& itr = entry;
            uint64_t cnt = itr.getCount();
            if (cnt) {
                ret.insert( std::make_pair(itr.getUpperBound(), cnt) );
            }
        }
        return ret;
    }

private:
    std::string statName;
    Histogram hist;
};

class LatencyCollector;
class MapWrapper {
    friend class LatencyCollector;
    friend class LatencyDump;
public:
    MapWrapper() {}
    MapWrapper(const MapWrapper &src) {
        copyFrom(src);
    }

    ~MapWrapper() {}

    size_t getSize() const {
        size_t ret = 0;
        for (auto& entry: map) {
            if (entry.second->getNumCalls()) {
                ret++;
            }
        }
        return ret;
    }

    void copyFrom(const MapWrapper &src) {
        // Make a clone (but the map will point to same LatencyItems)
        map = src.map;
    }

    LatencyItem* addItem(const std::string& bin_name) {
        LatencyItem* item = new LatencyItem(bin_name);
        map.insert( std::make_pair(bin_name, item) );
        return item;
    }

    void delItem(const std::string& bin_name) {
        LatencyItem* item = nullptr;
        auto entry = map.find(bin_name);
        if (entry != map.end()) {
            item = entry->second;
            map.erase(entry);
            delete item;
        }
    }

    LatencyItem* get(const std::string& bin_name) {
        LatencyItem* item = nullptr;
        auto entry = map.find(bin_name);
        if (entry != map.end()) {
            item = entry->second;
        }
        return item;
    }

    std::string dump(LatencyDump* dump_inst,
                     const LatencyCollectorDumpOptions& opt) {
        if (dump_inst) return dump_inst->dump(this, opt);
        return "null dump implementation";
    }

    std::string dumpTree(LatencyDump* dump_inst,
                         const LatencyCollectorDumpOptions& opt) {
        if (dump_inst) return dump_inst->dumpTree(this, opt);
        return "null dump implementation";
    }

    void freeAllItems() {
        for (auto& entry : map) {
            delete entry.second;
        }
    }

private:
    std::unordered_map<std::string, LatencyItem*> map;
};

inline std::unordered_map<std::string, LatencyItem*>&
    LatencyDump::getMap(MapWrapper* map_w)
{
    return map_w->map;
}

using MapWrapperSP = ashared_ptr<MapWrapper>;
//using MapWrapperSP = std::shared_ptr<MapWrapper>;

class LatencyCollector {
    friend class LatencyDump;

public:
    LatencyCollector() {
        latestMap = MapWrapperSP(new MapWrapper());
    }

    ~LatencyCollector() {
        latestMap->freeAllItems();
    }

    size_t getNumItems() const {
        return latestMap->getSize();
    }

    void addStatName(const std::string& lat_name) {
        MapWrapperSP cur_map = latestMap;
        if (!cur_map->get(lat_name)) {
            cur_map->addItem(lat_name);
        } // Otherwise: already exists.
    }

    void addLatency(const std::string& lat_name, uint64_t lat_value) {
        MapWrapperSP cur_map = nullptr;

        size_t ticks_allowed = MAX_ADD_NEW_ITEM_RETRIES;
        do {
            cur_map = latestMap;
            LatencyItem *item = cur_map->get(lat_name);
            if (item) {
                // Found existing latency.
                item->addLatency(lat_value);
                return;
            }

            // Not found,
            // 1) Create a new map containing new stat in an MVCC manner, and
            // 2) Replace 'latestMap' pointer atomically.

            // Note:
            // Below insertion process happens only when a new stat item
            // is added. Generally the number of stats is not pretty big (<100),
            // and adding new stats will be finished at the very early stage.
            // Once all stats are populated in the map, below codes will never
            // be called, and adding new latency will be done without blocking
            // anything.

            // Copy from the current map.
            MapWrapper* new_map_raw = new MapWrapper();
            new_map_raw->copyFrom(*cur_map);
            MapWrapperSP new_map = MapWrapperSP(new_map_raw);

            // Add a new item.
            item = new_map->addItem(lat_name);
            item->addLatency(lat_value);

            // Atomic CAS, from current map to new map
            MapWrapperSP expected = cur_map;
            if (latestMap.compare_exchange(expected, new_map)) {
                // Succeeded.
                return;
            }

            // Failed, other thread updated the map at the same time.
            // Delete newly added item.
            new_map_raw->delItem(lat_name);
            // Retry.
        } while (ticks_allowed--);

        // Update failed, ignore the given latency at this time.
    }

    LatencyItem getAggrItem(const std::string& lat_name) {
        LatencyItem ret;
        if (lat_name.empty()) return ret;

        MapWrapperSP cur_map_p = latestMap;
        MapWrapper* cur_map = cur_map_p.get();

        for (auto& entry: cur_map->map) {
            LatencyItem *item = entry.second;
            std::string actual_name = item->getActualFunction();

            if (actual_name != lat_name) continue;

            if (ret.getName().empty()) {
                // Initialize.
                ret = *item;
            } else {
                // Already exists.
                ret += *item;
            }
        }

        return ret;
    }

    uint64_t getAvgLatency(const std::string& lat_name) {
        MapWrapperSP cur_map = latestMap;
        LatencyItem *item = cur_map->get(lat_name);
        return (item)? item->getAvgLatency() : 0;
    }

    uint64_t getMinLatency(const std::string& lat_name) {
        MapWrapperSP cur_map = latestMap;
        LatencyItem *item = cur_map->get(lat_name);
        return (item && item->getNumCalls()) ? item->getMinLatency() : 0;
    }

    uint64_t getMaxLatency(const std::string& lat_name) {
        MapWrapperSP cur_map = latestMap;
        LatencyItem *item = cur_map->get(lat_name);
        return (item) ? item->getMaxLatency() : 0;
    }

    uint64_t getTotalTime(const std::string& lat_name) {
        MapWrapperSP cur_map = latestMap;
        LatencyItem *item = cur_map->get(lat_name);
        return (item) ? item->getTotalTime() : 0;
    }

    uint64_t getNumCalls(const std::string& lat_name) {
        MapWrapperSP cur_map = latestMap;
        LatencyItem *item = cur_map->get(lat_name);
        return (item) ? item->getNumCalls() : 0;
    }

    uint64_t getPercentile(const std::string& lat_name, double percentile) {
        MapWrapperSP cur_map = latestMap;
        LatencyItem *item = cur_map->get(lat_name);
        return (item) ? item->getPercentile(percentile) : 0;
    }

    std::string dump( LatencyDump* dump_inst,
                      const LatencyCollectorDumpOptions& opt
                          = LatencyCollectorDumpOptions() )
    {
        MapWrapperSP cur_map_p = latestMap;
        MapWrapper* cur_map = cur_map_p.get();

        if (opt.view_type == LatencyCollectorDumpOptions::TREE) {
            return cur_map->dumpTree(dump_inst, opt);
        } else {
            return cur_map->dump(dump_inst, opt);
        }
    }

private:
    static const size_t MAX_ADD_NEW_ITEM_RETRIES = 16;
    // Mutex for Compare-And-Swap of latestMap.
    std::mutex lock;
    MapWrapperSP latestMap;
};

struct ThreadTrackerItem {
    ThreadTrackerItem()
        : numStacks(0),
          aggrStackNameRaw(4096),
          lenName(0)
        {}

    void pushStackName(const std::string& cur_stack_name) {
        size_t cur_stack_name_len = cur_stack_name.size();
        while (lenName + 4 + cur_stack_name_len > aggrStackNameRaw.size()) {
            // Double the string buffer.
            aggrStackNameRaw.resize(aggrStackNameRaw.size() * 2);
        }

        // Remember the latest length for later poping up.
        lenStack.push_back(lenName);
        strcpy(&aggrStackNameRaw[0] + lenName, " ## ");
        lenName += 4;
        strcpy(&aggrStackNameRaw[0] + lenName, cur_stack_name.c_str());
        lenName += cur_stack_name_len;

        numStacks++;
    }

    size_t popLastStack() {
        lenName = *(lenStack.rbegin());
        lenStack.pop_back();

        return --numStacks;
    }

    std::string getAggrStackName() {
        aggrStackNameRaw[lenName] = 0;
        return &aggrStackNameRaw[0];
    }

    size_t numStacks;
    std::vector<char> aggrStackNameRaw;
    size_t lenName;
    std::list<size_t> lenStack;
};

struct LatencyCollectWrapper {
    using SystemClock = std::chrono::system_clock;
    using TimePoint = std::chrono::time_point<SystemClock>;
    using MicroSeconds = std::chrono::microseconds;

    LatencyCollectWrapper(LatencyCollector *_lat,
                          const std::string& _func_name) {
        lat = _lat;
        if (lat) {
            start = SystemClock::now();

            thread_local ThreadTrackerItem thr_item;
            cur_tracker = &thr_item;
            cur_tracker->pushStackName(_func_name);
        }
    }

    ~LatencyCollectWrapper() {
        if (lat) {
            TimePoint end = SystemClock::now();
            auto us = std::chrono::duration_cast<MicroSeconds>(end - start);

            lat->addLatency(cur_tracker->getAggrStackName(), us.count());
            cur_tracker->popLastStack();
        }
    }

    LatencyCollector *lat;
    ThreadTrackerItem *cur_tracker;
    TimePoint start;
};

#if defined(WIN32) || defined(_WIN32)
#define collectFuncLatency(lat) \
    LatencyCollectWrapper LCW__func_latency__((lat), __FUNCTION__)
#else
#define collectFuncLatency(lat) \
    LatencyCollectWrapper LCW__func_latency__((lat), __func__)
#endif

#define collectBlockLatency(lat, name) \
    LatencyCollectWrapper LCW__block_latency__((lat), name)

