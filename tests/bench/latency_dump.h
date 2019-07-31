/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright 2017 Jung-Sang Ahn
See URL: https://github.com/greensky00/latency-collector
         (v0.2.1)

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

#include "latency_collector.h"

#include <list>
#include <memory>
#include <vector>

class LatencyDumpDefaultImpl : public LatencyDump {
public:
    std::string dump(MapWrapper* map_w,
                     const LatencyCollectorDumpOptions& opt) {
        std::stringstream ss;
        if (!map_w->getSize()) {
            ss << "# stats: " << map_w->getSize() << std::endl;
            return ss.str();
        }

        std::multimap<uint64_t,
                      LatencyItem*,
                      std::greater<uint64_t> > map_uint64_t;
        std::map<std::string, LatencyItem*> map_string;
        size_t max_name_len = 9; // reserved for "STAT NAME" 9 chars

        std::unordered_map<std::string, LatencyItem*>& map = getMap(map_w);

        // Deduplication
        for (auto& entry: map) {
            LatencyItem *item = entry.second;
            if (!item->getNumCalls()) {
                continue;
            }
            std::string actual_name = getActualFunction(item->getName(), false);

            auto existing = map_string.find(actual_name);
            if (existing != map_string.end()) {
                LatencyItem* item_found = existing->second;
                *item_found += *item;
            } else {
                LatencyItem* new_item = new LatencyItem(*item);
                map_string.insert( std::make_pair(actual_name, new_item) );
            }

            if (actual_name.size() > max_name_len) {
                max_name_len = actual_name.size();
            }
        }

        ss << "# stats: " << map_string.size() << std::endl;

        for (auto& entry: map_string) {
            LatencyItem *item = entry.second;
            if (!item->getNumCalls()) continue;

            switch (opt.sort_by) {
            case LatencyCollectorDumpOptions::NAME: {
                // Do nothing
                break;
            }

            // Otherwise: dealing with uint64_t, map_uint64_t.
            case LatencyCollectorDumpOptions::TOTAL_TIME:
                addToUintMap(item->getTotalTime(), map_uint64_t, item);
                break;

            case LatencyCollectorDumpOptions::NUM_CALLS:
                addToUintMap(item->getNumCalls(), map_uint64_t, item);
                break;

            case LatencyCollectorDumpOptions::AVG_LATENCY:
                addToUintMap(item->getAvgLatency(), map_uint64_t, item);
                break;
            }
        }

        addDumpTitle(ss, max_name_len);

        if (opt.sort_by == LatencyCollectorDumpOptions::NAME) {
            // Name (string)
            for (auto& entry: map_string) {
                LatencyItem *item = entry.second;
                if (item->getNumCalls()) {
                    ss << dumpItem(item, max_name_len, 0, false)
                       << std::endl;
                }
            }
        } else {
            // Otherwise (number)
            for (auto& entry: map_uint64_t) {
                LatencyItem *item = entry.second;
                if (item->getNumCalls()) {
                    ss << dumpItem(item, max_name_len, 0, false)
                       << std::endl;
                }
            }
        }

        // Free all.
        for (auto& entry: map_string) {
            delete entry.second;
        }

        return ss.str();
    }

    std::string dumpTree(MapWrapper* map_w,
                         const LatencyCollectorDumpOptions& opt) {
        std::stringstream ss;
        DumpItem root;

        // Sort by name first.
        std::map<std::string, LatencyItem*> by_name;
        std::unordered_map<std::string, LatencyItem*>& map = getMap(map_w);
        for (auto& entry : map) {
            LatencyItem *item = entry.second;
            by_name.insert( std::make_pair(item->getName(), item) );
        }

        size_t max_name_len = 9;
        std::vector<DumpItem*> last_ptr(1);
        last_ptr[0] = &root;
        for (auto& entry : by_name) {
            LatencyItem *item = entry.second;
            std::string item_name = item->getName();
            size_t level = getNumStacks(item_name);
            if (!level) {
                // Not a thread-aware latency item, stop.
                return dump(map_w, opt);
            }

            DumpItem* parent = last_ptr[level-1];
            assert(parent); // Must exist

            DumpItemP dump_item(new DumpItem(level, item, parent->itself));
            if (level >= last_ptr.size()) {
                last_ptr.resize(level*2);
            }
            last_ptr[level] = dump_item.get();
            parent->child.push_back(std::move(dump_item));

            size_t actual_name_len = getActualFunction(item_name).size();
            if (actual_name_len > max_name_len) {
                max_name_len = actual_name_len;
            }
        }

        addDumpTitle(ss, max_name_len);
        dumpRecursive(ss, &root, max_name_len);

        return ss.str();
    }

private:
    static std::string usToString(uint64_t us) {
        std::stringstream ss;
        if (us < 1000) {
            // us
            ss << std::fixed << std::setprecision(0) << us << " us";
        } else if (us < 1000000) {
            // ms
            double tmp = static_cast<double>(us / 1000.0);
            ss << std::fixed << std::setprecision(1) << tmp << " ms";
        } else if (us < (uint64_t)600 * 1000000) {
            // second (from 1 second to 10 mins)
            double tmp = static_cast<double>(us / 1000000.0);
            ss << std::fixed << std::setprecision(1) << tmp << " s";
        } else {
            // minute
            double tmp = static_cast<double>(us / 60.0 / 1000000.0);
            ss << std::fixed << std::setprecision(0) << tmp << " m";
        }
        return ss.str();
    }

    static std::string countToString(uint64_t count) {
        std::stringstream ss;
        if (count < 1000) {
            ss << count;
        } else if (count < 1000000) {
            // K
            double tmp = static_cast<double>(count / 1000.0);
            ss << std::fixed << std::setprecision(1) << tmp << "K";
        } else if (count < (uint64_t)1000000000) {
            // M
            double tmp = static_cast<double>(count / 1000000.0);
            ss << std::fixed << std::setprecision(1) << tmp << "M";
        } else {
            // B
            double tmp = static_cast<double>(count / 1000000000.0);
            ss << std::fixed << std::setprecision(1) << tmp << "B";
        }
        return ss.str();
    }

    static std::string ratioToPercent(uint64_t a, uint64_t b) {
        std::stringstream ss;
        double tmp = (double)100.0 * a / b;
        ss << std::fixed << std::setprecision(1) << tmp << " %";
        return ss.str();
    }

    static size_t getNumStacks(const std::string& str) {
        size_t pos = 0;
        size_t str_size = str.size();
        size_t ret = 0;
        while (pos < str_size) {
            pos = str.find(" ## ", pos);
            if (pos == std::string::npos) break;
            pos += 4;
            ret++;
        }
        return ret;
    }

    static std::string getActualFunction(const std::string& str,
                                         bool add_tab = true) {
        size_t level = getNumStacks(str);
        if (!level) {
            return str;
        }

        size_t pos = str.rfind(" ## ");
        std::string ret = "";
        if (level > 1 && add_tab) {
            for (size_t i=1; i<level; ++i) {
                ret += "  ";
            }
        }
        ret += str.substr(pos + 4);
        return ret;
    }

    static std::string dumpItem(LatencyItem* item,
                                size_t max_filename_field = 0,
                                uint64_t parent_total_time = 0,
                                bool add_tab = true)
    {
        if (!max_filename_field) {
            max_filename_field = 32;
        }
        std::stringstream ss;
        ss << std::left << std::setw(max_filename_field)
           << getActualFunction(item->getStatName(), add_tab) << ": ";
        ss << std::right;
        ss << std::setw(8) << usToString(item->getTotalTime()) << " ";
        if (parent_total_time) {
            ss << std::setw(7)
               << ratioToPercent(item->getTotalTime(), parent_total_time)
               << " ";
        } else {
            ss << "    ---" << " ";
        }
        ss << std::setw(6) << countToString(item->getNumCalls()) << " ";
        ss << std::setw(8) << usToString(item->getAvgLatency()) << " ";
        ss << std::setw(8) << usToString(item->getPercentile(50)) << " ";
        ss << std::setw(8) << usToString(item->getPercentile(99)) << " ";
        ss << std::setw(8) << usToString(item->getPercentile(99.9));
        return ss.str();
    }

    struct DumpItem {
        using UPtr = std::unique_ptr<DumpItem>;

        DumpItem() : level(0), itself(nullptr), parent(nullptr) {}
        DumpItem(size_t _level, LatencyItem* _item, LatencyItem* _parent)
            : level(_level),
              itself(_item),
              parent(_parent) {}

        size_t level;
        LatencyItem* itself;
        LatencyItem* parent;
        std::list<UPtr> child;
    };
    using DumpItemP = DumpItem::UPtr;

    static void dumpRecursive(std::stringstream& ss,
                              DumpItem* dump_item,
                              size_t max_name_len) {
        if (dump_item->itself) {
            if (dump_item->parent) {
                ss << dumpItem(dump_item->itself, max_name_len,
                               dump_item->parent->getTotalTime());
            } else {
                ss << dumpItem(dump_item->itself, max_name_len);
            }
            ss << std::endl;
        }
        for (auto& entry : dump_item->child) {
            DumpItem* child = entry.get();
            dumpRecursive(ss, child, max_name_len);
        }
    }

    static void addDumpTitle(std::stringstream& ss, size_t max_name_len) {
        ss << std::left << std::setw(max_name_len) << "STAT NAME" << ": ";
        ss << std::right;
        ss << std::setw(8) << "TOTAL" << " ";
        ss << std::setw(7) << "RATIO" << " ";
        ss << std::setw(6) << "CALLS" << " ";
        ss << std::setw(8) << "AVERAGE" << " ";
        ss << std::setw(8) << "p50" << " ";
        ss << std::setw(8) << "p99" << " ";
        ss << std::setw(8) << "p99.9";
        ss << std::endl;
    }

    static void addToUintMap(uint64_t value,
                             std::multimap<uint64_t,
                                           LatencyItem*,
                                           std::greater<uint64_t> >& map,
                        LatencyItem* item)
    {
        map.insert( std::make_pair(value, item) );
    }
};


