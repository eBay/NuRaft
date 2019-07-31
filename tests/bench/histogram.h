/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/greensky00/latency-collector
         (v0.1.7)

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

#include <stdint.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <limits>

using HistBin = std::atomic<uint64_t>;

class Histogram;
class HistItr {
public:
    HistItr() : idx(0), maxBins(0), owner(nullptr) { }

    HistItr(size_t _idx, size_t _max_bins, const Histogram* _owner)
        : idx(_idx), maxBins(_max_bins), owner(_owner) {}

    // ++A
    HistItr& operator++() {
        idx++;
        if (idx > maxBins) idx = maxBins;
        return *this;
    }

    // A++
    HistItr operator++(int) {
        idx++;
        if (idx > maxBins) idx = maxBins;
        return *this;
    }

    // --A
    HistItr& operator--() {
        if (idx || idx == maxBins) {
            // end()
            idx = maxBins;
        } else {
            idx--;
        }
        return *this;
    }

    // A--
    HistItr operator--(int) {
        if (idx || idx == maxBins) {
            // end()
            idx = maxBins;
        } else {
            idx--;
        }
        return *this;
    }

    HistItr& operator*() {
        // Just return itself
        return *this;
    }


    bool operator==(const HistItr& val) const {
        return (idx == val.idx);
    }

    bool operator!=(const HistItr& val) const {
        return (idx != val.idx);
    }

    size_t getIdx() const { return idx; }

    inline uint64_t getCount();

    uint64_t getLowerBound() {
        size_t idx_rev = maxBins - idx - 1;
        uint64_t ret = 1;

        if (idx_rev) {
            return ret << (idx_rev-1);
        } else {
            return 0;
        }
    }

    uint64_t getUpperBound() {
        size_t idx_rev = maxBins - idx - 1;
        uint64_t ret = 1;

        if (!idx) return std::numeric_limits<std::uint64_t>::max();
        return ret << idx_rev;
    }

private:
    size_t idx;
    size_t maxBins;
    const Histogram* owner;
};

class Histogram {
    friend class HistItr;

public:
    using iterator = HistItr;

    Histogram(double base = 2.0)
        : EXP_BASE(base)
        , EXP_BASE_LOG( log(base) )
        , count(0)
        , sum(0)
        , max(0)
    {
        bins = new HistBin[MAX_BINS];
        for (size_t i=0; i<MAX_BINS; ++i) {
            bins[i] = 0;
        }
    }

    Histogram(const Histogram& src) {
        bins = new HistBin[MAX_BINS];
        // It will invoke `operator=()` below.
        *this = src;
    }

    ~Histogram() {
        delete[] bins;
    }

    // this = src
    Histogram& operator=(const Histogram& src) {
        EXP_BASE = src.EXP_BASE;
        EXP_BASE_LOG = src.EXP_BASE_LOG;
        count = src.getTotal();
        sum = src.getSum();
        max = src.getMax();
        for (size_t i=0; i<MAX_BINS; ++i) {
            bins[i].store( src.bins[i].load() );
        }
        return *this;
    }

    // this += rhs
    Histogram& operator+=(const Histogram& rhs) {
        count += rhs.getTotal();
        sum += rhs.getSum();
        if (max < rhs.getMax()) {
            max = rhs.getMax();
        }

        for (size_t i=0; i<MAX_BINS; ++i) {
            bins[i] += rhs.bins[i];
        }

        return *this;
    }

    // returning lhs + rhs
    friend Histogram operator+(Histogram lhs,
                               const Histogram& rhs) {
        lhs.count += rhs.getTotal();
        lhs.sum += rhs.getSum();
        if (lhs.max < rhs.getMax()) {
            lhs.max = rhs.getMax();
        }

        for (size_t i=0; i<MAX_BINS; ++i) {
            lhs.bins[i] += rhs.bins[i];
        }

        return lhs;
    }

    int getIdx(uint64_t val) {
        double log_val = (double)log((double)val) / EXP_BASE_LOG;
        int idx_rvs = (int)log_val + 2;
        if (idx_rvs > (int)MAX_BINS) return 0;
        return (int)MAX_BINS - idx_rvs;
    }

    void add(uint64_t val) {
        // if `val` == 1
        //          == 0x00...01
        //                     ^
        //                     64th bit
        //   then `idx` = 63.
        //
        // if `val` == UINT64_MAX
        //          == 0xff...ff
        //               ^
        //               1st bit
        //   then `idx` = 0.
        //
        // so we should handle `val` == 0 as a special case (`idx` = 64),
        // that's the reason why num bins is 65.

        int idx = MAX_BINS - 1;
        if (val) {
#if defined(__linux__) || defined(__APPLE__)
            idx = __builtin_clzl(val);

#elif defined(WIN32) || defined(_WIN32)
            idx = getIdx(val);
#endif
        }
        bins[idx].fetch_add(1, std::memory_order_relaxed);
        count.fetch_add(1, std::memory_order_relaxed);
        sum.fetch_add(val, std::memory_order_relaxed);

        size_t num_trial = 0;
        while (num_trial++ < MAX_TRIAL &&
               max.load(std::memory_order_relaxed) < val) {
            // 'max' may not be updated properly under race condition.
            max.store(val, std::memory_order_relaxed);
        }
    }

    uint64_t getTotal() const { return count; }
    uint64_t getSum() const { return sum; }
    uint64_t getAverage() const { return ( (count) ? (sum / count) : 0 ); }
    uint64_t getMax() const { return max; }

    iterator find(double percentile) {
        if (percentile <= 0 || percentile >= 100) {
            return end();
        }

        double rev = 100 - percentile;
        size_t i;
        uint64_t sum = 0;
        uint64_t total = getTotal();
        uint64_t threshold = (uint64_t)( (double)total * rev / 100.0 );

        for (i=0; i<MAX_BINS; ++i) {
            sum += bins[i].load(std::memory_order_relaxed);
            if (sum >= threshold) {
                return HistItr(i, MAX_BINS, this);
            }
        }
        return end();
    }

    uint64_t estimate(double percentile) {
        if (percentile <= 0 || percentile >= 100) {
            return 0;
        }

        double rev = 100 - percentile;
        size_t i;
        uint64_t sum = 0;
        uint64_t total = getTotal();
        uint64_t threshold = (uint64_t)( (double)total * rev / 100.0 );

        if (!threshold) {
            // No samples between the given percentile and the max number.
            // Return max number.
            return max;
        }

        for (i=0; i<MAX_BINS; ++i) {
            uint64_t n_entries = bins[i].load(std::memory_order_relaxed);
            sum += n_entries;
            if (sum < threshold) continue;

            uint64_t gap = sum - threshold;
            uint64_t u_bound = HistItr(i, MAX_BINS, this).getUpperBound();
            double base = EXP_BASE;
            if (max < u_bound) {
                base = (double)max / (u_bound / 2.0);
            }

            return (uint64_t)
                   ( std::pow(base, (double)gap / n_entries) * u_bound / 2 );
        }
        return 0;
    }

    iterator begin() const {
        size_t i;
        for (i=0; i<MAX_BINS; ++i) {
            if (bins[i].load(std::memory_order_relaxed)) break;
        }
        return HistItr(i, MAX_BINS, this);
    }

    iterator end() const {
        return HistItr(MAX_BINS, MAX_BINS, this);
    }

private:
    static const size_t MAX_BINS = 65;
    static const size_t MAX_TRIAL = 3;
    double EXP_BASE;
    double EXP_BASE_LOG;

    HistBin* bins;
    std::atomic<uint64_t> count;
    std::atomic<uint64_t> sum;
    std::atomic<uint64_t> max;
};

uint64_t HistItr::getCount() {
    return owner->bins[idx];
}

