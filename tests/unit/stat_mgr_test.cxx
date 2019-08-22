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

#include "nuraft.hxx"

#include "stat_mgr.hxx"

#include "test_common.h"

#include <cstring>
#include <random>
#include <string>

using namespace nuraft;

namespace stat_mgr_test {

int stat_mgr_basic_test() {
    stat_elem& counter = *stat_mgr::get_instance()->create_stat
        (stat_elem::COUNTER, "counter");

    stat_elem& gauge = *stat_mgr::get_instance()->create_stat
        (stat_elem::GAUGE, "gauge");

    stat_elem& histogram = *stat_mgr::get_instance()->create_stat
        (stat_elem::HISTOGRAM, "histogram");

    size_t exp_sum = 0;
    size_t NUM = 1000;
    for (size_t ii=0; ii<NUM; ++ii) {
        counter++;
        gauge += ii;
        exp_sum += ii;
        histogram += ii;
    }

    CHK_EQ(NUM, raft_server::get_stat_counter("counter"));
    CHK_EQ(NUM, raft_server::get_stat_gauge("counter"));
    CHK_EQ(exp_sum, raft_server::get_stat_counter("gauge"));
    CHK_EQ(exp_sum, raft_server::get_stat_gauge("gauge"));

    std::map<double, uint64_t> hist_dump;
    raft_server::get_stat_histogram("histogram", hist_dump);

    size_t hist_sum = 0;
    for (auto& entry: hist_dump) {
        TestSuite::_msg("%.1f %zu\n", entry.first, entry.second);
        hist_sum += entry.second;
    }
    CHK_EQ(NUM, hist_sum);

    raft_server::reset_stat("counter");
    CHK_EQ(0, raft_server::get_stat_counter("counter"));

    raft_server::reset_all_stats();
    CHK_EQ(0, raft_server::get_stat_gauge("gauge"));

    hist_dump.clear();
    hist_sum = 0;
    raft_server::get_stat_histogram("histogram", hist_dump);
    for (auto& entry: hist_dump) {
        TestSuite::_msg("%.1f %zu\n", entry.first, entry.second);
        hist_sum += entry.second;
    }
    CHK_EQ(0, hist_sum);

    return 0;
}

}  // namespace stat_mgr_test;
using namespace stat_mgr_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = false;

#ifdef ENABLE_RAFT_STATS
    ts.doTest( "stat mgr basic test",
               stat_mgr_basic_test );
#endif

    return 0;
}

