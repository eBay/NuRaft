/************************************************************************
Copyright 2017-present eBay Inc.
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

#include "buffer_serializer.hxx"
#include "debugging_options.hxx"
#include "in_memory_log_store.hxx"
#include "raft_package_asio.hxx"
#include "asio_test_common.hxx"

#include "event_awaiter.hxx"
#include "test_common.h"

#ifdef USE_BOOST_ASIO
    #include <boost/asio.hpp>
    using namespace boost;
    using asio_error_code = system::error_code;
#else
    #include <asio.hpp>
    using asio_error_code = asio::error_code;
#endif

#include <unordered_map>

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

namespace priority_based_election_test {

int priority_election_basic_test_internal() {
    reset_log_files();

    const size_t NUM_SERVERS = 9;
    std::vector<std::string> s_addrs;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        std::string addr = "tcp://127.0.0.1:" + std::to_string(20010 + ii * 10);
        s_addrs.push_back(addr);
    }

    std::vector<std::shared_ptr<RaftAsioPkg>> s_pkgs;
    std::vector<RaftAsioPkg*> pkgs;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        std::shared_ptr<RaftAsioPkg> pkg =
            std::make_shared<RaftAsioPkg>(ii + 1, s_addrs[ii]);
        s_pkgs.push_back(pkg);
        pkgs.push_back(pkg.get());
    }

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    // Adjust priority.
    // S1-3: 3
    // S4-6: 2
    // S7-9: 1
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        int32_t priority = 1;
        if (ii < 3) {
            priority = 3;
        } else if (ii < 6) {
            priority = 2;
        }
        pkgs[0]->raftServer->set_priority(ii + 1, priority);
    }
    TestSuite::sleep_sec(1, "wait for replication");

    // Stop S1.
    _msg("stopping S1\n");
    pkgs[0]->raftServer->shutdown();
    pkgs[0]->stopAsio();
    s_pkgs[0].reset();
    TestSuite::sleep_sec(1, "wait for S1 shutdown");

    // Either S2 or S3 should be elected as a leader,
    // since they have the highest priority.
    _msg("waiting for a new leader\n");
    size_t new_leader_idx = 0;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        if (ii == 0) {
            continue;
        }
        if (pkgs[ii]->raftServer->is_leader()) {
            new_leader_idx = ii;
            break;
        }
    }
    CHK_TRUE(new_leader_idx != 0);
    _msg("new leader is S%d\n", new_leader_idx + 1);
    CHK_TRUE(new_leader_idx == 1 || new_leader_idx == 2);

    // Stopping the new leader.
    _msg("stopping the new leader S%d\n", new_leader_idx + 1);
    pkgs[new_leader_idx]->raftServer->shutdown();
    pkgs[new_leader_idx]->stopAsio();
    s_pkgs[new_leader_idx].reset();
    TestSuite::sleep_sec(1, "wait for new leader shutdown");

    // Either S2 or S3 should be elected as a leader.
    _msg("waiting for a new leader\n");
    size_t new_leader_idx2 = 0;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        if (ii == 0 || ii == new_leader_idx) {
            continue;
        }
        if (pkgs[ii]->raftServer->is_leader()) {
            new_leader_idx2 = ii;
            break;
        }
    }
    CHK_TRUE(new_leader_idx2 != 0);
    _msg("new leader is S%d\n", new_leader_idx2 + 1);
    CHK_TRUE(new_leader_idx2 == 1 || new_leader_idx2 == 2);

    // Stopping the new leader.
    _msg("stopping the new leader S%d\n", new_leader_idx2 + 1);
    pkgs[new_leader_idx2]->raftServer->shutdown();
    pkgs[new_leader_idx2]->stopAsio();
    s_pkgs[new_leader_idx2].reset();
    TestSuite::sleep_sec(1, "wait for new leader shutdown");

    // Either S4, S5, or S6 should be elected as a leader.
    _msg("waiting for a new leader\n");
    size_t new_leader_idx3 = 0;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        if (ii <= 2) {
            continue;
        }
        if (pkgs[ii]->raftServer->is_leader()) {
            new_leader_idx3 = ii;
            break;
        }
    }
    CHK_TRUE(new_leader_idx3 != 0);
    _msg("new leader is S%d\n", new_leader_idx3 + 1);
    CHK_TRUE(new_leader_idx3 == 3 || new_leader_idx3 == 4 || new_leader_idx3 == 5);

    // Shutdown.
    for (size_t ii = 3; ii < NUM_SERVERS; ++ii) {
        pkgs[ii]->raftServer->shutdown();
        s_pkgs[ii].reset();
    }
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int priority_election_basic_test() {
    // NOTE: Since priority-based election is non-deterministic,
    //       it is not 100% guaranteed that the test will pass every time.
    //
    //       If it fails, run it two additional times
    //       and verify both of them pass (prob should be > 66%).
    int ret = priority_election_basic_test_internal();
    if (ret != 0) {
        _msg("priority_election_basic_test_internal() failed, retrying...\n");
        for (size_t ii = 0; ii < 2; ++ii) {
            CHK_Z(priority_election_basic_test_internal());
        }
    }
    return ret;
}

}  // namespace priority_based_election_test;
using namespace priority_based_election_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    ts.doTest( "priority election basic test",
               priority_election_basic_test );

#ifdef ENABLE_RAFT_STATS
    _msg("raft stats: ENABLED\n");
#else
    _msg("raft stats: DISABLED\n");
#endif
    TestSuite::Msg mm;
    mm << "num allocs: " << raft_server::get_stat_counter("num_buffer_allocs")
       << std::endl
       << "amount of allocs: " << raft_server::get_stat_counter("amount_buffer_allocs")
       << " bytes" << std::endl
       << "num active buffers: " << raft_server::get_stat_counter("num_active_buffers")
       << std::endl
       << "amount of active buffers: "
       << raft_server::get_stat_counter("amount_active_buffers") << " bytes" << std::endl;

    return 0;
}
