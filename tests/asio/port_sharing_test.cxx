/************************************************************************
Modifications Copyright 2025

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

#include "launcher.hxx"
#include "in_memory_log_store.hxx"
#include "test_common.h"
#include "raft_functional_common.hxx"

#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>
#include <string>

using namespace nuraft;
using namespace raft_functional_common;

// ============================================================================
// Helper Functions
// ============================================================================

static void reset_port_sharing_test_logs() {
    std::stringstream ss;
    ss << "port_sharing_srv*.log ";

#if defined(__linux__) || defined(__APPLE__)
    std::string cmd = "rm -f " + ss.str() + "2> /dev/null";
    FILE* fp = popen(cmd.c_str(), "r");
    int r = pclose(fp);
    (void)r;

#elif defined(WIN32) || defined(_WIN32)
    std::string cmd = "del /s /f /q " + ss.str() + "> NUL";
    int r = system(cmd.c_str());
    (void)r;

#else
    #error "not supported platform"
#endif
}

static ptr<raft_launcher> create_shared_port_launcher(int port)
{
    std::string log_file = "./port_sharing_srv_" + std::to_string(port) + ".log";
    ptr<logger_wrapper> log_wrapper = cs_new<logger_wrapper>(log_file);

    ptr<raft_launcher> launcher = cs_new<raft_launcher>();
    asio_service::options asio_opts;
    asio_opts.thread_pool_size_ = 2;

    if (!launcher->init_shared_port(port, log_wrapper, asio_opts)) {
        return nullptr;
    }

    return launcher;
}

static ptr<TestSm> create_state_machine(int group_id, int server_id)
{
    (void)group_id;
    (void)server_id;
    return cs_new<TestSm>();
}

static ptr<TestMgr> create_state_manager(int group_id, int server_id, const std::string& endpoint)
{
    (void)group_id;
    return cs_new<TestMgr>(server_id, endpoint);
}

static raft_params create_default_raft_params() {
    raft_params params;
    params.with_hb_interval(100);
    params.with_election_timeout_lower(200);
    params.with_election_timeout_upper(400);
    params.with_reserved_log_items(10);
    params.with_client_req_timeout(5000);
    return params;
}

static int wait_for_leader(const std::vector<ptr<raft_launcher>>& launchers,
                           int group_id,
                           int timeout_sec = 10)
{
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start <
           std::chrono::seconds(timeout_sec)) {

        for (auto& launcher : launchers) {
            ptr<raft_server> server = launcher->get_server(group_id);
            if (server && server->is_leader()) {
                return 0;
            }
        }

        TestSuite::sleep_ms(100);
    }

    // No leader found
    return -1;
}

static ptr<raft_server> find_leader(
    const std::vector<ptr<raft_launcher>>& launchers,
    int group_id)
{
    for (auto& launcher : launchers) {
        ptr<raft_server> server = launcher->get_server(group_id);
        if (server && server->is_leader()) {
            return server;
        }
    }
    return nullptr;
}

// ============================================================================
// Test 1: Multi-Group Shared Port (Basic Functionality)
// ============================================================================

static int test_multi_group_shared_port() {
    reset_port_sharing_test_logs();

    const int NUM_SERVERS = 2;
    const int NUM_GROUPS = 2;
    const int BASE_PORT = 21001;

    std::vector<ptr<raft_launcher>> launchers;

    _msg("=== Test 1: Multi-Group Shared Port ===\n");
    _msg("Creating %d launchers, each with %d groups\n", NUM_SERVERS, NUM_GROUPS);

    // Create launchers with DIFFERENT ports (each launcher represents a physical node)
    for (int i = 0; i < NUM_SERVERS; i++) {
        int port = BASE_PORT + i;
        ptr<raft_launcher> launcher = create_shared_port_launcher(port);
        if (!launcher) {
            TestSuite::Msg() << "Failed to create launcher " << i << std::endl;
            return -1;
        }
        launchers.push_back(launcher);
    }

    _msg("Launchers created successfully\n");

    // Create state machines and state managers for each group
    std::vector<std::vector<ptr<TestSm>>> state_machines(NUM_GROUPS);
    std::vector<std::vector<ptr<TestMgr>>> state_managers(NUM_GROUPS);

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;
        state_machines[g].resize(NUM_SERVERS);
        state_managers[g].resize(NUM_SERVERS);

        for (int i = 0; i < NUM_SERVERS; i++) {
            int server_id = i + 1;
            int port = BASE_PORT + i;
            // Each server listens on its own port
            std::string endpoint = "tcp://127.0.0.1:" + std::to_string(port);

            state_machines[g][i] = create_state_machine(group_id, server_id);
            state_managers[g][i] = create_state_manager(group_id, server_id, endpoint);
        }
    }

    // Add groups to launchers
    _msg("Adding groups to launchers...\n");

    for (int i = 0; i < NUM_SERVERS; i++) {
        for (int g = 0; g < NUM_GROUPS; g++) {
            int group_id = g + 1;
            raft_params params = create_default_raft_params();

            int ret = launchers[i]->add_group(
                group_id,
                state_machines[g][i],
                state_managers[g][i],
                params);

            if (ret != 0) {
                TestSuite::Msg() << "Failed to add group " << group_id
                                 << " to launcher " << i << std::endl;
                // Cleanup
                for (auto& l : launchers) {
                    l->shutdown(1);
                }
                return -1;
            }
        }
    }

    _msg("Groups added successfully\n");

    // Form Raft clusters for each group
    _msg("Forming Raft clusters for each group...\n");

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;

        // First, wait for a leader to be elected
        if (wait_for_leader(launchers, group_id, 10) != 0) {
            TestSuite::Msg() << "No leader elected for group " << group_id << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }

        ptr<raft_server> leader = find_leader(launchers, group_id);
        TestSuite::Msg mm;
        mm << "  Group " << group_id << ": leader is server "
           << leader->get_leader() << ", id=" << leader->get_id() << std::endl;

        // Add other servers to the cluster through the leader
        for (int i = 1; i < NUM_SERVERS; i++) {
            ptr<raft_server> server = launchers[i]->get_server(group_id);
            ptr<TestMgr> smgr = state_managers[g][i];

            if (!server) {
                TestSuite::Msg() << "Failed to get server for group " << group_id << std::endl;
                // Cleanup
                for (auto& l : launchers) {
                    l->shutdown(1);
                }
                return -1;
            }

            // Skip if this server is the leader
            if (server->get_id() == leader->get_leader()) {
                continue;
            }

            TestSuite::Msg mm;
            mm << "  Adding server " << (i + 1) << " (endpoint: "
               << smgr->get_srv_config()->get_endpoint()
               << ") to group " << group_id << std::endl;
            ptr<cmd_result<ptr<buffer>>> result = leader->add_srv(*smgr->get_srv_config());
            if (!result->get_accepted()) {
                TestSuite::Msg() << "  Failed to add server to cluster" << std::endl;
            }
        }
    }

    _msg("Raft clusters formed\n");

    // Wait for cluster configuration to propagate and heartbeats to start
    TestSuite::sleep_sec(5, "wait for cluster config and heartbeats");

    // Send messages to each group
    _msg("Sending messages to each group...\n");

    const int NUM_MESSAGES = 5;

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;
        ptr<raft_server> leader = find_leader(launchers, group_id);

        if (!leader) {
            TestSuite::Msg() << "Failed to find leader for group " << group_id << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }

        for (int m = 0; m < NUM_MESSAGES; m++) {
            std::string msg_str = "group_" + std::to_string(group_id) +
                                  "_msg_" + std::to_string(m);
            ptr<buffer> msg = buffer::alloc(msg_str.size() + 1);
            msg->put(msg_str);

            ptr<cmd_result<ptr<buffer>>> result = leader->append_entries({msg});

            if (!result->get_accepted()) {
                TestSuite::Msg() << "Message not accepted by group " << group_id << std::endl;
                // Cleanup
                for (auto& l : launchers) {
                    l->shutdown(1);
                }
                return -1;
            }
        }
    }

    _msg("Messages sent successfully\n");

    // Wait for messages to be committed
    TestSuite::sleep_sec(2, "wait for messages to commit");

    // Verify that each group has committed the correct number of messages
    _msg("Verifying message commits...\n");

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;
        size_t min_commits = SIZE_MAX;
        size_t max_commits = 0;

        for (int i = 0; i < NUM_SERVERS; i++) {
            size_t commit_count = state_machines[g][i]->last_commit_index();
            min_commits = std::min(min_commits, commit_count);
            max_commits = std::max(max_commits, commit_count);
        }

        TestSuite::Msg mm;
        mm << "  Group " << group_id << ": commits range ["
           << min_commits << ", " << max_commits << "]" << std::endl;

        // All servers should have at least NUM_MESSAGES commits
        if (min_commits < (size_t)NUM_MESSAGES) {
            TestSuite::Msg() << "FAIL: Group " << group_id << " has only " << min_commits
                             << " commits (expected " << NUM_MESSAGES << ")" << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    _msg("Verification passed: all groups committed messages independently\n");

    // Cleanup
    _msg("Cleaning up...\n");
    for (auto& l : launchers) {
        l->shutdown(1);
    }

    _msg("Test 1 PASSED\n\n");

    return 0;
}

// ============================================================================
// Test 2: Dynamic Group Management
// ============================================================================

static int test_dynamic_group_management() {
    reset_port_sharing_test_logs();

    const int BASE_PORT = 21002;
    const int NUM_SERVERS = 2;

    std::vector<ptr<raft_launcher>> launchers;

    _msg("=== Test 2: Dynamic Group Management ===\n");

    // Create launchers with DIFFERENT ports
    for (int i = 0; i < NUM_SERVERS; i++) {
        int port = BASE_PORT + i;
        ptr<raft_launcher> launcher = create_shared_port_launcher(port);
        if (!launcher) {
            TestSuite::Msg() << "Failed to create launcher " << i << std::endl;
            return -1;
        }
        launchers.push_back(launcher);
    }

    // Add initial groups (1 and 2)
    _msg("Adding initial groups 1 and 2...\n");

    std::vector<std::vector<ptr<TestSm>>> state_machines;
    std::vector<std::vector<ptr<TestMgr>>> state_managers;

    state_machines.resize(3);  // groups 1, 2, 3
    state_managers.resize(3);

    for (int g = 0; g < 2; g++) {  // only groups 1 and 2 initially
        int group_id = g + 1;
        state_machines[g].resize(NUM_SERVERS);
        state_managers[g].resize(NUM_SERVERS);

        for (int i = 0; i < NUM_SERVERS; i++) {
            int server_id = i + 1;
            int port = BASE_PORT + i;
            std::string endpoint = "tcp://127.0.0.1:" + std::to_string(port);

            state_machines[g][i] = create_state_machine(group_id, server_id);
            state_managers[g][i] = create_state_manager(group_id, server_id, endpoint);

            raft_params params = create_default_raft_params();
            int ret = launchers[i]->add_group(
                group_id,
                state_machines[g][i],
                state_managers[g][i],
                params);

            if (ret != 0) {
                TestSuite::Msg() << "Failed to add group " << group_id
                                 << " to launcher " << i << std::endl;
                // Cleanup
                for (auto& l : launchers) {
                    l->shutdown(1);
                }
                return -1;
            }
        }
    }

    _msg("Groups 1 and 2 added\n");

    // Form Raft clusters for groups 1 and 2
    _msg("Forming Raft clusters for groups 1 and 2...\n");

    for (int g = 0; g < 2; g++) {
        int group_id = g + 1;
        for (int i = 1; i < NUM_SERVERS; i++) {
            ptr<raft_server> leader = launchers[0]->get_server(group_id);
            ptr<raft_server> server = launchers[i]->get_server(group_id);
            ptr<TestMgr> smgr = state_managers[g][i];

            leader->add_srv(*smgr->get_srv_config());
        }
    }

    // Wait for leaders
    for (int g = 0; g < 2; g++) {
        int group_id = g + 1;
        if (wait_for_leader(launchers, group_id, 10) != 0) {
            TestSuite::Msg() << "No leader elected for group " << group_id << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    _msg("Groups 1 and 2 ready\n");

    // Dynamically add group 3
    _msg("Dynamically adding group 3...\n");

    int group_id_3 = 3;
    state_machines[2].resize(NUM_SERVERS);
    state_managers[2].resize(NUM_SERVERS);

    for (int i = 0; i < NUM_SERVERS; i++) {
        int server_id = i + 1;
        int port = BASE_PORT + i;
        std::string endpoint = "tcp://127.0.0.1:" + std::to_string(port);

        state_machines[2][i] = create_state_machine(group_id_3, server_id);
        state_managers[2][i] = create_state_manager(group_id_3, server_id, endpoint);

        raft_params params = create_default_raft_params();
        int ret = launchers[i]->add_group(
            group_id_3,
            state_machines[2][i],
            state_managers[2][i],
            params);

        if (ret != 0) {
            TestSuite::Msg() << "Failed to add group " << group_id_3
                             << " to launcher " << i << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    _msg("Group 3 added successfully\n");

    // Verify group 3 is accessible
    for (int i = 0; i < NUM_SERVERS; i++) {
        ptr<raft_server> server = launchers[i]->get_server(group_id_3);
        if (!server) {
            TestSuite::Msg() << "Failed to get group 3 from launcher " << i << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    _msg("Group 3 is accessible\n");

    // Form cluster for group 3
    for (int i = 1; i < NUM_SERVERS; i++) {
        ptr<raft_server> leader = launchers[0]->get_server(group_id_3);
        ptr<raft_server> server = launchers[i]->get_server(group_id_3);
        ptr<TestMgr> smgr = state_managers[2][i];

        leader->add_srv(*smgr->get_srv_config());
    }

    // Wait for leader for group 3
    if (wait_for_leader(launchers, group_id_3, 10) != 0) {
        TestSuite::Msg() << "No leader elected for group 3" << std::endl;
        // Cleanup
        for (auto& l : launchers) {
            l->shutdown(1);
        }
        return -1;
    }

    _msg("Group 3 leader elected\n");

    // Remove group 2
    _msg("Removing group 2...\n");

    int ret = launchers[0]->remove_group(2);
    if (ret != 0) {
        TestSuite::Msg() << "Failed to remove group 2 from launcher 0" << std::endl;
        // Cleanup
        for (auto& l : launchers) {
            l->shutdown(1);
        }
        return -1;
    }

    ret = launchers[1]->remove_group(2);
    if (ret != 0) {
        TestSuite::Msg() << "Failed to remove group 2 from launcher 1" << std::endl;
        // Cleanup
        for (auto& l : launchers) {
            l->shutdown(1);
        }
        return -1;
    }

    _msg("Group 2 removed successfully\n");

    // Verify group 2 is no longer accessible
    for (int i = 0; i < NUM_SERVERS; i++) {
        ptr<raft_server> server = launchers[i]->get_server(2);
        if (server != nullptr) {
            TestSuite::Msg() << "FAIL: Group 2 is still accessible from launcher "
                             << i << " after removal" << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    _msg("Group 2 is no longer accessible\n");

    // Verify groups 1 and 3 are still accessible
    for (int i = 0; i < NUM_SERVERS; i++) {
        ptr<raft_server> server1 = launchers[i]->get_server(1);
        ptr<raft_server> server3 = launchers[i]->get_server(3);

        if (!server1) {
            TestSuite::Msg() << "FAIL: Group 1 is not accessible from launcher " << i << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }

        if (!server3) {
            TestSuite::Msg() << "FAIL: Group 3 is not accessible from launcher " << i << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    _msg("Groups 1 and 3 are still accessible\n");

    // Cleanup
    for (auto& l : launchers) {
        l->shutdown(1);
    }

    _msg("Test 2 PASSED\n\n");

    return 0;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    TestSuite ts;

    ts.doTest("multi-group shared port", test_multi_group_shared_port);
    ts.doTest("dynamic group management", test_dynamic_group_management);

    return 0;
}
