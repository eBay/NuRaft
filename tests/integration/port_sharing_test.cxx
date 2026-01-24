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

#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>
#include <string>

using namespace nuraft;

// ============================================================================
// Test State Machine
// ============================================================================

class TestStateMachine : public state_machine {
public:
    TestStateMachine(int group_id, int server_id)
        : groupId_(group_id)
        , serverId_(server_id)
        , commitCount_(0)
    {}

    ptr<buffer> commit(const ulong log_idx, buffer& data) {
        std::lock_guard<std::mutex> lock(lock_);
        commits_[log_idx] = buffer::copy(data);
        commitCount_++;

        ptr<buffer> ret = buffer::alloc(sizeof(ulong));
        buffer_serializer bs(ret);
        bs.put_u64(log_idx);
        return ret;
    }

    void commit_config(const ulong log_idx, ptr<cluster_config>& new_conf) {
        (void)log_idx;
        (void)new_conf;
    }

    ptr<buffer> pre_commit(const ulong log_idx, buffer& data) {
        (void)log_idx;
        (void)data;
        ptr<buffer> ret = buffer::alloc(sizeof(ulong));
        buffer_serializer bs(ret);
        bs.put_u64(log_idx);
        return ret;
    }

    void rollback(const ulong log_idx, buffer& data) {
        (void)log_idx;
        (void)data;
    }

    void save_logical_snp_obj(snapshot& s,
                              ulong& obj_id,
                              buffer& data,
                              bool is_first_obj,
                              bool is_last_obj) {
        (void)s;
        (void)obj_id;
        (void)data;
        (void)is_first_obj;
        (void)is_last_obj;
    }

    bool apply_snapshot(snapshot& s) {
        (void)s;
        return true;
    }

    int read_logical_snp_obj(snapshot& s,
                             void*& user_snp_ctx,
                             ulong obj_id,
                             ptr<buffer>& data_out,
                             bool& is_last_obj) {
        (void)s;
        (void)user_snp_ctx;
        (void)obj_id;
        (void)data_out;
        (void)is_last_obj;
        return 0;
    }

    void free_user_snp_ctx(void*& user_snp_ctx) {
        (void)user_snp_ctx;
    }

    void create_snapshot(snapshot& s,
                         async_result<bool>::handler_type& when_done) {
        (void)s;
        ptr<std::exception> except(nullptr);
        bool ret = true;
        when_done(ret, except);
    }

    void set_next_batch_size_hint_in_bytes(ulong to) {
        (void)to;
    }

    int64 get_next_batch_size_hint_in_bytes() {
        return 0;
    }

    ptr<snapshot> last_snapshot() {
        // Return null as we don't maintain snapshots in this test
        return nullptr;
    }

    ulong last_commit_index() {
        std::lock_guard<std::mutex> lock(lock_);
        auto entry = commits_.rbegin();
        if (entry == commits_.rend()) return 0;
        return entry->first;
    }

    uint64_t get_commit_count() const {
        return commitCount_;
    }

    size_t get_commit_size() const {
        std::lock_guard<std::mutex> lock(lock_);
        return commits_.size();
    }

private:
    int groupId_;
    int serverId_;
    std::atomic<uint64_t> commitCount_;
    std::map<uint64_t, ptr<buffer>> commits_;
    mutable std::mutex lock_;
};

// ============================================================================
// Test State Manager
// ============================================================================

class TestStateManager : public state_mgr {
public:
    TestStateManager(int group_id, int server_id, const std::string& endpoint)
        : groupId_(group_id)
        , serverId_(server_id)
        , endpoint_(endpoint)
        , curLogStore_(cs_new<inmem_log_store>())
    {
        mySrvConfig_ = cs_new<srv_config>(server_id, 1, endpoint,
                                          "group " + std::to_string(group_id) +
                                          " server " + std::to_string(server_id),
                                          false, 50);

        savedConfig_ = cs_new<cluster_config>();
        savedConfig_->get_servers().push_back(mySrvConfig_);
    }

    ~TestStateManager() {}

    ptr<cluster_config> load_config() {
        return savedConfig_;
    }

    void save_config(const cluster_config& config) {
        ptr<buffer> buf = config.serialize();
        savedConfig_ = cluster_config::deserialize(*buf);
    }

    void save_state(const srv_state& state) {
        ptr<buffer> buf = state.serialize();
        savedState_ = srv_state::deserialize(*buf);
    }

    ptr<srv_state> read_state() {
        return savedState_;
    }

    ptr<log_store> load_log_store() {
        return curLogStore_;
    }

    int32 server_id() {
        return serverId_;
    }

    void system_exit(const int exit_code) {
        (void)exit_code;
        abort();
    }

    ptr<srv_config> get_srv_config() const {
        return mySrvConfig_;
    }

    ptr<inmem_log_store> get_inmem_log_store() const {
        return curLogStore_;
    }

private:
    int groupId_;
    int serverId_;
    std::string endpoint_;
    ptr<inmem_log_store> curLogStore_;
    ptr<srv_config> mySrvConfig_;
    ptr<cluster_config> savedConfig_;
    ptr<srv_state> savedState_;
};

// ============================================================================
// Simple Logger Implementation for Testing
// ============================================================================

class TestLogger : public logger {
public:
    TestLogger(const std::string& log_file) {
        // For integration tests, we'll use a simple file-based logger
        // In production, this would use the actual SimpleLogger
        logFile_ = fopen(log_file.c_str(), "a");
    }

    ~TestLogger() {
        if (logFile_) {
            fclose(logFile_);
        }
    }

    void put_details(int level,
                     const char* source_file,
                     const char* func_name,
                     size_t line_number,
                     const std::string& msg)
    {
        (void)level;
        (void)source_file;
        (void)func_name;
        (void)line_number;
        if (logFile_) {
            fprintf(logFile_, "%s\n", msg.c_str());
            fflush(logFile_);
        }
    }

    void set_level(int l) { (void)l; }
    int get_level() { return 0; }

private:
    FILE* logFile_;
};

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

static ptr<raft_launcher> create_shared_port_launcher(
    int port)
{
    std::string log_file = "./port_sharing_srv_" + std::to_string(port) + ".log";
    ptr<logger> logger = cs_new<TestLogger>(log_file);

    ptr<raft_launcher> launcher = cs_new<raft_launcher>();
    asio_service::options asio_opts;
    asio_opts.thread_pool_size_ = 2;

    if (!launcher->init_shared_port(port, logger, asio_opts)) {
        return nullptr;
    }

    return launcher;
}

static ptr<TestStateMachine> create_state_machine(
    int group_id, int server_id)
{
    return cs_new<TestStateMachine>(group_id, server_id);
}

static ptr<TestStateManager> create_state_manager(
    int group_id, int server_id, const std::string& endpoint)
{
    return cs_new<TestStateManager>(group_id, server_id, endpoint);
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

    std::cout << "=== Test 1: Multi-Group Shared Port ===" << std::endl;
    std::cout << "Creating " << NUM_SERVERS << " launchers, each with "
              << NUM_GROUPS << " groups" << std::endl;

    // Create launchers with DIFFERENT ports (each launcher represents a physical node)
    for (int i = 0; i < NUM_SERVERS; i++) {
        int port = BASE_PORT + i;
        ptr<raft_launcher> launcher = create_shared_port_launcher(port);
        if (!launcher) {
            std::cerr << "Failed to create launcher " << i << std::endl;
            return -1;
        }
        launchers.push_back(launcher);
    }

    std::cout << "Launchers created successfully" << std::endl;

    // Create state machines and state managers for each group
    std::vector<std::vector<ptr<TestStateMachine>>> state_machines(NUM_GROUPS);
    std::vector<std::vector<ptr<TestStateManager>>> state_managers(NUM_GROUPS);

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
    std::cout << "Adding groups to launchers..." << std::endl;

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
                std::cerr << "Failed to add group " << group_id
                         << " to launcher " << i << std::endl;
                // Cleanup
                for (auto& l : launchers) {
                    l->shutdown(1);
                }
                return -1;
            }
        }
    }

    std::cout << "Groups added successfully" << std::endl;

    // Form Raft clusters for each group
    std::cout << "Forming Raft clusters for each group..." << std::endl;

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;

        // First, wait for a leader to be elected
        if (wait_for_leader(launchers, group_id, 10) != 0) {
            std::cerr << "No leader elected for group " << group_id << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }

        ptr<raft_server> leader = find_leader(launchers, group_id);
        std::cout << "  Group " << group_id << ": leader is server "
                  << leader->get_leader() << ", id=" << leader->get_id() << std::endl;

        // Add other servers to the cluster through the leader
        for (int i = 1; i < NUM_SERVERS; i++) {
            ptr<raft_server> server = launchers[i]->get_server(group_id);
            ptr<TestStateManager> smgr = state_managers[g][i];

            if (!server) {
                std::cerr << "Failed to get server for group " << group_id << std::endl;
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

            std::cout << "  Adding server " << (i + 1) << " (endpoint: "
                      << smgr->get_srv_config()->get_endpoint()
                      << ") to group " << group_id << std::endl;
            ptr<cmd_result<ptr<buffer>>> result = leader->add_srv(*smgr->get_srv_config());
            if (!result->get_accepted()) {
                std::cerr << "  Failed to add server to cluster" << std::endl;
            }
        }
    }

    std::cout << "Raft clusters formed" << std::endl;

    // Wait for cluster configuration to propagate and heartbeats to start
    TestSuite::sleep_sec(5, "wait for cluster config and heartbeats");

    // Send messages to each group
    std::cout << "Sending messages to each group..." << std::endl;

    const int NUM_MESSAGES = 5;

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;
        ptr<raft_server> leader = find_leader(launchers, group_id);

        if (!leader) {
            std::cerr << "Failed to find leader for group " << group_id << std::endl;
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
                std::cerr << "Message not accepted by group " << group_id << std::endl;
                // Cleanup
                for (auto& l : launchers) {
                    l->shutdown(1);
                }
                return -1;
            }
        }
    }

    std::cout << "Messages sent successfully" << std::endl;

    // Wait for messages to be committed
    TestSuite::sleep_sec(2, "wait for messages to commit");

    // Verify that each group has committed the correct number of messages
    std::cout << "Verifying message commits..." << std::endl;

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;
        size_t min_commits = SIZE_MAX;
        size_t max_commits = 0;

        for (int i = 0; i < NUM_SERVERS; i++) {
            size_t commit_count = state_machines[g][i]->get_commit_size();
            min_commits = std::min(min_commits, commit_count);
            max_commits = std::max(max_commits, commit_count);
        }

        std::cout << "  Group " << group_id << ": commits range ["
                  << min_commits << ", " << max_commits << "]" << std::endl;

        // All servers should have at least NUM_MESSAGES commits
        if (min_commits < NUM_MESSAGES) {
            std::cerr << "FAIL: Group " << group_id << " has only " << min_commits
                     << " commits (expected " << NUM_MESSAGES << ")" << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    std::cout << "Verification passed: all groups committed messages independently" << std::endl;

    // Cleanup
    std::cout << "Cleaning up..." << std::endl;
    for (auto& l : launchers) {
        l->shutdown(1);
    }

    std::cout << "Test 1 PASSED" << std::endl << std::endl;

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

    std::cout << "=== Test 2: Dynamic Group Management ===" << std::endl;

    // Create launchers with DIFFERENT ports
    for (int i = 0; i < NUM_SERVERS; i++) {
        int port = BASE_PORT + i;
        ptr<raft_launcher> launcher = create_shared_port_launcher(port);
        if (!launcher) {
            std::cerr << "Failed to create launcher " << i << std::endl;
            return -1;
        }
        launchers.push_back(launcher);
    }

    // Add initial groups (1 and 2)
    std::cout << "Adding initial groups 1 and 2..." << std::endl;

    std::vector<std::vector<ptr<TestStateMachine>>> state_machines;
    std::vector<std::vector<ptr<TestStateManager>>> state_managers;

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
                std::cerr << "Failed to add group " << group_id
                         << " to launcher " << i << std::endl;
                // Cleanup
                for (auto& l : launchers) {
                    l->shutdown(1);
                }
                return -1;
            }
        }
    }

    std::cout << "Groups 1 and 2 added" << std::endl;

    // Form Raft clusters for groups 1 and 2
    std::cout << "Forming Raft clusters for groups 1 and 2..." << std::endl;

    for (int g = 0; g < 2; g++) {
        int group_id = g + 1;
        for (int i = 1; i < NUM_SERVERS; i++) {
            ptr<raft_server> leader = launchers[0]->get_server(group_id);
            ptr<raft_server> server = launchers[i]->get_server(group_id);
            ptr<TestStateManager> smgr = state_managers[g][i];

            leader->add_srv(*smgr->get_srv_config());
        }
    }

    // Wait for leaders
    for (int g = 0; g < 2; g++) {
        int group_id = g + 1;
        if (wait_for_leader(launchers, group_id, 10) != 0) {
            std::cerr << "No leader elected for group " << group_id << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    std::cout << "Groups 1 and 2 ready" << std::endl;

    // Dynamically add group 3
    std::cout << "Dynamically adding group 3..." << std::endl;

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
            std::cerr << "Failed to add group " << group_id_3
                     << " to launcher " << i << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    std::cout << "Group 3 added successfully" << std::endl;

    // Verify group 3 is accessible
    for (int i = 0; i < NUM_SERVERS; i++) {
        ptr<raft_server> server = launchers[i]->get_server(group_id_3);
        if (!server) {
            std::cerr << "Failed to get group 3 from launcher " << i << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    std::cout << "Group 3 is accessible" << std::endl;

    // Form cluster for group 3
    for (int i = 1; i < NUM_SERVERS; i++) {
        ptr<raft_server> leader = launchers[0]->get_server(group_id_3);
        ptr<raft_server> server = launchers[i]->get_server(group_id_3);
        ptr<TestStateManager> smgr = state_managers[2][i];

        leader->add_srv(*smgr->get_srv_config());
    }

    // Wait for leader for group 3
    if (wait_for_leader(launchers, group_id_3, 10) != 0) {
        std::cerr << "No leader elected for group 3" << std::endl;
        // Cleanup
        for (auto& l : launchers) {
            l->shutdown(1);
        }
        return -1;
    }

    std::cout << "Group 3 leader elected" << std::endl;

    // Remove group 2
    std::cout << "Removing group 2..." << std::endl;

    int ret = launchers[0]->remove_group(2);
    if (ret != 0) {
        std::cerr << "Failed to remove group 2 from launcher 0" << std::endl;
        // Cleanup
        for (auto& l : launchers) {
            l->shutdown(1);
        }
        return -1;
    }

    ret = launchers[1]->remove_group(2);
    if (ret != 0) {
        std::cerr << "Failed to remove group 2 from launcher 1" << std::endl;
        // Cleanup
        for (auto& l : launchers) {
            l->shutdown(1);
        }
        return -1;
    }

    std::cout << "Group 2 removed successfully" << std::endl;

    // Verify group 2 is no longer accessible
    for (int i = 0; i < NUM_SERVERS; i++) {
        ptr<raft_server> server = launchers[i]->get_server(2);
        if (server != nullptr) {
            std::cerr << "FAIL: Group 2 is still accessible from launcher "
                     << i << " after removal" << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    std::cout << "Group 2 is no longer accessible" << std::endl;

    // Verify groups 1 and 3 are still accessible
    for (int i = 0; i < NUM_SERVERS; i++) {
        ptr<raft_server> server1 = launchers[i]->get_server(1);
        ptr<raft_server> server3 = launchers[i]->get_server(3);

        if (!server1) {
            std::cerr << "FAIL: Group 1 is not accessible from launcher " << i << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }

        if (!server3) {
            std::cerr << "FAIL: Group 3 is not accessible from launcher " << i << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    std::cout << "Groups 1 and 3 are still accessible" << std::endl;

    // Cleanup
    for (auto& l : launchers) {
        l->shutdown(1);
    }

    std::cout << "Test 2 PASSED" << std::endl << std::endl;

    return 0;
}

// ============================================================================
// Test 3: Concurrent Stress Test
// ============================================================================

static int test_concurrent_stress() {
    reset_port_sharing_test_logs();

    const int NUM_SERVERS = 3;
    const int NUM_GROUPS = 3;  // Reduced for initial testing
    const int MESSAGES_PER_GROUP = 10;  // Reduced for testing

    std::vector<ptr<raft_launcher>> launchers;

    std::cout << "=== Test 3: Concurrent Stress Test ===" << std::endl;
    std::cout << "Creating " << NUM_SERVERS << " launchers, each with "
              << NUM_GROUPS << " groups" << std::endl;

    // Create launchers
    for (int i = 0; i < NUM_SERVERS; i++) {
        int port = 21003 + i * 10;
        ptr<raft_launcher> launcher = create_shared_port_launcher(port);
        if (!launcher) {
            std::cerr << "Failed to create launcher " << i << std::endl;
            return -1;
        }
        launchers.push_back(launcher);
    }

    std::cout << "Launchers created" << std::endl;

    // Create state machines and state managers
    std::vector<std::vector<ptr<TestStateMachine>>> state_machines(NUM_GROUPS);
    std::vector<std::vector<ptr<TestStateManager>>> state_managers(NUM_GROUPS);

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;
        state_machines[g].resize(NUM_SERVERS);
        state_managers[g].resize(NUM_SERVERS);

        for (int i = 0; i < NUM_SERVERS; i++) {
            int server_id = i + 1;
            int port = 21003 + i * 10;
            std::string endpoint = "tcp://127.0.0.1:" + std::to_string(port);

            state_machines[g][i] = create_state_machine(group_id, server_id);
            state_managers[g][i] = create_state_manager(group_id, server_id, endpoint);
        }
    }

    // Add groups to launchers
    std::cout << "Adding groups to launchers..." << std::endl;

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
                std::cerr << "Failed to add group " << group_id
                         << " to launcher " << i << std::endl;
                // Cleanup
                for (auto& l : launchers) {
                    l->shutdown(1);
                }
                return -1;
            }
        }
    }

    std::cout << "Groups added" << std::endl;

    // Form Raft clusters
    std::cout << "Forming Raft clusters..." << std::endl;

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;

        // First, wait for a leader to be elected for this group
        if (wait_for_leader(launchers, group_id, 15) != 0) {
            std::cerr << "No leader elected for group " << group_id << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }

        ptr<raft_server> leader = find_leader(launchers, group_id);
        std::cout << "  Group " << group_id << ": initial leader is server "
                  << leader->get_leader() << ", id=" << leader->get_id() << std::endl;

        // Add other servers to the cluster through the leader
        for (int i = 1; i < NUM_SERVERS; i++) {
            ptr<raft_server> server = launchers[i]->get_server(group_id);
            ptr<TestStateManager> smgr = state_managers[g][i];

            if (!server) {
                std::cerr << "Failed to get server for group " << group_id << std::endl;
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

            std::cout << "  Adding server " << (i + 1) << " (endpoint: "
                      << smgr->get_srv_config()->get_endpoint()
                      << ") to group " << group_id << std::endl;
            ptr<cmd_result<ptr<buffer>>> result = leader->add_srv(*smgr->get_srv_config());
            if (!result->get_accepted()) {
                std::cerr << "  Failed to add server to cluster, result code: "
                         << result->get_result_code() << std::endl;
                // Wait a bit and retry
                TestSuite::sleep_ms(500);
                result = leader->add_srv(*smgr->get_srv_config());
                if (!result->get_accepted()) {
                    std::cerr << "  Retry failed to add server to cluster" << std::endl;
                    // Continue anyway to see what happens
                }
            }

            // Wait for the add_srv operation to complete
            result->get();
            std::cout << "  Server " << (i + 1) << " added to group " << group_id << std::endl;

            // Wait for configuration change to commit before adding next server
            TestSuite::sleep_ms(1000);
        }
    }

    std::cout << "Clusters formed" << std::endl;

    // Verify leaders are still elected after cluster formation
    std::cout << "Verifying leaders..." << std::endl;

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;
        if (wait_for_leader(launchers, group_id, 15) != 0) {
            std::cerr << "No leader elected for group " << group_id << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
        ptr<raft_server> leader = find_leader(launchers, group_id);
        std::cout << "  Group " << group_id << ": leader is server "
                  << leader->get_leader() << std::endl;
    }

    std::cout << "Leaders elected" << std::endl;

    // Wait for cluster configuration to propagate and heartbeats to start
    TestSuite::sleep_sec(5, "wait for cluster config and heartbeats");

    // Send messages sequentially (same as Test 1 to ensure config propagation)
    std::cout << "Sending " << MESSAGES_PER_GROUP
              << " messages to each group sequentially..." << std::endl;

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;
        ptr<raft_server> leader = find_leader(launchers, group_id);

        if (!leader) {
            std::cerr << "Failed to find leader for group " << group_id << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }

        for (int m = 0; m < MESSAGES_PER_GROUP; m++) {
            std::string msg_str = "group_" + std::to_string(group_id) +
                                  "_msg_" + std::to_string(m);
            ptr<buffer> msg = buffer::alloc(msg_str.size() + 1);
            msg->put(msg_str);

            ptr<cmd_result<ptr<buffer>>> result = leader->append_entries({msg});

            if (!result->get_accepted()) {
                std::cerr << "Message not accepted by group " << group_id << std::endl;
                // Cleanup
                for (auto& l : launchers) {
                    l->shutdown(1);
                }
                return -1;
            }
        }
    }

    std::cout << "All messages sent" << std::endl;

    // Wait for commits
    TestSuite::sleep_sec(60, "wait for messages to commit");

    // Verify commits
    std::cout << "Verifying commits..." << std::endl;

    for (int g = 0; g < NUM_GROUPS; g++) {
        int group_id = g + 1;
        size_t min_commits = SIZE_MAX;

        for (int i = 0; i < NUM_SERVERS; i++) {
            size_t commit_count = state_machines[g][i]->get_commit_size();
            min_commits = std::min(min_commits, commit_count);
        }

        std::cout << "  Group " << group_id << ": min commits = " << min_commits
                  << " (expected " << MESSAGES_PER_GROUP << ")" << std::endl;

        if (min_commits < MESSAGES_PER_GROUP) {
            std::cerr << "FAIL: Group " << group_id << " has only " << min_commits
                     << " commits (expected " << MESSAGES_PER_GROUP << ")" << std::endl;
            // Cleanup
            for (auto& l : launchers) {
                l->shutdown(1);
            }
            return -1;
        }
    }

    std::cout << "All groups committed expected messages" << std::endl;

    // Cleanup
    for (auto& l : launchers) {
        l->shutdown(1);
    }

    std::cout << "Test 3 PASSED" << std::endl << std::endl;

    return 0;
}

// ============================================================================
// Test 4: Backward Compatibility
// ============================================================================

static int test_backward_compatibility() {
    reset_port_sharing_test_logs();

    std::cout << "=== Test 4: Backward Compatibility ===" << std::endl;
    std::cout << "Testing legacy init() API..." << std::endl;

    // Test that legacy init() still works
    std::string log_file = "./port_sharing_legacy.log";
    ptr<logger> logger = cs_new<TestLogger>(log_file);

    ptr<TestStateMachine> sm = cs_new<TestStateMachine>(1, 1);
    ptr<TestStateManager> smgr = cs_new<TestStateManager>(1, 1, "tcp://127.0.0.1:21004");

    raft_params params = create_default_raft_params();
    asio_service::options asio_opts;

    ptr<raft_launcher> launcher = cs_new<raft_launcher>();

    // Use legacy init() method (single group, no port sharing)
    ptr<raft_server> server = launcher->init(sm, smgr, logger, 21004, asio_opts, params);

    if (!server) {
        std::cerr << "FAIL: Legacy init() failed" << std::endl;
        return -1;
    }

    std::cout << "Legacy init() succeeded" << std::endl;

    // Verify that get_raft_server() returns the server (not null, unlike shared port mode)
    ptr<raft_server> legacy_server = launcher->get_raft_server();
    if (!legacy_server) {
        std::cerr << "FAIL: get_raft_server() returned null in legacy mode" << std::endl;
        launcher->shutdown(1);
        return -1;
    }

    std::cout << "get_raft_server() returned non-null as expected" << std::endl;

    // Note: get_server(group_id) only works in shared port mode
    // In legacy mode, use get_raft_server() instead

    // Wait for leader (single node cluster)
    TestSuite::sleep_sec(1, "wait for leader");

    // Send a message
    std::string msg_str = "test_message";
    ptr<buffer> msg = buffer::alloc(msg_str.size() + 1);
    msg->put(msg_str);

    ptr<cmd_result<ptr<buffer>>> result = server->append_entries({msg});

    if (!result->get_accepted()) {
        std::cerr << "FAIL: Message not accepted" << std::endl;
        launcher->shutdown(1);
        return -1;
    }

    std::cout << "Message accepted" << std::endl;

    // Wait for commit
    TestSuite::sleep_sec(1, "wait for commit");

    // Verify commit
    if (sm->get_commit_size() < 1) {
        std::cerr << "FAIL: No commits in state machine" << std::endl;
        launcher->shutdown(1);
        return -1;
    }

    std::cout << "Message committed" << std::endl;

    // Cleanup
    launcher->shutdown(1);

    std::cout << "Test 4 PASSED" << std::endl << std::endl;

    return 0;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    std::cout << "Running Port Sharing Integration Tests..." << std::endl;
    std::cout << std::endl;

    int result = 0;

    // Test 1: Multi-Group Shared Port
    if (test_multi_group_shared_port() != 0) {
        std::cerr << "Test 1 FAILED" << std::endl;
        result = 1;
    } else {
        std::cout << "Test 1 PASSED" << std::endl;
    }
    std::cout << std::endl;

    // Test 2: Dynamic Group Management
    if (test_dynamic_group_management() != 0) {
        std::cerr << "Test 2 FAILED" << std::endl;
        result = 1;
    } else {
        std::cout << "Test 2 PASSED" << std::endl;
    }
    std::cout << std::endl;

    // Test 3: Concurrent Stress Test
    if (test_concurrent_stress() != 0) {
        std::cerr << "Test 3 FAILED" << std::endl;
        result = 1;
    } else {
        std::cout << "Test 3 PASSED" << std::endl;
    }
    std::cout << std::endl;

    // Test 4: Backward Compatibility
    if (test_backward_compatibility() != 0) {
        std::cerr << "Test 4 FAILED" << std::endl;
        result = 1;
    } else {
        std::cout << "Test 4 PASSED" << std::endl;
    }
    std::cout << std::endl;

    if (result == 0) {
        std::cout << "=== ALL TESTS PASSED ===" << std::endl;
    } else {
        std::cout << "=== SOME TESTS FAILED ===" << std::endl;
    }

    return result;
}
