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
#include "logger.hxx"
#include "test_common.h"
#include <string>
#include <memory>
#include <stdexcept>

namespace nuraft {

// Simple logger implementation for testing
class simple_logger : public logger {
public:
    simple_logger() {}
    virtual ~simple_logger() {}

    void put_details(int level,
                     const char* source_file,
                     const char* func_name,
                     size_t line_number,
                     const std::string& log_message)
    {
        // Suppress output for cleaner test results
    }

    void set_level(int) {}
    int get_level() { return 0; }
};

} // namespace nuraft

using namespace nuraft;

// Test functions
static int test_init_shared_port_basic() {
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;

    bool result = launcher.init_shared_port(10001, logger, asio_opts);
    CHK_TRUE(result);

    ptr<asio_service> asio_svc = launcher.get_asio_service();
    ptr<rpc_listener> listener = launcher.get_rpc_listener();

    CHK_NONNULL(asio_svc);
    CHK_NONNULL(listener);

    launcher.shutdown(1);
    return 0;
}

static int test_get_raft_server_null_in_shared_mode() {
    // Test that get_raft_server returns null in shared port mode
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;

    launcher.init_shared_port(10002, logger, asio_opts);

    ptr<raft_server> legacy_server = launcher.get_raft_server();
    CHK_NULL(legacy_server);

    launcher.shutdown(1);
    return 0;
}

static int test_double_init_fails() {
    // Test that double initialization fails
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;

    // First init
    bool result = launcher.init_shared_port(10003, logger, asio_opts);
    CHK_TRUE(result);

    // Second init should fail
    result = launcher.init_shared_port(10004, logger, asio_opts);
    CHK_TRUE(!result);

    launcher.shutdown(1);
    return 0;
}

static int test_get_server_null_before_add() {
    // Test that get_server returns null before adding any groups
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;

    launcher.init_shared_port(10005, logger, asio_opts);

    ptr<raft_server> server = launcher.get_server(1);
    CHK_NULL(server);

    launcher.shutdown(1);
    return 0;
}

static int test_remove_nonexistent_group() {
    // Test that removing non-existent group fails
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;

    launcher.init_shared_port(10006, logger, asio_opts);

    int ret = launcher.remove_group(999);
    CHK_EQ(-1, ret);

    launcher.shutdown(1);
    return 0;
}

static int test_add_group_without_init_fails() {
    // Test that add_group fails without init_shared_port
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;
    raft_params params;

    // Don't call init_shared_port

    // add_group should fail
    ptr<state_machine> sm = nullptr;
    ptr<state_mgr> smgr = nullptr;

    int ret = launcher.add_group(1, sm, smgr, params);
    CHK_EQ(-1, ret);
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts;

    ts.doTest("init_shared_port basic", test_init_shared_port_basic);
    ts.doTest("get_raft_server null in shared mode", test_get_raft_server_null_in_shared_mode);
    ts.doTest("double init fails", test_double_init_fails);
    ts.doTest("get_server null before add", test_get_server_null_before_add);
    ts.doTest("remove nonexistent group", test_remove_nonexistent_group);
    ts.doTest("add_group without init fails", test_add_group_without_init_fails);

    return 0;
}
