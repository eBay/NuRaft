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
#include <iostream>
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
void test_init_shared_port_basic() {
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;

    bool result = launcher.init_shared_port(10001, logger, asio_opts);

    if (!result) {
        throw std::runtime_error("init_shared_port failed");
    }

    ptr<asio_service> asio_svc = launcher.get_asio_service();
    ptr<rpc_listener> listener = launcher.get_rpc_listener();

    if (!asio_svc) {
        throw std::runtime_error("asio_service is null");
    }
    if (!listener) {
        throw std::runtime_error("rpc_listener is null");
    }

    launcher.shutdown(1);
}

void test_get_raft_server_null_in_shared_mode() {
    // Test that get_raft_server returns null in shared port mode
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;

    launcher.init_shared_port(10002, logger, asio_opts);

    ptr<raft_server> legacy_server = launcher.get_raft_server();
    if (legacy_server != nullptr) {
        throw std::runtime_error("get_raft_server should return null in shared port mode");
    }

    launcher.shutdown(1);
}

void test_double_init_fails() {
    // Test that double initialization fails
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;

    // First init
    bool result = launcher.init_shared_port(10003, logger, asio_opts);
    if (!result) {
        throw std::runtime_error("first init_shared_port failed");
    }

    // Second init should fail
    result = launcher.init_shared_port(10004, logger, asio_opts);
    if (result) {
        throw std::runtime_error("second init_shared_port should fail");
    }

    launcher.shutdown(1);
}

void test_get_server_null_before_add() {
    // Test that get_server returns null before adding any groups
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;

    launcher.init_shared_port(10005, logger, asio_opts);

    ptr<raft_server> server = launcher.get_server(1);
    if (server != nullptr) {
        throw std::runtime_error("get_server should return null before add_group");
    }

    launcher.shutdown(1);
}

void test_remove_nonexistent_group() {
    // Test that removing non-existent group fails
    raft_launcher launcher;
    ptr<logger> logger = cs_new<simple_logger>();
    asio_service::options asio_opts;

    launcher.init_shared_port(10006, logger, asio_opts);

    int ret = launcher.remove_group(999);
    if (ret != -1) {
        throw std::runtime_error("remove_group for non-existent group should fail");
    }

    launcher.shutdown(1);
}

void test_add_group_without_init_fails() {
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
    if (ret != -1) {
        throw std::runtime_error("add_group should fail without init_shared_port");
    }
}

int main() {
    std::cout << "Running launcher tests..." << std::endl;
    try {
        test_init_shared_port_basic();
        std::cout << "  ✓ init_shared_port_basic" << std::endl;

        test_get_raft_server_null_in_shared_mode();
        std::cout << "  ✓ get_raft_server_null_in_shared_mode" << std::endl;

        test_double_init_fails();
        std::cout << "  ✓ double_init_fails" << std::endl;

        test_get_server_null_before_add();
        std::cout << "  ✓ get_server_null_before_add" << std::endl;

        test_remove_nonexistent_group();
        std::cout << "  ✓ remove_nonexistent_group" << std::endl;

        test_add_group_without_init_fails();
        std::cout << "  ✓ add_group_without_init_fails" << std::endl;

        std::cout << "\nAll tests passed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\nTest failed: " << e.what() << std::endl;
        return 1;
    }
}
