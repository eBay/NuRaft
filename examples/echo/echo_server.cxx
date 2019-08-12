/************************************************************************
Copyright 2017-2019 eBay Inc.
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

#include "echo_state_machine.hxx"
#include "in_memory_state_mgr.hxx"
#include "logger_wrapper.hxx"

#include "nuraft.hxx"

#include "test_common.h"

#include <iostream>
#include <sstream>

#include <stdio.h>

using namespace nuraft;

namespace echo_server {

static const raft_params::return_method_type CALL_TYPE
    = raft_params::blocking;
//  = raft_params::async_handler;

#include "example_common.hxx"

void handle_result(ptr<TestSuite::Timer> timer,
                   raft_result& result,
                   ptr<std::exception>& err)
{
    if (result.get_result_code() != cmd_result_code::OK) {
        // Something went wrong.
        // This means committing this log failed,
        // but the log itself is still in the log store.
        std::cout << "failed: " << result.get_result_code() << ", "
                  << TestSuite::usToString( timer->getTimeUs() )
                  << std::endl;
        return;
    }
    std::cout << "succeeded, "
              << TestSuite::usToString( timer->getTimeUs() )
              << std::endl;
}

void append_log(const std::string& cmd,
                const std::vector<std::string>& tokens)
{
    if (tokens.size() < 2) {
        std::cout << "too few arguments" << std::endl;
        return;
    }

    std::string cascaded_str;
    for (size_t ii=1; ii<tokens.size(); ++ii) {
        cascaded_str += tokens[ii] + " ";
    }

    // Create a new log which will contain
    // 4-byte length and string data.
    ptr<buffer> new_log = buffer::alloc(sizeof(int) + cascaded_str.size());
    buffer_serializer bs(new_log);
    bs.put_str(cascaded_str);

    // To measure the elapsed time.
    ptr<TestSuite::Timer> timer = cs_new<TestSuite::Timer>();

    // Do append.
    ptr<raft_result> ret = stuff.raft_instance_->append_entries( {new_log} );

    if (!ret->get_accepted()) {
        // Log append rejected, usually because this node is not a leader.
        std::cout << "failed to replicate: "
                  << ret->get_result_code() << ", "
                  << TestSuite::usToString( timer->getTimeUs() )
                  << std::endl;
        return;
    }
    // Log append accepted, but that doesn't mean the log is committed.
    // Commit result can be obtained below.

    if (CALL_TYPE == raft_params::blocking) {
        // Blocking mode:
        //   `append_entries` returns after getting a consensus,
        //   so that `ret` already has the result from state machine.
        ptr<std::exception> err(nullptr);
        handle_result(timer, *ret, err);

    } else if (CALL_TYPE == raft_params::async_handler) {
        // Async mode:
        //   `append_entries` returns immediately.
        //   `handle_result` will be invoked asynchronously,
        //   after getting a consensus.
        ret->when_ready( std::bind( handle_result,
                                    timer,
                                    std::placeholders::_1,
                                    std::placeholders::_2 ) );

    } else {
        assert(0);
    }
}

void print_status(const std::string& cmd,
                  const std::vector<std::string>& tokens)
{
    ptr<log_store> ls = stuff.smgr_->load_log_store();
    std::cout
        << "my server id: " << stuff.server_id_ << std::endl
        << "leader id: " << stuff.raft_instance_->get_leader() << std::endl
        << "Raft log range: "
            << ls->start_index()
            << " - " << (ls->next_slot() - 1) << std::endl
        << "last committed index: "
            << stuff.raft_instance_->get_committed_log_idx() << std::endl;
}

void help(const std::string& cmd,
          const std::vector<std::string>& tokens)
{
    std::cout
    << "echo message: msg <operand>\n"
    << "    e.g.) msg hello world!\n"
    << "\n"
    << "add server: add <server id> <address>:<port>\n"
    << "    e.g.) add 2 127.0.0.1:20000\n"
    << "\n"
    << "get current server status: st (or stat)\n"
    << "\n"
    << "get the list of members: ls (or list)\n"
    << "\n";
}

bool do_cmd(const std::vector<std::string>& tokens) {
    if (!tokens.size()) return true;

    const std::string& cmd = tokens[0];

    if (cmd == "q" || cmd == "exit") {
        stuff.launcher_.shutdown(5);
        return false;

    } else if ( cmd == "msg" ) {
        // e.g.) msg hello world
        append_log(cmd, tokens);

    } else if ( cmd == "add" ) {
        // e.g.) add 2 localhost:12345
        add_server(cmd, tokens);

    } else if ( cmd == "st" || cmd == "stat" ) {
        print_status(cmd, tokens);

    } else if ( cmd == "ls" || cmd == "list" ) {
        server_list(cmd, tokens);

    } else if ( cmd == "h" || cmd == "help" ) {
        help(cmd, tokens);
    }
    return true;
}

}; // namespace echo_server;
using namespace echo_server;

int main(int argc, char** argv) {
    if (argc < 3) usage(argc, argv);

    set_server_info(argc, argv);

    std::cout << "    -- Echo Server with Raft --" << std::endl;
    std::cout << "               Version 0.1.0" << std::endl;
    std::cout << "    Server ID:    " << stuff.server_id_ << std::endl;
    std::cout << "    Endpoint:     " << stuff.endpoint_ << std::endl;
    init_raft( cs_new<echo_state_machine>() );
    loop();

    return 0;
}

