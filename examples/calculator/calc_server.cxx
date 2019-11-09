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

#include "calc_state_machine.hxx"
#include "in_memory_state_mgr.hxx"
#include "logger_wrapper.hxx"

#include "nuraft.hxx"

#include "test_common.h"

#include <iostream>
#include <sstream>

#include <stdio.h>

using namespace nuraft;

namespace calc_server {

static const raft_params::return_method_type CALL_TYPE
    = raft_params::blocking;
//  = raft_params::async_handler;

#include "example_common.hxx"

calc_state_machine* get_sm() {
    return static_cast<calc_state_machine*>( stuff.sm_.get() );
}

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
    ptr<buffer> buf = result.get();
    uint64_t ret_value = buf->get_ulong();
    std::cout << "succeeded, "
              << TestSuite::usToString( timer->getTimeUs() )
              << ", return value: "
              << ret_value
              << ", state machine value: "
              << get_sm()->get_current_value()
              << std::endl;
}

void append_log(const std::string& cmd,
                const std::vector<std::string>& tokens)
{
    char cmd_char = cmd[0];
    int operand = atoi( tokens[0].substr(1).c_str() );
    calc_state_machine::op_type op = calc_state_machine::ADD;
    switch (cmd_char) {
    case '+':   op = calc_state_machine::ADD;   break;
    case '-':   op = calc_state_machine::SUB;   break;
    case '*':   op = calc_state_machine::MUL;   break;
    case '/':
        op = calc_state_machine::DIV;
        if (!operand) {
            std::cout << "cannot divide by zero" << std::endl;
            return;
        }
        break;
    default:    op = calc_state_machine::SET;   break;
    };

    // Serialize and generate Raft log to append.
    ptr<buffer> new_log = calc_state_machine::enc_log( {op, operand} );

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
            << stuff.raft_instance_->get_committed_log_idx() << std::endl
        << "state machine value: "
            << get_sm()->get_current_value() << std::endl;
}

void help(const std::string& cmd,
          const std::vector<std::string>& tokens)
{
    std::cout
    << "modify value: <+|-|*|/><operand>\n"
    << "    +: add <operand> to state machine's value.\n"
    << "    -: subtract <operand> from state machine's value.\n"
    << "    *: multiple state machine'value by <operand>.\n"
    << "    /: divide state machine's value by <operand>.\n"
    << "    e.g.) +123\n"
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
        stuff.reset();
        return false;

    } else if ( cmd[0] == '+' ||
                cmd[0] == '-' ||
                cmd[0] == '*' ||
                cmd[0] == '/' ) {
        // e.g.) +1
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

}; // namespace calc_server;
using namespace calc_server;

int main(int argc, char** argv) {
    if (argc < 3) usage(argc, argv);

    set_server_info(argc, argv);

    std::cout << "    -- Replicated Calculator with Raft --" << std::endl;
    std::cout << "                         Version 0.1.0" << std::endl;
    std::cout << "    Server ID:    " << stuff.server_id_ << std::endl;
    std::cout << "    Endpoint:     " << stuff.endpoint_ << std::endl;
    init_raft( cs_new<calc_state_machine>() );
    loop();

    return 0;
}

