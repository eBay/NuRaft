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

#include "task_scheduler_state_machine.hxx"
#include "in_memory_state_mgr.hxx"
#include "logger_wrapper.hxx"

#include "nuraft.hxx"

#include "include/scheduler.hxx"
#include "include/task.hxx"

#include <iostream>
#include <sstream>
#include <stdio.h>
#include <unistd.h>

using namespace nuraft;

namespace task_scheduler {

static raft_params::return_method_type CALL_TYPE
    = raft_params::blocking;

static bool ASYNC_SNAPSHOT_CREATION = false;

// Simple TestSuite replacement
class TestSuite {
public:
    static void sleep_ms(int ms) {
        usleep(ms * 1000);
    }
};

// Color macros
#define _CLM_GREEN "\033[0;32m"
#define _CLM_END "\033[0m"

#include "example_common.hxx"

task_scheduler_state_machine* get_sm() {
    return static_cast<task_scheduler_state_machine*>( stuff.sm_.get() );
}

void handle_result(raft_result& result)
{
    if (result.get_result_code() != cmd_result_code::OK) {
        std::cout << "failed: " << result.get_result_code() << std::endl;
        return;
    }
    std::cout << "succeeded" << std::endl;
}


void fetch_task(const std::string& cmd,
              const std::vector<std::string>& tokens)
{
    // Parse task parameters from tokens
    // Format: add <func_name> <arrival_time> <duration> <cpu> <memory> <deadline> <priority>
    if (tokens.size() < 1) {
        std::cout << "Usage: fetch" << std::endl;
        return;
    }

    // Serialize task data
    ptr<buffer> new_log = task_scheduler_state_machine::enc_task_fetch();
ptr<raft_result> ret = stuff.raft_instance_->append_entries( {new_log} );
    if (!ret->get_accepted()) {
        std::cout << "failed to replicate: "
                  << ret->get_result_code() << std::endl;
        return;
    }
    if (CALL_TYPE == raft_params::blocking) {
        handle_result(*ret);
    } else if (CALL_TYPE == raft_params::async_handler) {
        ret->when_ready( std::bind( handle_result,
                                    std::placeholders::_1 ) );
    } else {
        assert(0);
    }
}

void complete_task(const std::string& cmd,
              const std::vector<std::string>& tokens)
{
    // Parse task parameters from tokens
    // Format: add <func_name> <arrival_time> <duration> <cpu> <memory> <deadline> <priority>
    if (tokens.size() < 2) {
        std::cout << "Usage: complete <task_uuid>" << std::endl;
        return;
    }
  std::string uuid = tokens[1];

    // Serialize task data
    ptr<buffer> new_log = task_scheduler_state_machine::enc_task_complete(uuid);

    ptr<raft_result> ret = stuff.raft_instance_->append_entries( {new_log} );

    if (!ret->get_accepted()) {
        std::cout << "failed to replicate: "
                  << ret->get_result_code() << std::endl;
        return;
    }

    if (CALL_TYPE == raft_params::blocking) {
        handle_result(*ret);
    } else if (CALL_TYPE == raft_params::async_handler) {
        ret->when_ready( std::bind( handle_result,
                                    std::placeholders::_1 ) );
    } else {
        assert(0);
    }
}


void add_task(const std::string& cmd,
              const std::vector<std::string>& tokens)
{
    // Parse task parameters from tokens
    // Format: push <task_name> <arrival_time> <duration> <cpu> <memory> <deadline> <priority>
    if (tokens.size() < 8) {
        std::cout << "Usage: push <task_name> <arrival_time> <duration> <cpu> <memory> <deadline> <priority>" << std::endl;
        return;
    }
std::string task_name = tokens[1];
  double arrival_time = std::stod(tokens[2]);
  double duration = std::stod(tokens[3]);
  double cpu = std::stod(tokens[4]);
  double memory = std::stod(tokens[5]);
  double deadline = std::stod(tokens[6]);
  int priority = std::stoi(tokens[7]);

    // Serialize task data
    ptr<buffer> new_log = task_scheduler_state_machine::enc_task_add(task_name,
        arrival_time, duration, cpu, memory, deadline, priority);

    ptr<raft_result> ret = stuff.raft_instance_->append_entries( {new_log} );

    if (!ret->get_accepted()) {
        std::cout << "failed to replicate: "
                  << ret->get_result_code() << std::endl;
        return;
    }

    if (CALL_TYPE == raft_params::blocking) {
        handle_result(*ret);
    } else if (CALL_TYPE == raft_params::async_handler) {
        ret->when_ready( std::bind( handle_result,
                                    std::placeholders::_1 ) );
    } else {
        assert(0);
    }
}

void run_scheduler(const std::string& cmd,
                   const std::vector<std::string>& tokens)
{
    ptr<log_store> ls = stuff.smgr_->load_log_store();

    std::cout
        << "my server id: " << stuff.server_id_ << std::endl
        << "leader id: " << stuff.raft_instance_->get_leader() << std::endl
        << "Raft log range: ";
    if (ls->start_index() >= ls->next_slot()) {
        std::cout << "(empty)" << std::endl;
    } else {
        std::cout << ls->start_index()
                  << " - " << (ls->next_slot() - 1) << std::endl;
    }
    std::cout
        << "last committed index: "
            << stuff.raft_instance_->get_committed_log_idx() << std::endl
        << "current term: "
            << stuff.raft_instance_->get_term() << std::endl
        << "pending tasks: "
            << get_sm()->get_scheduler().get_pending_task_count() << std::endl
        << "completed tasks: "
            << get_sm()->get_scheduler().get_completed_tasks_count() << std::endl;
}

void print_status(const std::string& cmd,
                  const std::vector<std::string>& tokens)
{
    ptr<log_store> ls = stuff.smgr_->load_log_store();

    std::cout
        << "my server id: " << stuff.server_id_ << std::endl
        << "leader id: " << stuff.raft_instance_->get_leader() << std::endl
        << "Raft log range: ";
    if (ls->start_index() >= ls->next_slot()) {
        std::cout << "(empty)" << std::endl;
    } else {
        std::cout << ls->start_index()
                  << " - " << (ls->next_slot() - 1) << std::endl;
    }
    std::cout
        << "last committed index: "
            << stuff.raft_instance_->get_committed_log_idx() << std::endl
        << "current term: "
            << stuff.raft_instance_->get_term() << std::endl
        << "last snapshot log index: "
            << (stuff.sm_->last_snapshot()
                ? stuff.sm_->last_snapshot()->get_last_log_idx() : 0) << std::endl
        << "last snapshot log term: "
            << (stuff.sm_->last_snapshot()
                ? stuff.sm_->last_snapshot()->get_last_log_term() : 0) << std::endl
        << "scheduler type: "
            << get_sm()->get_scheduler().get_policy_name() << std::endl
        << "pending tasks: "
            << get_sm()->get_scheduler().get_pending_task_count() << std::endl
        << "completed tasks: "
            << get_sm()->get_scheduler().get_completed_tasks_count() << std::endl;
}

void help(const std::string& cmd,
          const std::vector<std::string>& tokens)
{
    std::cout
    << "add task: add <arrival_time> <duration> <cpu> <memory> <deadline> <priority>\n"
    << "    arrival_time: when task arrives (double)\n"
    << "    duration: expected execution duration (double)\n"
    << "    cpu: CPU cores required (double)\n"
    << "    memory: memory required (double)\n"
    << "    deadline: task deadline (double)\n"
    << "    priority: task priority (int)\n"
    << "    e.g.) add 0.0 1.5 0.5 0.2 10.0 5\n"
    << "\n"
    << "run scheduler: run\n"
    << "    execute the scheduling algorithm\n"
    << "\n"
    << "get current status: st (or stat)\n"
    << "\n"
    << "add server: add <server id> <address>:<port>\n"
    << "    e.g.) add 2 127.0.0.1:20000\n"
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

    } else if (cmd == "add") {
      // Add server command
      add_server(cmd, tokens);
    } else if (cmd == "push") {
      if (tokens.size() >= 8) {
        // Add task command
        add_task(cmd, tokens);
      } else {
        std::cout << "Usage: push <task_name> <arrival_time> <duration> <cpu> <memory> <deadline> <priority>" << std::endl;
      }
    } else if (cmd == "fetch") {
      fetch_task(cmd, tokens);
    } else if (cmd == "complete") {
      complete_task(cmd, tokens);
    }
    else if (cmd == "st" || cmd == "stat") {
        print_status(cmd, tokens);

    } else if (cmd == "ls" || cmd == "list") {
        server_list(cmd, tokens);

    } else if (cmd == "h" || cmd == "help") {
        help(cmd, tokens);
    }
    return true;
}

void check_additional_flags(int argc, char** argv) {
    for (int ii = 1; ii < argc; ++ii) {
        if (strcmp(argv[ii], "--async-handler") == 0) {
            CALL_TYPE = raft_params::async_handler;
        } else if (strcmp(argv[ii], "--async-snapshot-creation") == 0) {
            ASYNC_SNAPSHOT_CREATION = true;
        }
    }
}

void task_scheduler_usage(int argc, char** argv) {
    std::stringstream ss;
    ss << "Usage: \n";
    ss << "    " << argv[0] << " <server id> <IP address and port> [<options>]";
    ss << std::endl << std::endl;
    ss << "    options:" << std::endl;
    ss << "      --async-handler: use async type handler." << std::endl;
    ss << "      --async-snapshot-creation: create snapshots asynchronously."
       << std::endl << std::endl;

    std::cout << ss.str();
    exit(0);
}

}; // namespace task_scheduler
using namespace task_scheduler;

int main(int argc, char** argv) {
    if (argc < 3) task_scheduler_usage(argc, argv);

    set_server_info(argc, argv);
    check_additional_flags(argc, argv);

    std::cout << "    -- Replicated Task Scheduler with Raft --" << std::endl;
    std::cout << "                         Version 0.1.0" << std::endl;
    std::cout << "    Server ID:    " << stuff.server_id_ << std::endl;
    std::cout << "    Endpoint:     " << stuff.endpoint_ << std::endl;
    if (CALL_TYPE == raft_params::async_handler) {
        std::cout << "    async handler is enabled" << std::endl;
    }
    if (ASYNC_SNAPSHOT_CREATION) {
        std::cout << "    snapshots are created asynchronously" << std::endl;
    }
    init_raft( cs_new<task_scheduler_state_machine>(ASYNC_SNAPSHOT_CREATION) );
    loop();

    return 0;
}