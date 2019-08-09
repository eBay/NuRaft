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

#include "libnuraft/nuraft.hxx"

#include <chrono>
#include <string>
#include <thread>

using namespace nuraft;

int main(int argc, char** argv) {
    // Replace with your logger, state machine, and state manager.
    ptr<logger>         my_logger = nullptr;
    ptr<state_machine>  my_state_machine = cs_new<echo_state_machine>();
    ptr<state_mgr>      my_state_manager =
        cs_new<inmem_state_mgr>(1, "localhost:12345");

    asio_service::options   asio_opt;   // your Asio options
    raft_params             params;     // your Raft parameters

    // Initialize Raft server listening on port 12345.
    // It will organize a single-node Raft cluster.
    raft_launcher       launcher;
    int                 port_number = 12345;
    ptr<raft_server>    server = launcher.init(my_state_machine,
                                               my_state_manager,
                                               my_logger,
                                               port_number,
                                               asio_opt,
                                               params);
    // Need to wait for initialization.
    while (!server->is_initialized()) {
        std::this_thread::sleep_for( std::chrono::milliseconds(100) );
    }

    // Append a log, containing a string `hello world` and its 4-byte length.
    std::string msg = "hello world";
    ptr<buffer> log = buffer::alloc(sizeof(int) + msg.size());
    buffer_serializer bs_log(log);
    bs_log.put_str(msg);
    server->append_entries({log});

    // Shutdown.
    launcher.shutdown();
    return 0;
}

