
Quick Start Guide
-----------------
[examples/quick_start.cxx](../examples/quick_start.cxx)

```C++
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
```

Please refer to [Quick Tutorial](quick_tutorial.md) and [Echo server](../examples/echo) example for more details.
