Customizing Asio
================

In NuRaft, [standalone Asio](https://github.com/chriskohlhoff/asio) is used for managing thread pool, network transmission, and asynchronous timer. NuRaft offers multiple options for customizing Asio, or even replacing it with your own implementation.


Building with Boost
-------------------
If your application already uses Boost, you might prefer using it instead of the standalone library. You can build NuRaft with the following flags.
```
build$ cmake -DBOOST_INCLUDE_PATH=<boost_include_path> -DBOOST_LIBRARY_PATH=<boost_libary_path> ..
```


Using External Thread Pool
--------------------------
If your application already uses Asio and its thread pool, having an additional thread pool within NuRaft might be redundant. In such a case, you can provide your `io_context` to [`asio_options`](../include/libnuraft/asio_service_options.hxx) as follows:
```c++
asio_service::options asio_opt;
asio_opt.custom_io_context_ = &your_io_context;
```
then NuRaft will not create its own thread pool. However, your thread pool will be responsible for handling all the asynchronous tasks and timers initiated by NuRaft.


Integrating Your Own Implementation
-----------------------------------
If you prefer to use your own network layer, thread pool, and timer instead of Asio, you can start by disabling Asio with the following flag.
```
build$ cmake -DDISABLE_ASIO=1 ..
```

And then you should implement your own version of [`rpc_listener`](../include/libnuraft/rpc_listener.hxx), [`rpc_client_factory`](../include/libnuraft/rpc_cli_factory.hxx), and [`delayed_task_scheduler`](../include/libnuraft/delayed_task_scheduler.hxx). You can initialize `raft_server` as follows:
```c++
// Replace with your logger, state machine, and state manager.
ptr<logger>                 my_logger = nullptr;
ptr<state_machine>          my_state_machine = cs_new<echo_state_machine>();
ptr<state_mgr>              my_state_manager = cs_new<inmem_state_mgr>(1, "ip:port");

ptr<rpc_listener>           my_rpc_listener = cs_new<my_rpc_listener_impl>();
ptr<delayed_task_scheduler> my_timer = cs_new<my_timer_impl>();
ptr<rpc_client_factory>     my_rpc_cli_factory = cs_new<my_rpc_cli_factory_impl>();

raft_params                 params;     // your Raft parameters.
raft_server::init_options   opt;        // initialization options.

context* ctx = new context( my_state_manager,
                            my_state_machine,
                            my_rpc_listener,
                            my_logger,
                            my_rpc_cli_factory,
                            my_timer,
                            params );
ptr<raft_server> server = cs_new<raft_server>(ctx, opt);
my_rpc_listener->listen( server );
```

You can refer to the implementation in [`raft_package_fake.hxx`](../tests/unit/raft_package_fake.hxx), which is used for testing purposes. It is based on a mock network and timer implementation in [`fake_network.hxx`](../tests/unit/fake_network.hxx).

