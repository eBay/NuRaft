

Basic Operations
================

Initializing Raft Server
------------------------
First of all, you should implement your own [log store](../include/libnuraft/log_store.hxx), [state machine](../include/libnuraft/state_machine.hxx), and [state manager](../include/libnuraft/state_mgr.hxx) (optionally [debugging logger](../include/libnuraft/logger.hxx)):
```C++
ptr<logger> my_logger;
ptr<state_machine> my_state_machine;
ptr<state_mgr> my_state_manager;
```
Log store will not be passed at the initialization time but will be loaded by `load_log_store()` API in the state manager later. So you need properly implement that function.

After that, set your [Asio](../include/libnuraft/asio_service_options.hxx) and [Raft](../include/libnuraft/raft_params.hxx) options:
```C++
asio_service::options asio_opt;
raft_params params;
```

And then you can use [Launcher](../include/libnuraft/launcher.hxx) for initialization:
```C++
ptr<raft_server> server = launcher.init(my_state_machine,
                                        my_state_manager,
                                        my_logger,
                                        12345,
                                        asio_opt,
                                        params);
```
Note that the initialization of Raft server will be done asynchronously, and you can check it by using `is_initialized()` API:
```C++
if (server->is_initialized()) {
    // Raft server is initialized.
}
```

### What Is Happening on Raft Initialization? ###

Once you initialize the Raft server, it will invoke the below APIs from your custom modules:

* `state_mgr::load_log_store()`
    * This function should return your log store instance.
* `state_mgr::load_config()`
    * This function should return the last committed Raft cluster config containing the membership info.
    * At the very first launch, you can return a cluster config containing the server itself only. After adding servers, the cluster config will change, and you should make it durable (if necessary).
* `state_mgr::read_state()`
    * This function should return the last [server state](../include/libnuraft/srv_state.hxx), that contains term and voting info.
* `state_machine::last_commit_index()`
    * You should make the last committed log number durable (if necessary), and return it here. Otherwise, the Raft server attempts to do the log replaying from the beginning.
* `state_machine::last_snapshot()`
    * You should make the last snapshot durable (if necessary), and return the handle of it here.


Shutting Down Raft Server
-------------------------
You can simply use [Launcher](../include/libnuraft/launcher.hxx)'s shutdown API:
```C++
bool success = launcher.shutdown();
```
This API is a blocking call, so the server termination is guaranteed once the function returns `true`.

Adding Server -- Organizing a Group
---
Set [`srv_config`](../include/libnuraft/srv_config.hxx) of the server to be added:
```C++
srv_config server_to_add(...);
```

And then call `add_srv()` API at the server to be the initial leader:
```C++
server->add_srv( server_to_add );
```
Note:
* `add_srv()` API is an asynchronous task, thus need to check the result using `get_srv_config()` API.
* The server to be added should be running at the time you add the server.
* If the leader's logs have already been compacted (i.e., the smallest log number is greater than 1), the leader will transfer a snapshot first. Before receiving the snapshot is done, the server is officially not a member of the Raft group. In the meantime, you also cannot add other servers concurrently.

Removing Server
---
Call `remove_srv()` API with server ID to remove:
```C++
int server_id_to_remove = 2;
server->remove_srv( server_id_to_remove );
```
The same as `add_srv()` API, `remove_srv()` is also an asynchronous task so that you need to check the result by using `get_srv_config()` API.

The server to be removed should be running at the time you remove the server. Otherwise, the leader will attempt to communicate with it a few times and then force remove it.

Appending Log -- Replication
---
You can allocate [`buffer`](../include/libnuraft/buffer.hxx), and put your data into it using [`buffer_serializer`](../include/libnuraft/buffer_serializer.hxx):
```C++
ptr<buffer> b = buffer::alloc(...);
buffer_serializer s(b);
...
```
And then request your data to be replicated:
```C++
auto result = server->append_entries( {b} );
```
Note that you can put multiple buffers together, and a single Raft log number will be assigned to each buffer.

If you use `blocking` mode, `append_entries()` API will be a blocking task and returned after the data is successfully committed. You can get the return value from the state machine from `result`:
```C++
ptr<buffer> return_value = result->get();
```

Otherwise, in `async_handler` mode, `append_entries()` API will be returned immediately. You can set your handler to the returned `result`. The handler will be invoked once the data is committed:
```C++
result->when_ready( your_handler );
```
The return value from the state machine will be available by `result->get()` once the handler is called.