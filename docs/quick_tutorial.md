How to Use NuRaft: A Quick Tutorial
===

If you are ready to explore NuRaft, the best starting point to learn using NuRaft is the examples within the [examples directory](../examples) in the repo. There are two examples: [echo](../examples/echo) and [calculator](../examples/calculator), both share some common files. For a hello-world equivalent example, see [Quick Start Guide](quick_start_guide.md).

The major tasks to use NuRaft include the following:

### Providing a Log Store ###

[in_memory_log_store.hxx](../examples/in_memory_log_store.hxx) and [in_memory_log_store.cxx](../examples/in_memory_log_store.cxx) provide a log store example for both examples.

### Defining the State Machine ###

Defining log entry format, and state machine actions, and providing state machine snapshot support. See [echo/echo_state_machine.hxx](../examples/echo/echo_state_machine.hxx) and [calculator/calc_state_machine.hxx](../examples/calculator/calc_state_machine.hxx) for the examples.

### Server state and cluster configuration ###

There is a cluster-wide configuration [cluster_config](../include/libnuraft/cluster_config.hxx), which contains a list of [srv_config](../include/libnuraft/srv_config.hxx) for each server. Class [state_mgr](../include/libnuraft/state_mgr.hxx) manages the configuration and [srv_state](../include/libnuraft/srv_state.hxx). You need to override the base class with custom specifics. See [in_memory_state_mgr.hxx](../examples/in_memory_state_mgr.hxx) for example.

### Determining members and their parameters ###

In [example_common.hxx](../examples/example_common.hxx) there is code to define a server, and initialize raft protocol with parameters (init_raft).

Assuming we have a declaration
```C++
raft_params params;
```
Some important protocol parameters to consider are the following:

- `params.heart_beat_interval_`: a leader will send a heartbeat message to followers if this interval (in milliseconds) has passed since the last replication or heartbeat message.

- `params.election_timeout_lower_bound_` and `params.election_timeout_upper_bound_`: they determine the time (in milliseconds, between the lower and upper) a follower will wait before initiating leader election. These three parameters together determine how long a leader failure will be detected. When a leader fails, writes will be temporarily unavailable until a new leader is elected. The average lapse will be about half way between this timeout interval.

- `params.reserved_log_items_`: the number of trailing log entries will be preserved when a snapshot is taken. If one member falls behind the others temporarily, it needs to catch up from the leader using the Raft log. If the log is truncated too soon, the member would have to use a snapshot to catch up due to needed log entries being unavailable. Snapshot based recovery is costly, especially if the data volume is large. Keeping enough trailing log entries will help avoid the costly snapshot-based catch up.

- `params.snapshot_distance_`: snapshot frequency (in number of log entries). When a member restarts or recovers from a remote snapshot, it will replay the log entries after the snapshot. Frequent snapshots will reduce the number of log entries to play, but incurs more overhead of snapshots. On the other hand, less frequent snapshot may increase the time of restart or catch up with less cost of snapshots.


### Servers ###

[echo/echo_server.cxx](../examples/echo/echo_server.cxx) and [calculator/calc_server.cxx](../examples/calculator/calc_server.cxx) contains both server and user interface code.

### Starting or restarting a cluster and adding a member ###

You start servers and then add to the cluster. [Launcher](../include/libnuraft/launcher.hxx) can help to start Raft. See `add_server()` and `server_list()` in [example_common.hxx](../examples/example_common.hxx) for example.

### Removing a member and shutting down the cluster ###

You remove a server, and then shut it down. Use launcher for shutdown, see server code in response to the quit command.

### Client API Considerations ###

The examples contain user interface code in the main server. If you use a client-server model, you need to define the communication API, say using [gRPC](https://grpc.io/) for example. You need to expose server role status to the client for it to differentiate the leader from the followers.

- Writes will go to the leader only. Raft can be configured to forward requests to the leader, but this may incur more hops, thus longer latency. If a node is no longer a leader, it can return the current leader to the client.

- Reads from the leader for latest values.

- Reads from the followers for the values that may be delayed.

For more details and advanced topics, please refer to the [How to Use](../docs/how_to_use.md) document.
