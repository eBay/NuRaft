
How to Use This Library
=======================

The fundamental logic is described in the original paper. We skip the details here and only focus on the library itself.

* Diego Ongaro and John K. Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf), USENIX ATC 2014.

If you are in a hurry, go to [Quick Start Guide](quick_start_guide.md) or [Quick Tutorial](quick_tutorial.md).


Modules
-------
It basically consists of 5 modules: Raft server, Asio layer, log store, state machine, and state manager. Raft server and Asio are provided by this library, while users should implement their own implementations of log store, state machine, and state manager.
* Raft server: coordinating all incoming requests and responses from users and other nodes.
* Asio layer: dealing with network communication and timer, as well as thread pool management.
* Log store: managing read, write, and compact operations of Raft logs.
    * [Interface](../include/libnuraft/log_store.hxx)
    * [Example - in-memory log store](../examples/in_memory_log_store.cxx)
* State machine: executing commit (optionally pre-commit and rollback), and managing snapshots.
    * [Interface](../include/libnuraft/state_machine.hxx)
    * [Example #1 - echo state machine](../examples/echo/echo_state_machine.hxx)
    * [Example #2 - calculator state machine](../examples/calculator/calc_state_machine.hxx)
* State manager: saving and loading cluster configuration and status.
    * [Interface](../include/libnuraft/state_mgr.hxx)
    * [Example - in-memory state manager](../examples/in_memory_state_mgr.hxx)
* (Optional) Debugging logger: for system logging.
    * [Interface](../include/libnuraft/logger.hxx)
    * [Example - example logger](../examples/logger_wrapper.hxx)


Contents
--------
* [Quick Start Guide](quick_start_guide.md)
* [Replication Flow & Threading Model](replication_flow.md)
* [Basic Operations](basic_operations.md)
* [Dealing with Buffer](dealing_with_buffer.md)
* [Snapshot Transmission](snapshot_transmission.md)
* [Pre-vote Protocol](prevote_protocol.md)
* [Leadership Expiration](leadership_expiration.md)
* [Leader Election Priority](leader_election_priority.md)
* [Custom Quorum Size](custom_quorum_size.md)
* [Enabling SSL/TLS](enabling_ssl.md)
* [Custom Metadata for Each Message](custom_metadata.md)
* [Custom Resolver](custom_resolver.md)
* [Read-Only Member (Learner)](readonly_member.md)
* [Asynchronous Replication](async_replication.md)
* [Replication Log Timestamp](log_timestamp.md)
* [Parellel Log Appending](parallel_log_appending.md)
* [Custom Commit Policy](custom_commit_policy.md)
* [Streaming Mode](streaming_mode.md)
* [Customizing Asio (Network Layer, Thread Pool, Timer)](customizing_asio.md)