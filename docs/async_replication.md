Asynchronous Replication
------------------------

The original Raft's commit happens after reaching quorum, which means that network communication is always involved in users' operation path.

However, there are some loosened cases that we want to achieve low latency by sacrificing consistency and resolving conflicts manually. Then waiting for the acknowledges from a majority of servers is a waste of time.

To support such cases, we provide `async_replication_` flag in [`cluster_config`](../include/libnuraft/cluster_config.hxx). If that flag is set, `append_entries()` API immediately returns with the result of `state_machine::pre_commit()`, and replication is done in background later.

Below diagram shows the overall flow. You can compare it with [original sequence](replication_flow.md):
```
User    Leader  Follower(s)
|       |       |
X------>|       |   raft_server::append_entries()
|       X       |   log_store::append()
|       X       |   RESULT <- state_machine::pre_commit()
|<------X       |   Return with RESULT
|       X------>|   Send logs
|       |      (X)  (if conflict) state_machine::rollback()
|       |      (X)  (if conflict) log_store::write_at()
|       |      (X)  (if conflict) state_machine::pre_commit()
|       |       X   log_store::append()
|       |       X   state_machine::pre_commit()
|       |      (X)  (commit of previous logs) state_machine::commit()
|       |<------X   Respond
|       X       |   state_machine::commit()
|       |       |
```

To enable asynchronous replication, `state_machine::pre_commit()` function should do the actual execution, instead of `state_machine::commit()`. In addition to that, you also should implement `state_machine::rollback()` correctly, to revert any changes done by `pre_commit()`.

In synchronous replication mode, we provide another option: `async_handler` in [`raft_params`](../include/libnuraft/raft_params.hxx). Here are the differences between asynchronous replication mode and synchronous replication with `async_handler` mode:

* Synchronous replication with `async_handler` mode:
    * The actual execution in state machine happens after reaching consensus.
    * Although `append_entries()` API returns immediately, the given data is not committed yet. The result of `commit()` will be set later, by invoking user-defined handler as a notification.
* Asynchronous replication mode:
    * The actual execution in state machine happens before replication.
    * `append_entries()` API returns immediately, which already contains the result of state machine execution. There is no later notification.
