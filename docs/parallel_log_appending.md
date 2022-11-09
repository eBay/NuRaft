Parallel Log Appending
======================

This is an experimental option, can be enabled by setting `parallel_log_appending_` in [`raft_params`](../include/libnuraft/raft_params.hxx) to `true`, and its value is `false` by default. Once this option is enabled, the leader will do log replication and appending to its local log store in parallel. In many environments where disk write is slower than network round-trip time, this option will reduce the replication latency compared to the original sequential execution.

The leader will commit the log whenever servers in the quorum have it, but the biggest difference is that the leader itself may not be in the quorum.

* If the disk write is done earlier than the replication, the commit behavior is the same as the original protocol.

* If the replication is done earlier than the disk write, the leader will commit the log based on the quorum except for the leader itself. In this case, the leader can apply the log to the state machine even before the completion of the disk write of the log.

It will still provide the same safety since at least a majority of servers have the log at the moment it is committed, although the leader is not in the quorum.

The overall replication flow with parallel log appending will be as follows:

```
User    Leader  Follower(s)
|       |       |
X------>|       |   raft_server::append_entries()
|       X       |   log_store::append() => (1) Trigger async disk write for the log
|       X       |   state_machine::pre_commit()
|       X------>|   Send logs
|       |      (X)  (if conflict) state_machine::rollback()
|       |      (X)  (if conflict) log_store::write_at()
|       |      (X)  (if conflict) state_machine::pre_commit()
|       |       X   log_store::append()
|       |       X   state_machine::pre_commit()
|       |      (X)  (commit of previous logs) state_machine::commit()
|       |<------X   Respond => (2) Completion of replication
|       X       |   RESULT <- state_machine::commit()
|<-----(X)      |   return raft_server::append_entries() with RESULT
|       X       |   (3) Completion of the async disk write for the log
```

(3) can happen either before (2) or after (2).

To make it work with the existing [log store APIs](../include/libnuraft/log_store.hxx), `log_store::append`, `log_store::write_at`, or `log_store::end_of_append_batch` API need to trigger asynchronous disk writes without blocking the thread. Even while the disk write is in progress, the other read APIs of log store should be able to read the latest log. Once the asynchronous disk write is done, user should call `raft_server::notify_log_append_completion`, to notify the completion of the task. And also, `log_store::last_durable_index` API should be appropriately implemented to return the most recent durable log index on disk.

Note that parallel log appending is applied for the leader only. Followers will always wait for `notify_log_append_completion` call before returning the response to the leader.

