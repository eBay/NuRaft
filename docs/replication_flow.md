
Replication Flow
----------------
Below is the sequence of events once user calls `raft_server::append_entries()` API. `L` and `F` denote leader node and follower node, respectively.

```
User    Leader  Follower(s)
|       |       |
X------>|       |   raft_server::append_entries()
|       X       |   log_store::append()
|       X       |   state_machine::pre_commit()
|<-----(X)      |   (async_handler mode) return raft_server::append_entries()
|       X------>|   Send logs
|       |      (X)  (if conflict) state_machine::rollback()
|       |      (X)  (if conflict) log_store::write_at()
|       |      (X)  (if conflict) state_machine::pre_commit()
|       |       X   log_store::append()
|       |       X   state_machine::pre_commit()
|       |      (X)  (commit of previous logs) state_machine::commit()
|       |<------X   Respond
|       X       |   RESULT <- state_machine::commit()
|<-----(X)      |   (blocking mode) return raft_server::append_entries()
|       |       |                   with RESULT
|       |       |   (async_handler mode) invoke user-defined handler
|       |       |                        with RESULT
```

1. [`L`] User calls `raft_server::append_entries()` with one or more logs to append.
2. [`L`] Logs are appended using `log_store::append()`.
3. [`L`] For each log, `state_machine::pre_commit()` is invoked after calling `log_store::append()`.
4. [`L`] Sends proper logs to followers.
5. [`F`] Receives logs and appends them using either `log_store::append()` or `log_store::write_at()` (in case of log conflicting).
    * Before calling `log_store::write_at()`, `state_machine::rollback()` is invoked.
    * For each log, `state_machine::pre_commit()` is invoked after calling either `log_store::append()` or `log_store::write_at()`.
6. [`F`] Returns a response back to leader.
7. [`L`] Commits logs on proper index number, and calls `state_machine::commit()`.
8. [`L`] Sends committed log index number to follower (as a part of the next replication request).
9. [`F`] Commits logs on given index number, and calls `state_machine::commit()`.


Threading Model
---------------
Threads can be categorized into two groups.

* User threads and threads from Asio thread pool: execute Raft operations. APIs called by this thread group need to be lightweight, so as not to block threads long time. It will invoke
    * Log store operations.
    * State machine's pre-commit and rollback.
    * Reading/writing snapshot chunks, via below APIs:
        * `state_machine::read_logical_snp_obj`
        * `state_machine::save_logical_snp_obj`
* Background commit thread: it keeps running in background and doing commit. It will invoke
    * Log store operations.
    * State machine's commit.
    * Snapshot creation, by `state_machine::create_snapshot`.
    * Log compaction, by `log_store::compact`.

Log store operations can be called by different threads in parallel, thus they need to be thread-safe.