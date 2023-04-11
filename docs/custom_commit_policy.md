Custom Commit Policy
====================

In addition to the basic quorum-based consensus, NuRaft provides 1) full consensus mode and 2) selective quorum.


Full Consensus Mode
-------------------
The leader commits a log only when all healthy members have the log, in order to achieve strong consistency; once a log is committed, the latest data can be read from any members.

However, stronger consistency worsens availability, and full consensus mode is the extreme case. If there is at least one unreachable member, the entire protocol stops. To avoid such a bad availability, full consensus mode dynamically excludes *unhealthy* members from the quorum. If there is any node not responding longer than `response_limit_` (in [`raft_server::limits`](../include/libnuraft/raft_server.hxx)) multiplied by heartbeat period, NuRaft automatically regards it as *unhealthy*. The full consensus is achieved among healthy members only. Unhealthy members become healthy as soon as they respond to the leader's message.

If the number of unhealthy members becomes a majority, then the leader will not be able to commit logs, the same as the basic quorum-based consensus.


Selective Quorum
----------------
[State machine](../include/libnuraft/state_machine.hxx) interface provides `adjust_commit_index` API for selective quorum. This API is called for each commit decision, with `adjust_commit_index_params`. This parameter contains the list of <peer ID, its last log index> pairs, along with the current commit index and the new commit index determined by NuRaft. This API will return the new log index to commit.

With the given information, we can pin some servers in the quorum so as to make them always have the latest committed log. For example, let's assume we have 5 servers, and their ID and last log index are as follows:
```
{{1, 10}, {2, 9}, {3, 10}, {4, 10}, {5, 8}}
```
In such a case, with the original Raft algorithm, the commit index should be `10`, as `{1, 3, 4}` can form a quorum.

However, if we want to make server 2 always have the latest committed log, server 2 should be pinned in the quorum for commit. So quorum can be either `{1, 2, 3}` or `{1, 2, 4}` in this example, and the commit index number should be `9`. We can inform such a decision to NuRaft by letting `adjust_commit_index` return `9`. In this case, the `append_entries` request for log `10` will be pending until log `10` is committed, i.e., until server 2 receives log `10`.

Similar to full consensus mode, pinning a fixed set of servers in the quorum will sacrifice availability. Users who implement the selective quorum are responsible for tuning and its trade-off.
