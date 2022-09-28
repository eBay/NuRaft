Read-Only Member (Learner)
--------------------------

Read-only member is a node who does not initiate or participate in a leader election, and just receives new updates from the leader. When we count the number of nodes in a quorum, read-only members will not be included. For example, if there are 4 nodes where 3 nodes are normal members while the other one is a read-only member, the quorum size is still 2.

Read-only member is useful when you want to replicate data to geo-distributed nodes in a remote datacenter. If you set those nodes as read-only members, they are not counted in a quorum so that you can still organize a quorum within the same datacenter, which keeps commit latency relatively low. Remote nodes will catch up with the leader eventually.

To make a member read-only, you need to set `learner_` flag to `true`, when you generate [`srv_config`](../include/libnuraft/srv_config.hxx):
```C++
srv_config normal_member(1, 0, "10.10.10.1:12345", "", false);
srv_config       learner(2, 0, "10.10.10.2:12345", "", true);
```

The major differences between a read-only member and a member with `priority = 0` are as follows:

* Both read-only members and zero-priority members do not initiate leader election, and never be a leader.
* Read-only members are not counted in the quorum, while zero-priority members are counted in.
* read-only members do not receive vote requests, while zero-priority members do. Zero-priority members can vote for others.
