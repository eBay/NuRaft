Custom Quorum Size
------------------

The motivation comes from [Flexible Paxos](https://fpaxos.github.io/) paper:

* Howard et al., [Flexible Paxos: Quorum Intersection Revisited](https://arxiv.org/pdf/1608.06696v1.pdf), 2016.

The basic idea is that as long as there is at least one overlapping node between the quorum for commit and the quorum for leader election, the entire group is safe. For example, let's say Qc and Qe represent the size of quorum for commit and leader election, respectively. If we have 5 servers, the set of {Qc, Qe} pairs {1, 5}, {2, 4}, {3, 3} (original algorithm), {4, 2}, and {5, 1} provides the same level of safety. Note that availability will be sacrificed as the value of |Qc - Qe| increases.

For custom quorum size, we provide two parameters: `custom_commit_quorum_size_` and `custom_election_quorum_size_` in [`raft_params`](../include/libnuraft/raft_params.hxx).

```C++
raft_params params;
params.custom_commit_quorum_size_ = 2;
params.custom_election_quorum_size_ = 4;
```

The default value of both parameters is 0, which follows the original algorithm.

Those parameters are dynamically adjustable; you can change the size of quorum without shutting down Raft server:

```C++
raft_params params = server->get_current_params();
params.custom_commit_quorum_size_ = 2;
params.custom_election_quorum_size_ = 4;
server->update_params(params);
```

Note that it is also possible to set those quorum sizes without intersection: {2, 2} out of 5 servers for example. In such case, data loss or log diverging is inevitable and resolving those problems is your responsibility.
