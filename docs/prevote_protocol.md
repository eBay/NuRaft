Pre-Vote Protocol
=================

Why Is It Needed?
-----------------
The motivation is briefly described in [Diego's thesis](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf).

Suppose that we have 5 servers: `S1` to `S5`, and `S1` is the current leader. Now let's say there are a few network partitions so that only `{S1, S2, S3}`, `{S2, S3, S5}`, `{S4, S5}`, and `{S1, S4}` can communicate each other.
```
S1----S4
| \    |
|  S3  |
| /  \ |
S2----S5
```

Since `S5` cannot receive heartbeat from `S1`, it will initiate leader election with newer term. `S5` can reach quorum so that it may become next leader. After that, `S1` cannot receive heartbeat from the new leader `S5`, thus it attempts to initiate another leader election. This series of events will eventually disrupt each other continuously.

Note that even though `S2, S3, and S4` reject the vote request from either `S1` or `S5`, it is still problematic since vote request increases their terms which causes the denial of `append_entries` request from current leader. Once leader realizes that newer term exists, it immediately becomes follower which results in another leader election.

Pre-Vote Overview
-----------------
To address above issue, before initiating actual vote, each node sends "pre-vote" request first. The goal of pre-vote request is simple: to check if voters are currently seeing live leader. If a voter has received heartbeat from the leader recently before its election timer expires, that means the leader is possibly alive. Then the node rejects the pre-vote request and the vote initiator will not move forward. As a result, the term of the node will remain the same.

Otherwise, the election timer of a voter has been expired, then the voter treats it as the death of the leader so that it accepts the pre-vote request. Once the vote initiator receives acceptance from a majority of servers, it finally increases its term and initiates the actual vote.

Now let's re-visit above issue. `S5` will initiate pre-vote first. Since `S2`, `S3`, and `S4` keep receiving heartbeat from `S1`, they will always reject pre-vote requests, and there will be no disruption.

The overall process in this library is as follows:
```
Initiator   Voter(s)
|           |
X           |   raft_server::handle_election_timeout()
X           |   raft_server::request_prevote()
X---------->|   Send pre-vote request
|           X   raft_server::handle_prevote_req()
|<----------X   Send response
X           |   raft_server::handle_prevote_resp()
X           |   raft_server::initiate_vote()
X           |   raft_server::request_vote()
X---------->|   Send vote request
|           X   raft_server::handle_vote_req()
|<----------X   Send response
X           |   raft_server::handle_vote_resp()
X           |   raft_server::become_leader()
|           |
```


Downside
--------
When the leader is actually dead, to make pre-vote succeed, at least a majority of servers should have encountered election timeout. That makes the overall time taken by the leader election process longer.
