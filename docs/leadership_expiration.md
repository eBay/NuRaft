Leadership Expiration
=====================

Why Is It Needed?
-----------------
Once network partition happens so that the current leader is disconnected from all the others, the other nodes will form a quorum and elect next leader shortly. However, the previous stale leader is not reachable from the current leader, thus cannot receive any heartbeat. As a result, it keeps claiming itself as a leader.

From clients' point of view, to achieve strongly consistent read, they always should contact the current leader. If two leaders coexist due to aforementioned network partition, we cannot guarantee strong consistency for read operation if clients send requests to the stale leader.

Below diagram shows an example. `S1` is the previous leader, and let's suppose `S2` is the new leader. `Client2` is able to get the latest committed data, while `Client1` is not, and even worse `Client1` may not know what is going on; just keeps retrying.
```
Client1 --> S1
            |  \
           ~~~~~~~~ network partition
            |    \
Client2 --> S2 -- S3
```

To address this issue, we introduce leadership expiration.


How To Enable Leadership Expiration
-----------------------------------
We provide a parameter `leadership_expiry_` in [`raft_params`](../include/libnuraft/raft_params.hxx) to set the expiration time (in millisecond) of the current leader:

```C++
raft_params params;
params.leadership_expiry_ = 5000;
```

If the current leader does not receive any response from quorum nodes for the given expiration time, the leader immediately yields its leadership and becomes follower.

If the expiry is set to
* `0`: it is automatically adjusted to 20x of the current `heart_beat_interval_`.
* Negative number: the leadership will never be expired (the same as the original Raft).

The default value is `0`.


If You Want to Avoid Multiple Leaders
-------------------------------------
Once you set `leadership_expiry_` equal to or smaller than `election_timeout_lower_bound_`, by help of [pre-vote](prevote_protocol.md), there will be no overlapping time between the previous leader and the new leader.

However, if the expiration time is too short, the entire Raft group may be too sensitive to network hiccup or something like that. As a result, it may cause the system to be unstable.

