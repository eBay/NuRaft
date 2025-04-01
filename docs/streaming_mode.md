Streaming Mode
==============

Motivation
----------
In Raft, a leader waits for confirmation that the previous message has been successfully acknowledged before sending a new message to a follower. Consequently, there is always one message in-flight for each follower. This approach ensures Raft's core principle of preventing any gaps in the sequence of log index numbers.

Having one message at a time simplifies the implementation but sacrifices the amortized latency of appending a stream of logs. The diagram below illustrates the problem, assuming that the network round-trip time (RTT) is 10 ms and the time taken to append log entries to a local log store is 100 microseconds.

```
                leader                  follower
                |                       |
        log 1 ->|_______ log 1          |
    (after 5 ms)|   ^   \_______        |
        log 2 ->|   |           \______>|
    (after 3 ms)| 10.1 ms               | append log 1 (100 us)
        log 3 ->|   |            _______|
                |   v    _______/       |
     commit 1 <-|<______/               |
                |_______ log 2 + 3      |
                |   ^   \_______        |
                |   |           \______>|
                | 10.1 ms               | append log 2 + 3 (100 us)
                |   |            _______|
                |   v    _______/       |
 commit 2 + 3 <-|<______/               |
                |                       |
```

Once the append of log1 is requested, it is immediately sent to the follower. The follower appends the log to its local log store, and returns the acknowledgement back to the leader, and finally the leader commits log1. Overall, committing log1 takes 10.1 ms: 10 ms for network round trip, and 0.1 ms for appending the log into the follower’s log store.

Let's consider a scenario where there are additional requests for log2 and log3. The request for log2 is made 5 ms subsequent to log1, and the request for log3 follows 3 ms after log2. They cannot be immediately replicated, as the message for log1 is still in-flight. After completing the replication of log1, the leader groups together log2 and log3, replicating them simultaneously in a combined message. This combined replication process also requires 10.1 ms.

However, due to the wait time before replication, the operational latency for log2 and log3 is extended. From the viewpoint of the requests, the replication of log2 has a total duration of 15.2 ms, which includes a 5.1 ms wait, a 10 ms network round trip, and 0.1 ms to append the log. In a similar fashion, the replication time for log3 totals 12.2 ms.

Consequently, this "one-message-at-a-time" policy results in an increase in tail latency beyond the inherent network latency. Our objective is to minimize this waiting period to ensure that the overall latency approaches the actual network latency.


Streamed Replication
--------------------
Our proposed solution involves allowing the leader to send messages in a streaming fashion, without the need to wait for acknowledgments from the previous message. However, we are committed to maintaining the core principles of the Raft algorithm, ensuring the continuity of log entries without permitting any gaps. To achieve this, the logic on the follower's side (i.e., the receiver) will remain as it is. All followers will continue to verify that the log entries they receive are sequential. For any discontinuity, the followers will reject the message in the same manner that the original Raft protocol does.

To enable streaming mode, ensure `streaming_mode_` in [`asio_service_options`](../include/libnuraft/asio_service_options.hxx) is set to `true`. Additionally, there are two parameters for operating in streaming mode, in [`raft_params`](../include/libnuraft/raft_params.hxx).

* `max_log_gap_in_stream_`: If set to a non-zero value combined with `streaming_mode_` being set to `true`, streaming mode is enabled and `append_entries` requests are dispatched instantly without awaiting the response from the prior request. The count of logs in-flight will be capped by this value, allowing it to function as a throttling mechanism, in conjunction with `max_bytes_in_flight_in_stream_` below.

* `max_bytes_in_flight_in_stream_`: If non-zero, the volume of data in-flight will be restricted to this specified byte limit. This limitation is effective only in streaming mode.


If a replica goes offline, streaming to that replica will be disabled to avoid wasting resources. The streaming will be reactivated once the replica comes back online.

