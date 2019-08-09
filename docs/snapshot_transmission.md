
Snapshot
========

File-Based Snapshot vs. Object-Based Snapshot
---------------------------------------------
The notion of "*Snapshot*" in the original paper is close to a physical file consists of multiple chunks. However, that is not practical in real-world deployment; if you are using back-end database as a state machine, most likely each snapshot will be huge so that sending and installing snapshot take long time. Then we will end up with one of two issues:

* The database file cannot be modified while you are transferring the file itself, which blocks the commit of the state machine. OR,
* A snapshot can be a separate physical clone of the state machine. In such case, taking a snapshot becomes a super expensive operation. Moreover, each snapshot occupies disk space as big as the original state machine.

If back-end database supports its own logical snapshot and proper isolation, we are happily willing to use it. To support such concept, we provide more generalized form of snapshot, i.e., object-based snapshot.

In this library, a snapshot consists of one or more logical objects, where each object has unique object ID that starts from 0. The definition of an object depends on how you define a snapshot; a snapshot may have a single object, or many. Note that object ID does not need to be consecutive or even ordered, but object ID 0 should always exist, as a starting point.

You can still support a physical file-based snapshot by using the object-based snapshot: just associate each chunk with an object.


Snapshot Transmission
---------------------
Below diagram shows the overall protocol of snapshot transmission:
```
Leader      Follower
|           |
X           |   read_logical_snp_obj( obj_id = 0 )
|           |       => returns data D_0
X---------->|   send {0, D_0}
|           X   save_logical_snp_obj( obj_id = 0, D0 )
|           |       => returns obj_id = X_1
|<----------X   request obj_id = X_1
X           |   read_logical_snp_obj( obj_id = X1 )
|           |       => returns data D_{X_1}
X---------->|   send {X_1, D_{X_1}}
|           X   save_logical_snp_obj( obj_id = X1, D_{X_1} )
|           |       => returns obj_id = X_2
|<----------X   request obj_id = X_2
     ...
X           |   read_logical_snp_obj( obj_id = Y )
|           |       => returns data D_Y, is_last_obj = true
X---------->|   send {Y, D_Y}
|           X   save_logical_snp_obj( obj_id = Y, D_Y )
|           X   apply_snapshot()
|           |
```

Once a new empty node joins the Raft group, or there is a lagging node whose the last log is older than the last log compaction, Raft starts to send a snapshot. It first calls [`read_logical_snp_obj()`](../include/libnuraft/state_machine.hxx) with `obj_id = 0` in leader's side. Then your state machine needs to return corresponding data (i.e., a binary blob) `D_0`, and `{0, D_0}` pair will be sent to the follower who receives the snapshot.

Once the follower receives the object, it invokes [`save_logical_snp_obj()`](../include/libnuraft/state_machine.hxx) with received object ID and data. Your state machine properly stores the received data, and then change the given `obj_id` value to the next object ID. That new object ID will be sent to leader, and leader will invoke `read_logical_snp_obj()` with the new ID.

Above process will be repeated until `read_logical_snp_obj()` sets `is_last_obj = true`. Once `is_last_obj` is set to `true`, follower's Raft will call `save_logical_snp_obj()` with the given data, and then call `apply_snapshot()` where your implementation needs to replace the data in state machine with the newly received one.
