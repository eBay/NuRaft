
Replicated Calculator
---------
Simple CLI-based calculator with Raft replication.
* State machine: defined as an 8-byte signed integer.
* Raft log: defined as a tuple of mutate operation and its operand.
    * 4 operators: `add`, `subtract`, `multiply`, and `divide`.

Files
-----
* [calc_server.cxx](calc_server.cxx)
    * Main server file. Initiate Raft server and handle CLI commands.
* [calc_state_machine.hxx](calc_state_machine.hxx):
    * State machine implementation (volatile).

Consistency and Durability
-----
Note that everything is volatile; nothing will be written to disk, and server will lose data once its process terminates.

However, as long as quorum nodes are alive, committed data will not be lost in the entire Raft group's point of view. When a server exits and then re-starts, it will do catch-up with the current leader and recover all committed data.

How to Run
-----
Run calculator instances in different terminals.
```
build/examples$ ./calc_server 1 localhost:10001
```
```
build/examples$ ./calc_server 2 localhost:10002
```
```
build/examples$ ./calc_server 3 localhost:10003
```

Choose a server that will be the initial leader, and add the other two.
```
calc 1> add 2 localhost:10002
async request is in progress (check with `list` command)
calc 1> add 3 localhost:10003
async request is in progress (check with `list` command)
calc 1>
```

Now 3 servers organize a Raft group.
```
calc 1> list
server id 1: tcp://localhost:10001 (LEADER)
server id 2: tcp://localhost:10002
server id 3: tcp://localhost:10003
calc 1>
```

Issue mutate operations.
```
calc 1> +100
succeeded, 1.1 ms, return value: 3, state machine value: 100
calc 1> -50
succeeded, 837 us, return value: 4, state machine value: 50
calc 1> *2
succeeded, 891 us, return value: 5, state machine value: 100
calc 1> /3
succeeded, 877 us, return value: 6, state machine value: 33
calc 1>
```

All servers should have the same state machine value.
```
calc 2> st
my server id: 2
leader id: 1
Raft log range: 1 - 7
last committed index: 7
state machine value: 33
calc 2>
```

Server loses its data after termination. However, on server re-start, it will recover committed data by catch-up with the current leader.
```
calc 3> exit

build/examples$ ./calc_server 3 localhost:10003
calc 3> st
my server id: 3
leader id: 1
Raft log range: 6 - 7
last committed index: 7
state machine value: 33
calc 3>
```