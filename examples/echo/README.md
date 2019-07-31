
Echo Server
-----------
Displaying replicated messages on terminal.
* State machine: displays messages.
* Raft log: defined as a tuple of a message to print and its 4-byte length.

Files
-----
* [echo_server.cxx](echo_server.cxx)
    * Main server file. Initiate Raft server and handle CLI commands.
* [echo_state_machine.hxx](echo_state_machine.hxx):
    * State machine implementation.

How to Run
-----
Run echo server instances in different terminals.
```
build/examples$ ./echo_server 1 localhost:10001
```
```
build/examples$ ./echo_server 2 localhost:10002
```
```
build/examples$ ./echo_server 3 localhost:10003
```

Choose a server that will be the initial leader, and add the other two.
```
echo 1> add 2 localhost:10002
async request is in progress (check with `list` command)
echo 1> add 3 localhost:10003
async request is in progress (check with `list` command)
echo 1>
```

Now 3 servers organize a Raft group.
```
echo 1> list
server id 1: tcp://localhost:10001 (LEADER)
server id 2: tcp://localhost:10002
server id 3: tcp://localhost:10003
echo 1>
```

Send a message.
```
echo 1> msg hello world!
pre_commit 2: hello world!
commit 2: hello world!
succeeded, 505 us
echo 1>
```

All servers should print out the same message.
```
echo 2> pre_commit 3: hello world!
commit 3: hello world!
```
