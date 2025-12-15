# NuRaft Task Scheduler Example

A distributed task scheduling system built on top of NuRaft that demonstrates how to implement a replicated state machine with scheduling capabilities.

## Features

- **Multiple Scheduling Policies**: Supports fairness-based, EDF (Earliest Deadline First), CPU-aware, and priority-based scheduling
- **Distributed Replication**: Uses NuRaft for consensus and fault tolerance
- **Task Management**: Add, fetch, and complete tasks with various attributes
- **Snapshot Support**: Persists state through snapshots for fast recovery
- **Thread-Safe Operations**: All operations are protected with proper synchronization

## Scheduling Policies

1. **Fairness Scheduler** (default): Balances task duration and wait time
2. **EDF Scheduler**: Prioritizes tasks with earliest deadlines
3. **CPU-Aware Scheduler**: Considers CPU requirements and priority
4. **Priority Scheduler**: Focuses on task priority levels

## Task Attributes

- **task_name**: Human-readable identifier
- **arrival_time**: When the task enters the system
- **duration**: Expected execution time
- **cpu**: CPU cores required
- **memory**: Memory requirement
- **deadline**: Task deadline
- **priority**: Task priority level

## Building

```bash
mkdir build && cd build
cmake ..
make task_scheduler
```

## Running

Start multiple servers:

```bash
# Server 1
./task_scheduler 1 localhost:25000

# Server 2 (in another terminal)
./task_scheduler 2 localhost:25001

# Server 3 (in another terminal)
./task_scheduler 3 localhost:25002
```

## Usage

### Add a server to the cluster
```
add <server_id> <address>:<port>
```

### Add a task
```
push <task_name> <arrival_time> <duration> <cpu> <memory> <deadline> <priority>
```

### Fetch next task
```
fetch
```

### Complete a task
```
complete <task_uuid>
```

### View status
```
st
```

### List cluster members
```
ls
```

## Example Session

```bash
# Add servers to form a cluster
add 2 localhost:25001
add 3 localhost:25002

# Add tasks
push task1 0.0 1.5 0.5 0.2 10.0 5
push task2 0.1 2.0 1.0 0.5 15.0 3
push task3 0.2 1.0 0.3 0.1 8.0 7

# Fetch tasks
fetch
fetch
fetch

# Complete tasks
complete <task_uuid_from_fetch>
```

## Architecture

- **Scheduler**: Core scheduling logic with pluggable policies
- **Task**: Represents a unit of work with attributes
- **StateMachine**: Handles Raft log replication and state transitions
- **Policy Engine**: Implements different scheduling algorithms
- **Utilities**: UUID generation, time functions, circular buffers

## Design Decisions

- **C++11 Compatibility**: Uses custom `optional` implementation for older compilers
- **Memory Management**: RAII with smart pointers
- **Thread Safety**: Mutex protection for shared state
- **Snapshot-based**: Efficient state persistence through snapshots