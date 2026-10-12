# Port Sharing Mode

## Purpose

Port sharing mode allows multiple Raft groups to share a single TCP port. Instead of requiring one port per Raft group, you can run multiple groups on the same port with message routing based on `group_id`.

This is useful for:
- Reducing port consumption in containerized environments
- Simplifying network configuration (single endpoint for multiple groups)
- Multi-tenant scenarios where multiple independent Raft groups coexist

## Configuration

### Header Version

Port sharing uses an extended RPC message header that includes a `group_id` field for routing. Configure this via `asio_service::options`:

```cpp
asio_service::options asio_opts;
asio_opts.header_version_ = 1;  // Enable extended header with group_id
```

- `header_version_ = 0` (default): Standard header format, no group_id routing
- `header_version_ = 1`: Extended header format with group_id for port sharing

### API Methods

The `raft_launcher` class provides the following methods for port sharing mode:

| Method | Description |
|--------|-------------|
| `init_shared_port(port, logger, asio_opts)` | Initialize shared port mode |
| `init_with_group_id(group_id, sm, smgr, logger, params, opt)` | Add a Raft group |
| `remove_group(group_id)` | Remove a Raft group |
| `get_server(group_id)` | Get raft_server for a specific group |

## Usage

### Basic Setup

```cpp
#include "libnuraft/raft_launcher.hxx"

using namespace nuraft;

// Create launcher
raft_launcher launcher;

// Configure for port sharing
asio_service::options asio_opts;
asio_opts.header_version_ = 1;  // Required for port sharing

// Initialize shared port
ptr<logger> launcher_logger = cs_new<simple_logger>("launcher");
bool ok = launcher.init_shared_port(20000, launcher_logger, asio_opts);
if (!ok) {
    // Handle initialization failure
}
```

### Adding Raft Groups

Each group requires its own state machine, state manager, and logger:

```cpp
// Add multiple groups sharing the same port
for (int32 group_id = 1; group_id <= 5; group_id++) {
    ptr<state_machine> sm = create_state_machine(group_id);
    ptr<state_mgr> smgr = create_state_manager(group_id);
    ptr<logger> lg = cs_new<simple_logger>("group_" + std::to_string(group_id));

    raft_params params;
    params.with_election_timeout_lower(200)
          .with_election_timeout_upper(400);

    raft_server::init_options opt;

    ptr<raft_server> server = launcher.init_with_group_id(
        group_id, sm, smgr, lg, params, opt);

    if (!server) {
        std::cerr << "Failed to initialize group " << group_id << std::endl;
    }
}
```

### Dynamic Group Management

Groups can be added and removed at runtime:

```cpp
// Add a new group
ptr<raft_server> new_server = launcher.init_with_group_id(
    10, sm10, smgr10, lg10, params, opt);

// Remove a group
int ret = launcher.remove_group(3);
if (ret != 0) {
    std::cerr << "Group 3 not found" << std::endl;
}

// Get server for a specific group
ptr<raft_server> server = launcher.get_server(1);
if (server) {
    // Use the server...
}
```

### Shutdown

```cpp
launcher.shutdown(5);  // 5 second timeout
```

## Client Configuration

Clients connecting to a port-sharing server must also use `header_version_ = 1`:

```cpp
asio_service::options asio_opts;
asio_opts.header_version_ = 1;

ptr<asio_service> asio_svc = cs_new<asio_service>(asio_opts);

// Create client with group_id for routing to specific Raft group
ptr<rpc_client> client = asio_svc->create_client("127.0.0.1:20000", 1);
```

## Compatibility

- Servers with `header_version_ = 1` can accept connections from both version 0 and version 1 clients
- Servers with `header_version_ = 0` only accept version 0 clients
- For rolling upgrades, deploy new code first (which supports both versions), then enable `header_version_ = 1`

## Performance

Port sharing has minimal overhead:
- **Network**: Extended headers add ~19 bytes for requests, ~4 bytes for responses
- **CPU**: Group lookup is O(log N) where N is the number of groups
- **Memory**: Negligible per-group overhead (~100 bytes)

For single-group deployments that don't need port sharing, use `header_version_ = 0` (default) to minimize overhead.
