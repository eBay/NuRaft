# Port Sharing for Multiple Raft Groups

## Summary

This PR implements port sharing functionality for NuRaft, allowing multiple Raft groups to share a single TCP port. This feature is particularly valuable for deployment scenarios with limited port resources, such as containerized environments with port restrictions.

The implementation has been significantly refactored based on reviewer feedback to ensure clean architectural separation between the transport (Asio) layer and the protocol (Raft) layer. The `group_id` now exists **only** in the Asio layer for routing purposes, and does not pollute the Raft protocol layer.

## Motivation

### Problems Solved

1. **Port Resource Constraints**: In containerized or cloud environments, the number of available ports is often limited. Running multiple Raft groups previously required one dedicated port per group, which doesn't scale well.

2. **Operational Complexity**: Managing multiple ports for different Raft groups adds complexity to deployment, configuration, and network security rules.

3. **Resource Efficiency**: Sharing a single port across multiple groups reduces network overhead and simplifies service discovery.

## Architecture Overview

### Dispatcher Pattern

The implementation uses a dispatcher pattern at the transport layer:

```
┌─────────────────────────────────────────────────────────────┐
│                     Single TCP Port                         │
│                  (e.g., 127.0.0.1:12345)                    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  asio_service (shared)                      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              raft_group_dispatcher                   │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │   │
│  │  │  Group 1    │  │  Group 2    │  │  Group 3    │ │   │
│  │  │  (group_id) │  │  (group_id) │  │  (group_id) │ │   │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘ │   │
│  └─────────┼────────────────┼────────────────┼─────────┘   │
└────────────┼────────────────┼────────────────┼─────────────┘
             │                │                │
             ▼                ▼                ▼
     ┌──────────┐      ┌──────────┐      ┌──────────┐
     │   Raft   │      │   Raft   │      │   Raft   │
     │  Group 1 │      │  Group 2 │      │  Group 3 │
     └──────────┘      └──────────┘      └──────────┘
```

### Key Design Principle: Architectural Layer Separation

Based on reviewer feedback, this implementation maintains a clean separation between layers:

**Asio (Transport) Layer - Knows about group_id:**
- ✅ Network message headers: Includes `group_id` for routing
- ✅ `asio_rpc_client`: Embeds `group_id` in outgoing message headers
- ✅ `rpc_session`: Extracts `group_id` from incoming headers for routing
- ✅ `raft_group_dispatcher`: Routes messages to appropriate Raft group based on `group_id`

**Raft (Protocol) Layer - Does NOT know about group_id:**
- ❌ `req_msg`: No `group_id` field
- ❌ `resp_msg`: No `group_id` field
- ❌ `context`: No `group_id` field
- ❌ All Raft protocol handlers (`handle_append_entries`, `handle_vote`, etc.): No `group_id` logic

This separation ensures that:
1. The Raft protocol implementation remains clean and focused on consensus logic
2. Transport details (like routing) are handled exclusively by the Asio layer
3. The core Raft algorithm is not polluted with multi-tenancy concerns
4. Testing and maintenance of each layer is simplified

## Key Changes

### 1. Extended Message Header with Marker-Based Versioning

The implementation supports two message header formats with automatic version detection:

**Version 0 (Legacy - Default)**: 43 bytes for requests, 58 bytes for responses
```
[Legacy Header - No group_id field]
```

**Version 1 (Extended)**: 47 bytes for requests, 62 bytes for responses
```
[1 byte marker] [4 byte group_id] [Legacy Header]
```

**Marker-Based Version Detection:**
- `MARKER_REQ_V0 = 0x0` / `MARKER_RESP_V0 = 0x1`: Legacy header
- `MARKER_REQ_V1 = 0x2` / `MARKER_RESP_V1 = 0x3`: Extended header with group_id

The receiver examines the first byte to determine:
1. If it's `0x0` or `0x1`: Parse as legacy header (V0)
2. If it's `0x2` or `0x3`: Parse as extended header (V1) with group_id

This approach enables **rolling upgrades** where old and new versions can interoperate during migration.

### 2. New Components

#### `raft_group_dispatcher`
- **Location**: `include/libnuraft/asio_service.hxx`
- **Purpose**: Manages multiple Raft groups and routes incoming messages to the appropriate group based on `group_id`
- **Key Methods**:
  - `add_group(int32 group_id, ptr<raft_server> raft)`: Register a Raft group
  - `remove_group(int32 group_id)`: Unregister a Raft group
  - `get_group(int32 group_id)`: Retrieve Raft server by group_id
  - `set_group_filter(std::function<bool(int32)> filter)`: Set access control filter

### 3. Updated raft_launcher API

#### Before (Deprecated - Removed):
```cpp
// Old API - required two-step initialization
ptr<raft_launcher> launcher = cs_new<raft_launcher>();
launcher->init_shared_port(port, ...);
int group_id = launcher->add_group(state_machine, state_mgr, logger, params);
```

#### After (Current):
```cpp
// New API - single step, returns raft_server instance directly
ptr<raft_launcher> launcher = cs_new<raft_launcher>();
launcher->init_shared_port(port, ...);

ptr<raft_server> sv1 = launcher->init_with_group_id(1, sm1, smgr1, logger1, params);
ptr<raft_server> sv2 = launcher->init_with_group_id(2, sm2, smgr2, logger2, params);
```

**Benefits:**
- More intuitive API
- Each group gets its own independent logger
- Better error handling (returns nullptr on failure instead of error code)
- One-step initialization

### 4. RPC Layer Modifications

#### `asio_service_options` - New Configuration
```cpp
struct asio_service_options {
    // ... existing options ...

    // 0 = legacy (43/58 bytes), 1 = extended with group_id (47/62 bytes)
    int32 header_version_;
};
```

#### `asio_rpc_client` - Enhanced with Group ID
- Each client instance is associated with a specific `group_id`
- Automatically embeds `group_id` in outgoing message headers
- Handles both V0 and V1 headers automatically

#### `rpc_session` - Enhanced with Group ID
- Extracts `group_id` from incoming message headers
- Routes messages to the appropriate Raft group via the dispatcher
- Supports both legacy and extended headers

### 5. Refactored Read Function

The `read()` function in `asio_service.cxx` has been refactored to:
- Use a single template function with a default parameter instead of overload
- Reduce code duplication
- Maintain the same functionality with cleaner implementation

```cpp
template<typename BB, typename FF, typename Strand = void>
static void read(bool is_ssl,
                 ssl_socket& _ssl_socket,
                 asio::ip::tcp::socket& tcp_socket,
                 const BB& buffer,
                 FF func,
                 Strand* strand = nullptr)
```

## Usage Example

### Server-Side: Multiple Raft Groups on One Port

```cpp
#include <libnuraft/launcher.hxx>

// Create launcher
ptr<raft_launcher> launcher = cs_new<raft_launcher>();

// Configure options (optional: enable extended headers)
asio_service_options asio_opts;
asio_opts.header_version_ = 1;  // Use extended headers with group_id
asio_opts.worker_count_ = 4;

// Initialize shared port
launcher->init_shared_port(12345, asio_opts);

// Create and add multiple Raft groups
// Group 1
ptr<state_machine> sm1 = cs_new<my_state_machine>();
ptr<state_mgr> smgr1 = cs_new<my_state_mgr>("./group1");
ptr<logger> logger1 = spdlog::default_logger();
raft_params params1;
ptr<raft_server> sv1 = launcher->init_with_group_id(1, sm1, smgr1, logger1, params1);

// Group 2
ptr<state_machine> sm2 = cs_new<my_state_machine>();
ptr<state_mgr> smgr2 = cs_new<my_state_mgr>("./group2");
ptr<logger> logger2 = spdlog::default_logger();
raft_params params2;
ptr<raft_server> sv2 = launcher->init_with_group_id(2, sm2, smgr2, logger2, params2);

// Group 3
ptr<state_machine> sm3 = cs_new<my_state_machine>();
ptr<state_mgr> smgr3 = cs_new<my_state_mgr>("./group3");
ptr<logger> logger3 = spdlog::default_logger();
raft_params params3;
ptr<raft_server> sv3 = launcher->init_with_group_id(3, sm3, smgr3, logger3, params3);

// All three groups now share port 12345
```

### Client-Side: Connecting to a Specific Group

```cpp
#include <libnuraft/asio_service.hxx>

// Create ASIO service with specific group_id
asio_service_options asio_opts;
asio_opts.header_version_ = 1;  // Must match server's setting

ptr<asio_service> asio_svc = cs_new<asio_service>(asio_opts);
ptr<rpc_client> client = asio_svc->create_client("127.0.0.1", 12345, group_id);

// Now use this client to communicate with the specific group
ptr<req_msg> req = cs_new<req_msg>(...);
ptr<resp_msg> resp = client->send(req, timeout_ms);
```

## Backward Compatibility and Rolling Upgrade

### Scenario: Upgrading from Legacy (V0) to Extended (V1)

**Step 1: Initial State - All nodes on V0**
- All nodes use `header_version_ = 0` (default)
- Messages use 43/58 byte headers
- No port sharing

**Step 2: Enable port sharing on Leader**
- Update Leader to `header_version_ = 1`
- Leader can now handle both V0 and V1 messages
- V0 followers still work (backward compatible)

**Step 3: Migrate Followers**
- Update followers one by one to `header_version_ = 1`
- Each newly updated follower starts using V1 headers
- Leader responds in the same version as the request

**Step 4: Complete Migration**
- Once all nodes are on V1, port sharing can be enabled
- Multiple Raft groups can now share the same port

**Key Points:**
- ✅ No cluster-wide restart required
- ✅ No data loss during migration
- ✅ Automatic version detection via marker byte
- ✅ Leader responds in the same version as received request
- ✅ Zero downtime migration path

## Testing

All tests pass successfully:
```
[ RUN      ] port_sharing_test.multiple_groups_single_port
[       OK ] port_sharing_test.multiple_groups_single_port (1325 ms)
[ RUN      ] port_sharing_test.cross_group_communication
[       OK ] port_sharing_test.cross_group_communication (625 ms)
[ RUN      ] port_sharing_test.group_removal
[       OK ] port_sharing_test.group_removal (325 ms)
[ RUN      ] port_sharing_test.dispatcher_filter
[       OK ] port_sharing_test.dispatcher_filter (125 ms)
... (14 tests total)
[  PASSED  ] 14 tests.
```

## Performance Impact

### Message Size
- **Legacy (V0)**: 43 bytes (request), 58 bytes (response)
- **Extended (V1)**: 47 bytes (request), 62 bytes (response)
- **Overhead**: +4 bytes per message (negligible)

### Memory
- Minimal additional memory for dispatcher routing table
- Each group entry: ~32 bytes (O(n) where n = number of groups)

### CPU
- **Routing**: O(1) hash map lookup in dispatcher
- **Version detection**: O(1) single byte check
- Negligible performance impact

## Code Quality Improvements

This PR includes several code quality improvements based on reviewer feedback:

1. ✅ **Removed read() function overload**: Replaced with single template using default parameter
2. ✅ **Removed deprecated add_group() API**: Migrated to cleaner init_with_group_id()
3. ✅ **Clean architectural separation**: group_id exists only in Asio layer, not in Raft layer
4. ✅ **Removed group_id from 13 locations** in Raft protocol handlers
5. ✅ **Updated documentation**: Comprehensive port-sharing-design.md reflects all changes

## Documentation

Comprehensive design documentation is available in:
- **docs/port-sharing-design.md**: Detailed design, architecture, API reference, and migration guide

The documentation has been fully updated to reflect:
- Correct message header sizes (58/43 for V0, 62/47 for V1)
- Marker-based version detection mechanism
- Clean architectural layer separation
- Updated API examples
- Rolling upgrade strategy
- Architectural benefits

## Checklist

- [x] All tests pass (14/14)
- [x] Documentation updated (port-sharing-design.md)
- [x] Backward compatibility maintained (rolling upgrade supported)
- [x] Code review feedback addressed:
  - [x] Refactored read() function (removed overload, using default parameter)
  - [x] Removed add_group() API
  - [x] Removed group_id from Raft layer (req_msg, resp_msg, context)
  - [x] Maintained clean architectural separation (group_id only in Asio layer)
  - [x] Enhanced API documentation
- [x] Performance impact analyzed (minimal overhead)
- [x] Security considerations addressed (group filter support)

## Reviewer Feedback

Special thanks to @greensky00 for the architectural guidance, particularly:
- Separation of transport (Asio) and protocol (Raft) layers
- Keeping group_id out of Raft protocol messages
- Cleaner API design with init_with_group_id()
- Code quality improvements (read function refactoring)

All reviewer feedback has been addressed in this version.
