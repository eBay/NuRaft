# NuRaft Port Sharing Design

## 1. Background and Problem

### 1.1 Current Architecture Limitations

In the current NuRaft implementation, each Raft group requires a dedicated TCP port:

```
group1 -> 127.0.0.1:20010
group2 -> 127.0.0.1:20020
group3 -> 127.0.0.1:20030
```

**Code locations**:
- `src/asio_service.cxx:982` - Each `asio_rpc_listener` binds to one port
- `src/asio_service.cxx:2447` - `create_rpc_listener()` creates a single-port listener
- Each `raft_launcher` has independent `asio_service` and `asio_listener`

### 1.2 Problems

1. **High Port Resource Consumption**
   - N Raft groups require N ports
   - Limited port availability in containerized environments
   - Complex firewall rule management

2. **Unfriendly Multi-Tenant Scenarios**
   - Cannot serve multiple tenants in the same process
   - Each tenant requires a dedicated port, high operational cost

3. **Cloud-Native Deployment Challenges**
   - Kubernetes requires creating separate Service for each group
   - Cannot load balance through a single endpoint
   - Complex port management during dynamic scaling

4. **Complex Client Connections**
   - Clients need to know specific ports for each group
   - Cannot access multiple groups through a single endpoint

---

## 2. Design Solution: Extended Message Header + Dispatcher Architecture

### 2.1 Core Concept

**Target Architecture**: `1 port = 1 asio_listener = N raft_servers (group_id routing)`

**Implementation Strategy**:
1. Extend RPC message header with `group_id` field
2. Create `raft_group_dispatcher` to manage group routing
3. Modify RPC session to support group_id parsing and dispatching
4. Maintain backward compatibility

### 2.2 Architecture Diagram

```
┌────────────────────────────────────────────────────┐
│           asio_listener (single port: 20000)       │
│  - listen(port)                                    │
│  - accept connections                              │
└────────────────┬───────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────┐
│         asio_rpc_session (per connection)          │
│  - parse message header                            │
│  - extract group_id                                │
│  - invoke dispatcher                               │
└────────────────┬───────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────┐
│       raft_group_dispatcher (new core class)       │
│  - std::map<int32, ptr<raft_server>> groups_      │
│  - register_group(group_id, server)                │
│  - deregister_group(group_id)                      │
│  - dispatch(group_id, req) -> resp                 │
└────────────────┬───────────────────────────────────┘
                 │
      ┌──────────┼──────────┬──────────┐
      ▼          ▼          ▼          ▼
   group1     group2     group3     group4
  (raft_s)   (raft_s)   (raft_s)   (raft_s)
```

### 2.3 Message Format Extension

#### 2.3.1 Current Message Header Format

**Request Header** (39 bytes):
```cpp
// Location: src/asio_service.cxx:95-108
byte         marker (req = 0x0)            (1)
msg_type     type                         (1)
int32        src                          (4)
int32        dst                          (4)
ulong        term                         (8)
ulong        last_log_term                (8)
ulong        last_log_idx                 (8)
ulong        commit_idx                   (8)
int32        data_size                    (4)
ulong        flags + CRC32                (8)
-------------------------------------
              total                       (39)
```

#### 2.3.2 Extended Message Header Format

**New Constants**:
```cpp
// Location: include/libnuraft/asio_service.hxx
#define RPC_REQ_HEADER_SIZE_V0  39   // Legacy format (version 0)
#define RPC_REQ_HEADER_SIZE_V1  58   // Extended format (version 1)
#define RPC_RESP_HEADER_SIZE_V0 39   // Legacy format (version 0)
#define RPC_RESP_HEADER_SIZE_V1 43   // Extended format (version 1)

// Markers for version identification
#define MARKER_REQ_V0  0x0
#define MARKER_REQ_V1  0x2
#define MARKER_RESP_V0 0x1
#define MARKER_RESP_V1 0x3
```

**Extended Request Header** (58 bytes):
```cpp
byte         marker (req = 0x2)            (1)  // Version 1 marker
msg_type     type                         (1)
int32        src                          (4)
int32        dst                          (4)
ulong        term                         (8)
ulong        last_log_term                (8)
ulong        last_log_idx                 (8)
ulong        commit_idx                   (8)
int32        group_id                     (4)  // NEW: Raft group identifier
int32        data_size                    (4)
ulong        flags + CRC32                (8)
int32        log_data_size                (4)  // Additional fields for version 1
-------------------------------------
              total                       (58)
```

**Extended Response Header** (43 bytes):
```cpp
byte         marker (resp = 0x3)          (1)  // Version 1 marker
msg_type     type                         (1)
int32        src                          (4)
int32        dst                          (4)
ulong        term                         (8)
ulong        next_idx                     (8)
bool         accepted                     (1)
int32        ctx data size                (4)
int32        group_id                     (4)  // NEW: Raft group identifier
ulong        flags + CRC32                (8)
-------------------------------------
              total                       (43)
```

**Important**: The `group_id` field exists ONLY in the network message header for routing purposes. It is NOT part of the `req_msg` or `resp_msg` objects in the Raft layer.

#### 2.3.3 Backward Compatibility

**Compatibility Mechanism**:
1. Use different marker values to identify header versions
   - Version 0: `MARKER_REQ_V0 = 0x0`, `MARKER_RESP_V0 = 0x1`
   - Version 1: `MARKER_REQ_V1 = 0x2`, `MARKER_RESP_V1 = 0x3`
2. Sender decides which format to use based on `header_version_` option
3. Receiver automatically detects both formats via marker byte

**Configuration**:
```cpp
// In asio_service_options:
int32 header_version_;  // 0 = legacy, 1 = extended with group_id
```

**Rolling Upgrade**:
- New servers can handle both version 0 and version 1 messages
- Old servers only understand version 0 messages
- Mixed version clusters can coexist during rolling upgrade

---

## 3. Core Component Design

### 3.1 raft_group_dispatcher Class

**File Location**: `include/libnuraft/raft_group_dispatcher.hxx` (new file)

**Interface Definition**:
```cpp
namespace nuraft {

class raft_group_dispatcher {
public:
    raft_group_dispatcher();

    virtual ~raft_group_dispatcher();

    /**
     * Register a Raft group
     * @param group_id Group identifier
     * @param server Raft server instance
     * @return 0 on success, -1 on failure (e.g., group_id already exists)
     */
    int register_group(int32 group_id, ptr<raft_server>& server);

    /**
     * Deregister a Raft group
     * @param group_id Group identifier
     * @return 0 on success, -1 on failure (e.g., group_id doesn't exist)
     */
    int deregister_group(int32 group_id);

    /**
     * Dispatch request to corresponding Raft group
     * @param group_id Group identifier
     * @param req Request message
     * @param ext_params Extended parameters
     * @return Response message, nullptr if group doesn't exist
     */
    ptr<resp_msg> dispatch(int32 group_id,
                           req_msg& req,
                           const req_ext_params* ext_params = nullptr);

    /**
     * Check if group is registered
     */
    bool group_exists(int32 group_id) const;

    /**
     * Get number of registered groups
     */
    size_t get_group_count() const;

private:
    struct group_entry {
        ptr<raft_server> server;
        int64_t last_activity;
    };

    std::map<int32, group_entry> groups_;
    mutable std::mutex lock_;
};

} // namespace nuraft
```

### 3.2 Modify asio_rpc_session

**File Location**: `src/asio_service.cxx`

**Key Changes**:

#### 3.2.1 Add Member Variable
```cpp
class asio_rpc_session : public rpc_session {
    // Existing members...
    ptr<raft_group_dispatcher> dispatcher_;  // NEW
};
```

#### 3.2.2 Modify Message Header Parsing
```cpp
// Location: src/asio_service.cxx (read_complete method)

void asio_rpc_session::read_complete(...) {
    // ... existing code ...

    // Detect if extended format
    bool is_extended = (flags & FLAG_EXTENDED_HEADER) != 0;
    int header_size = is_extended ? RPC_REQ_HEADER_EXT_SIZE : RPC_REQ_HEADER_SIZE;

    // Read header
    if (bytes_transferred < header_size) {
        // ... error handling ...
    }

    // Parse header (compatible with both formats)
    if (is_extended) {
        // Parse extended header, including group_id
        group_id = read_int32(buf + offset);
        offset += 4;
    } else {
        // Default group_id = 0 (backward compatible)
        group_id = 0;
    }

    // ... create req_msg ...
}
```

#### 3.2.3 Modify Request Handling
```cpp
void asio_rpc_session::handle_request(ptr<buffer>& req_buf) {
    // 1. Deserialize request
    ptr<req_msg> req = deserialize_request(req_buf);

    // 2. Extract group_id
    int32 group_id = req->get_group_id();  // New method

    // 3. Dispatch request
    ptr<resp_msg> resp;
    if (dispatcher_) {
        resp = dispatcher_->dispatch(group_id, *req, &ext_params);
    } else {
        // Backward compatible: directly call handler
        resp = handler_->process_req(*req, ext_params);
    }

    // 4. Send response
    if (resp) {
        send_response(resp, group_id);
    }
}
```

### 3.3 Architecture: Asio Layer vs Raft Layer

**Important Design Principle**: `group_id` is ONLY used in the Asio (transport) layer, NOT in the Raft (logic) layer.

#### 3.3.1 Layer Separation

```
┌─────────────────────────────────────────────────────────────┐
│                    Asio Layer (Transport)                    │
│  - Knows about group_id                                      │
│  - Reads/writes group_id from/to message headers             │
│  - Uses group_id for routing to correct raft_server          │
│  - Manages asio_rpc_client with group_id                     │
└─────────────────────────────────────────────────────────────┘
                           ↓ (passes req/resp, NOT group_id)
┌─────────────────────────────────────────────────────────────┐
│                    Raft Layer (Logic)                        │
│  - NO knowledge of group_id                                  │
│  - req_msg and resp_msg do NOT contain group_id              │
│  - Pure Raft protocol logic                                  │
│  - Works independently of transport layer                    │
└─────────────────────────────────────────────────────────────┘
```

#### 3.3.2 req_msg and resp_msg - No Changes

**File Locations**:
- `include/libnuraft/req_msg.hxx` - UNCHANGED (no group_id)
- `include/libnuraft/resp_msg.hxx` - UNCHANGED (no group_id)

**Key Point**: `req_msg` and `resp_msg` do NOT contain `group_id` field. The group_id exists only in:
1. Network message headers (for routing)
2. `asio_rpc_client` (for sending)
3. `rpc_session` (for receiving and routing)

This ensures clean separation between transport and protocol layers.

### 3.4 raft_launcher API

**File Location**: `include/libnuraft/launcher.hxx`

**APIs**:

```cpp
class raft_launcher {
public:
    // Existing API (unchanged) - for single Raft group
    ptr<raft_server> init(ptr<state_machine> sm,
                          ptr<state_mgr> smgr,
                          ptr<logger> lg,
                          int port_number,
                          const asio_service::options& asio_options,
                          const raft_params& params,
                          const raft_server::init_options& opt);

    // NEW: Shared port mode APIs

    /**
     * Initialize shared port mode for multiple Raft groups
     * @param port_number Shared port number
     * @param lg Logger for the launcher
     * @param asio_options ASIO options (include header_version_)
     * @return true on success, false on error
     */
    bool init_shared_port(int port_number,
                          ptr<logger> lg,
                          const asio_service::options& asio_options);

    /**
     * Initialize a Raft group with its own resources
     * MUST be called after init_shared_port()
     *
     * @param group_id Unique group identifier
     * @param sm State machine for this group
     * @param smgr State manager for this group
     * @param lg Logger for this group (independent per group)
     * @param params Raft parameters
     * @param opt Raft server init options
     * @return Raft server instance on success, nullptr on failure
     */
    ptr<raft_server> init_with_group_id(int32 group_id,
                                         ptr<state_machine> sm,
                                         ptr<state_mgr> smgr,
                                         ptr<logger> lg,
                                         const raft_params& params,
                                         const raft_server::init_options& opt);

    /**
     * Remove a Raft group from the shared port
     * @param group_id Group identifier
     * @return 0 on success, -1 if group_id not found
     */
    int remove_group(int32 group_id);

    /**
     * Get the raft_server for a specific group
     * @param group_id Group identifier
     * @return raft_server pointer, or nullptr if not found
     */
    ptr<raft_server> get_server(int32 group_id);

private:
    // New members for shared port mode
    ptr<raft_group_dispatcher> dispatcher_;
    ptr<asio_service> asio_svc_;
    ptr<asio_listener> asio_listener_;
    ptr<logger> logger_;
    std::map<int32, ptr<raft_server>> servers_;
    bool shared_port_mode_;
};
```

**Usage Example**:
```cpp
raft_launcher launcher;

// Initialize shared port
asio_service::options asio_opts;
asio_opts.header_version_ = 1;  // Enable extended header with group_id
launcher.init_shared_port(20000, logger, asio_opts);

// Add multiple groups
for (int i = 1; i <= 5; i++) {
    auto server = launcher.init_with_group_id(i, sm[i], smgr[i], lg[i], params[i]);
}
```

---

## 4. Serialization/Deserialization in Asio Layer

### 4.1 Request Sending (Client Side)

**File Location**: `src/asio_service.cxx` (asio_rpc_client::register_req_send)

**Process**:
1. Create `req_msg` object (without group_id)
2. Determine header version from `asio_service_options::header_version_`
3. Serialize message header with marker, group_id (if version 1)
4. **Key Point**: group_id comes from `asio_rpc_client::group_id_`, NOT from req_msg

```cpp
// In asio_rpc_client::register_req_send
int32 group_id = group_id_;  // Use client's group_id

size_t header_size;
byte req_marker;
if (header_version >= 1) {
    header_size = RPC_REQ_HEADER_SIZE_V1;
    req_marker = MARKER_REQ_V1;
} else {
    header_size = RPC_REQ_HEADER_SIZE_V0;
    req_marker = MARKER_REQ_V0;
}

ptr<buffer> req_buf = buffer::alloc(header_size + ...);
buffer_serializer req_buf_bs(req_buf);

req_buf_bs.put_u8(req_marker);
req_buf_bs.put_u8((byte)req->get_type());
// ... other fields ...

// Add group_id for version 1
if (header_version >= 1) {
    req_buf_bs.put_i32(group_id);
}
```

### 4.2 Request Receiving (Server Side)

**File Location**: `src/asio_service.cxx` (rpc_session::read_complete)

**Process**:
1. Read first byte to detect marker (header version)
2. Read remaining header based on detected version
3. Extract `current_group_id_` from message header
4. Create `req_msg` object (without group_id)
5. Use `current_group_id_` for routing

```cpp
// In rpc_session::read_complete
// Read first byte (marker)
byte marker = ...;

// Determine header version and size
size_t header_size;
if (marker == MARKER_REQ_V1) {
    header_size = RPC_REQ_HEADER_SIZE_V1;
} else {
    header_size = RPC_REQ_HEADER_SIZE_V0;
}

// Read complete header
// ...

// Extract group_id (version 1 only)
current_group_id_ = 0;
if (header_size >= RPC_REQ_HEADER_SIZE_V1) {
    current_group_id_ = h_bs.get_i32();
}

// Create req_msg WITHOUT group_id
ptr<req_msg> req = cs_new<req_msg>(term, t, src, dst,
                                     last_term, last_idx, commit_idx);

// Route based on current_group_id_
if (dispatcher_ && current_group_id_ != 0) {
    resp = dispatcher_->dispatch(current_group_id_, *req);
} else {
    resp = raft_server_handler::process_req(handler_.get(), *req);
}
```

### 4.3 Response Sending (Server Side)

**Process**:
1. Raft handler returns `resp_msg` (without group_id)
2. Asio layer uses `current_group_id_` from request
3. Serialize response header with marker, group_id (if version 1)

```cpp
// Use group_id from request header, NOT from resp_msg
int32 group_id = current_group_id_;

byte resp_marker;
size_t header_size;
if (header_version >= 1) {
    header_size = RPC_RESP_HEADER_SIZE_V1;
    resp_marker = MARKER_RESP_V1;
} else {
    header_size = RPC_RESP_HEADER_SIZE_V0;
    resp_marker = MARKER_RESP_V0;
}

// Serialize with group_id
if (header_version >= 1) {
    bs.put_i32(group_id);
}
```

---

## 5. Usage Examples

### 5.1 Single Port Multi-Group Deployment

```cpp
#include "libnuraft/raft_launcher.hxx"

using namespace nuraft;

int main() {
    raft_launcher launcher;

    // 1. Initialize shared port
    asio_service::options asio_opts;
    asio_opts.header_version_ = 1;  // Enable extended header with group_id

    launcher.init_shared_port(20000, launcher_logger, asio_opts);

    // 2. Create multiple Raft groups
    for (int i = 1; i <= 5; i++) {
        // Each group has its own state machine, state manager, and logger
        ptr<state_machine> sm = create_state_machine(i);
        ptr<state_mgr> smgr = create_state_manager(i);
        ptr<logger> lg = create_logger(i);

        raft_params params = create_default_params();

        // Initialize group (returns raft_server instance)
        ptr<raft_server> server = launcher.init_with_group_id(
            i, sm, smgr, lg, params);

        if (!server) {
            std::cerr << "Failed to initialize group " << i << std::endl;
            return 1;
        }
    }

    // 3. All groups now share port 20000
    // Client connection: 127.0.0.1:20000, message header includes group_id

    // ... run ...

    launcher.shutdown(5);
    return 0;
}
```

### 5.2 Client Connection Example

```cpp
// Create RPC client with group_id
asio_service::options asio_opts;
asio_opts.header_version_ = 1;  // Enable extended header

ptr<asio_service> asio_svc = cs_new<asio_service>(asio_opts);
ptr<rpc_client> client = asio_svc->create_client("127.0.0.1:20000", 3);

// Create request (NO group_id in req_msg object)
ptr<req_msg> req = cs_new<req_msg>(term, type, src, dst,
                                     last_log_term, last_log_idx, commit_idx);

// Send request - Asio layer adds group_id to message header
ptr<resp_msg> resp = client->send(req);
```

**Key Point**: The `req_msg` object does NOT contain `group_id`. The `asio_rpc_client` adds `group_id` to the network message header during serialization.

### 5.3 Dynamic Group Management

```cpp
// Add new group at runtime
ptr<raft_server> new_server = launcher.init_with_group_id(
    6, sm6, smgr6, lg6, params6);

if (new_server) {
    std::cout << "Group 6 added successfully" << std::endl;
}

// Remove group at runtime
int ret = launcher.remove_group(3);
if (ret == 0) {
    std::cout << "Group 3 removed successfully" << std::endl;
}

// Get server instance for a group
ptr<raft_server> server = launcher.get_server(1);
if (server) {
    // Use server instance
    server->send_append_entries(...);
}
```

---

## 6. Compatibility Strategy

### 6.1 Header Version Configuration

**Version Configuration**:
```cpp
// In asio_service_options
struct asio_service_options {
    int32 header_version_;  // 0 = legacy, 1 = extended with group_id
    // ... other options ...
};
```

**Default Value**:
- `header_version_ = 0` (legacy format) by default
- Set to `1` to enable port sharing with group_id routing

### 6.2 Rolling Upgrade Strategy

**Phase 1**: Deploy new code with `header_version_ = 0`
- Code supports both version 0 and version 1
- Uses legacy format (39/43 bytes headers)
- No group_id routing

**Phase 2**: Enable `header_version_ = 1` gradually
- Set `header_version_ = 1` in asio_service_options
- New format (58/43 bytes headers) with group_id
- Old clients can still connect (version detection via marker)

**Phase 3**: Full migration
- All nodes using version 1
- Full port sharing functionality enabled

### 6.3 Marker-Based Version Detection

**Version Markers**:
```cpp
// Request markers
#define MARKER_REQ_V0  0x0  // Legacy
#define MARKER_REQ_V1  0x2  // Extended with group_id

// Response markers
#define MARKER_RESP_V0 0x1  // Legacy
#define MARKER_RESP_V1 0x3  // Extended with group_id
```

**Detection Logic**:
```cpp
// Server side (rpc_session)
byte marker = header_->data();
if (marker == MARKER_REQ_V1) {
    // Version 1: read extended header
    current_header_size_ = RPC_REQ_HEADER_SIZE_V1;
} else {
    // Version 0: read legacy header
    current_header_size_ = RPC_REQ_HEADER_SIZE_V0;
}
```

**Advantages**:
- No additional handshake required
- Backward compatible with old clients
- Seamless version detection

---

## 7. Performance Impact Analysis

### 7.1 Network Overhead

**Version 0 (Legacy)**:
- Request: 39 bytes
- Response: 39 bytes

**Version 1 (Extended)**:
- Request: 58 bytes (+19 bytes, ~49% increase)
- Response: 43 bytes (+4 bytes, ~10% increase)

**Impact**:
- More significant for small messages (e.g., heartbeat)
- Negligible for large messages (e.g., log entries with data)
- Configurable via `header_version_` option

**Optimization**: Use version 0 for single-group deployments to minimize overhead

### 7.2 CPU Overhead

- **Dispatcher lookup**: O(log N), where N is number of groups
- **Actual impact**: Negligible (std::map lookup is very fast)
- **No additional work** in Raft layer

### 7.3 Memory Overhead

- **Dispatcher memory**: ~100 bytes per group (Map entry)
- **1000 groups**: ~100 KB (negligible)
- **asio_rpc_client**: +4 bytes per client (group_id_)
- **rpc_session**: +4 bytes per session (current_group_id_)

---

## 8. Architectural Benefits

### 8.1 Clean Layer Separation

**Before** (Coupled):
```
Network Message → Asio → req_msg (with group_id) → Raft Handler
                                      ↑
                                   (Raft knows about group_id)
```

**After** (Decoupled):
```
Network Message (with group_id) → Asio → req_msg (NO group_id) → Raft Handler
                                      ↑
                                   (Raft doesn't know about group_id)
```

**Benefits**:
1. **Separation of Concerns**: Raft protocol logic is independent of transport details
2. **Easier Testing**: Raft layer can be tested without Asio layer
3. **Flexibility**: Can change transport layer without affecting Raft protocol
4. **Maintainability**: Cleaner code, easier to understand and modify

### 8.2 Simplified Raft Layer

**What Changed**:
- `req_msg` constructor: 7 parameters (was 8 with group_id)
- `resp_msg` constructor: 5 parameters (was 6 with group_id)
- `context`: No `group_id_` field
- All `handle_*` functions: Don't need to pass `group_id`

**Code Reduction**:
- ~68 lines of code removed from Raft layer
- No need to propagate `group_id` through call chains
- Cleaner API interfaces

---

## 9. Future Extensions

### 9.1 Possible Optimizations

1. **Automatic Group ID Allocation**
   - Use UUID instead of int32
   - Support namespaces

2. **Load Balancing**
   - Dispatcher supports weight configuration
   - Automatic traffic scheduling

3. **Monitoring and Observability**
   - Independent metrics per group
   - Request tracing

### 9.2 Advanced Features

1. **Hot Migration**
   - Dynamically migrate group to other port
   - Zero downtime

2. **Multi-Tenant Isolation**
   - Resource quota management
   - QoS guarantees

---

## 10. Summary

This design enables port sharing in NuRaft by extending message headers and introducing a Dispatcher mechanism, effectively solving the port resource consumption problem in the current architecture. The design fully considers backward compatibility, performance, and extensibility, providing a solid foundation for multi-tenant and cloud-native deployments.

**Key Advantages**:
- ✅ Saves port resources (N groups → 1 port)
- ✅ Clean architecture (Asio vs Raft layer separation)
- ✅ Backward compatible (version 0 and version 1)
- ✅ Configurable (header_version_ option)
- ✅ Easy to use (init_shared_port + init_with_group_id)
- ✅ Each group has independent logger
- ✅ Returns raft_server instance (better than error codes)
- ✅ Highly extensible (supports future enhancements)

**Architecture Highlight**:
```
The key innovation is keeping group_id ONLY in the Asio layer:
- Network headers: YES (for routing)
- asio_rpc_client: YES (for sending)
- rpc_session: YES (for receiving and routing)
- req_msg/resp_msg: NO (Raft layer doesn't know)
- context: NO (Raft layer doesn't know)
- Raft handlers: NO (Pure protocol logic)

This ensures proper separation of concerns and makes the codebase
more maintainable and testable.
```
