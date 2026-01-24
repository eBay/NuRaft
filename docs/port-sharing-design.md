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
// Location: src/asio_service.cxx
#define RPC_REQ_HEADER_SIZE     39   // Unchanged (backward compatible)
#define RPC_REQ_HEADER_EXT_SIZE 43   // New extended format
#define FLAG_EXTENDED_HEADER    0x01 // Flag to identify extended format
```

**Extended Request Header** (43 bytes):
```cpp
byte         marker (req = 0x0)            (1)
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
-------------------------------------
              total                       (43)
```

**Extended Response Header** (43 bytes):
```cpp
byte         marker (resp = 0x1)          (1)
msg_type     type                         (1)
int32        src                          (4)
int32        dst                          (4)
ulong        term                         (8)
ulong        next_idx                     (8)
bool         accepted                     (1)
byte         _padding                     (1)  // NEW: alignment
int32        ctx data size                (4)
int32        group_id                     (4)  // NEW: Raft group identifier
ulong        flags + CRC32                (8)
-------------------------------------
              total                       (43)
```

#### 2.3.3 Backward Compatibility

**Compatibility Mechanism**:
1. Use the lowest bit of `flags` field to identify extended format
2. Sender decides whether to use extended format based on peer capability
3. Receiver automatically recognizes both formats

**Handshake Protocol** (optional, for capability negotiation):
```cpp
// First message after connection establishment can include version info
struct handshake_msg {
    uint32 version;
    uint32 capabilities;  // bit 0: support group_id
};
```

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

### 3.3 Modify req_msg and resp_msg

**File Locations**:
- `include/libnuraft/req_msg.hxx`
- `include/libnuraft/resp_msg.hxx`

**Modifications**:

```cpp
class req_msg {
public:
    // ... existing members ...

    // New methods
    int32 get_group_id() const          { return group_id_; }
    void set_group_id(int32 id)         { group_id_ = id; }

private:
    int32 group_id_;  // New field
};
```

```cpp
class resp_msg {
public:
    // ... existing members ...

    // New methods
    int32 get_group_id() const          { return group_id_; }
    void set_group_id(int32 id)         { group_id_ = id; }

private:
    int32 group_id_;  // New field
};
```

### 3.4 Modify raft_launcher

**File Location**: `src/launcher.cxx`

**New APIs**:

```cpp
class raft_launcher {
public:
    // Existing API (unchanged)
    ptr<raft_server> init(...);

    // NEW: Shared port mode
    /**
     * Initialize shared port mode
     * @param port Listening port number
     * @return 0 on success, -1 on failure
     */
    int init_shared_port(int port);

    /**
     * Add Raft group to shared port
     * @param group_id Group identifier
     * @param server Raft server instance
     * @return 0 on success, -1 on failure
     */
    int add_group(int32 group_id, ptr<raft_server> server);

    /**
     * Remove Raft group
     * @param group_id Group identifier
     * @return 0 on success, -1 on failure
     */
    int remove_group(int32 group_id);

private:
    // New members
    bool use_shared_port_;
    ptr<raft_group_dispatcher> dispatcher_;
    ptr<asio_service> asio_svc_;
    ptr<asio_listener> asio_listener_;
};
```

---

## 4. Serialization/Deserialization Modifications

### 4.1 Request Serialization

**File Location**: `src/asio_service.cxx`

**Modify serialize_req_header method**:

```cpp
void asio_rpc_session::serialize_req_header(ptr<buffer>& buf,
                                            ptr<req_msg>& req,
                                            bool use_extended) {
    int header_size = use_extended ? RPC_REQ_HEADER_EXT_SIZE : RPC_REQ_HEADER_SIZE;
    buf = buffer::alloc(header_size + req->get_log_entries().size());

    buffer_serializer bs(buf);

    // Existing fields...
    bs.put_byte(0x0);  // marker
    bs.put_byte(static_cast<byte>(req->get_type()));
    bs.put_int32(req->get_src());
    bs.put_int32(req->get_dst());
    bs.put_uint64(req->get_term());
    bs.put_uint64(req->get_last_log_term());
    bs.put_uint64(req->get_last_log_idx());
    bs.put_uint64(req->get_commit_idx());

    // NEW: group_id (extended format only)
    if (use_extended) {
        bs.put_int32(req->get_group_id());
    }

    bs.put_int32(static_cast<int>(req->get_log_entries().size()));

    ulong flags = 0;
    if (use_extended) {
        flags |= FLAG_EXTENDED_HEADER;
    }
    bs.put_uint64(flags);
}
```

### 4.2 Response Serialization

**Modify serialize_resp_header method**:

```cpp
void asio_rpc_session::serialize_resp_header(ptr<buffer>& buf,
                                             ptr<resp_msg>& resp,
                                             bool use_extended) {
    int header_size = use_extended ? RPC_RESP_HEADER_EXT_SIZE : RPC_RESP_HEADER_SIZE;
    // ... similar modifications ...
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
    launcher.init_shared_port(20000);

    // 2. Create multiple Raft groups
    for (int i = 1; i <= 5; i++) {
        // Create raft_server
        ptr<raft_server> server = create_raft_server(...);

        // Add to shared port
        launcher.add_group(i, server);
    }

    // 3. All groups now share port 20000
    // Client connection: 127.0.0.1:20000, message includes group_id

    // ... run ...

    return 0;
}
```

### 5.2 Client Connection Example

```cpp
// Client needs to specify group_id
ptr<rpc_client> client = create_rpc_client("127.0.0.1:20000");

// Send request to specific group
ptr<req_msg> req = cs_new<req_msg>(...);
req->set_group_id(3);  // Send to group 3

ptr<resp_msg> resp = client->send(req);
```

### 5.3 Dynamic Group Management

```cpp
// Add new group at runtime
ptr<raft_server> new_server = create_raft_server(...);
launcher.add_group(6, new_server);

// Remove group at runtime
launcher.remove_group(3);
```

---

## 6. Compatibility Strategy

### 6.1 Version Negotiation

**Capability Detection**:
```cpp
struct connection_capability {
    bool support_group_id;
    uint32 max_version;
};
```

**Handshake Flow**:
1. Client sends handshake message after connection
2. Server returns its capabilities
3. Both parties negotiate to use extended or legacy format

### 6.2 Smooth Upgrade

**Phase 1**: Deploy code supporting extended format, but default to legacy format
**Phase 2**: Gradually migrate to extended format
**Phase 3**: After all nodes upgrade, legacy format support can be removed

### 6.3 Fallback Strategy

If peer doesn't support extended format:
- Use legacy 39-byte header
- group_id defaults to 0
- Functionality falls back to single-port single-group mode

---

## 7. Performance Impact Analysis

### 7.1 Network Overhead

- **Message size increase**: 39 → 43 bytes (+4 bytes)
- **Overhead ratio**: ~10% (more significant for small messages)
- **Optimization suggestion**: Make extended format configurable

### 7.2 CPU Overhead

- **Dispatcher lookup**: O(log N), where N is number of groups
- **Actual impact**: Negligible (Map lookup is very fast)

### 7.3 Memory Overhead

- **Dispatcher memory**: ~100 bytes per group (Map entry)
- **1000 groups**: ~100 KB (negligible)

---

## 8. Future Extensions

### 8.1 Possible Optimizations

1. **Automatic Group ID Allocation**
   - Use UUID instead of int32
   - Support namespaces

2. **Load Balancing**
   - Dispatcher supports weight configuration
   - Automatic traffic scheduling

3. **Monitoring and Observability**
   - Independent metrics per group
   - Request tracing

### 8.2 Advanced Features

1. **Hot Migration**
   - Dynamically migrate group to other port
   - Zero downtime

2. **Multi-Tenant Isolation**
   - Resource quota management
   - QoS guarantees

---

## 9. Summary

This design enables port sharing in NuRaft by extending message headers and introducing a Dispatcher mechanism, effectively solving the port resource consumption problem in the current architecture. The design fully considers backward compatibility, performance, and extensibility, providing a solid foundation for multi-tenant and cloud-native deployments.

**Key Advantages**:
- ✅ Saves port resources (N groups → 1 port)
- ✅ Backward compatible (gradual upgrade)
- ✅ Minimal performance impact (+4 bytes)
- ✅ Easy to use (clean and clear API)
- ✅ Highly extensible (supports future enhancements)
