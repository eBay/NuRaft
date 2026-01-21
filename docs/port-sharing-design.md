# NuRaft 端口复用设计方案

## 一、背景与问题

### 1.1 当前架构的问题

在 NuRaft 的当前实现中，每个 Raft group 都必须使用独立的 TCP 端口：

```
group1 -> 127.0.0.1:20010
group2 -> 127.0.0.1:20020
group3 -> 127.0.0.1:20030
```

**代码根源**：
- `src/asio_service.cxx:982` - 每个 `asio_rpc_listener` 构造时绑定一个端口
- `src/asio_service.cxx:2447` - `create_rpc_listener()` 创建单一端口的监听器
- 每个 `raft_launcher` 有独立的 `asio_service` 和 `asio_listener`

### 1.2 存在的问题

1. **端口资源消耗严重**
   - N 个 Raft group 需要 N 个端口
   - 容器化环境端口数量受限
   - 防火墙规则管理复杂

2. **多租户场景不友好**
   - 无法在同一进程为多个租户提供服务
   - 每个租户需要独立端口，运维成本高

3. **云原生部署困难**
   - Kubernetes 需要为每个 group 创建独立 Service
   - 无法利用单一 endpoint 进行负载均衡
   - 动态扩缩容时端口管理复杂

4. **客户端连接复杂**
   - 需要知道每个 group 的具体端口
   - 无法通过单一 endpoint 访问多个 group

---

## 二、设计方案：扩展消息头部 + Dispatcher 架构

### 2.1 核心思想

**目标架构**：`1 port = 1 asio_listener = N raft_servers (group_id 路由)`

**实现策略**：
1. 扩展 RPC 消息头部，添加 `group_id` 字段
2. 创建 `raft_group_dispatcher` 管理 group 路由
3. 修改 RPC session 支持 group_id 解析和分发
4. 保持向后兼容性

### 2.2 架构图

```
┌────────────────────────────────────────────────────┐
│           asio_listener (单一端口: 20000)          │
│  - listen(port)                                    │
│  - accept 连接                                      │
└────────────────┬───────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────┐
│         asio_rpc_session (每个连接一个)              │
│  - 解析消息头部                                      │
│  - 提取 group_id                                    │
│  - 调用 dispatcher                                  │
└────────────────┬───────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────┐
│       raft_group_dispatcher (新增核心类)            │
│  - std::map<int32, ptr<raft_server>> groups_       │
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

### 2.3 消息格式扩展

#### 2.3.1 当前消息头部格式

**请求头部** (39 字节):
```cpp
// 位置：src/asio_service.cxx:95-108
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

#### 2.3.2 扩展后的消息头部格式

**新增常量**：
```cpp
// 位置：src/asio_service.cxx
#define RPC_REQ_HEADER_SIZE     39   // 保持不变（向后兼容）
#define RPC_REQ_HEADER_EXT_SIZE 43   // 新增扩展格式
#define FLAG_EXTENDED_HEADER    0x01 // 标识使用扩展格式
```

**扩展请求头部** (43 字节):
```cpp
byte         marker (req = 0x0)            (1)
msg_type     type                         (1)
int32        src                          (4)
int32        dst                          (4)
ulong        term                         (8)
ulong        last_log_term                (8)
ulong        last_log_idx                 (8)
ulong        commit_idx                   (8)
int32        group_id                     (4)  // 新增：Raft 组标识
int32        data_size                    (4)
ulong        flags + CRC32                (8)
-------------------------------------
              total                       (43)
```

**扩展响应头部** (43 字节):
```cpp
byte         marker (resp = 0x1)          (1)
msg_type     type                         (1)
int32        src                          (4)
int32        dst                          (4)
ulong        term                         (8)
ulong        next_idx                     (8)
bool         accepted                     (1)
byte         _padding                     (1)  // 新增对齐
int32        ctx data size                (4)
int32        group_id                     (4)  // 新增：Raft 组标识
ulong        flags + CRC32                (8)
-------------------------------------
              total                       (43)
```

#### 2.3.3 向后兼容性

**兼容机制**：
1. 使用 `flags` 字段的最低位标识扩展格式
2. 发送方根据对方能力决定是否使用扩展格式
3. 接收方自动识别两种格式

**握手协议**（可选，用于能力协商）：
```cpp
// 连接建立时的第一个消息可以包含版本信息
struct handshake_msg {
    uint32 version;
    uint32 capabilities;  // bit 0: 支持 group_id
};
```

---

## 三、核心组件设计

### 3.1 raft_group_dispatcher 类

**文件位置**：`include/libnuraft/raft_group_dispatcher.hxx` (新文件)

**接口定义**：
```cpp
namespace nuraft {

class raft_group_dispatcher {
public:
    raft_group_dispatcher();

    virtual ~raft_group_dispatcher();

    /**
     * 注册一个 Raft group
     * @param group_id 组标识符
     * @param server Raft server 实例
     * @return 0 成功，-1 失败（如 group_id 已存在）
     */
    int register_group(int32 group_id, ptr<raft_server>& server);

    /**
     * 注销一个 Raft group
     * @param group_id 组标识符
     * @return 0 成功，-1 失败（如 group_id 不存在）
     */
    int deregister_group(int32 group_id);

    /**
     * 分发请求到对应的 Raft group
     * @param group_id 组标识符
     * @param req 请求消息
     * @param ext_params 扩展参数
     * @return 响应消息，如果 group 不存在返回 nullptr
     */
    ptr<resp_msg> dispatch(int32 group_id,
                           req_msg& req,
                           const req_ext_params* ext_params = nullptr);

    /**
     * 检查 group 是否已注册
     */
    bool group_exists(int32 group_id) const;

    /**
     * 获取已注册的 group 数量
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

### 3.2 修改 asio_rpc_session

**文件位置**：`src/asio_service.cxx`

**关键修改**：

#### 3.2.1 添加成员变量
```cpp
class asio_rpc_session : public rpc_session {
    // 原有成员...
    ptr<raft_group_dispatcher> dispatcher_;  // 新增
};
```

#### 3.2.2 修改消息头部解析
```cpp
// 位置：src/asio_service.cxx (read_complete 方法)

void asio_rpc_session::read_complete(...) {
    // ... 原有代码 ...

    // 检测是否为扩展格式
    bool is_extended = (flags & FLAG_EXTENDED_HEADER) != 0;
    int header_size = is_extended ? RPC_REQ_HEADER_EXT_SIZE : RPC_REQ_HEADER_SIZE;

    // 读取头部
    if (bytes_transferred < header_size) {
        // ... 错误处理 ...
    }

    // 解析头部（兼容两种格式）
    if (is_extended) {
        // 解析扩展头部，包含 group_id
        group_id = read_int32(buf + offset);
        offset += 4;
    } else {
        // 默认 group_id = 0（向后兼容）
        group_id = 0;
    }

    // ... 创建 req_msg ...
}
```

#### 3.2.3 修改请求处理
```cpp
void asio_rpc_session::handle_request(ptr<buffer>& req_buf) {
    // 1. 反序列化请求
    ptr<req_msg> req = deserialize_request(req_buf);

    // 2. 提取 group_id
    int32 group_id = req->get_group_id();  // 新增方法

    // 3. 分发请求
    ptr<resp_msg> resp;
    if (dispatcher_) {
        resp = dispatcher_->dispatch(group_id, *req, &ext_params);
    } else {
        // 向后兼容：直接调用 handler
        resp = handler_->process_req(*req, ext_params);
    }

    // 4. 发送响应
    if (resp) {
        send_response(resp, group_id);
    }
}
```

### 3.3 修改 req_msg 和 resp_msg

**文件位置**：
- `include/libnuraft/req_msg.hxx`
- `include/libnuraft/resp_msg.hxx`

**修改内容**：

```cpp
class req_msg {
public:
    // ... 原有成员 ...

    // 新增方法
    int32 get_group_id() const          { return group_id_; }
    void set_group_id(int32 id)         { group_id_ = id; }

private:
    int32 group_id_;  // 新增字段
};
```

```cpp
class resp_msg {
public:
    // ... 原有成员 ...

    // 新增方法
    int32 get_group_id() const          { return group_id_; }
    void set_group_id(int32 id)         { group_id_ = id; }

private:
    int32 group_id_;  // 新增字段
};
```

### 3.4 修改 raft_launcher

**文件位置**：`src/launcher.cxx`

**新增 API**：

```cpp
class raft_launcher {
public:
    // 原有 API (保持不变)
    ptr<raft_server> init(...);

    // 新增：共享端口模式
    /**
     * 初始化共享端口模式
     * @param port 监听端口号
     * @return 0 成功，-1 失败
     */
    int init_shared_port(int port);

    /**
     * 添加 Raft group 到共享端口
     * @param group_id 组标识符
     * @param server Raft server 实例
     * @return 0 成功，-1 失败
     */
    int add_group(int32 group_id, ptr<raft_server> server);

    /**
     * 移除 Raft group
     * @param group_id 组标识符
     * @return 0 成功，-1 失败
     */
    int remove_group(int32 group_id);

private:
    // 新增成员
    bool use_shared_port_;
    ptr<raft_group_dispatcher> dispatcher_;
    ptr<asio_service> asio_svc_;
    ptr<asio_listener> asio_listener_;
};
```

---

## 四、序列化/反序列化修改

### 4.1 请求序列化

**文件位置**：`src/asio_service.cxx`

**修改 serialize_req_header 方法**：

```cpp
void asio_rpc_session::serialize_req_header(ptr<buffer>& buf,
                                            ptr<req_msg>& req,
                                            bool use_extended) {
    int header_size = use_extended ? RPC_REQ_HEADER_EXT_SIZE : RPC_REQ_HEADER_SIZE;
    buf = buffer::alloc(header_size + req->get_log_entries().size());

    buffer_serializer bs(buf);

    // 原有字段...
    bs.put_byte(0x0);  // marker
    bs.put_byte(static_cast<byte>(req->get_type()));
    bs.put_int32(req->get_src());
    bs.put_int32(req->get_dst());
    bs.put_uint64(req->get_term());
    bs.put_uint64(req->get_last_log_term());
    bs.put_uint64(req->get_last_log_idx());
    bs.put_uint64(req->get_commit_idx());

    // 新增：group_id (仅扩展格式)
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

### 4.2 响应序列化

**修改 serialize_resp_header 方法**：

```cpp
void asio_rpc_session::serialize_resp_header(ptr<buffer>& buf,
                                             ptr<resp_msg>& resp,
                                             bool use_extended) {
    int header_size = use_extended ? RPC_RESP_HEADER_EXT_SIZE : RPC_RESP_HEADER_SIZE;
    // ... 类似修改 ...
}
```

---

## 五、使用示例

### 5.1 单一端口多 Group 部署

```cpp
#include "libnuraft/raft_launcher.hxx"

using namespace nuraft;

int main() {
    raft_launcher launcher;

    // 1. 初始化共享端口
    launcher.init_shared_port(20000);

    // 2. 创建多个 Raft group
    for (int i = 1; i <= 5; i++) {
        // 创建 raft_server
        ptr<raft_server> server = create_raft_server(...);

        // 添加到共享端口
        launcher.add_group(i, server);
    }

    // 3. 所有 group 现在共享端口 20000
    // 客户端连接: 127.0.0.1:20000，消息中包含 group_id

    // ... 运行 ...

    return 0;
}
```

### 5.2 客户端连接示例

```cpp
// 客户端需要指定 group_id
ptr<rpc_client> client = create_rpc_client("127.0.0.1:20000");

// 发送请求到特定 group
ptr<req_msg> req = cs_new<req_msg>(...);
req->set_group_id(3);  // 发送到 group 3

ptr<resp_msg> resp = client->send(req);
```

### 5.3 动态管理 Group

```cpp
// 运行时添加新 group
ptr<raft_server> new_server = create_raft_server(...);
launcher.add_group(6, new_server);

// 运行时移除 group
launcher.remove_group(3);
```

---

## 六、兼容性策略

### 6.1 版本协商

**能力检测**：
```cpp
struct connection_capability {
    bool support_group_id;
    uint32 max_version;
};
```

**握手流程**：
1. 客户端连接后发送握手消息
2. 服务端返回自己的能力
3. 双方协商使用扩展格式还是传统格式

### 6.2 平滑升级

**阶段 1**：部署支持扩展格式的代码，但默认仍使用传统格式
**阶段 2**：逐步迁移到扩展格式
**阶段 3**：所有节点升级后，可以移除传统格式支持

### 6.3 降级策略

如果检测到对端不支持扩展格式：
- 使用传统 39 字节头部
- group_id 默认为 0
- 功能回退到单端口单 group 模式

---

## 七、性能影响分析

### 7.1 网络开销

- **消息大小增加**：39 → 43 字节（增加 4 字节）
- **开销比例**：约 10%（对于小消息影响较大）
- **优化建议**：可以配置是否使用扩展格式

### 7.2 CPU 开销

- **Dispatcher 查找**：O(log N)，N 为 group 数量
- **实际影响**：可忽略（Map 查找非常快）

### 7.3 内存开销

- **Dispatcher 内存**：每个 group 约 100 字节（Map entry）
- **1000 groups**：约 100 KB（可忽略）

---

## 八、测试计划

### 8.1 单元测试

1. **raft_group_dispatcher 测试**
   - 注册/注销 group
   - 请求分发正确性
   - 并发访问安全性

2. **消息序列化测试**
   - 扩展格式序列化/反序列化
   - 传统格式兼容性
   - 边界条件测试

### 8.2 集成测试

1. **多 Group 共享端口**
   - 5 个 group 共享端口 20000
   - 验证请求正确路由
   - 验证响应正确返回

2. **动态管理测试**
   - 运行时添加 group
   - 运行时删除 group
   - 高并发场景

### 8.3 性能测试

1. **吞吐量对比**
   - 单端口单 group vs 单端口多 group
   - 预期性能损失 < 5%

2. **延迟测试**
   - P50、P95、P99 延迟对比

### 8.4 兼容性测试

1. **新旧版本互通**
   - 新版本客户端 ↔ 旧版本服务端
   - 旧版本客户端 ↔ 新版本服务端

---

## 九、实施步骤

### 阶段 1：基础架构（2-3 天）
- [ ] 创建 `raft_group_dispatcher` 类
- [ ] 扩展消息头部定义
- [ ] 修改 `req_msg` 和 `resp_msg`

### 阶段 2：RPC 层集成（3-4 天）
- [ ] 修改 `asio_rpc_session` 解析 group_id
- [ ] 修改序列化/反序列化逻辑
- [ ] 添加向后兼容性检查

### 阶段 3：API 层支持（2 天）
- [ ] 修改 `raft_launcher` 添加新 API
- [ ] 更新文档和示例

### 阶段 4：测试（3-4 天）
- [ ] 单元测试
- [ ] 集成测试
- [ ] 性能测试
- [ ] 兼容性测试

### 阶段 5：文档与发布（1-2 天）
- [ ] 更新 API 文档
- [ ] 编写迁移指南
- [ ] 发布说明

**总计**：约 11-15 个工作日

---

## 十、风险评估与缓解

### 10.1 风险

1. **消息格式变更风险**
   - 风险级别：高
   - 影响：可能导致新旧版本不兼容

2. **性能回归风险**
   - 风险级别：中
   - 影响：增加延迟和 CPU 开销

3. **并发安全性风险**
   - 风险级别：中
   - 影响：多线程访问 Dispatcher 可能导致竞态条件

### 10.2 缓解措施

1. **严格的兼容性测试**
   - 建立完整的测试矩阵
   - 验证所有版本组合

2. **性能基准测试**
   - 建立性能基线
   - 持续监控性能指标

3. **代码审查与静态分析**
   - 多人审查并发代码
   - 使用线程检测工具（如 ThreadSanitizer）

---

## 十一、未来扩展

### 11.1 可能的优化

1. **Group ID 自动分配**
   - 使用 UUID 替代 int32
   - 支持命名空间

2. **负载均衡**
   - Dispatcher 支持权重配置
   - 自动流量调度

3. **监控与可观测性**
   - 每个独立的 metrics
   - 请求追踪

### 11.2 高级特性

1. **热迁移**
   - 动态迁移 group 到其他端口
   - 零停机时间

2. **多租户隔离**
   - 资源配额管理
   - QoS 保证

---

## 十二、总结

本设计方案通过扩展消息头部和引入 Dispatcher 机制，实现了 NuRaft 的端口复用能力，有效解决了当前架构中端口资源消耗的问题。设计充分考虑了向后兼容性、性能和可扩展性，为多租户和云原生部署提供了良好的基础。

**关键优势**：
- ✅ 节省端口资源（N groups → 1 port）
- ✅ 向后兼容（渐进式升级）
- ✅ 性能影响最小（增加 4 字节）
- ✅ 易于使用（API 简洁清晰）
- ✅ 可扩展性强（支持未来增强）
