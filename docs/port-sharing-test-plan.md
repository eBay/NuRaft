# 端口复用功能测试方案

## 一、测试目标

验证 NuRaft 端口复用功能的正确性、性能和兼容性：
- ✅ 多个 Raft group 可以共享同一端口
- ✅ 请求正确路由到对应的 group
- ✅ 向后兼容旧版本
- ✅ 性能损失可接受
- ✅ 并发安全性

---

## 二、测试环境

### 2.1 测试框架
- 使用 NuRaft 现有的 `TestSuite` 框架
- 断言宏：`CHK_Z`, `CHK_TRUE`, `CHK_EQ`, `CHK_GT`, `CHK_NULL` 等
- 测试辅助类：`RaftAsioPkg`, `RaftPkg`

### 2.2 测试文件组织
```
tests/
├── asio/
│   └── port_sharing_test.cxx       # 新建：端口复用集成测试
└── unit/
    ├── dispatcher_test.cxx          # 新建：Dispatcher 单元测试
    └── msg_ext_test.cxx             # 新建：扩展消息格式测试
```

---

## 三、测试用例详细设计

### 测试 1：raft_group_dispatcher 单元测试

**文件**：`tests/unit/dispatcher_test.cxx`

**测试内容**：

#### 1.1 基础注册/注销
```cpp
int dispatcher_register_test() {
    ptr<raft_group_dispatcher> dispatcher = cs_new<raft_group_dispatcher>();

    // 测试 1: 注册单个 group
    ptr<raft_server> server1 = create_mock_server(1);
    CHK_Z( dispatcher->register_group(1, server1) );
    CHK_TRUE( dispatcher->group_exists(1) );
    CHK_EQ(1, dispatcher->get_group_count());

    // 测试 2: 注册多个 groups
    ptr<raft_server> server2 = create_mock_server(2);
    ptr<raft_server> server3 = create_mock_server(3);
    CHK_Z( dispatcher->register_group(2, server2) );
    CHK_Z( dispatcher->register_group(3, server3) );
    CHK_EQ(3, dispatcher->get_group_count());

    // 测试 3: 重复注册应失败
    CHK_NOT_Z( dispatcher->register_group(1, server1) );

    // 测试 4: 注销 group
    CHK_Z( dispatcher->deregister_group(2) );
    CHK_TRUE( !dispatcher->group_exists(2) );
    CHK_EQ(2, dispatcher->get_group_count());

    // 测试 5: 注销不存在的 group 应失败
    CHK_NOT_Z( dispatcher->deregister_group(999) );

    return 0;
}
```

#### 1.2 请求分发测试
```cpp
int dispatcher_dispatch_test() {
    ptr<raft_group_dispatcher> dispatcher = cs_new<raft_group_dispatcher>();

    // 创建 3 个 mock servers
    ptr<raft_server> server1 = create_mock_server(1);
    ptr<raft_server> server2 = create_mock_server(2);
    ptr<raft_server> server3 = create_mock_server(3);

    dispatcher->register_group(1, server1);
    dispatcher->register_group(2, server2);
    dispatcher->register_group(3, server3);

    // 创建请求消息
    ptr<req_msg> req = cs_new<req_msg>(
        0, msg_type::append_entries_request, 0,
        10, 0, 0, 0, 0, 0
    );

    // 测试分发到不同 groups
    ptr<resp_msg> resp1 = dispatcher->dispatch(1, *req);
    CHK_NONNULL( resp1 );
    CHK_EQ(1, resp1->get_dst());  // 验证路由到正确的 server

    ptr<resp_msg> resp2 = dispatcher->dispatch(2, *req);
    CHK_NONNULL( resp2 );

    ptr<resp_msg> resp3 = dispatcher->dispatch(3, *req);
    CHK_NONNULL( resp3 );

    // 测试分发到不存在的 group
    ptr<resp_msg> resp_invalid = dispatcher->dispatch(999, *req);
    CHK_NULL( resp_invalid );

    return 0;
}
```

#### 1.3 并发安全性测试
```cpp
int dispatcher_concurrent_test() {
    ptr<raft_group_dispatcher> dispatcher = cs_new<raft_group_dispatcher>();

    const int num_threads = 10;
    const int num_groups = 100;
    std::vector<std::thread> threads;

    // 多线程并发注册
    for (int i = 0; i < num_threads; ++i) {
        threads.push_back(std::thread([&, i]() {
            for (int j = 0; j < num_groups; ++j) {
                int group_id = i * num_groups + j + 1;
                ptr<raft_server> server = create_mock_server(group_id);
                dispatcher->register_group(group_id, server);

                // 并发请求分发
                ptr<req_msg> req = create_test_req();
                ptr<resp_msg> resp = dispatcher->dispatch(group_id, *req);
            }
        }));
    }

    for (auto& t : threads) {
        t.join();
    }

    // 验证最终状态
    CHK_EQ(num_threads * num_groups, dispatcher->get_group_count());

    return 0;
}
```

---

### 测试 2：扩展消息格式测试

**文件**：`tests/unit/msg_ext_test.cxx`

**测试内容**：

#### 2.1 扩展请求消息序列化/反序列化
```cpp
int req_msg_ext_serialization_test() {
    // 创建扩展格式请求消息
    std::vector<ptr<buffer>> logs;
    ptr<buffer> log_buf = buffer::alloc(100);
    logs.push_back(log_buf);

    ptr<req_msg> req = cs_new<req_msg>(
        0,                          // term
        msg_type::append_entries_request,
        1,                          // src
        2,                          // dst
        100,                        // last log term
        200,                        // last log idx
        300,                        // commit idx
        0,                          // log entries (set below)
        12345,                      // 新增：group_id
        logs
    );

    // 验证 group_id 正确设置
    CHK_EQ(12345, req->get_group_id());

    // 序列化（扩展格式）
    ptr<buffer> buf = buffer::alloc(RPC_REQ_HEADER_EXT_SIZE + 100);
    buffer_serializer bs(buf);
    req->serialize_hdr(bs, true);  // true = 使用扩展格式

    // 反序列化
    buf->pos(0);
    ptr<req_msg> req2 = req_msg::deserialize(buf, true);

    // 验证
    CHK_EQ(12345, req2->get_group_id());
    CHK_EQ(req->get_type(), req2->get_type());
    CHK_EQ(req->get_src(), req2->get_src());
    CHK_EQ(req->get_dst(), req2->get_dst());
    // ... 其他字段验证

    return 0;
}
```

#### 2.2 传统格式兼容性测试
```cpp
int req_msg_backward_compat_test() {
    // 创建传统格式请求（不含 group_id）
    std::vector<ptr<buffer>> logs;
    ptr<req_msg> req = cs_new<req_msg>(
        0, msg_type::client_request, 0, 0, 0, 0, 0, 0, logs
    );

    // 序列化（传统格式）
    ptr<buffer> buf = buffer::alloc(RPC_REQ_HEADER_SIZE);
    buffer_serializer bs(buf);
    req->serialize_hdr(bs, false);

    // 反序列化
    buf->pos(0);
    ptr<req_msg> req2 = req_msg::deserialize(buf, false);

    // 验证 group_id 为默认值 0
    CHK_EQ(0, req2->get_group_id());

    return 0;
}
```

#### 2.3 响应消息扩展测试
```cpp
int resp_msg_ext_test() {
    // 创建扩展格式响应消息
    ptr<resp_msg> resp = cs_new<resp_msg>(
        0,                          // term
        msg_type::append_entries_response,
        1,                          // src
        2,                          // dst
        100,                        // next idx
        true,                       // accepted
        0,                          // ctx size
        54321,                      // 新增：group_id
        0                           // flags
    );

    // 序列化/反序列化
    ptr<buffer> buf = buffer::alloc(RPC_RESP_HEADER_EXT_SIZE);
    buffer_serializer bs(buf);
    resp->serialize_hdr(bs, true);

    buf->pos(0);
    ptr<resp_msg> resp2 = resp_msg::deserialize(buf, true);

    // 验证
    CHK_EQ(54321, resp2->get_group_id());
    CHK_TRUE( resp2->get_accepted() );

    return 0;
}
```

---

### 测试 3：端口复用集成测试

**文件**：`tests/asio/port_sharing_test.cxx`

**测试内容**：

#### 3.1 基础多 Group 共享端口测试
```cpp
int multi_group_shared_port_test() {
    reset_log_files();

    // 使用单一端口 20000
    const int shared_port = 20000;

    // 创建 3 个 Raft groups，共享端口
    std::vector<int> group_ids = {1, 2, 3};

    // 初始化共享端口模式
    ptr<raft_launcher> launcher = cs_new<raft_launcher>();
    CHK_Z( launcher->init_shared_port(shared_port) );

    // 为每个 group 创建 raft server
    std::vector<ptr<raft_server>> servers;
    for (int group_id : group_ids) {
        // 创建 3 个节点的集群
        std::vector<ptr<raft_server>> group_servers;
        for (int i = 1; i <= 3; ++i) {
            int server_id = group_id * 10 + i;  // 11,12,13, 21,22,23, 31,32,33
            std::string addr = "127.0.0.1:" + std::to_string(shared_port);

            ptr<raft_server> server = create_raft_server(server_id, addr);
            group_servers.push_back(server);
        }

        // 添加到共享端口
        CHK_Z( launcher->add_group(group_id, group_servers[0]) );
        servers.push_back(group_servers[0]);
    }

    // 等待 leader 选举
    TestSuite::sleep_ms(1000);

    // 验证每个 group 都有 leader
    for (size_t i = 0; i < servers.size(); ++i) {
        int leader_id = servers[i]->get_leader();
        CHK_GT(leader_id, 0);

        _msg("Group %d: leader is %d\n", group_ids[i], leader_id);
    }

    return 0;
}
```

#### 3.2 请求路由正确性测试
```cpp
int request_routing_test() {
    reset_log_files();

    const int shared_port = 20010;
    ptr<raft_launcher> launcher = cs_new<raft_launcher>();
    launcher->init_shared_port(shared_port);

    // 创建 2 个 groups
    ptr<raft_server> group1_server = create_and_init_group(launcher, 1);
    ptr<raft_server> group2_server = create_and_init_group(launcher, 2);

    // 等待稳定
    TestSuite::sleep_ms(500);

    // 向 group 1 发送请求
    ptr<buffer> msg1 = buffer::alloc(100);
    msg1->put("message for group 1");

    raft_result result1 = group1_server->append_entries({msg1});
    CHK_TRUE( result1.accepted() );

    // 向 group 2 发送请求
    ptr<buffer> msg2 = buffer::alloc(100);
    msg2->put("message for group 2");

    raft_result result2 = group2_server->append_entries({msg2});
    CHK_TRUE( result2.accepted() );

    // 验证消息被路由到正确的 group
    // （通过检查各自的状态机日志）

    return 0;
}
```

#### 3.3 动态添加/删除 Group 测试
```cpp
int dynamic_group_management_test() {
    reset_log_files();

    const int shared_port = 20020;
    ptr<raft_launcher> launcher = cs_new<raft_launcher>();
    launcher->init_shared_port(shared_port);

    // 初始创建 2 个 groups
    ptr<raft_server> group1 = create_and_add_group(launcher, 1);
    ptr<raft_server> group2 = create_and_add_group(launcher, 2);

    CHK_EQ(2, launcher->get_group_count());

    TestSuite::sleep_ms(500);

    // 运行时添加第 3 个 group
    ptr<raft_server> group3 = create_and_add_group(launcher, 3);
    CHK_EQ(3, launcher->get_group_count());

    TestSuite::sleep_ms(500);

    // 验证 group 3 正常工作
    int leader3 = group3->get_leader();
    CHK_GT(leader3, 0);

    // 删除 group 2
    CHK_Z( launcher->remove_group(2) );
    CHK_EQ(2, launcher->get_group_count());

    // 验证 group 1 和 3 仍正常工作
    CHK_GT(group1->get_leader(), 0);
    CHK_GT(group3->get_leader(), 0);

    return 0;
}
```

#### 3.4 并发多 Group 压力测试
```cpp
int concurrent_multi_group_stress_test() {
    reset_log_files();

    const int shared_port = 20030;
    const int num_groups = 10;  // 10 个 groups
    const int msgs_per_group = 100;

    ptr<raft_launcher> launcher = cs_new<raft_launcher>();
    launcher->init_shared_port(shared_port);

    // 创建 10 个 groups
    std::vector<ptr<raft_server>> servers;
    for (int i = 1; i <= num_groups; ++i) {
        ptr<raft_server> server = create_and_add_group(launcher, i);
        servers.push_back(server);
    }

    TestSuite::sleep_ms(1000);

    // 并发向所有 groups 发送消息
    std::vector<std::thread> threads;
    for (int i = 0; i < num_groups; ++i) {
        threads.push_back(std::thread([&, i]() {
            for (int j = 0; j < msgs_per_group; ++j) {
                ptr<buffer> msg = buffer::alloc(50);
                std::string msg_str = "group " + std::to_string(i+1) + " msg " + std::to_string(j);
                msg->put(msg_str);

                raft_result result = servers[i]->append_entries({msg});
                CHK_TRUE( result.accepted() );
            }
        }));
    }

    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }

    _msg("Sent %d messages to each of %d groups\n",
         msgs_per_group, num_groups);

    return 0;
}
```

---

### 测试 4：兼容性测试

**文件**：`tests/asio/port_sharing_compat_test.cxx`

**测试内容**：

#### 4.1 新版本客户端 → 旧版本服务端
```cpp
int new_client_old_server_test() {
    // 旧版本服务端：使用传统端口绑定
    std::string server_addr = "127.0.0.1:30000";
    RaftAsioPkg old_server(1, server_addr);
    old_server.init();  // 传统模式

    // 新版本客户端：支持扩展格式
    ptr<rpc_client> client = create_rpc_client(server_addr);
    client->enable_extended_format(false);  // 检测到旧版本，回退

    // 发送传统格式请求
    ptr<req_msg> req = create_test_req();
    ptr<resp_msg> resp = client->send(req);

    CHK_NONNULL( resp );

    return 0;
}
```

#### 4.2 旧版本客户端 → 新版本服务端
```cpp
int old_client_new_server_test() {
    // 新版本服务端：共享端口模式
    ptr<raft_launcher> launcher = cs_new<raft_launcher>();
    launcher->init_shared_port(30010);

    ptr<raft_server> server = create_and_add_group(launcher, 1);

    // 旧版本客户端：发送传统格式消息
    // (模拟旧客户端行为)
    ptr<req_msg> old_req = create_legacy_req();  // 不含 group_id

    // 服务端应能处理（group_id 默认为 0）
    ptr<resp_msg> resp = server->process_req(*old_req);

    CHK_NONNULL( resp );

    return 0;
}
```

---

### 测试 5：性能测试

**文件**：`tests/bench/port_sharing_bench.cxx`

**测试内容**：

#### 5.1 吞吐量对比测试
```cpp
int throughput_comparison_test() {
    const int num_msgs = 10000;

    // 基准：单端口单 group（传统模式）
    {
        RaftAsioPkg pkg(1, "127.0.0.1:40000");
        pkg.init();

        auto start = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < num_msgs; ++i) {
            ptr<buffer> msg = buffer::alloc(100);
            pkg.raftServer->append_entries({msg});
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto baseline_duration = end - start;

        _msg("Baseline (1 port, 1 group): %ld ms\n",
             std::chrono::duration_cast<std::chrono::milliseconds>(baseline_duration).count());
    }

    // 对比：单端口多 group（端口复用）
    {
        ptr<raft_launcher> launcher = cs_new<raft_launcher>();
        launcher->init_shared_port(40010);

        // 创建 5 个 groups
        for (int i = 1; i <= 5; ++i) {
            create_and_add_group(launcher, i);
        }

        auto start = std::chrono::high_resolution_clock::now();

        // 向每个 group 发送相同数量的消息
        for (int i = 1; i <= 5; ++i) {
            ptr<raft_server> server = launcher->get_server(i);
            for (int j = 0; j < num_msgs / 5; ++j) {
                ptr<buffer> msg = buffer::alloc(100);
                server->append_entries({msg});
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto shared_duration = end - start;

        _msg("Shared port (1 port, 5 groups): %ld ms\n",
             std::chrono::duration_cast<std::chrono::milliseconds>(shared_duration).count());

        // 计算性能损失
        double overhead = (double)(shared_duration - baseline_duration) / baseline_duration * 100.0;
        _msg("Performance overhead: %.2f%%\n", overhead);

        // 验证：性能损失应 < 10%
        CHK_TRUE( overhead < 10.0 );
    }

    return 0;
}
```

#### 5.2 延迟对比测试
```cpp
int latency_comparison_test() {
    const int num_samples = 1000;
    std::vector<double> latencies_baseline;
    std::vector<double> latencies_shared;

    // 测量传统模式延迟
    RaftAsioPkg pkg(1, "127.0.0.1:40020");
    pkg.init();

    for (int i = 0; i < num_samples; ++i) {
        auto start = std::chrono::high_resolution_clock::now();

        ptr<buffer> msg = buffer::alloc(100);
        pkg.raftServer->append_entries({msg});

        auto end = std::chrono::high_resolution_clock::now();
        double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        latencies_baseline.push_back(latency_us);
    }

    // 测量端口复用模式延迟
    ptr<raft_launcher> launcher = cs_new<raft_launcher>();
    launcher->init_shared_port(40030);
    ptr<raft_server> server = create_and_add_group(launcher, 1);

    for (int i = 0; i < num_samples; ++i) {
        auto start = std::chrono::high_resolution_clock::now();

        ptr<buffer> msg = buffer::alloc(100);
        server->append_entries({msg});

        auto end = std::chrono::high_resolution_clock::now();
        double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        latencies_shared.push_back(latency_us);
    }

    // 计算 P50, P95, P99
    auto calc_percentile = [](const std::vector<double>& v, double p) {
        std::vector<double> sorted = v;
        std::sort(sorted.begin(), sorted.end());
        size_t idx = (size_t)(p * sorted.size());
        return sorted[idx];
    };

    _msg("\n=== Latency Comparison ===\n");
    _msg("Baseline P50: %.2f us\n", calc_percentile(latencies_baseline, 0.5));
    _msg("Baseline P95: %.2f us\n", calc_percentile(latencies_baseline, 0.95));
    _msg("Baseline P99: %.2f us\n", calc_percentile(latencies_baseline, 0.99));

    _msg("Shared P50: %.2f us\n", calc_percentile(latencies_shared, 0.5));
    _msg("Shared P95: %.2f us\n", calc_percentile(latencies_shared, 0.95));
    _msg("Shared P99: %.2f us\n", calc_percentile(latencies_shared, 0.99));

    return 0;
}
```

---

## 四、成功标准

### 4.1 功能正确性
- ✅ 所有单元测试通过（dispatcher、消息格式）
- ✅ 所有集成测试通过（多 group、路由、动态管理）
- ✅ 兼容性测试通过（新旧版本互通）

### 4.2 性能标准
- ✅ 吞吐量损失 < 10%
- ✅ P95 延迟增加 < 15%
- ✅ P99 延迟增加 < 20%

### 4.3 稳定性标准
- ✅ 并发测试无崩溃
- ✅ 压力测试无内存泄漏
- ✅ Valgrind/ThreadSanitizer 无错误

---

## 五、测试执行计划

### 5.1 单元测试（优先级：高）
**预计时间**：2 天

- [ ] `dispatcher_test.cxx` - 所有测试通过
- [ ] `msg_ext_test.cxx` - 所有测试通过

### 5.2 集成测试（优先级：高）
**预计时间**：3 天

- [ ] `port_sharing_test.cxx` - 所有测试通过
- [ ] 动态管理测试通过
- [ ] 并发压力测试通过

### 5.3 兼容性测试（优先级：中）
**预计时间**：2 天

- [ ] 新→旧兼容性测试通过
- [ ] 旧→新兼容性测试通过

### 5.4 性能测试（优先级：中）
**预计时间**：2 天

- [ ] 吞吐量对比测试通过
- [ ] 延迟对比测试通过
- [ ] 性能指标达标

---

## 六、测试工具

### 6.1 现有工具
- **TestSuite**：NuRaft 内置测试框架
- **Valgrind**：内存泄漏检测
- **ThreadSanitizer**：并发问题检测

### 6.2 辅助脚本
```bash
# 运行所有端口复用测试
./tests/port_sharing_test_runner.sh

# 运行性能测试
./tests/bench/port_sharing_bench.sh

# 内存泄漏检测
valgrind --leak-check=full ./tests/unit/dispatcher_test

# 线程安全检测
./configure --enable-thread-sanitizer
make check
```

---

## 七、边界条件和异常处理测试

### 7.1 边界条件
- [ ] group_id = 0（默认值）
- [ ] group_id = INT32_MAX
- [ ] group_id = -1（无效值）
- [ ] 单个 group（最小规模）
- [ ] 1000 个 groups（大规模）

### 7.2 异常处理
- [ ] 请求发送到不存在的 group_id
- [ ] 重复注册相同 group_id
- [ ] 注销不存在的 group_id
- [ ] 并发注册/注销相同 group_id
- [ ] 网络异常时的 group 状态

---

## 八、Mock 对象设计

为了支持单元测试，需要设计以下 Mock 对象：

### 8.1 Mock Raft Server
```cpp
class mock_raft_server : public raft_server {
public:
    mock_raft_server(int id) : server_id_(id) {}

    ptr<resp_msg> process_req(req_msg& req) override {
        // 返回模拟响应
        return cs_new<resp_msg>(
            req.get_term(),
            req.get_type(),
            req.get_dst(),  // src = server_id
            req.get_src(),  // dst = client
            0,              // next_idx
            true,           // accepted
            0,              // ctx_size
            req.get_group_id(),
            0               // flags
        );
    }

    int get_server_id() const { return server_id_; }

private:
    int server_id_;
};
```

### 8.2 Mock Network
```cpp
class mock_network {
public:
    // 模拟发送扩展格式消息
    static ptr<buffer> create_ext_req_msg(int32 group_id) {
        ptr<buffer> buf = buffer::alloc(RPC_REQ_HEADER_EXT_SIZE);
        buffer_serializer bs(buf);

        bs.put_byte(0x0);  // marker
        bs.put_byte((byte)msg_type::append_entries_request);
        // ... 其他字段 ...
        bs.put_int32(group_id);  // group_id

        return buf;
    }
};
```

---

## 九、测试数据准备

### 9.1 测试消息
```cpp
// 标准测试消息
ptr<buffer> create_test_msg(size_t size) {
    ptr<buffer> msg = buffer::alloc(size);
    std::string content = "test message";
    msg->put(content);
    return msg;
}

// 大消息测试（10 KB）
ptr<buffer> create_large_msg() {
    return create_test_msg(10240);
}

// 小消息测试（10 字节）
ptr<buffer> create_small_msg() {
    return create_test_msg(10);
}
```

### 9.2 测试配置
```cpp
struct test_config {
    int num_groups;
    int num_servers_per_group;
    int port;
    int msg_count;
};

test_config small_scale = {3, 3, 20000, 100};
test_config medium_scale = {10, 3, 20010, 1000};
test_config large_scale = {100, 3, 20020, 10000};
```

---

## 十、总结

本测试方案覆盖了端口复用功能的所有关键方面：

1. **单元测试**：验证核心组件（dispatcher、消息格式）的正确性
2. **集成测试**：验证多 group 共享端口的端到端功能
3. **兼容性测试**：确保新旧版本可以互通
4. **性能测试**：确保性能损失在可接受范围内
5. **稳定性测试**：确保并发和压力场景下的稳定性

**测试覆盖率目标**：
- 代码覆盖率 > 90%
- 分支覆盖率 > 85%
- 边界条件覆盖率 100%

**下一步**：等待开发者确认测试方案 ✅
