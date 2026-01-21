# NuRaft 开发计划

本文档记录 NuRaft 项目的重要开发任务、设计方案和实现细节。

---

## 当前任务

### 端口复用功能开发

**目标**：让多个 Raft group 共享同一个 TCP 端口，解决端口资源消耗问题。

**状态**：✅ 阶段 1 完成，阶段 2 准备中

**开发分支**：`feature/port-sharing`

**详细设计文档**：[docs/port-sharing-design.md](docs/port-sharing-design.md)

**设计方案摘要**：
- 采用方案 A：扩展消息头部 + Dispatcher 架构
- 在 RPC 消息头部添加 `group_id` 字段（39 → 43 字节）
- 创建 `raft_group_dispatcher` 管理多个 Raft group 的路由
- 修改 `asio_rpc_session` 支持 group_id 解析和分发
- 保持向后兼容性（flags 字段标识扩展格式）

**核心架构**：
```
1 port = 1 asio_listener = N raft_servers (group_id 路由)
```

---

## 阶段 1 完成情况 ✅

**完成时间**：2025-01-21

**提交**：a8c26e6 "Add port sharing infrastructure (Phase 1)"

### 已完成的工作

1. **raft_group_dispatcher 类** ✅
   - 头文件：`include/libnuraft/raft_group_dispatcher.hxx`
   - 实现：`src/raft_group_dispatcher.cxx`
   - 功能：
     - 注册/注销 Raft groups
     - 根据 group_id 路由请求
     - 线程安全（mutex 保护）
     - 活动时间跟踪

2. **消息格式扩展** ✅
   - `req_msg` 添加 `group_id_` 字段
   - `resp_msg` 添加 `group_id_` 字段
   - 向后兼容（默认值 0）
   - 扩展格式常量：
     - `RPC_REQ_HEADER_EXT_SIZE` = 43
     - `RPC_RESP_HEADER_EXT_SIZE` = 43
     - `FLAG_EXTENDED_HEADER` = 0x80

3. **单元测试** ✅
   - 测试文件：`tests/unit/dispatcher_test.cxx`
   - 编译通过
   - 测试通过：1/1
   - 执行时间：44 微秒

4. **代码集成** ✅
   - dispatcher 声明为 `raft_server` 的 friend 类
   - CMakeLists.txt 配置完成
   - 成功编译和运行

---

## 阶段 2 待完成 🔄

**预计工作量**：3-5 天

### 待完成的任务

1. **修改消息序列化/反序列化**
   - [ ] 修改 `asio_service.cxx` 消息解析逻辑
   - [ ] 支持扩展格式（43 字节头部）
   - [ ] 向后兼容处理
   - [ ] 测试新旧格式互操作性

2. **修改 rpc_session**
   - [ ] 集成 dispatcher
   - [ ] 根据 group_id 路由请求
   - [ ] 错误处理

3. **修改 raft_launcher**
   - [ ] 添加共享端口 API
   - [ ] 实现多 group 管理
   - [ ] API 文档

4. **集成测试**
   - [ ] 多 group 共享端口测试
   - [ ] 动态添加/删除 group 测试
   - [ ] 并发压力测试
   - [ ] 性能测试（吞吐量、延迟）
   - [ ] 兼容性测试

---

## 历史任务

---

## 历史任务

（暂无）

---

## 相关资源

- **开发规则**：参见 [notes.md](notes.md)
- **项目根目录**：/home/dodo/code/NuRaft
- **主要代码目录**：
  - `include/libnuraft/` - 公共头文件
  - `src/` - 实现代码
  - `tests/` - 测试代码
  - `examples/` - 示例代码

---

## 设计决策记录

### 2025-01-21：选择端口复用方案 A

**决策**：采用扩展消息头部 + Dispatcher 架构

**理由**：
- 设计清晰，职责分离
- 支持运行时动态添加/删除 group
- 向后兼容（flags 标识新版本）
- 性能影响最小（仅增加 4 字节）

**参考**：[docs/port-sharing-design.md](docs/port-sharing-design.md)

---

## 待办事项

- [ ] 完成端口复用功能的设计方案确认
- [ ] 对齐测试用例
- [ ] 实现 `raft_group_dispatcher` 类
- [ ] 扩展消息头部定义
- [ ] 修改 RPC 层代码
- [ ] 更新 API
- [ ] 编写测试
- [ ] 更新文档

---

*最后更新：2025-01-21*
