#pragma once

#include "test_common.h"
#include "raft_package_fake.hxx"

struct ExecArgs : TestSuite::ThreadArgs {
    ExecArgs(RaftPkg* _leader)
        : leader(_leader)
        , stopSignal(false)
        , msgToWrite(nullptr)
        {}

    void setMsg(ptr<buffer>& to) {
        std::lock_guard<std::mutex> l(msgToWriteLock);
        msgToWrite = to;
    }

    ptr<buffer> getMsg() {
        std::lock_guard<std::mutex> l(msgToWriteLock);
        return msgToWrite;
    }

    RaftPkg* leader;
    std::atomic<bool> stopSignal;
    ptr<buffer> msgToWrite;
    std::mutex msgToWriteLock;
    EventAwaiter eaExecuter;
};

static constexpr size_t EXECUTOR_WAIT_MS = 100;

// Mimic the user of Raft server, which has a separate executer thread.
inline int fake_executer(TestSuite::ThreadArgs* _args) {
    ExecArgs* args = static_cast<ExecArgs*>(_args);

    while (!args->stopSignal) {
        args->eaExecuter.wait_ms(10000);
        args->eaExecuter.reset();
        if (args->stopSignal) break;

        ptr<buffer> msg = nullptr;
        {   std::lock_guard<std::mutex> l(args->msgToWriteLock);
            if (!args->msgToWrite) continue;
            msg = args->msgToWrite;
            args->msgToWrite.reset();
        }

        args->leader->dbgLog(" --- append ---");
        args->leader->raftServer->append_entries( {msg} );
    }

    return 0;
}

inline int fake_executer_killer(TestSuite::ThreadArgs* _args) {
    ExecArgs* args = static_cast<ExecArgs*>(_args);
    args->stopSignal = true;
    args->eaExecuter.invoke();
    return 0;
}
