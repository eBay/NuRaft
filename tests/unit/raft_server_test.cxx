/************************************************************************
Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "fake_network.hxx"
#include "raft_package_fake.hxx"

#include "event_awaiter.h"
#include "test_common.h"

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

using raft_result = cmd_result< ptr<buffer> >;

namespace raft_server_test {

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

// Mimic the user of Raft server, which has a separate executer thread.
int fake_executer(TestSuite::ThreadArgs* _args) {
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

int fake_executer_killer(TestSuite::ThreadArgs* _args) {
    ExecArgs* args = static_cast<ExecArgs*>(_args);
    args->stopSignal = true;
    args->eaExecuter.invoke();
    return 0;
}

int make_group_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Now all servers should know each other.
    for (auto& entry: pkgs) {
        RaftPkg* pkg = entry;
        std::vector< ptr<srv_config> > configs;
        pkg->raftServer->get_srv_config_all(configs);
        CHK_EQ(3, configs.size());

        for (int ii=1; ii<=3; ++ii) {
            // DC ID should be 1.
            CHK_EQ( 1, s1.raftServer->get_dc_id(ii) );

            // Aux should be `server <ID>`.
            std::string exp = "server " + std::to_string(ii);
            CHK_EQ( exp, s1.raftServer->get_aux(ii) );
        }
    }

    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    // Append a message using separate thread.
    std::string test_msg = "test";
    ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
    msg->put(test_msg);
    {   std::lock_guard<std::mutex> l(exec_args.msgToWriteLock);
        exec_args.msgToWrite = msg;
    }
    exec_args.eaExecuter.invoke();

    // Wait for executer thread.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    {   std::lock_guard<std::mutex> l(exec_args.msgToWriteLock);
        CHK_NULL( exec_args.msgToWrite.get() );
    }
    // Packet for pre-commit.
    s1.fNet->execReqResp();
    // Packet for commit.
    s1.fNet->execReqResp();
    // Wait for bg commit.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Test message should be the same.
    uint64_t last_idx = s1.getTestSm()->getLastCommittedIdx();
    CHK_GT(last_idx, 0);
    ptr<buffer> buf = s1.getTestSm()->getData(last_idx);
    CHK_NONNULL( buf.get() );
    buf->pos(0);
    CHK_Z( memcmp(buf->data(), test_msg.data(), test_msg.size()) );

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

int update_params_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        int old_value = param.election_timeout_upper_bound_;
        param.with_election_timeout_upper( old_value + 1 );
        pp->raftServer->update_params(param);

        param = pp->raftServer->get_current_params();
        CHK_EQ( old_value + 1, param.election_timeout_upper_bound_ );
    }

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int add_node_error_cases_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );

    size_t num_srvs = pkgs.size();
    CHK_GT(num_srvs, 0);

    ptr<FakeNetwork> c_net = cs_new<FakeNetwork>("client", f_base);
    f_base->addNetwork(c_net);
    c_net->create_client(s1_addr);
    c_net->create_client(s2_addr);

    std::atomic<bool> invoked(false);
    rpc_handler bad_req_handler = [&invoked]( ptr<resp_msg>& resp,
                                              ptr<rpc_exception>& err ) -> int {
        invoked.store(true);
        CHK_EQ( cmd_result_code::BAD_REQUEST, resp->get_result_code() );
        return 0;
    };

    {   // Attempt to add more than one server at once.
        ptr<req_msg> req = cs_new<req_msg>
                           ( (ulong)0, msg_type::add_server_request, 0, 0,
                             (ulong)0, (ulong)0, (ulong)0 );
        for (size_t ii=1; ii<num_srvs; ++ii) {
            RaftPkg* ff = pkgs[ii];
            ptr<srv_config> srv = ff->getTestMgr()->get_srv_config();
            ptr<buffer> buf(srv->serialize());
            ptr<log_entry> log( cs_new<log_entry>
                                ( 0, buf, log_val_type::cluster_server ) );
            req->log_entries().push_back(log);
        }
        c_net->findClient(s1_addr)->send( req, bad_req_handler );
        c_net->execReqResp();
    }
    CHK_TRUE(invoked.load());
    invoked = false;

    {   // Attempt to add server with wrong message type.
        ptr<req_msg> req = cs_new<req_msg>
                           ( (ulong)0, msg_type::add_server_request, 0, 0,
                             (ulong)0, (ulong)0, (ulong)0 );
        RaftPkg* ff = pkgs[1];
        ptr<srv_config> srv = ff->getTestMgr()->get_srv_config();
        ptr<buffer> buf(srv->serialize());
        ptr<log_entry> log( cs_new<log_entry>
                            ( 0, buf, log_val_type::conf ) );
        req->log_entries().push_back(log);
        c_net->findClient(s1_addr)->send( req, bad_req_handler );
        c_net->execReqResp();
    }
    CHK_TRUE(invoked.load());
    invoked = false;

    {   // Attempt to add server while previous one is in progress.

        // Add S2 to S1.
        s1.raftServer->add_srv( *(s2.getTestMgr()->get_srv_config()) );

        // Now adding S2 is in progress, add S3 to S1.
        ptr<raft_result> ret =
            s1.raftServer->add_srv( *(s3.getTestMgr()->get_srv_config()) );

        // Should fail.
        CHK_EQ( cmd_result_code::SERVER_IS_JOINING, ret->get_result_code() );

        // Join req/resp.
        s1.fNet->execReqResp();

        // Now config change is in progress, add S3 to S1.
        ret = s1.raftServer->add_srv( *(s3.getTestMgr()->get_srv_config()) );

        // May fail (depends on commit thread wake-up timing).
        size_t expected_cluster_size = 2;
        if (ret->get_result_code() == cmd_result_code::OK) {
            // If succeed, S3 is also a member of group.
            expected_cluster_size = 3;
        } else {
            // If not, error code should be CONFIG_CHANGNING.
            CHK_EQ( cmd_result_code::CONFIG_CHANGING, ret->get_result_code() );
        }

        // Finish adding S2 task.
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        TestSuite::sleep_ms(COMMIT_TIME_MS);

        // Heartbeat.
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        TestSuite::sleep_ms(COMMIT_TIME_MS);

        std::vector< ptr< srv_config > > configs_out;
        s1.raftServer->get_srv_config_all(configs_out);

        CHK_EQ(expected_cluster_size, configs_out.size());
    }

    {   // Attempt to add S2 again.
        ptr<raft_result> ret =
            s1.raftServer->add_srv( *(s2.getTestMgr()->get_srv_config()) );
        CHK_EQ( cmd_result_code::SERVER_ALREADY_EXISTS, ret->get_result_code() );
    }

    {   // Attempt to add S3 to S2 (non-leader).
        ptr<raft_result> ret =
            s2.raftServer->add_srv( *(s3.getTestMgr()->get_srv_config()) );
        CHK_EQ( cmd_result_code::NOT_LEADER, ret->get_result_code() );
    }

    rpc_handler nl_handler = [&invoked]( ptr<resp_msg>& resp,
                                         ptr<rpc_exception>& err ) -> int {
        invoked.store(true);
        CHK_EQ( cmd_result_code::NOT_LEADER, resp->get_result_code() );
        return 0;
    };
    {   // Attempt to add S3 to S2 (non-leader), through RPC.
        ptr<req_msg> req = cs_new<req_msg>
                           ( (ulong)0, msg_type::add_server_request, 0, 0,
                             (ulong)0, (ulong)0, (ulong)0 );
        ptr<srv_config> srv = s3.getTestMgr()->get_srv_config();
        ptr<buffer> buf(srv->serialize());
        ptr<log_entry> log( cs_new<log_entry>
                            ( 0, buf, log_val_type::cluster_server ) );
        req->log_entries().push_back(log);
        c_net->findClient(s2_addr)->send( req, nl_handler );
        c_net->execReqResp();
    }
    CHK_TRUE(invoked.load());
    invoked = false;

    {   // Now, normally add S3 to S1.
        s1.raftServer->add_srv( *(s3.getTestMgr()->get_srv_config()) );
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        TestSuite::sleep_ms(COMMIT_TIME_MS);

        // Heartbeat.
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        TestSuite::sleep_ms(COMMIT_TIME_MS);

        std::vector< ptr< srv_config > > configs_out;
        s1.raftServer->get_srv_config_all(configs_out);

        // All 3 servers should exist.
        CHK_EQ(3, configs_out.size());
    }

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int remove_node_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Try to remove s3 from non leader, should return error.
    ptr< cmd_result< ptr<buffer> > > ret =
        s2.raftServer->remove_srv( s3.getTestMgr()->get_srv_config()->get_id() );
    CHK_FALSE( ret->get_accepted() );
    CHK_EQ( cmd_result_code::NOT_LEADER, ret->get_result_code() );

    // Remove s3 from leader.
    s1.dbgLog(" --- remove ---");
    s1.raftServer->remove_srv( s3.getTestMgr()->get_srv_config()->get_id() );

    // Leave req/resp.
    s1.fNet->execReqResp();
    // Leave done, notify to peers.
    s1.fNet->execReqResp();
    // Notify new commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Now server 1 and 2 should know each other,
    // and server 3 only sees itself.
    for (auto& entry: pkgs) {
        RaftPkg* pkg = entry;
        std::vector< ptr<srv_config> > configs;
        pkg->raftServer->get_srv_config_all(configs);

        if (pkg == &s3) {
            CHK_EQ(1, configs.size());
            ptr<srv_config>& conf = configs[0];
            CHK_EQ(s3.myId, conf->get_id());
        } else {
            CHK_EQ(2, configs.size());
        }
    }

    // Invoke election timer for S3, to make it step down.
    s3.fTimer->invoke( timer_task_type::election_timer );
    s3.fTimer->invoke( timer_task_type::election_timer );
    // Pending timer task should be zero in S3.
    CHK_Z( s3.fTimer->getNumPendingTasks() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leader_election_basic_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Trigger election timer of S2.
    s2.dbgLog(" --- invoke election timer of S2 ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote requests, and probably rejected by S1 and S3.
    s2.fNet->execReqResp();

    // Trigger election timer of S3.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Send pre-vote requests, it will be rejected by S1, accepted by S2.
    // As a part of resp handling, it will initiate vote request.
    s3.fNet->execReqResp();
    // Send vote requests, S3 will be elected as a leader.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s3.raftServer->shutdown();
    s1.raftServer->shutdown();
    s2.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leader_election_priority_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Set the priority of S2 to 100.
    s1.raftServer->set_priority(2, 100);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Set the priority of S3 to 85.
    s1.raftServer->set_priority(3, 85);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Trigger election timer of S2.
    s2.dbgLog(" --- invoke election timer of S2 ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote requests, and probably rejected by S1 and S3.
    s2.fNet->execReqResp();

    // Trigger election timer of S3.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    // It will not initiate vote due to priority.
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Trigger election timer of S3 again.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    // Now it will initiate vote by help of priority decay.
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Send pre-vote requests, it will be rejected by S1, accepted by S2.
    // As a part of resp handling, it will initiate vote request.
    s3.fNet->execReqResp();
    // Send vote requests, S2 will deny due to priority.
    s3.fNet->execReqResp();

    // Trigger election timer of S2.
    s2.dbgLog(" --- invoke election timer of S2 ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote requests, and accepted by S3.
    // As a result of response, it will initiate actual vote.
    s2.fNet->execReqResp();
    // Send vote requests, S3 will vote for it.
    s2.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Send new config as a new leader.
    s2.fNet->execReqResp();
    // Follow-up: commit.
    s2.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_TRUE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int priority_broadcast_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Set the priority of S2 to 100.
    s1.raftServer->set_priority(2, 100);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Set the priority of S3 to 85.
    s1.raftServer->set_priority(3, 85);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Trigger election timer of S2.
    s2.dbgLog(" --- invoke election timer of S2 ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote requests, and probably rejected by S1 and S3.
    s2.fNet->execReqResp();

    // Trigger election timer of S3.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    // It will not initiate vote due to priority.
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Trigger election timer of S3 again.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    // Now it will initiate vote by help of priority decay.
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Send pre-vote requests, it will be rejected by S1, accepted by S2.
    // As a part of resp handling, it will initiate vote request.
    s3.fNet->execReqResp();
    // Send vote requests, S2 will deny due to priority.
    s3.fNet->execReqResp();

    // Now there should be no leader.
    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    // Set priority on non-leader node.
    s2.raftServer->set_priority(1, 90);
    // Send priority change reqs.
    s2.fNet->execReqResp();

    // Now all servers should have the same priorities.
    std::map<int, int> baseline;
    for (auto& entry: pkgs) {
        RaftPkg* rr = entry;
        for (int ii=1; ii<=3; ++ii) {
            ptr<srv_config> sc = rr->raftServer->get_srv_config(ii);
            if (rr == &s1) {
                // The first node: add to baseline
                baseline.insert( std::make_pair(ii, sc->get_priority()) );
            } else {
                // Otherwise: priority should be the same as baseline.
                CHK_EQ( baseline[ii], sc->get_priority() );
            }
        }
    }

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int custom_user_context_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Set custom context into Raft cluster config.
    const std::string CUSTOM_CTX = "hello world";
    s1.raftServer->set_user_ctx(CUSTOM_CTX);
    // Replicate and commit.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Get from followers.
    CHK_EQ( CUSTOM_CTX, s2.raftServer->get_user_ctx() );
    CHK_EQ( CUSTOM_CTX, s3.raftServer->get_user_ctx() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int follower_reconnect_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Follower 1 (server 2) requests reconnect.
    s2.raftServer->send_reconnect_request();
    s2.fNet->execReqResp();
    // Wait for reconnect timer.
    TestSuite::sleep_ms(3500, "wait for reconnect");

    // Now leader send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();

    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    // Append a message using separate thread.
    std::string test_msg = "test";
    ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
    msg->put(test_msg);
    exec_args.setMsg(msg);
    exec_args.eaExecuter.invoke();

    // Wait for executer thread.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    CHK_NULL( exec_args.getMsg().get() );
    // Packet for pre-commit.
    s1.fNet->execReqResp();
    // Packet for commit.
    s1.fNet->execReqResp();
    // Wait for bg commit.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Test message should be the same.
    uint64_t last_idx = s1.getTestSm()->getLastCommittedIdx();
    CHK_GT(last_idx, 0);
    ptr<buffer> buf = s1.getTestSm()->getData(last_idx);
    CHK_NONNULL( buf.get() );
    buf->pos(0);
    CHK_Z( memcmp(buf->data(), test_msg.data(), test_msg.size()) );

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

int snapshot_basic_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (size_t ii=0; ii<5; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        exec_args.setMsg(msg);
        exec_args.eaExecuter.invoke();

        // Wait for executer thread.
        TestSuite::sleep_ms(COMMIT_TIME_MS);

        CHK_NULL( exec_args.getMsg().get() );

        // NOTE: Send it to S2 only, S3 will be lagging behind.
        s1.fNet->execReqResp("S2"); // replication.
        s1.fNet->execReqResp("S2"); // commit.
        TestSuite::sleep_ms(COMMIT_TIME_MS); // commit execution.
    }
    // Make req to S3 failed.
    s1.fNet->makeReqFail("S3");

    // Trigger heartbeat to S3, it will initiate snapshot transmission.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    // Send the entire snapshot.
    do {
        s1.fNet->execReqResp();
    } while (s3.raftServer->is_receiving_snapshot());

    s1.fNet->execReqResp(); // commit.
    TestSuite::sleep_ms(COMMIT_TIME_MS); // commit execution.

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

int join_empty_node_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};
    CHK_Z( launch_servers( pkgs ) );

    // Organize group by using S1 and S2 only.
    CHK_Z( make_group( {&s1, &s2} ) );

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (size_t ii=0; ii<5; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        exec_args.setMsg(msg);
        exec_args.eaExecuter.invoke();

        // Wait for executer thread.
        TestSuite::sleep_ms(COMMIT_TIME_MS);

        CHK_NULL( exec_args.getMsg().get() );

        // NOTE: Send it to S2 only, S3 will be lagging behind.
        s1.fNet->execReqResp("S2"); // replication.
        s1.fNet->execReqResp("S2"); // commit.
        TestSuite::sleep_ms(COMMIT_TIME_MS); // commit execution.
    }

    // Now add S3 to leader.
    s1.raftServer->add_srv( *(s3.getTestMgr()->get_srv_config()) );
    s1.fNet->execReqResp(); // join req/resp.
    TestSuite::sleep_ms(COMMIT_TIME_MS); // S1 & S3: commit config.

    s1.fNet->execReqResp(); // req to S2 for new config.
    TestSuite::sleep_ms(COMMIT_TIME_MS); // S2: commit config.

    // First heartbeat to S3, it will initiate snapshot transmission.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp();

    // Send the entire snapshot.
    do {
        s1.fNet->execReqResp();
    } while (s3.raftServer->is_receiving_snapshot());

    // Configuration change.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp(); // replication.
    s1.fNet->execReqResp(); // commit.
    TestSuite::sleep_ms(COMMIT_TIME_MS); // commit execution.

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp(); // replication.
    s1.fNet->execReqResp(); // commit.
    TestSuite::sleep_ms(COMMIT_TIME_MS); // commit execution.
    print_stats(pkgs);

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );

    // For S3, do not check pre-commit list.
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

static int async_handler(std::list<ulong>* idx_list,
                         ptr< cmd_result< ptr<buffer> > >& cmd_result,
                         cmd_result_code expected_code,
                         ptr<buffer>& result,
                         ptr<std::exception>& err)
{
    CHK_EQ( expected_code, cmd_result->get_result_code() );

    if (expected_code == cmd_result_code::OK) {
        result->pos(0);
        ulong idx = result->get_ulong();
        if (idx_list) {
            idx_list->push_back(idx);
        }

    } else {
        CHK_NULL( result.get() );
    }
    return 0;
}

int async_append_handler_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 10;

    // Append messages asynchronously.
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );
        CHK_EQ( cmd_result_code::OK, ret->get_result_code() );

        handlers.push_back(ret);
    }

    // Packet for pre-commit.
    s1.fNet->execReqResp();
    // Packet for commit.
    s1.fNet->execReqResp();
    // Wait for bg commit.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // One more time to make sure.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Now all async handlers should have result.
    std::list<ulong> idx_list;
    for (auto& entry: handlers) {
        ptr< cmd_result< ptr<buffer> > > result = entry;
        cmd_result< ptr<buffer> >::handler_type my_handler =
            std::bind( async_handler,
                       &idx_list,
                       result,
                       cmd_result_code::OK,
                       std::placeholders::_1,
                       std::placeholders::_2 );
        result->when_ready( my_handler );
    }

    // Check if all messages are committed.
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        uint64_t idx = s1.getTestSm()->isCommitted(test_msg);
        CHK_GT(idx, 0);
    }

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int async_append_handler_cancel_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 10;

    // Append messages asynchronously.
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );
        CHK_EQ( cmd_result_code::OK, ret->get_result_code() );

        handlers.push_back(ret);
    }

    // Make append request failed.
    s1.fNet->makeReqFail("S2");
    s1.fNet->makeReqFail("S3");

    // S2 initiates leader election.
    // Trigger election timer of S2.
    s2.dbgLog(" --- invoke election timer of S2 ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote requests, and probably rejected by S1 and S3.
    s2.fNet->execReqResp();

    // Trigger election timer of S3.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Send pre-vote requests, it will be rejected by S1, accepted by S2.
    // As a part of resp handling, it will initiate vote request.
    s3.fNet->execReqResp();
    // Send vote requests, S3 will be elected as a leader.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    TestSuite::sleep_ms(COMMIT_TIME_MS);

    // Now all async handlers should have been cancelled.
    std::list<ulong> idx_list;
    for (auto& entry: handlers) {
        ptr< cmd_result< ptr<buffer> > > result = entry;
        cmd_result< ptr<buffer> >::handler_type my_handler =
            std::bind( async_handler,
                       &idx_list,
                       result,
                       cmd_result_code::CANCELLED,
                       std::placeholders::_1,
                       std::placeholders::_2 );
        result->when_ready( my_handler );
    }

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

}  // namespace raft_server_test;
using namespace raft_server_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    ts.doTest( "make group test",
               make_group_test );

    ts.doTest( "update params test",
               update_params_test );

    ts.doTest( "add node error cases test",
               add_node_error_cases_test );

    ts.doTest( "remove node test",
               remove_node_test );

    ts.doTest( "leader election basic test",
               leader_election_basic_test );

    ts.doTest( "leader election priority test",
               leader_election_priority_test );

    ts.doTest( "priority broadcast test",
               priority_broadcast_test );

    ts.doTest( "custom user context test",
               custom_user_context_test );

    ts.doTest( "follower reconnect test",
               follower_reconnect_test );

    ts.doTest( "snapshot basic test",
               snapshot_basic_test );

    ts.doTest( "join empty node test",
               join_empty_node_test );

    ts.doTest( "async append handler test",
               async_append_handler_test );

    ts.doTest( "async append handler cancel test",
               async_append_handler_cancel_test );

    return 0;
}

