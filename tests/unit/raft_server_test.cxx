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

static size_t EXECUTOR_WAIT_MS = 100;

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
    TestSuite::sleep_ms(EXECUTOR_WAIT_MS, "wait for synchronous executor");

    {   std::lock_guard<std::mutex> l(exec_args.msgToWriteLock);
        CHK_NULL( exec_args.msgToWrite.get() );
    }
    // Packet for pre-commit.
    s1.fNet->execReqResp();
    // Packet for commit.
    s1.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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

int init_options_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    size_t num_srvs = pkgs.size();
    CHK_GT(num_srvs, 0);

    raft_server::init_options opt;

    for (size_t ii = 0; ii < num_srvs; ++ii) {
        RaftPkg* ff = pkgs[ii];

        // For s2 and s3, initialize Raft servers with
        // election timer skip option.
        opt.skip_initial_election_timeout_ = (ii > 0);
        opt.raft_callback_ = cb_default;
        ff->initServer(nullptr, opt);
        ff->fNet->listen(ff->raftServer);
        ff->fTimer->invoke( timer_task_type::election_timer );
    }

    // s2 and s3 should never be a leader.
    for (size_t ii = 0; ii < num_srvs; ++ii) {
        RaftPkg* ff = pkgs[ii];
        if (ii == 0) {
            CHK_TRUE( ff->raftServer->is_leader() );
        } else {
            CHK_FALSE( ff->raftServer->is_leader() );
        }
    }

    // Make group should succeed as long as s1 is the current leader.
    CHK_Z( make_group( pkgs ) );
    for (RaftPkg* ff: pkgs) {
        CHK_EQ(1, ff->raftServer->get_leader());
    }

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

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
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

        // Heartbeat.
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

        // Heartbeat.
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // All servers should see S1 and S2 only.
    for (auto& entry: pkgs) {
        RaftPkg* pkg = entry;
        std::vector< ptr<srv_config> > configs;
        pkg->raftServer->get_srv_config_all(configs);

        TestSuite::setInfo("id = %d", pkg->myId);
        CHK_EQ(2, configs.size());
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

int remove_node_error_cases_test() {
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

    {   // Attempt to remove more than one server at once.
        ptr<req_msg> req = cs_new<req_msg>
                           ( (ulong)0, msg_type::remove_server_request, 0, 0,
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

    {   // Attempt to remove S3 from S2 (non-leader).
        ptr<raft_result> ret = s2.raftServer->remove_srv(s3.myId);
        CHK_EQ( cmd_result_code::NOT_LEADER, ret->get_result_code() );
    }

    rpc_handler nl_handler = [&invoked]( ptr<resp_msg>& resp,
                                         ptr<rpc_exception>& err ) -> int {
        invoked.store(true);
        CHK_EQ( cmd_result_code::NOT_LEADER, resp->get_result_code() );
        return 0;
    };
    {   // Attempt to remove S3 to S2 (non-leader), through RPC.
        ptr<req_msg> req = cs_new<req_msg>
                           ( (ulong)0, msg_type::remove_server_request, 0, 0,
                             (ulong)0, (ulong)0, (ulong)0 );
        ptr<buffer> buf(buffer::alloc(sz_int));
        buf->put(s3.myId);
        buf->pos(0);
        ptr<log_entry> log(cs_new<log_entry>(0, buf, log_val_type::cluster_server));
        req->log_entries().push_back(log);
        c_net->findClient(s2_addr)->send( req, nl_handler );
        c_net->execReqResp();
    }
    CHK_TRUE(invoked.load());
    invoked = false;

    {   // Attempt to remove non-existing server ID.
        ptr<raft_result> ret = s1.raftServer->remove_srv(9999);
        CHK_EQ( cmd_result_code::SERVER_NOT_FOUND, ret->get_result_code() );
    }

    {   // Attempt to remove leader itself.
        ptr<raft_result> ret = s1.raftServer->remove_srv(s1.myId);
        CHK_EQ( cmd_result_code::CANNOT_REMOVE_LEADER, ret->get_result_code() );
    }

    {   // Attempt to remove server while previous one is in progress.

        // Remove S2 from S1.
        s1.raftServer->remove_srv(s2.myId);

        // Leave req/resp.
        s1.fNet->execReqResp();

        // Now config change is in progress, remove S3.
        ptr<raft_result> ret = s1.raftServer->remove_srv(s3.myId);

        // May fail (depends on commit thread wake-up timing).
        size_t expected_cluster_size = 2;
        if (ret->get_result_code() == cmd_result_code::OK) {
            // If succeed, S3 is also removed.
            expected_cluster_size = 1;
        }

        // Finish the task.
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

        // Heartbeat.
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

        std::vector< ptr< srv_config > > configs_out;
        s1.raftServer->get_srv_config_all(configs_out);
        CHK_EQ(expected_cluster_size, configs_out.size());

        // If S3 still exists, remove it here.
        if (expected_cluster_size > 1) {
            s1.raftServer->remove_srv(s3.myId);
            s1.fNet->execReqResp();
            s1.fNet->execReqResp();
            CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

            s1.fTimer->invoke( timer_task_type::heartbeat_timer );
            s1.fNet->execReqResp();
            s1.fNet->execReqResp();
            CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

            configs_out.clear();
            s1.raftServer->get_srv_config_all(configs_out);
            CHK_EQ(1, configs_out.size());
        }
    }

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int remove_and_then_add_test() {
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
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Make a group using S1 and S2 only.
    CHK_Z( make_group( {&s1, &s2} ) );

    // Append logs to create a snapshot and then compact logs.
    const size_t NUM = 10;
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
    // Pre-commit and commit.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Remove S2 from leader.
    s1.dbgLog(" --- remove ---");
    s1.raftServer->remove_srv( s2.getTestMgr()->get_srv_config()->get_id() );

    // Leave req/resp.
    s1.fNet->execReqResp();
    // Leave done, notify to peers.
    s1.fNet->execReqResp();
    // Notify new commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now add S3 to leader.
    s1.raftServer->add_srv( *(s3.getTestMgr()->get_srv_config()) );
    s1.fNet->execReqResp();
    // Send the entire snapshot.
    do {
        s1.fNet->execReqResp();
    } while (s3.raftServer->is_receiving_snapshot());
    // Commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // First HB.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Second HB.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // S3 should see S1 and itself.
    CHK_EQ(2, s3.raftServer->get_config()->get_servers().size());

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int multiple_config_change_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";
    std::string s4_addr = "S4";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    RaftPkg s4(f_base, 4, s4_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3, &s4};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Remove two nodes without waiting commit.
    s1.raftServer->remove_srv( s3.getTestMgr()->get_srv_config()->get_id() );

    // Cannot remove multiple servers at once, should return error.
    ptr<raft_result> ret =
        s1.raftServer->remove_srv( s4.getTestMgr()->get_srv_config()->get_id() );
    CHK_GT(0, ret->get_result_code());

    // Priority change is OK.
    s1.raftServer->set_priority(s4.getTestMgr()->get_srv_config()->get_id(), 10);

    // Leave req/resp.
    s1.fNet->execReqResp();
    // Leave done, notify to peers.
    s1.fNet->execReqResp();
    // Probably one more.
    s1.fNet->execReqResp();
    // Notify new commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // S3 should be removed.
    for (RaftPkg* pp: pkgs) {
        if (pp->getTestMgr()->get_srv_config()->get_id() == 3) continue;

        std::vector< ptr< srv_config > > configs_out;
        pp->raftServer->get_srv_config_all(configs_out);

        // Only S1, S2, and S4 should exist.
        CHK_EQ(3, configs_out.size());
        for (auto& entry: configs_out) {
            ptr<srv_config>& s_conf = entry;
            CHK_TRUE( s_conf->get_id() == 1 ||
                      s_conf->get_id() == 2 ||
                      s_conf->get_id() == 4 );
        }

        // S4's priority should be 10.
        ptr<cluster_config> c_conf = pp->raftServer->get_config();
        ptr<srv_config> s4_conf = c_conf->get_server(4);
        CHK_EQ(10, s4_conf->get_priority());
    }

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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 85.
    s1.raftServer->set_priority(3, 85);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s2.fNet->execReqResp();
    // Follow-up: commit.
    s2.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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

int leader_election_with_aggressive_node_test() {
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

    // Set the priority of S1 to 100.
    s1.raftServer->set_priority(1, 100);
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S2 to 50.
    s1.raftServer->set_priority(2, 50);
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 1.
    s1.raftServer->set_priority(3, 1);
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // --- Now assume S1 is not reachable. ---

    // Trigger election timer of S2.
    s2.dbgLog(" --- invoke election timer of S2 ---");
    // It will not initiate vote due to priority.
    s2.fTimer->invoke( timer_task_type::election_timer );

    // Keep triggering election timer of S3, until it becomes leader.
    const size_t MAX_ATTEMPTS = 1000;
    size_t num_attempts = 0;
    TestSuite::UnknownProgress pp("leader election attempts: ");
    do {
        s3.dbgLog(" --- invoke election timer of S3 ---");
        s3.fTimer->invoke( timer_task_type::election_timer );
        // Drop all packets to S1.
        s3.fNet->makeReqFailAll(s1_addr);
        // Send pre-vote requests.
        s3.fNet->execReqResp();
        // Send vote request (if exists).
        s3.fNet->execReqResp();
        pp.update(++num_attempts);
    } while ( !s3.raftServer->is_leader() &&
              num_attempts < MAX_ATTEMPTS );
    pp.done();
    _msg("%zu attempts\n", num_attempts);
    CHK_SM(num_attempts, MAX_ATTEMPTS);

    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    s3.fNet->makeReqFailAll(s1_addr);
    s3.fNet->execReqResp();
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leadership_takeover_basic_test() {
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

    // Set the priority of S2 to 10.
    s1.raftServer->set_priority(2, 10);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    s1.raftServer->set_priority(3, 5);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Yield leadership.
    s1.dbgLog(" --- yield leadership ---");
    s1.raftServer->yield_leadership();
    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    // After getting response of heartbeat, S1 will resign.
    s1.fNet->execReqResp();

    // Now S2 should have received takeover request.
    // Send vote requests.
    s2.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s2.fNet->execReqResp();
    // Follow-up: commit.
    s2.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_TRUE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    // Re-yield leadership, now S1 should be the leader again.
    s2.dbgLog(" --- yield leadership ---");
    s2.raftServer->yield_leadership();
    // Send heartbeat.
    s2.fTimer->invoke( timer_task_type::heartbeat_timer );
    s2.fNet->execReqResp();
    // After getting response of heartbeat, S2 will resign.
    s2.fNet->execReqResp();

    // Now S1 should have received takeover request.
    // Send vote requests.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s1.fNet->execReqResp();
    // Follow-up: commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leadership_takeover_designated_successor_test() {
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

    // Set the priority of S2 to 10.
    s1.raftServer->set_priority(2, 10);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    s1.raftServer->set_priority(3, 5);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Yield leadership to S3.
    s1.dbgLog(" --- yield leadership ---");
    s1.raftServer->yield_leadership(false, 3);
    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    // After getting response of heartbeat, S1 will resign.
    s1.fNet->execReqResp();

    // Now S3 should have received takeover request.
    // Send vote requests.
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    // Re-yield leadership to S2.
    s3.dbgLog(" --- yield leadership ---");
    s3.raftServer->yield_leadership(false, 2);
    // Send heartbeat.
    s3.fTimer->invoke( timer_task_type::heartbeat_timer );
    s3.fNet->execReqResp();
    // After getting response of heartbeat, S3 will resign.
    s3.fNet->execReqResp();

    // Now S2 should have received takeover request.
    // Send vote requests.
    s2.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s2.fNet->execReqResp();
    // Follow-up: commit.
    s2.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_TRUE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    // Re-yield leadership with wrong successor,
    // S1 (highest priority server) will take over.
    s2.dbgLog(" --- yield leadership ---");
    s2.raftServer->yield_leadership(false, 12345);
    // Send heartbeat.
    s2.fTimer->invoke( timer_task_type::heartbeat_timer );
    s2.fNet->execReqResp();
    // After getting response of heartbeat, S2 will resign.
    s2.fNet->execReqResp();

    // Now S1 should have received takeover request.
    // Send vote requests.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s1.fNet->execReqResp();
    // Follow-up: commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leadership_takeover_by_request_test() {
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

    // Set the priority of S2 to 10.
    s1.raftServer->set_priority(2, 10);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    s1.raftServer->set_priority(3, 5);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Request leadership by the current leader, should fail.
    CHK_FALSE( s1.raftServer->request_leadership() );

    // S3 requests the leadership from S1.
    s1.dbgLog(" --- request leadership ---");
    CHK_TRUE( s3.raftServer->request_leadership() );
    // Send request.
    s3.fNet->execReqResp();

    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    // After getting response of heartbeat, S1 will resign.
    s1.fNet->execReqResp();

    // Now S3 should have received takeover request.
    // Send vote requests.
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leadership_takeover_offline_candidate_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    raft_params custom_params;
    custom_params.election_timeout_lower_bound_ = 0;
    custom_params.election_timeout_upper_bound_ = 1000;
    custom_params.heart_beat_interval_ = 500;
    CHK_Z( launch_servers( pkgs, &custom_params ) );
    CHK_Z( make_group( pkgs ) );

    // Set the priority of S2 to 10.
    s1.raftServer->set_priority(2, 10);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    s1.raftServer->set_priority(3, 5);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Wait longer than heartbeat.
    TestSuite::sleep_ms(600);

    // Now S2 is offline.
    s2.fNet->goesOffline();

    // Send heartbeat to S3 only.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();

    // Yield leadership, but now S2 is not responding sudo that
    // candidate for the next leader should be S3.
    s1.dbgLog(" --- yield leadership ---");
    s1.raftServer->yield_leadership();

    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    // After getting response of heartbeat, S1 will resign.
    s1.fNet->execReqResp();

    // Now S3 should have received takeover request.
    // Send vote requests.
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    // Re-yield leadership, now S1 should be the leader again.
    s3.dbgLog(" --- yield leadership ---");
    s3.raftServer->yield_leadership();
    // Send heartbeat.
    s3.fTimer->invoke( timer_task_type::heartbeat_timer );
    s3.fNet->execReqResp();
    // After getting response of heartbeat, S2 will resign.
    s3.fNet->execReqResp();

    // Now S1 should have received takeover request.
    // Send vote requests.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s1.fNet->execReqResp();
    // Follow-up: commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int temporary_leader_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    raft_params custom_params;
    custom_params.election_timeout_lower_bound_ = 0;
    custom_params.election_timeout_upper_bound_ = 1000;
    custom_params.heart_beat_interval_ = 10;
    CHK_Z( launch_servers( pkgs, &custom_params ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Set the priority of S3 to 0.
    s1.raftServer->set_priority(3, 0);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now S2 goes offline.
    s2.fNet->goesOffline();

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

    // Replicate.
    for (size_t ii=0; ii<3; ++ii) s1.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now S1 goes offline, and S2 goes online.
    s1.fNet->goesOffline();
    s2.fNet->goesOnline();

    const size_t MAX_ATTEMPTS = 100;
    size_t attempts = 0;
    do {
        // Vote, S2 should be rejected all the time.
        s2.fTimer->invoke( timer_task_type::election_timer );
        // Pre-vote and vote.
        s2.fNet->execReqResp();
        s2.fNet->execReqResp();

        s3.fTimer->invoke( timer_task_type::election_timer );
        // Pre-vote and vote.
        s3.fNet->execReqResp();
        s3.fNet->execReqResp();

        // Sleep.
        attempts++;
        TestSuite::sleep_ms(custom_params.heart_beat_interval_);

        // Repeat until S3 becomes (temporary) leader.
    } while (!s3.raftServer->is_leader() && attempts < MAX_ATTEMPTS);
    CHK_SM(attempts, MAX_ATTEMPTS);

    // Commit for reconfigure.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now S3 will yield leadership for S2.
    s3.fTimer->invoke( timer_task_type::heartbeat_timer );
    // Catch-up and commit.
    s3.fNet->execReqResp();
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Resign.
    s3.fNet->execReqResp();
    s3.fNet->execReqResp();

    // Now S2 initiates leader election.
    s2.fNet->execReqResp();
    s2.fNet->execReqResp();

    CHK_TRUE( s2.raftServer->is_leader() );
    s2.fNet->execReqResp();
    s2.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 50.
    s1.raftServer->set_priority(3, 50);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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

    // S1 should be still leader.
    CHK_TRUE( s1.raftServer->is_leader() );
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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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
    TestSuite::sleep_ms(EXECUTOR_WAIT_MS);

    CHK_NULL( exec_args.getMsg().get() );
    // Packet for pre-commit.
    s1.fNet->execReqResp();
    // Packet for commit.
    s1.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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
        TestSuite::sleep_ms(EXECUTOR_WAIT_MS);

        CHK_NULL( exec_args.getMsg().get() );

        // NOTE: Send it to S2 only, S3 will be lagging behind.
        s1.fNet->execReqResp("S2"); // replication.
        s1.fNet->execReqResp("S2"); // commit.
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.
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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

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
        TestSuite::sleep_ms(EXECUTOR_WAIT_MS);

        CHK_NULL( exec_args.getMsg().get() );

        // NOTE: Send it to S2 only, S3 will be lagging behind.
        s1.fNet->execReqResp("S2"); // replication.
        s1.fNet->execReqResp("S2"); // commit.
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.
    }

    // Now add S3 to leader.
    s1.raftServer->add_srv( *(s3.getTestMgr()->get_srv_config()) );
    s1.fNet->execReqResp(); // join req/resp.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // S1 & S3: commit config.

    s1.fNet->execReqResp(); // req to S2 for new config.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // S2: commit config.

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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp(); // replication.
    s1.fNet->execReqResp(); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.
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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // One more time to make sure.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

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

int apply_config_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    raft_params custom_params;
    custom_params.election_timeout_lower_bound_ = 0;
    custom_params.election_timeout_upper_bound_ = 1000;
    custom_params.heart_beat_interval_ = 500;
    custom_params.snapshot_distance_ = 100;
    CHK_Z( launch_servers( pkgs, &custom_params ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Make S3 offline.
    s3.fNet->goesOffline();

    // Append some logs.
    const size_t NUM = 10;
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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // One more time to make sure.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Add S4.
    std::string s4_addr = "S4";
    RaftPkg s4(f_base, 4, s4_addr);
    CHK_Z( launch_servers( {&s4} ) );

    // Add to leader.
    s1.raftServer->add_srv( *(s4.getTestMgr()->get_srv_config()) );

    // Join req/resp.
    s1.fNet->execReqResp();
    // Add new server, notify existing peers.
    // After getting response, it will make configuration commit.
    s1.fNet->execReqResp();
    // Notify new commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now heartbeat to new node is enabled.

    // Heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    // Heartbeat req/resp, to finish the catch-up phase.
    s1.fNet->execReqResp();
    // Need one-more req/resp.
    s1.fNet->execReqResp();
    // Wait for bg commit for new node.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 85.
    s1.raftServer->set_priority(3, 85);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S4 to 100.
    s1.raftServer->set_priority(4, 100);
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Remove S2.
    s1.raftServer->remove_srv( s2.getTestMgr()->get_srv_config()->get_id() );

    // Leave req/resp.
    s1.fNet->execReqResp();
    // Leave done, notify to peers.
    s1.fNet->execReqResp();
    // Notify new commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    print_stats(pkgs);

    // Shutdown S3, and do offline replay of pending configs.

    std::string err_msg;
    {   // NULL argument should fail.
        ptr<log_entry> le;
        ptr<state_mgr> smgr;
        CHK_FALSE( raft_server::apply_config_log_entry( le, smgr, err_msg ) );
    }

    size_t last_s3_commit = s3.sm->last_commit_index();
    ptr<log_store> s1_log_store = s1.sMgr->load_log_store();
    size_t last_log_idx = s1_log_store->next_slot() - 1;

    s3.raftServer->shutdown();
    for (size_t ii=last_s3_commit+1; ii<=last_log_idx; ++ii) {
        ptr<log_entry> le = s1_log_store->entry_at(ii);
        bool expected_ok = (le->get_val_type() == log_val_type::conf);

        bool ret_ok = raft_server::apply_config_log_entry(le, s3.sMgr, err_msg);
        CHK_EQ(expected_ok, ret_ok);
    }

    // S3 and S1 (leader) should have exactly the same config.
    ptr<cluster_config> s1_conf = s1.sMgr->load_config();
    ptr<cluster_config> s3_conf = s3.sMgr->load_config();
    ptr<buffer> s1_conf_buf = s1_conf->serialize();
    ptr<buffer> s3_conf_buf = s3_conf->serialize();
    CHK_EQ( s1_conf_buf->size(), s3_conf_buf->size() );
    CHK_Z( memcmp( s1_conf_buf->data_begin(),
                   s3_conf_buf->data_begin(),
                   s1_conf_buf->size() ) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s4.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int custom_term_counter_test() {
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

    auto custom_term = [](ulong cur_term) -> ulong {
        // Increase by 10.
        return (cur_term / 10) + 10;
    };
    for (RaftPkg* rp: pkgs) {
        rp->raftServer->set_inc_term_func(custom_term);
    }

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
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    // Check S3's term. It should be 10.
    CHK_EQ( 10, s3.raftServer->get_term() );

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

    ts.doTest( "init options test",
               init_options_test );

    ts.doTest( "update params test",
               update_params_test );

    ts.doTest( "add node error cases test",
               add_node_error_cases_test );

    ts.doTest( "remove node test",
               remove_node_test );

    ts.doTest( "remove node error cases test",
               remove_node_error_cases_test );

    ts.doTest( "remove and then add test",
               remove_and_then_add_test );

    ts.doTest( "multiple config change test",
               multiple_config_change_test );

    ts.doTest( "leader election basic test",
               leader_election_basic_test );

    ts.doTest( "leader election priority test",
               leader_election_priority_test );

    ts.doTest( "leader election with aggressive node test",
               leader_election_with_aggressive_node_test );

    ts.doTest( "leadership takeover basic test",
               leadership_takeover_basic_test );

    ts.doTest( "leadership takeover with designated successor test",
               leadership_takeover_designated_successor_test );

    ts.doTest( "leadership takeover by request test",
               leadership_takeover_by_request_test );

    ts.doTest( "leadership takeover with offline candidate test",
               leadership_takeover_offline_candidate_test );

    ts.doTest( "temporary leader test",
               temporary_leader_test );

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

    ts.doTest( "apply config log entry test",
               apply_config_test );

    ts.doTest( "custom term counter test",
               custom_term_counter_test );

#ifdef ENABLE_RAFT_STATS
    _msg("raft stats: ENABLED\n");
#else
    _msg("raft stats: DISABLED\n");
#endif
    _msg("num allocs: %zu\n"
         "amount of allocs: %zu bytes\n"
         "num active buffers: %zu\n"
         "amount of active buffers: %zu bytes\n",
         raft_server::get_stat_counter("num_buffer_allocs"),
         raft_server::get_stat_counter("amount_buffer_allocs"),
         raft_server::get_stat_counter("num_active_buffers"),
         raft_server::get_stat_counter("amount_active_buffers"));

    return 0;
}

