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

namespace failure_test {

int simple_conflict_test() {
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

    const size_t NUM = 10;

    // Append messages asynchronously.
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s1.raftServer->append_entries( {msg} );
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

    // Check if all messages are committed.
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        uint64_t idx = s1.getTestSm()->isCommitted(test_msg);
        CHK_GT(idx, 0);
    }

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // Remember last log index before diverging.
    uint64_t idx_before_div = s1.getTestMgr()->load_log_store()->next_slot() - 1;

    // Append more messages to S1.
    const size_t MORE1 = 10;
    for (size_t ii=NUM; ii<NUM+MORE1; ++ii) {
        std::string test_msg = "more" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s1.raftServer->append_entries( {msg} );
    }

    // Without replication of above messages,
    // initiate leader election.
    s2.dbgLog(" --- S2 will start leader election ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    s3.fTimer->invoke( timer_task_type::election_timer );
    // Send it to S3 only.
    s2.fNet->execReqResp( s3_addr );
    s2.fNet->execReqResp( s3_addr );
    s2.fNet->execReqResp( s3_addr );
    s2.fNet->execReqResp( s3_addr );
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    // Now S2 should be the new leader.
    s2.dbgLog(" --- Now S2 is leader ---");

    // Drop all messages of S2 and S3.
    s2.fNet->makeReqFailAll( s1_addr );
    s3.fNet->makeReqFailAll( s1_addr );
    s3.fNet->makeReqFailAll( s2_addr );

    // Append new (diverged) messages to S2 (new leader).
    s2.dbgLog(" --- Append diverged logs to S2 ---");
    const size_t MORE2 = 5;
    for (size_t ii=NUM; ii<NUM+MORE2; ++ii) {
        std::string test_msg = "diverged" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s2.raftServer->append_entries( {msg} );
    }

    // S1's log index should be greater than S2's log index.
    uint64_t idx_after_div = s1.getTestMgr()->load_log_store()->next_slot() - 1;
    CHK_GT( idx_after_div,
            s2.getTestMgr()->load_log_store()->next_slot() - 1 );

    // S1 attempts to replicate messages.
    // It should be rejected.
    s1.fNet->execReqResp();

    // Now S2 replicate messages.
    // S1 has conflict, so that it should discard its local logs.
    s2.dbgLog(" --- S2 starts to replicate ---");
    s2.fNet->execReqResp();
    s2.fNet->execReqResp();
    s2.fNet->execReqResp();
    s2.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Check if all messages are committed.
    for (size_t ii=0; ii<NUM+MORE2; ++ii) {
        std::string test_msg;
        if (ii < NUM) {
            test_msg = "test" + std::to_string(ii);
        } else {
            test_msg = "diverged" + std::to_string(ii);
        }
        uint64_t idx = s2.getTestSm()->isCommitted(test_msg);
        CHK_GT(idx, 0);
    }

    // State machine should be identical.
    CHK_OK( s1.getTestSm()->isSame( *s2.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s2.getTestSm() ) );

    // Log store's last index should be identical.
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot(),
            s2.getTestMgr()->load_log_store()->next_slot() );
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot(),
            s3.getTestMgr()->load_log_store()->next_slot() );

    // Rolled back indexes should be
    //   1) from `idx_before_div` (exclusive) to `idx_after_div` (inclusive),
    //   2) descending order, and
    //   3) consecutive.
    const std::list<uint64_t> r_idxs = s1.getTestSm()->getRollbackIdxs();
    CHK_EQ(idx_after_div - idx_before_div, r_idxs.size());
    for (uint64_t idx: r_idxs) {
        CHK_EQ(idx_after_div--, idx);
    }
    CHK_EQ(idx_before_div, idx_after_div);

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int rmv_not_resp_srv_wq_test(bool explicit_failure) {
    // * Remove server that is not responding.
    // * Can reach quorum.

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

    // Remove s3 from leader.
    s1.dbgLog(" --- remove ---");
    s1.raftServer->remove_srv( s3.getTestMgr()->get_srv_config()->get_id() );

    s1.fNet->execReqResp(s2_addr);
    // Fail to send it to S3.
    if (explicit_failure) {
        s1.fNet->makeReqFailAll(s3_addr);
    }

    // Heartbeat multiple times.
    for (size_t ii=0; ii<10; ++ii) {
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp(s2_addr);
        // Fail to send it to S3.
        if (explicit_failure) {
            s1.fNet->makeReqFailAll(s3_addr);
        }
    }

    // Wait for commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // For server 1 and 2, only 2 servers should exist.
    for (auto& entry: pkgs) {
        RaftPkg* pkg = entry;
        std::vector< ptr<srv_config> > configs;
        pkg->raftServer->get_srv_config_all(configs);

        if (pkg != &s3) {
            CHK_EQ(2, configs.size());
        }
    }

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

cb_func::ReturnCode ool_detect_cb(std::atomic<bool>* invoked,
                                  size_t purge_upto,
                                  cb_func::Type type,
                                  cb_func::Param* params)
{
    if (type == cb_func::Type::OutOfLogRangeWarning) {
        cb_func::OutOfLogRangeWarningArgs* ool_args =
            (cb_func::OutOfLogRangeWarningArgs*)params->ctx;
        auto chk_func = [purge_upto, ool_args]() -> int {
            CHK_EQ(purge_upto + 1, ool_args->startIdxOfLeader);
            return 0;
        };
        if (chk_func() == 0) {
            invoked->store(true);
        }
    } else {
        return cb_default(type, params);
    }

    return cb_func::ReturnCode::Ok;
}

int force_log_compaction_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    const size_t NUM_APPENDS = 10;
    const size_t PURGE_UPTO = 5;

    std::atomic<bool> invoked(false);
    for (size_t ii = 0; ii < pkgs.size(); ++ii) {
        RaftPkg* ff = pkgs[ii];
        raft_server::init_options opt;
        if (ii < 2) {
            opt.raft_callback_ = cb_default;
            ff->initServer(nullptr, opt);
        } else {
            // S3: set callback function to detect out of log range.
            opt.raft_callback_ = std::bind( ool_detect_cb,
                                            &invoked,
                                            PURGE_UPTO,
                                            std::placeholders::_1,
                                            std::placeholders::_2 );
            ff->initServer(nullptr, opt);
        }
        ff->fNet->listen(ff->raftServer);
        ff->fTimer->invoke( timer_task_type::election_timer );
    }
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        // Do not create snapshot.
        param.snapshot_distance_ = 0;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    for (size_t ii=0; ii<NUM_APPENDS; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s1.raftServer->append_entries( {msg} );
    }

    // Send it to S2 only.
    s1.fNet->execReqResp(s2_addr);
    s1.fNet->execReqResp(s2_addr);
    s1.fNet->makeReqFailAll(s3_addr);

    // Wait for commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Force log compaction.
    s1.sMgr->load_log_store()->compact(PURGE_UPTO);

    // Trigger heartbeat, it should be ok, without any crash.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();

    // One more time, after 100ms.
    TestSuite::sleep_ms(100);
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();

    // Callback function should have been invoked.
    CHK_TRUE(invoked);

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int uncommitted_conf_new_leader_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";
    std::string s4_addr = "S4";
    std::string s5_addr = "S5";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    RaftPkg s4(f_base, 4, s4_addr);
    RaftPkg s5(f_base, 5, s5_addr);

    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3, &s4, &s5};
    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.max_append_size_ = 1;
        // Do not create snapshot.
        param.snapshot_distance_ = 0;
        pp->raftServer->update_params(param);
    }

    const size_t NUM_APPENDS_1 = 3;
    const size_t NUM_APPENDS_2 = 6;

    // Append 3 messages.
    for (size_t ii=0; ii<NUM_APPENDS_1; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s1.raftServer->append_entries( {msg} );
    }
    // Replicate to all.
    for (size_t ii=0; ii<=NUM_APPENDS_1; ++ii) {
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Append 3 more messages.
    for (size_t ii=NUM_APPENDS_1; ii<NUM_APPENDS_2; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s1.raftServer->append_entries( {msg} );
    }
    // Replicate to S2 and S3 only.
    for (size_t ii=NUM_APPENDS_1; ii<=NUM_APPENDS_2; ++ii) {
        s1.fNet->execReqResp(s2_addr);
        s1.fNet->execReqResp(s3_addr);
    }
    // One more time, to make sure there is no message in-flight.
    s1.fNet->execReqResp(s2_addr);
    s1.fNet->execReqResp(s3_addr);

    // Now remove S2 (who was a member of the latest quorum).
    s1.raftServer->remove_srv(2);
    for (size_t ii=0; ii<3; ++ii) {
        s1.fNet->execReqResp(s2_addr);
    }
    // Send new update to S3 only.
    s1.fNet->execReqResp(s3_addr);

    // Invoke election timer of S4 and S5 (to make pre-vote of S3 succeed).
    s4.fTimer->invoke( timer_task_type::election_timer );
    s4.fNet->execReqResp();
    s5.fTimer->invoke( timer_task_type::election_timer );
    s5.fNet->execReqResp();

    // Now S3's vote should succeed.
    s3.fTimer->invoke( timer_task_type::election_timer );
    s3.fNet->execReqResp();
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    s3.fTimer->invoke( timer_task_type::heartbeat_timer );
    s3.fNet->execReqResp();
    s3.fNet->execReqResp();

    // Removing S2 should be in the latest config.
    ptr<cluster_config> c_config = s3.raftServer->get_config();
    CHK_NULL(c_config->get_server(2).get());

    print_stats( pkgs );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    s4.raftServer->shutdown();
    s5.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int removed_server_late_step_down_test() {
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

    // Remove s3 from leader.
    s1.raftServer->remove_srv( s3.getTestMgr()->get_srv_config()->get_id() );

    // Leave req/resp.
    s1.fNet->execReqResp();
    // Leave done, notify to peers, but except for S3.
    s1.fNet->execReqResp(s2_addr);
    // Notify new commit.
    s1.fNet->execReqResp(s2_addr);
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // S1 and S2: should see S1 and S2 only.
    // S3: should see everyone.
    for (auto& entry: pkgs) {
        RaftPkg* pkg = entry;
        std::vector< ptr<srv_config> > configs;
        pkg->raftServer->get_srv_config_all(configs);

        TestSuite::setInfo("id = %d", pkg->myId);
        if (pkg->myId != 3) {
            CHK_EQ(2, configs.size());
        } else {
            CHK_EQ(3, configs.size());
        }
    }

    // Removing server again should fail.
    ptr< cmd_result< ptr<buffer> > > ret =
        s1.raftServer->remove_srv( s3.getTestMgr()->get_srv_config()->get_id() );
    CHK_FALSE(ret->get_accepted());

    // More catch-up for to-be-removed server.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now all servers should see S1 and S2 only.
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

int remove_server_on_pending_configs_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Make some dummy configs by setting user ctx.
    s1.raftServer->set_user_ctx("a");
    s1.raftServer->set_user_ctx("aa");

    // Without commit & replication of above configs,
    // remove S2.
    s1.raftServer->remove_srv(2);

    // Make failure.
    s1.fNet->makeReqFailAll(s2_addr);
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->makeReqFailAll(s2_addr);

    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Adding server should succeed without error about duplicate ID.
    ptr< cmd_result< ptr<buffer> > > ret =
        s1.raftServer->add_srv( *s2.getTestMgr()->get_srv_config() );
    CHK_Z( ret->get_result_code() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

}  // namespace failure_test;
using namespace failure_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    ts.doTest( "simple conflict test",
               simple_conflict_test );

    ts.doTest( "remove not responding server with quorum test",
               rmv_not_resp_srv_wq_test,
               TestRange<bool>({false, true}) );

    ts.doTest( "force log compaction test",
               force_log_compaction_test );

    ts.doTest( "uncommitted config for new leader test",
               uncommitted_conf_new_leader_test );

    ts.doTest( "removed server late step down test",
               removed_server_late_step_down_test );

    ts.doTest( "remove server on pending configs test",
               remove_server_on_pending_configs_test );

    return 0;
}

