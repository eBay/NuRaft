/************************************************************************
Copyright 2017-present eBay Inc.
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

#include "debugging_options.hxx"
#include "fake_network.hxx"
#include "raft_package_fake.hxx"

#include "raft_params.hxx"
#include "test_common.h"

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

using raft_result = cmd_result< ptr<buffer> >;

namespace learner_new_joiner_test {

int append_logs(size_t num_appends,
                RaftPkg& leader,
                const std::vector<RaftPkg*>& pkgs)
{
    // Append messages asynchronously.
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii = 0; ii < num_appends; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            leader.raftServer->append_entries( {msg} );
        CHK_TRUE( ret->get_accepted() );
        handlers.push_back(ret);
    }

    // Packet for pre-commit.
    leader.fNet->execReqResp();
    // Packet for commit.
    leader.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // One more time to make sure.
    leader.fNet->execReqResp();
    leader.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    return 0;
}

int add_new_joiner(RaftPkg& leader,
                   RaftPkg& new_joiner,
                   const std::vector<RaftPkg*>& pkgs_old,
                   const std::vector<RaftPkg*>& pkgs_new)
{
    // Now add a new member.
    leader.raftServer->add_srv( *(new_joiner.getTestMgr()->get_srv_config()) );

    // Join req/resp.
    leader.fNet->execReqResp();
    // Add new server, notify existing peers.
    // After getting response, it will make configuration commit.
    leader.fNet->execReqResp();
    // Notify new commit.
    leader.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs_new, COMMIT_TIMEOUT_SEC) );

    // The original members should see S3 as a new joiner.
    for (auto m: pkgs_old) {
        CHK_TRUE( m->raftServer->get_srv_config(new_joiner.myId)->is_new_joiner() );
    }

    return 0;
}

int basic_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);

    // Exclude s3 at first.
    std::vector<RaftPkg*> pkgs_old = {&s1, &s2};
    std::vector<RaftPkg*> pkgs_new = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs_new ) );
    CHK_Z( make_group( pkgs_old ) );

    for (auto& entry: pkgs_new) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.use_new_joiner_type_ = true;
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 10;
    CHK_Z( append_logs(NUM, s1, pkgs_old) );
    CHK_Z( add_new_joiner(s1, s3, pkgs_old, pkgs_new) );

    // Send snapshot.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    for (size_t ii = 0; ii < NUM + 5; ++ii) {
        s1.fNet->execReqResp();
    }
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs_new, COMMIT_TIMEOUT_SEC) );
    // After getting response, it will make configuration commit.
    s1.fNet->execReqResp();
    // Notify new commit.
    s1.fNet->execReqResp();

    // Now all of them see S3 as a normal member.
    CHK_FALSE( s1.raftServer->get_srv_config(3)->is_new_joiner() );
    CHK_FALSE( s2.raftServer->get_srv_config(3)->is_new_joiner() );
    CHK_FALSE( s3.raftServer->get_srv_config(3)->is_new_joiner() );

    print_stats(pkgs_new);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int initiate_vote_test() {
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

    // Exclude s3 at first.
    std::vector<RaftPkg*> pkgs_quorum = {&s1, &s2};
    std::vector<RaftPkg*> pkgs_old = {&s1, &s2, &s3};
    std::vector<RaftPkg*> pkgs_new = {&s1, &s2, &s3, &s4};

    CHK_Z( launch_servers( pkgs_new ) );
    CHK_Z( make_group( pkgs_old ) );

    for (auto& entry: pkgs_new) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.use_new_joiner_type_ = true;

        // Avoid snapshot.
        param.reserved_log_items_ = 1000;
        param.snapshot_distance_ = 1000;
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 10;
    CHK_Z( append_logs(NUM, s1, pkgs_old) );
    CHK_Z( add_new_joiner(s1, s4, pkgs_old, pkgs_new) );

    // Append more logs, but replicate to S2 only.
    // It should be able to commit even with 2 servers, as S4 is still new joiner.
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii = 0; ii < NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );
        CHK_TRUE( ret->get_accepted() );
        handlers.push_back(ret);
    }

    s1.fNet->execReqResp(s2_addr);
    s1.fNet->execReqResp(s2_addr);
    CHK_Z( wait_for_sm_exec(pkgs_quorum, COMMIT_TIMEOUT_SEC) );

    s1.fNet->execReqResp(s2_addr);
    s1.fNet->execReqResp(s2_addr);
    CHK_Z( wait_for_sm_exec(pkgs_quorum, COMMIT_TIMEOUT_SEC) );

    // Even with 2 servers, all logs should be committed.
    CHK_EQ( s1.raftServer->get_last_log_idx(),
            s1.raftServer->get_committed_log_idx() );

    // Replicate logs to S4, up to the first config, but not all.
    // As a result, S4 will recognize itself as a new joiner,
    // but still there is a log gap.
    for (auto& entry: pkgs_new) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.max_append_size_ = 1;
        pp->raftServer->update_params(param);
    }

    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    for (size_t ii = 0; ii < NUM + 5; ++ii) {
        s1.fNet->execReqResp();
    }
    CHK_Z( wait_for_sm_exec(pkgs_new, COMMIT_TIMEOUT_SEC) );

    // Initiate election timer for S4, it should do nothing.
    s4.fTimer->invoke( timer_task_type::election_timer );
    for (const auto& endpoint: {s1_addr, s2_addr, s3_addr}) {
        CHK_Z( s4.fNet->getNumPendingReqs(endpoint) );
    }
    CHK_Z( s4.fTimer->getNumPendingTasks() );

    // Initiate election timer for S3, it should start election,
    // but it should not send vote request to S4.
    s3.fTimer->invoke( timer_task_type::election_timer );
    CHK_Z( s3.fNet->getNumPendingReqs(s4_addr) );

    s2.fTimer->invoke( timer_task_type::election_timer );
    CHK_Z( s2.fNet->getNumPendingReqs(s4_addr) );

    // Leader election should succeed by only vote from S2.
    s3.fNet->execReqResp(s2_addr);
    s3.fNet->execReqResp(s2_addr);
    CHK_Z( wait_for_sm_exec(pkgs_new, COMMIT_TIMEOUT_SEC) );

    s3.fNet->execReqResp(s2_addr);
    s3.fNet->execReqResp(s2_addr);
    CHK_Z( wait_for_sm_exec(pkgs_new, COMMIT_TIMEOUT_SEC) );

    CHK_TRUE( s3.raftServer->is_leader() );
    CHK_EQ(3, s2.raftServer->get_leader() );

    print_stats(pkgs_new);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int new_joiner_take_over_test() {
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

    // Exclude s3 at first.
    std::vector<RaftPkg*> pkgs_quorum = {&s1, &s2};
    std::vector<RaftPkg*> pkgs_old = {&s1, &s2, &s3};
    std::vector<RaftPkg*> pkgs_new = {&s1, &s2, &s3, &s4};

    CHK_Z( launch_servers( pkgs_new ) );
    CHK_Z( make_group( pkgs_old ) );

    for (auto& entry: pkgs_new) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.use_new_joiner_type_ = true;
        pp->raftServer->update_params(param);
    }

    // Setting it to 8, as 10 makes the last config index 15.
    const size_t NUM = 8;
    CHK_Z( append_logs(NUM, s1, pkgs_old) );
    CHK_Z( add_new_joiner(s1, s4, pkgs_old, pkgs_new) );

    // Now S3 takes over the leadership.
    s2.fTimer->invoke( timer_task_type::election_timer );
    s3.fTimer->invoke( timer_task_type::election_timer );

    s3.fNet->execReqResp();
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs_new, COMMIT_TIMEOUT_SEC) );

    s3.fNet->execReqResp();
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs_new, COMMIT_TIMEOUT_SEC) );

    // S3 takes over the new joiner and send snapshot.
    s3.fTimer->invoke( timer_task_type::heartbeat_timer );
    for (size_t ii = 0; ii < NUM + 5; ++ii) {
        s3.fNet->execReqResp();
    }
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs_new, COMMIT_TIMEOUT_SEC) );
    // After getting response, it will make configuration commit.
    s3.fNet->execReqResp();
    // Notify new commit.
    s3.fNet->execReqResp();

    // Now all of them see S4 as a normal member.
    for (auto& ss: pkgs_new) {
        TestSuite::setInfo("server id %d", ss->myId);
        CHK_FALSE( ss->raftServer->get_srv_config(4)->is_new_joiner() );
    }

    print_stats(pkgs_new);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int learner_to_normal_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);

    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    // Initialization callback: make S3 as a learner.
    auto init_cb = [&](RaftPkg* pp) {
        if (pp->myId == 3) {
            pp->getTestMgr()->get_srv_config()->set_learner(true);
        }
    };
    CHK_Z( launch_servers( pkgs, nullptr, false, cb_default, init_cb ) );

    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 10;
    CHK_Z( append_logs(NUM, s1, pkgs) );

    // Trigger election timer of S3.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Since it is a learner, it should not send vote request.
    CHK_Z(s3.fNet->getNumPendingReqs(s1_addr));
    CHK_Z(s3.fNet->getNumPendingReqs(s2_addr));

    // Update config and make S3 a normal member.
    s3.dbgLog(" --- update config to make S3 a normal member ---");
    auto enc_config = s3.getTestMgr()->get_srv_config()->serialize();
    ptr<srv_config> new_config = srv_config::deserialize(*enc_config);
    new_config->set_learner(false);
    CHK_TRUE(s1.raftServer->update_srv_config(*new_config));

    // Packet for pre-commit.
    s1.fNet->execReqResp();
    // Packet for commit.
    s1.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // One more time for member that was not in quorum.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Trigger election timer of S3.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    s3.fTimer->invoke( timer_task_type::election_timer );

    // This time it should make progress.
    CHK_GT(s3.fNet->getNumPendingReqs(s1_addr), 0);
    CHK_GT(s3.fNet->getNumPendingReqs(s2_addr), 0);

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

}  // namespace learner_new_joiner_test;
using namespace learner_new_joiner_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    // Disable reconnection timer for deterministic test.
    debugging_options::get_instance().disable_reconn_backoff_ = true;

    ts.doTest( "basic test",
               basic_test );

    ts.doTest( "initiate vote test",
               initiate_vote_test );

    ts.doTest( "new joiner take over test",
               new_joiner_take_over_test );

    ts.doTest( "learner to normal test",
               learner_to_normal_test );

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

