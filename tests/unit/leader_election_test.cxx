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
#include "fake_executer.hxx"

#include "event_awaiter.hxx"
#include "raft_params.hxx"
#include "test_common.h"

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

using raft_result = cmd_result< ptr<buffer> >;

namespace leader_election_test {

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

    // Keep the last log index.
    uint64_t last_idx = s1.raftServer->get_last_log_idx();

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

    // S3's log index at becoming leader should be the next index of
    // the last one.
    TestSuite::Msg mm;
    uint64_t idx_at_leader = s3.raftServer->get_log_idx_at_becoming_leader();
    mm << "last index " << last_idx << ", log index at becoming leader "
       << idx_at_leader << std::endl;
    CHK_EQ( last_idx + 1, idx_at_leader );

    // S1 and S2's log index should be 0.
    CHK_Z( s1.raftServer->get_log_idx_at_becoming_leader() );
    CHK_Z( s2.raftServer->get_log_idx_at_becoming_leader() );

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
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 100) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 85.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 85) );
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
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(1, 100) );
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S2 to 50.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 50) );
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 1.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 1) );
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
int leader_election_with_catching_up_server_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2};

    CHK_Z( launch_servers( pkgs ) );

    // Set priority of S1 to 80 and S2 to 100.
    // Each server thinks it's a leader
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(1, 80) );
    CHK_EQ( raft_server::PrioritySetResult::SET, s2.raftServer->set_priority(2, 100) );

    // Append logs to S1 to trigger log compaction.
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.log_sync_stop_gap_ = 0;
        pp->raftServer->update_params(param);
    }
    const size_t NUM = 10;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );

        handlers.push_back(ret);
    }
    // Pre-commit and commit.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // All handlers should be OK.
    for (auto& entry: handlers) {
        CHK_TRUE( entry->has_result() );
        CHK_EQ( cmd_result_code::OK, entry->get_result_code() );
    }

    // Add S2 to S1.
    s1.raftServer->add_srv( *s2.getTestMgr()->get_srv_config() );

    // Join req/resp.
    s1.fNet->execReqResp();
    // Add new server, notify existing peers.
    // After getting response, it will make configuration commit.
    s1.fNet->execReqResp();
    // Notify new commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now S2 is the member of the cluster.

    // S1 resigns immediately (to schedule election timer).
    s1.raftServer->yield_leadership(true);
    CHK_FALSE( s1.raftServer->is_leader() );

    // Invoke election timer of S1.
    s1.dbgLog(" --- invoke election timer of S1 ---");
    s1.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote request, S2 should accept it as it is in catch-up mode.
    s1.fNet->execReqResp();
    // Send vote request, S2 should accept it as it is in catch-up mode.
    s1.fNet->execReqResp();

    // Leader election should succeed.
    CHK_TRUE( s1.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();

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
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 10) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 5) );
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
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 10) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 5) );
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
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 10) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 5) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Request leadership by the current leader, should fail.
    CHK_FALSE( s1.raftServer->request_leadership() );

    // Set callback function to refuse resignation.
    bool refuse_request = true;
    s1.ctx->set_cb_func([&](cb_func::Type t, cb_func::Param* p) -> cb_func::ReturnCode {
        if (t != cb_func::Type::ResignationFromLeader) {
            return cb_default(t, p);
        }
        if (refuse_request) {
            return cb_func::ReturnCode::ReturnNull;
        }
        return cb_func::ReturnCode::Ok;
    });

    // S3 requests the leadership from S1, and it is supposed to be declined.
    s1.dbgLog(" --- request leadership ---");
    CHK_TRUE( s3.raftServer->request_leadership() );
    // Send request.
    s3.fNet->execReqResp();

    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();

    // S1 should still be the leader.
    CHK_TRUE( s1.raftServer->is_leader() );

    // S3 requests the leadership from S1. Now it should succeed.
    refuse_request = false;
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
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 10) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 5) );
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
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 0) );
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

        handlers.push_back(ret);
    }

    // Replicate.
    for (size_t ii=0; ii<3; ++ii) s1.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // All handlers should be OK.
    for (auto& entry: handlers) {
        CHK_TRUE( entry->has_result() );
        CHK_EQ( cmd_result_code::OK, entry->get_result_code() );
    }

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

}  // namespace leader_election_test;
using namespace leader_election_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    // Disable reconnection timer for deterministic test.
    debugging_options::get_instance().disable_reconn_backoff_ = true;

    ts.doTest( "leader election basic test",
               leader_election_basic_test );

    ts.doTest( "leader election priority test",
               leader_election_priority_test );

    ts.doTest( "leader election with aggressive node test",
               leader_election_with_aggressive_node_test );

    ts.doTest( "leader election with catching-up server test",
               leader_election_with_catching_up_server_test );

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

