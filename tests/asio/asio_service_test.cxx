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

#include "buffer_serializer.hxx"
#include "debugging_options.hxx"
#include "in_memory_log_store.hxx"
#include "raft_package_asio.hxx"
#include "asio_test_common.hxx"

#include "event_awaiter.hxx"
#include "test_common.h"

#ifdef USE_BOOST_ASIO
    #include <boost/asio.hpp>
    using namespace boost;
    using asio_error_code = system::error_code;
#else
    #include <asio.hpp>
    using asio_error_code = asio::error_code;
#endif

#include <unordered_map>

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

static bool flag_bg_snapshot_io = false;

namespace asio_service_test {

int make_group_test() {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());

    // Sleep a while and check peer info.
    for (auto& entry: {s2, s3}) {
        // Non leader should not accept this API.
        raft_server::peer_info pi = entry.raftServer->get_peer_info(1);
        CHK_EQ(-1, pi.id_);

        std::vector<raft_server::peer_info> v_pi = entry.raftServer->get_peer_info_all();
        CHK_Z(v_pi.size());
    }

    for (auto srv_id: {2, 3}) {
        raft_server::peer_info pi = s1.raftServer->get_peer_info(srv_id);
        uint64_t last_log_idx = s1.raftServer->get_last_log_idx();
        CHK_EQ(srv_id, pi.id_);
        CHK_EQ(last_log_idx, pi.last_log_idx_);
        TestSuite::Msg mm;
        mm << "srv " << pi.id_ << ": " << pi.last_log_idx_ << ", responded " << std::fixed
           << std::setprecision(1) << pi.last_succ_resp_us_ / 1000.0 << " ms ago"
           << std::endl;
    }

    // Sleep a while and get all info.
    TestSuite::sleep_ms(10);

    std::vector<raft_server::peer_info> v_pi = s1.raftServer->get_peer_info_all();
    CHK_GT(v_pi.size(), 0);
    for (raft_server::peer_info& pi: v_pi) {
        uint64_t last_log_idx = s1.raftServer->get_last_log_idx();
        CHK_EQ(last_log_idx, pi.last_log_idx_);
        TestSuite::Msg mm;
        mm << "srv " << pi.id_ << ": " << pi.last_log_idx_ << ", responded " << std::fixed
           << std::setprecision(1) << pi.last_succ_resp_us_ / 1000.0 << " ms ago"
           << std::endl;
    }

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int become_follower_test() {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");

    std::set<int> got_become_follower;
    raft_server::init_options i_opt;
    i_opt.raft_callback_ = [&](cb_func::Type type, cb_func::Param* param)
        -> cb_func::ReturnCode {
        if (type == cb_func::BecomeFollower) {
            got_become_follower.insert(param->myId);
        }
        return cb_func::ReturnCode::Ok;
    };
    CHK_Z( launch_servers(pkgs, false, false, true, i_opt) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());

    std::unordered_set<int> expected_followers = {2, 3};
    for (auto& entry: got_become_follower) {
        CHK_TRUE( expected_followers.find(entry) != expected_followers.end() );
        _msg("server %d got become_follower callback\n", entry);
    }

    // Now update leader to use `new_joiner` option.
    for (auto& entry: pkgs) {
        raft_params param = entry->raftServer->get_current_params();
        param.use_new_joiner_type_ = true;
        entry->raftServer->update_params(param);
    }

    // Launch S4 and add it to S1.
    std::string s4_addr = "localhost:20040";
    RaftAsioPkg s4(4, s4_addr);
    pkgs.push_back(&s4);
    CHK_Z( launch_servers({&s4}, false, false, true, i_opt) );

    s1.raftServer->add_srv( *(s4.getTestMgr()->get_srv_config()) );
    // Wait longer than upper timeout.
    TestSuite::sleep_sec(1);

    // S4 should be a follower.
    expected_followers.insert(4);
    for (auto& entry: got_become_follower) {
        CHK_TRUE( expected_followers.find(entry) != expected_followers.end() );
        _msg("server %d got become_follower callback\n", entry);
    }

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    s4.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int leader_election_test(bool crc_on_entire_message) {
    reset_log_files();

    std::string s1_addr = "tcp://localhost:20010";
    std::string s2_addr = "tcp://localhost:20020";
    std::string s3_addr = "tcp://localhost:20030";

    RaftAsioPkg* s1 = new RaftAsioPkg(1, s1_addr);
    RaftAsioPkg* s2 = new RaftAsioPkg(2, s2_addr);
    RaftAsioPkg* s3 = new RaftAsioPkg(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {s1, s2, s3};
    for (auto& pp: pkgs) {
        pp->setCrcOnEntireMessage(crc_on_entire_message);
    }

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1->raftServer->is_leader() );
    CHK_EQ(1, s1->raftServer->get_leader());
    CHK_EQ(1, s2->raftServer->get_leader());
    CHK_EQ(1, s3->raftServer->get_leader());

    s1->raftServer->shutdown();
    s1->stopAsio();
    delete s1;
    TestSuite::sleep_sec(2, "leader election is happening");

    s1 = new RaftAsioPkg(1, s1_addr);
    s1->initServer();
    TestSuite::sleep_sec(1, "restart previous leader");

    // Leader should be 2 or 3.
    int cur_leader = s2->raftServer->get_leader();
    _msg("new leader id: %d\n", cur_leader);

    CHK_EQ(cur_leader, s1->raftServer->get_leader());
    CHK_EQ(cur_leader, s3->raftServer->get_leader());
    CHK_FALSE( s1->raftServer->is_leader() );

    // Now manually yield leadership.
    RaftAsioPkg* leader_pkg = pkgs[cur_leader - 1];
    leader_pkg->raftServer->yield_leadership();
    TestSuite::sleep_sec(2, "yield leadership, "
                            "leader election is happening again");

    // New leader should have been elected.
    cur_leader = s1->raftServer->get_leader();
    _msg("new leader id: %d\n", cur_leader);

    CHK_EQ(cur_leader, s1->raftServer->get_leader());
    CHK_EQ(cur_leader, s2->raftServer->get_leader());
    CHK_EQ(cur_leader, s3->raftServer->get_leader());

    s1->raftServer->shutdown();
    s2->raftServer->shutdown();
    s3->raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    s1->stopAsio();
    s2->stopAsio();
    s3->stopAsio();
    delete s1;
    delete s2;
    delete s3;

    SimpleLogger::shutdown();
    return 0;
}

int ssl_test() {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers with SSL\n");
    CHK_Z( launch_servers(pkgs, true) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    s1.stopAsio();
    s2.stopAsio();
    s3.stopAsio();

    SimpleLogger::shutdown();
    return 0;
}

int async_append_handler_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    const size_t NUM = 10;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    std::list<ulong> idx_list;
    std::mutex idx_list_lock;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        cmd_result< ptr<buffer> >::handler_type my_handler =
            std::bind( async_handler,
                       &idx_list,
                       &idx_list_lock,
                       std::placeholders::_1,
                       std::placeholders::_2 );
        ret->when_ready( my_handler );

        handlers.push_back(ret);
    }
    TestSuite::sleep_sec(1, "replication");

    // Now all async handlers should have result.
    {
        std::lock_guard<std::mutex> l(idx_list_lock);
        CHK_EQ(NUM, idx_list.size());
    }

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int async_append_handler_with_order_inversion_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Set debugging parameter to inject sleep so as to mimic the thread
    // execution order inversion.
    debugging_options::get_instance().handle_cli_req_sleep_us_ =
        RaftAsioPkg::HEARTBEAT_MS * 1500;

    TestSuite::GcFunc gcf([](){ // Auto rollback.
        debugging_options::get_instance().handle_cli_req_sleep_us_ = 0;
    });

    std::atomic<bool> handler_invoked(false);
    {
        std::string test_msg = "test" + std::to_string(1234);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries({msg});
        ret->when_ready( [&handler_invoked]
                         ( cmd_result< ptr<buffer> >& result,
                           ptr<std::exception>& err ) -> int {
            CHK_NONNULL(result.get());
            handler_invoked = true;
            return 0;
        });
        CHK_TRUE(ret->get_accepted());
    }
    TestSuite::sleep_sec(1, "wait for handler");

    // The handler should have been invoked.
    CHK_TRUE(handler_invoked);

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int global_mgr_basic_test() {
    reset_log_files();

    nuraft_global_mgr::init();

    std::string s1_addr = "127.0.0.1:20010";
    std::string s2_addr = "127.0.0.1:20020";
    std::string s3_addr = "127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers(pkgs, false, true) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    const size_t NUM_OP = 500;
    TestSuite::Progress prog(NUM_OP, "append op");
    for (size_t ii=0; ii<NUM_OP; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
        // To utilize thread pool, have enough break time
        // between each `append_entries`. If we don't have this,
        // append_entries's response handler will trigger the
        // next request, not by the global thread pool.
        TestSuite::sleep_ms(10);
        prog.update(ii);
    }
    prog.done();
    TestSuite::sleep_sec(1, "wait for replication");

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    nuraft_global_mgr::shutdown();
    return 0;
}

int global_mgr_heavy_test() {
    reset_log_files();

    nuraft_global_config g_config;
    g_config.num_commit_threads_ = 2;
    g_config.num_append_threads_ = 2;
    nuraft_global_mgr::init(g_config);
    const size_t NUM_SERVERS = 50;

    std::vector<RaftAsioPkg*> pkgs;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        std::string addr = "127.0.0.1:" + std::to_string(20000 + (ii+1) * 10);
        RaftAsioPkg* pkg = new RaftAsioPkg(ii+1, addr);
        pkgs.push_back(pkg);
    }

    CHK_Z( launch_servers(pkgs, false, true) );
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    // Set async.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    for (size_t ii=0; ii<500; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);

        for (auto& entry: pkgs) {
            RaftAsioPkg* pkg = entry;
            pkg->raftServer->append_entries( {msg} );
        }
    }
    TestSuite::sleep_sec(1, "wait for replication");

    for (auto& entry: pkgs) {
        RaftAsioPkg* pkg = entry;
        pkg->raftServer->shutdown();
        delete pkg;
    }
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    nuraft_global_mgr::shutdown();
    return 0;
}

int leadership_transfer_test() {
    reset_log_files();

    std::string s1_addr = "tcp://localhost:20010";
    std::string s2_addr = "tcp://localhost:20020";
    std::string s3_addr = "tcp://localhost:20030";

    RaftAsioPkg* s1 = new RaftAsioPkg(1, s1_addr);
    RaftAsioPkg* s2 = new RaftAsioPkg(2, s2_addr);
    RaftAsioPkg* s3 = new RaftAsioPkg(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {s1, s2, s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1->raftServer->is_leader() );
    CHK_EQ(1, s1->raftServer->get_leader());
    CHK_EQ(1, s2->raftServer->get_leader());
    CHK_EQ(1, s3->raftServer->get_leader());

    // Set the priority of S2 to 10.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1->raftServer->set_priority(2, 10) );
    TestSuite::sleep_ms(500, "set priority of S2");

    // Set the priority of S3 to 5.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1->raftServer->set_priority(3, 5) );
    TestSuite::sleep_ms(500, "set priority of S3");

    // Yield the leadership to S2.
    s1->raftServer->yield_leadership(false, 2);
    TestSuite::sleep_sec(1, "yield leadership to S2");

    // Now S2 should be the leader.
    CHK_TRUE( s2->raftServer->is_leader() );
    CHK_EQ(2, s1->raftServer->get_leader());
    CHK_EQ(2, s2->raftServer->get_leader());
    CHK_EQ(2, s3->raftServer->get_leader());

    // Leadership transfer shouldn't happen.
    TestSuite::sleep_sec(1, "wait more");
    CHK_TRUE( s2->raftServer->is_leader() );

    // Now set the parameter to enable transfer.
    raft_params params = s2->raftServer->get_current_params();
    params.leadership_transfer_min_wait_time_ = 1000;
    s2->raftServer->update_params(params);

    // S1 should be the leader now.
    TestSuite::sleep_sec(1, "enable transfer and wait");
    CHK_TRUE( s1->raftServer->is_leader() );
    CHK_EQ(1, s1->raftServer->get_leader());
    CHK_EQ(1, s2->raftServer->get_leader());
    CHK_EQ(1, s3->raftServer->get_leader());

    // Shutdown S3
    s3->raftServer->shutdown();
    s3->stopAsio();
    delete s3;

    // Wait enough time so that S1 can detect S3's failure.
    TestSuite::sleep_sec(2, "shutdown S3 and wait");

    // Set the parameter to enable transfer (S1).
    s1->raftServer->update_params(params);

    // Set S2's priority higher than S1
    CHK_EQ( raft_server::PrioritySetResult::SET, s1->raftServer->set_priority(2, 100) );

    // Due to S3, transfer shouldn't happen.
    TestSuite::sleep_sec(2, "set priority of S2 and wait");
    CHK_TRUE( s1->raftServer->is_leader() );

    s3 = new RaftAsioPkg(3, s3_addr);
    s3->initServer();
    TestSuite::sleep_sec(2, "restart S3");

    // Now leader trasnfer should happen.
    CHK_TRUE( s2->raftServer->is_leader() );
    CHK_EQ(2, s1->raftServer->get_leader());
    CHK_EQ(2, s2->raftServer->get_leader());
    CHK_EQ(2, s3->raftServer->get_leader());

    s1->raftServer->shutdown();
    s2->raftServer->shutdown();
    s3->raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    s1->stopAsio();
    s2->stopAsio();
    s3->stopAsio();
    delete s1;
    delete s2;
    delete s3;

    SimpleLogger::shutdown();
    return 0;
}

int auto_forwarding_timeout_test() {
    std::string s1_addr = "127.0.0.1:20010";
    std::string s2_addr = "127.0.0.1:20020";
    std::string s3_addr = "127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    raft_server::init_options opt;

    /// Make leader quite slow
    opt.raft_callback_ = [](cb_func::Type type, cb_func::Param* param)
                         -> cb_func::ReturnCode {
        if (type == cb_func::Type::AppendLogs) {
            TestSuite::sleep_ms(150);
        }
        return cb_func::ReturnCode::Ok;
    };

    CHK_Z( launch_servers(pkgs, false, false, true, opt) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());

    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.auto_forwarding_ = true;
        pp->raftServer->update_params(param);
    }

    std::string test_msg = "test";
    ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
    msg->put(test_msg);

    // Forwarded as expected
    auto ret1 = s3.raftServer->append_entries({msg});
    CHK_TRUE(ret1->get_accepted());
    CHK_EQ(nuraft::cmd_result_code::OK, ret1->get_result_code());

    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.auto_forwarding_req_timeout_ = 100;
        pp->raftServer->update_params(param);
    }

    auto ret2 = s3.raftServer->append_entries({msg});

    // Timeout happened
    CHK_FALSE(ret2->get_accepted());

    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.auto_forwarding_req_timeout_ = 0;
        pp->raftServer->update_params(param);
    }

    // Work again
    auto ret3 = s3.raftServer->append_entries({msg});
    CHK_TRUE(ret3->get_accepted());
    CHK_EQ(nuraft::cmd_result_code::OK, ret3->get_result_code());

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();

    return 0;
}

int auto_forwarding_test(bool async) {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.auto_forwarding_ = true;
        param.auto_forwarding_max_connections_ = 2;
        if (async) {
            param.return_method_ = raft_params::async_handler;
        }
        pp->raftServer->update_params(param);
    }

    // Append messages in parallel into S2 (follower).
    struct MsgArgs : TestSuite::ThreadArgs {
        size_t ii;
    };

    std::mutex handlers_lock;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    auto send_msg = [&](TestSuite::ThreadArgs* t_args) -> int {
        MsgArgs* args = (MsgArgs*)t_args;
        std::string test_msg = "test" + std::to_string(args->ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s2.raftServer->append_entries( {msg} );

        std::lock_guard<std::mutex> l(handlers_lock);
        handlers.push_back(ret);
        return 0;
    };

    const size_t NUM_PARALLEL_MSGS = 20;
    std::vector<TestSuite::ThreadHolder> th(NUM_PARALLEL_MSGS);
    std::vector<MsgArgs> m_args(NUM_PARALLEL_MSGS);
    for (size_t ii = 0; ii < NUM_PARALLEL_MSGS; ++ii) {
        m_args[ii].ii = ii;
        th[ii].spawn(&m_args[ii], send_msg, nullptr);
    }
    TestSuite::sleep_sec(1, "replication");
    for (size_t ii = 0; ii < NUM_PARALLEL_MSGS; ++ii) {
        th[ii].join();
        CHK_Z(th[ii].getResult());
    }

    // All messages should have been committed in the state machine.
    for (size_t ii = 0; ii < NUM_PARALLEL_MSGS; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        CHK_GT(s1.getTestSm()->isCommitted(test_msg), 0);
    }

    // All handlers should have the result.
    {
        std::set<uint64_t> commit_results;
        std::lock_guard<std::mutex> l(handlers_lock);
        for (auto& handler: handlers) {
            ptr<buffer> h_result = handler->get();
            CHK_NONNULL(h_result);
            CHK_EQ(8, h_result->size());
            buffer_serializer bs(h_result);
            uint64_t val = bs.get_u64();
            commit_results.insert(val);
        }
        // All messages should have delivered their results.
        CHK_EQ(NUM_PARALLEL_MSGS, commit_results.size());
    }

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int enforced_state_machine_catchup_test() {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    for (size_t ii=0; ii<100; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");

    // Adjust the priority of S2 to zero, to block it becoming a leader.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 0) );

    TestSuite::sleep_sec(1, "set S2's priority to zero");

    // Stop S3, delete data.
    uint64_t last_committed_idx = s3.raftServer->get_committed_log_idx();
    s3.raftServer->shutdown();
    s3.stopAsio();
    s3.getTestSm()->truncateData(last_committed_idx - 5);

    // Stop S1.
    s1.raftServer->shutdown();
    s1.stopAsio();
    TestSuite::sleep_sec(1, "stop S1 and S3");

    // Restart S3 with grace period option.
    raft_params new_params = s1.raftServer->get_current_params();
    new_params.grace_period_of_lagging_state_machine_ = 1000; // 1 second.
    s3.restartServer(&new_params);
    TestSuite::sleep_ms(500, "restarting S3");

    // Before the grace period, there should be no leader.
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    // After the grace period, S3 should be the leader.
    TestSuite::sleep_sec(1, "grace period");
    CHK_TRUE( s3.raftServer->is_leader() );
    CHK_EQ(3, s2.raftServer->get_leader());

    // Stop both S2 and S3 and then restart them.
    s2.raftServer->shutdown();
    s2.stopAsio();
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_sec(1, "stop S2 and S3");

    s2.restartServer();
    s3.restartServer(&new_params);
    TestSuite::sleep_ms(500, "restarting S2 and S3");

    // Even before the grace period, S3 should be the leader.
    CHK_TRUE( s3.raftServer->is_leader() );
    CHK_EQ(3, s2.raftServer->get_leader());

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int enforced_state_machine_catchup_with_term_inc_test() {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    for (size_t ii=0; ii<100; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");

    // Adjust the priority of S2 to zero, to block it becoming a leader.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 0) );
    TestSuite::sleep_sec(1, "set S2's priority to zero");

    // Stop S3, delete data.
    uint64_t last_committed_idx = s3.raftServer->get_committed_log_idx();
    s3.raftServer->shutdown();
    s3.stopAsio();
    s3.getTestSm()->truncateData(last_committed_idx - 5);
    TestSuite::sleep_ms(500, "stop S3");

    // A few leader changes to increase the term.
    s1.raftServer->yield_leadership(false, 2);
    TestSuite::sleep_sec(1, "leader change: S1 -> S2");
    s2.raftServer->yield_leadership(false, 1);
    TestSuite::sleep_sec(1, "leader change: S2 -> S1");

    // Stop S1.
    s1.raftServer->shutdown();
    s1.stopAsio();
    TestSuite::sleep_sec(1, "stop S1");

    // Restart S3 with grace period option.
    raft_params new_params = s1.raftServer->get_current_params();
    new_params.grace_period_of_lagging_state_machine_ = 1000; // 1 second.
    s3.restartServer(&new_params);
    TestSuite::sleep_ms(500, "restarting S3");

    // Before the grace period, there should be no leader.
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    // Even after the grace period, S3 can't be the leader due to term.
    TestSuite::sleep_ms(1500, "grace period");
    CHK_FALSE( s3.raftServer->is_leader() );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

void wait_for_catch_up( const RaftAsioPkg& ll,
                        const RaftAsioPkg& rr,
                        size_t count_limit = 3 )
{
    for (size_t ii = 0; ii < count_limit; ++ii) {
        uint64_t l_idx = ll.raftServer->get_committed_log_idx();
        uint64_t r_idx = rr.raftServer->get_committed_log_idx();
        if (l_idx == r_idx) {
            break;
        }
        std::stringstream ss;
        ss << "waiting for catch-up: " << l_idx << " vs. " << r_idx;
        TestSuite::sleep_sec(1, ss.str());
    }
}

int try_adding_server( RaftAsioPkg& leader,
                       const RaftAsioPkg& srv_to_add,
                       size_t count_limit = 3 )
{
    for (size_t ii = 0; ii < count_limit; ++ii) {
        ptr<srv_config> s_conf = srv_to_add.getTestMgr()->get_srv_config();
        ptr< cmd_result< ptr<buffer> > > ret = leader.raftServer->add_srv(*s_conf);

        std::string ret_string = "adding S" + std::to_string(s_conf->get_id());
        bool succeeded = false;

        if (ret->get_result_code() == cmd_result_code::OK) {
            succeeded = true;
        } else {
            ret_string += " failed: " + std::to_string(ret->get_result_code());
        }
        TestSuite::sleep_sec(1, ret_string);
        if (succeeded) {
            return 0;
        }
    }
    return -1;
}

int snapshot_read_failure_during_join_test(size_t log_sync_gap) {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false, false, flag_bg_snapshot_io) );

    _msg("organizing raft group\n");
    CHK_Z( make_group({&s1, &s2}) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    for (size_t ii=0; ii<100; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");

    raft_params params = s1.raftServer->get_current_params();
    params.log_sync_stop_gap_ = log_sync_gap;
    s1.raftServer->update_params(params);

    // Make the first two snapshot reads fail.
    s1.getTestSm()->setSnpReadFailure(2);

    // Add S3.
    CHK_Z( try_adding_server(s1, s3) );

    // Wait until S3 completes catch-up.
    wait_for_catch_up(s1, s3);

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );

    // FIXME:
    //   Disable this line due to intermittent failure on code coverage mode.
    //CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );
    if (!s3.getTestSm()->isSame(*s1.getTestSm())) {
        // Print log for debugging.
        std::ifstream fs;
        fs.open("srv3.log");
        if (fs.good()) {
            std::stringstream ss;
            ss << fs.rdbuf();
            fs.close();
            std::cout << ss.str();
        }
    }

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int snapshot_read_failure_for_lagging_server_test(size_t num_failures) {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false, false, flag_bg_snapshot_io) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    // Stop S3.
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_sec(1, "stop S3");

    // Replication.
    for (size_t ii=0; ii<100; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");

    // Make the snapshot read fail.
    s1.getTestSm()->setSnpReadFailure(num_failures);

    // Restart S3.
    s3.restartServer();
    TestSuite::sleep_sec(1, "restarting S3");

    // Wait until S3 completes catch-up.
    wait_for_catch_up(s1, s3);

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int snapshot_context_timeout_normal_test() {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false, false, flag_bg_snapshot_io) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    // Stop S3.
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_sec(1, "stop S3");

    // Replication.
    for (size_t ii=0; ii<100; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");

    // Set snapshot delay for S3 and restart.
    s3.getTestSm()->setSnpDelay(100);
    s3.restartServer();
    TestSuite::sleep_sec(1, "restarting S3");

    // User snapshot ctx should exist.
    CHK_EQ(1, s1.getTestSm()->getNumOpenedUserCtxs());

    // Stop S3 again, and wait.
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 25, "stop S3");

    // User snapshot ctx should be empty.
    CHK_Z(s1.getTestSm()->getNumOpenedUserCtxs());

    // Clear snapshot delay for S3 and restart.
    s3.getTestSm()->setSnpDelay(0);
    s3.restartServer();
    TestSuite::sleep_sec(1, "restarting S3");

    // Wait until S3 completes catch-up.
    wait_for_catch_up(s1, s3);

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int snapshot_context_timeout_join_test() {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false, false, flag_bg_snapshot_io) );

    _msg("organizing raft group\n");
    CHK_Z( make_group( {&s1, &s2} ) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    // Replication.
    for (size_t ii=0; ii<100; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");

    raft_params params = s1.raftServer->get_current_params();
    params.log_sync_stop_gap_ = 10;
    s1.raftServer->update_params(params);

    // Set snapshot delay for S3 and add it to the group.
    s3.getTestSm()->setSnpDelay(100);
    CHK_Z( try_adding_server(s1, s3) );

    // User snapshot ctx should exist.
    CHK_EQ(1, s1.getTestSm()->getNumOpenedUserCtxs());

    // Stop S3, and wait.
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 25, "stop S3");

    // User snapshot ctx should be empty.
    // FIXME:
    //   Asio connection is not immediately closed under the code coverage mode,
    //   hence the disconnection event is not correctly fired and snapshot
    //   timeout checking code is not invoked in time.
    //
    //   Disabling the below code until it is addressed.
    //CHK_Z(s1.getTestSm()->getNumOpenedUserCtxs());

    // Clear snapshot delay for S3 and restart.
    s3.getTestSm()->setSnpDelay(0);
    s3.restartServer();
    TestSuite::sleep_sec(1, "restarting S3");
    TestSuite::sleep_sec(2, "wait for previous adding server to be expired");

    // Re-attempt adding S3.
    CHK_Z( try_adding_server(s1, s3) );

    // Wait until S3 completes catch-up.
    wait_for_catch_up(s1, s3);

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );

    // FIXME:
    //   Disable this line due to intermittent failure on code coverage mode.
    //CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );
    if (!s3.getTestSm()->isSame(*s1.getTestSm())) {
        // Print log for debugging.
        std::ifstream fs;
        fs.open("srv3.log");
        if (fs.good()) {
            std::stringstream ss;
            ss << fs.rdbuf();
            fs.close();
            std::cout << ss.str();
        }
    }

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int snapshot_context_timeout_removed_server_test() {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false, false, flag_bg_snapshot_io) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    // Stop S3.
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_sec(1, "stop S3");

    // Replication.
    for (size_t ii=0; ii<100; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");

    // Set snapshot delay for S3 and restart.
    s3.getTestSm()->setSnpDelay(100);
    s3.restartServer();
    TestSuite::sleep_sec(1, "restarting S3");

    // User snapshot ctx should exist.
    CHK_EQ(1, s1.getTestSm()->getNumOpenedUserCtxs());

    // Now remove S3 from the group while it is still receiving snapshot.
    s1.raftServer->remove_srv(3);
    TestSuite::sleep_sec(1, "removing S3");

    // S3 shouldn't exist in the group.
    CHK_NULL( s1.raftServer->get_srv_config(3).get() );

    // User snapshot ctx should be empty.
    CHK_Z(s1.getTestSm()->getNumOpenedUserCtxs());

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int pause_state_machine_execution_test(bool use_global_mgr) {
    reset_log_files();

    if (use_global_mgr) {
        nuraft_global_mgr::init();
    }

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    bool block_commit_thread = false;
    EventAwaiter ea_commit_thread;
    auto raft_cb_func = [&](cb_func::Type type, cb_func::Param* param) {
        if (!block_commit_thread || param->myId != 3) {
            return cb_func::ReturnCode::Ok;
        }
        if (type == cb_func::Type::StateMachineExecution) {
            std::thread pause_thread([&]() {
                s3.raftServer->pause_state_machine_execution(1);
                _msg("state machine of S3 is paused\n");
                ea_commit_thread.invoke();
            });
            _msg("got callback, pause state machine of S3\n");
            pause_thread.detach();
            block_commit_thread = false;
            ea_commit_thread.wait();
        }
        return cb_func::ReturnCode::Ok;
    };
    raft_server::init_options opt;
    opt.raft_callback_ = raft_cb_func;

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false, false, true, opt) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    const size_t NUM = 10;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    std::list<ulong> idx_list;
    std::mutex idx_list_lock;
    auto do_async_append = [&]() {
        handlers.clear();
        idx_list.clear();
        for (size_t ii=0; ii<NUM; ++ii) {
            std::string test_msg = "test" + std::to_string(ii);
            ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
            msg->put(test_msg);
            ptr< cmd_result< ptr<buffer> > > ret =
                s1.raftServer->append_entries( {msg} );

            cmd_result< ptr<buffer> >::handler_type my_handler =
                std::bind( async_handler,
                        &idx_list,
                        &idx_list_lock,
                        std::placeholders::_1,
                        std::placeholders::_2 );
            ret->when_ready( my_handler );

            handlers.push_back(ret);
        }
    };

    // Pause S3's state machine.
    s3.raftServer->pause_state_machine_execution(1000);
    CHK_TRUE( s3.raftServer->is_state_machine_execution_paused() );

    do_async_append();

    // Now all async handlers should have result.
    TestSuite::sleep_sec(1, "replication");
    {
        std::lock_guard<std::mutex> l(idx_list_lock);
        CHK_EQ(NUM, idx_list.size());
    }

    // The state machines of S1 and S2 should be identical, but not S3.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_FALSE( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // Resume the state machine.
    s3.raftServer->resume_state_machine_execution();
    TestSuite::sleep_sec(1, "resuming state machine execution");

    // Now it should have the same data.
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // Pause again.
    s3.raftServer->pause_state_machine_execution(1000);

    // Do append again.
    do_async_append();
    TestSuite::sleep_sec(1, "replication");
    {
        std::lock_guard<std::mutex> l(idx_list_lock);
        CHK_EQ(NUM, idx_list.size());
    }

    // S2 should have the same data, but not S3.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_FALSE( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // Restart S3.
    // Even with paused state machine, shutdown should work.
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_sec(1, "stop S1");

    // (Pause flag will be reset upon restart.)
    s3.restartServer(nullptr, false, false, opt);
    TestSuite::sleep_sec(1, "restarting S3");

    // It should have the same data.
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // Block the commit thread of S3.
    block_commit_thread = true;

    // Do append again.
    do_async_append();
    TestSuite::sleep_sec(1, "replication");
    {
        std::lock_guard<std::mutex> l(idx_list_lock);
        CHK_EQ(NUM, idx_list.size());
    }

    // S3 will be paused while it SM is lagging behind.

    // S2 should have the same data, but not S3.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_FALSE( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // Even with lagging state machine, shutdown should work.
    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    if (use_global_mgr) {
        nuraft_global_mgr::shutdown();
    }
    return 0;
}

int flip_learner_flag_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );
    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );
    // Set async.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }
    // Set to learner.
    ptr<cmd_result<ptr<buffer>>> result = s1.raftServer->flip_learner_flag(s3.myId, true);
    CHK_EQ(cmd_result_code::OK, result->get_result_code());
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 5, "wait for replication");
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        ptr<cluster_config> conf = pp->raftServer->get_config();
        CHK_TRUE(conf->get_server(s3.myId)->is_learner());
    }
    // Clear leaner.
    result = s1.raftServer->flip_learner_flag(s3.myId, false);
    CHK_EQ(cmd_result_code::OK, result->get_result_code());
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 5, "wait for replication");
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        ptr<cluster_config> conf = pp->raftServer->get_config();
        CHK_FALSE(conf->get_server(s3.myId)->is_learner());
    }

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int parallel_log_append_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set disk delay (2s for S1, 10ms for S2 and S3).
    s1.getTestMgr()->set_disk_delay(s1.raftServer.get(), 2000);
    s2.getTestMgr()->set_disk_delay(s2.raftServer.get(), 10);
    s3.getTestMgr()->set_disk_delay(s3.raftServer.get(), 10);

    // Set async mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.parallel_log_appending_ = true;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    const size_t NUM = 10;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    std::list<ulong> idx_list;
    std::mutex idx_list_lock;
    auto do_async_append = [&]() {
        handlers.clear();
        idx_list.clear();
        for (size_t ii=0; ii<NUM; ++ii) {
            std::string test_msg = "test" + std::to_string(ii);
            ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
            msg->put(test_msg);
            ptr< cmd_result< ptr<buffer> > > ret =
                s1.raftServer->append_entries( {msg} );

            cmd_result< ptr<buffer> >::handler_type my_handler =
                std::bind( async_handler,
                           &idx_list,
                           &idx_list_lock,
                           std::placeholders::_1,
                           std::placeholders::_2 );
            ret->when_ready( my_handler );

            handlers.push_back(ret);
        }
    };
    do_async_append();

    TestSuite::sleep_sec(1, "wait for replication");

    // Still durable index is smaller than the last index.
    CHK_SM( s1.getTestMgr()->load_log_store()->last_durable_index(),
            s1.getTestMgr()->load_log_store()->next_slot() - 1 );

    // All servers should have the same log index.
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s2.getTestMgr()->load_log_store()->next_slot() - 1 );
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s3.getTestMgr()->load_log_store()->next_slot() - 1 );

    // Even with disk delay, logs should have been committed by S2 and S3.
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    TestSuite::sleep_ms(1500, "wait for disk delay");
    CHK_EQ( s1.getTestMgr()->load_log_store()->last_durable_index(),
            s1.getTestMgr()->load_log_store()->next_slot() - 1 );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int custom_resolver_test() {
    reset_log_files();

    std::string s1_addr = "S1:1234";
    std::string s2_addr = "S2:1234";
    std::string s3_addr = "S3:1234";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    // Enable custom resolver.
    s1.useCustomResolver = s2.useCustomResolver = s3.useCustomResolver = true;

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    const size_t NUM = 10;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    std::list<ulong> idx_list;
    std::mutex idx_list_lock;
    auto do_async_append = [&]() {
        handlers.clear();
        idx_list.clear();
        for (size_t ii=0; ii<NUM; ++ii) {
            std::string test_msg = "test" + std::to_string(ii);
            ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
            msg->put(test_msg);
            ptr< cmd_result< ptr<buffer> > > ret =
                s1.raftServer->append_entries( {msg} );

            cmd_result< ptr<buffer> >::handler_type my_handler =
                std::bind( async_handler,
                           &idx_list,
                           &idx_list_lock,
                           std::placeholders::_1,
                           std::placeholders::_2 );
            ret->when_ready( my_handler );

            handlers.push_back(ret);
        }
    };
    do_async_append();

    TestSuite::sleep_sec(1, "wait for replication");

    // Now all async handlers should have result.
    {
        std::lock_guard<std::mutex> l(idx_list_lock);
        CHK_EQ(NUM, idx_list.size());
    }

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int log_timestamp_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    // Enable log entry timestamp replication.
    s1.useLogTimestamp = s2.useLogTimestamp = s3.useLogTimestamp = true;

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.reserved_log_items_ = 100;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    const size_t NUM = 5;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    std::list<ulong> idx_list;
    std::mutex idx_list_lock;
    auto do_async_append = [&](RaftAsioPkg& target_srv) {
        handlers.clear();
        idx_list.clear();
        for (size_t ii=0; ii<NUM; ++ii) {
            std::string test_msg = "test" + std::to_string(ii);
            ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
            msg->put(test_msg);
            ptr< cmd_result< ptr<buffer> > > ret =
                target_srv.raftServer->append_entries( {msg} );

            cmd_result< ptr<buffer> >::handler_type my_handler =
                std::bind( async_handler,
                           &idx_list,
                           &idx_list_lock,
                           std::placeholders::_1,
                           std::placeholders::_2 );
            ret->when_ready( my_handler );

            handlers.push_back(ret);
        }
    };
    do_async_append(s1);

    TestSuite::sleep_sec(1, "wait for replication");
    // Now all async handlers should have result.
    {
        std::lock_guard<std::mutex> l(idx_list_lock);
        CHK_EQ(NUM, idx_list.size());
    }

    // Make S2 leader and append logs.
    s2.raftServer->request_leadership();
    TestSuite::sleep_sec(1, "make S2 leader");
    do_async_append(s2);
    TestSuite::sleep_sec(1, "wait for replication");
    // Now all async handlers should have result.
    {
        std::lock_guard<std::mutex> l(idx_list_lock);
        CHK_EQ(NUM, idx_list.size());
    }

    // Make S3 leader and append logs.
    s3.raftServer->request_leadership();
    TestSuite::sleep_sec(1, "make S3 leader");
    do_async_append(s3);
    TestSuite::sleep_sec(1, "wait for replication");
    // Now all async handlers should have result.
    {
        std::lock_guard<std::mutex> l(idx_list_lock);
        CHK_EQ(NUM, idx_list.size());
    }

    // Remove S2, and shut it down.
    s3.raftServer->remove_srv(2);
    TestSuite::sleep_sec(1, "removing S2");

    // Shutdown S2
    s2.raftServer->shutdown();
    s2.stopAsio();
    TestSuite::sleep_sec(1, "shutting down S2");

    // Add S4
    std::string s4_addr = "tcp://127.0.0.1:20040";
    RaftAsioPkg s4(4, s4_addr);
    s4.useLogTimestamp = true;
    s4.initServer();
    {
        raft_params param = s4.raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.reserved_log_items_ = 100;
        s4.raftServer->update_params(param);
    }
    TestSuite::sleep_sec(1, "starting S4");

    s3.raftServer->add_srv( *(s4.getTestMgr()->get_srv_config()) );
    TestSuite::sleep_sec(1, "adding S4");

    // State machine should be identical.
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s4.getTestSm()->isSame( *s1.getTestSm() ) );

    // All log entries should have their timestamp,
    // and they should be identical across all members.
    for (auto& ss: {s3, s4}) {
        ptr<inmem_log_store> src_log_store = s1.getTestMgr()->get_inmem_log_store();
        ptr<inmem_log_store> dst_log_store = ss.getTestMgr()->get_inmem_log_store();

        size_t start_idx = src_log_store->start_index();
        size_t end_idx = src_log_store->next_slot() - 1;
        for (size_t ii = start_idx; ii <= end_idx; ++ii) {
            if (ii == 1) {
                // Log index 1 is a special log: electing itself as a leader.
                // We don't need to compare it.
                continue;
            }
            ptr<log_entry> src_le = src_log_store->entry_at(ii);
            ptr<log_entry> dst_le = dst_log_store->entry_at(ii);
            TestSuite::_msg("index %2lu, type %d, %lu %lu\n",
                            ii, src_le->get_val_type(),
                            src_le->get_timestamp(), dst_le->get_timestamp());
            CHK_NEQ(0, src_le->get_timestamp());
            CHK_EQ(src_le->get_timestamp(), dst_le->get_timestamp());
        }
    }

    s1.raftServer->shutdown();
    s3.raftServer->shutdown();
    s4.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int custom_io_context_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    asio::io_context custom_io_context;
    asio::steady_timer custom_timer(custom_io_context);
    custom_timer.expires_after(std::chrono::seconds(3600));
    custom_timer.async_wait([](const asio_error_code& ec) {
        if (ec) {
            TestSuite::_msg("custom timer error: %s\n", ec.message().c_str());
        } else {
            TestSuite::_msg("custom timer expired\n");
        }
    });

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    s1.customIoContext = s2.customIoContext = s3.customIoContext = &custom_io_context;
    auto custom_worker = [&]() {
        TestSuite::_msg("custom worker spawned\n");
        custom_io_context.run();
        TestSuite::_msg("custom worker finished\n");
    };
    // Create 6 workers.
    std::vector<std::thread> workers;
    for (size_t ii = 0; ii < 6; ++ii) {
        workers.emplace_back(custom_worker);
    }

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    const size_t NUM = 10;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    std::list<ulong> idx_list;
    std::mutex idx_list_lock;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        cmd_result< ptr<buffer> >::handler_type my_handler =
            std::bind( async_handler,
                       &idx_list,
                       &idx_list_lock,
                       std::placeholders::_1,
                       std::placeholders::_2 );
        ret->when_ready( my_handler );

        handlers.push_back(ret);
    }
    TestSuite::sleep_sec(1, "replication");

    // Now all async handlers should have result.
    {
        std::lock_guard<std::mutex> l(idx_list_lock);
        CHK_EQ(NUM, idx_list.size());
    }

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    custom_io_context.stop();
    while (!custom_io_context.stopped()) {
        TestSuite::sleep_sec(1, "waiting for io_context to stop");
    }
    for (auto& worker: workers) {
        worker.join();
    }

    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}


}  // namespace asio_service_test;
using namespace asio_service_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    ts.doTest( "make group test",
               make_group_test );

    ts.doTest( "become_follower_test",
               become_follower_test );

    ts.doTest( "leader election test",
               leader_election_test,
               TestRange<bool>( {false, true} ) );

#if !SSL_LIBRARY_NOT_FOUND && (defined(__linux__) || defined(__APPLE__))
    ts.doTest( "ssl test",
               ssl_test );
#endif

    ts.doTest( "async append handler test",
               async_append_handler_test );

    ts.doTest( "async append handler with order inversion test",
               async_append_handler_with_order_inversion_test );

    ts.doTest( "global manager basic test",
               global_mgr_basic_test );

    ts.doTest( "global manager heavy test",
               global_mgr_heavy_test );

    ts.doTest( "leadership transfer test",
               leadership_transfer_test );

    ts.doTest( "auto forwarding timeout test",
               auto_forwarding_timeout_test );

    ts.doTest( "auto forwarding test",
               auto_forwarding_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "enforced state machine catch-up test",
               enforced_state_machine_catchup_test );

    ts.doTest( "enforced state machine catch-up with term increment test",
               enforced_state_machine_catchup_with_term_inc_test );

    for (bool flag: {true, false}) {
        flag_bg_snapshot_io = flag;
        std::string opt_str = flag_bg_snapshot_io ? " (async)" : " (sync)";

        ts.doTest( "snapshot read failure during join test" + opt_str,
                   snapshot_read_failure_during_join_test,
                   TestRange<size_t>( {10, 999999} ) );

        ts.doTest( "snapshot read failure for lagging server test" + opt_str,
                   snapshot_read_failure_for_lagging_server_test,
                   TestRange<size_t>( {1, 5} ) );

        ts.doTest( "snapshot context timeout normal test" + opt_str,
                   snapshot_context_timeout_normal_test );

        ts.doTest( "snapshot context timeout join test" + opt_str,
                   snapshot_context_timeout_join_test );

        ts.doTest( "snapshot context timeout removed server test" + opt_str,
                   snapshot_context_timeout_removed_server_test );
    }

    ts.doTest( "pause state machine execution test",
               pause_state_machine_execution_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "flip learner flag test",
               flip_learner_flag_test);

    ts.doTest( "parallel log append test",
               parallel_log_append_test );

    ts.doTest( "custom resolver test",
               custom_resolver_test );

    ts.doTest( "log timestamp test",
               log_timestamp_test );

    ts.doTest( "custom io_context test",
               custom_io_context_test );

#ifdef ENABLE_RAFT_STATS
    _msg("raft stats: ENABLED\n");
#else
    _msg("raft stats: DISABLED\n");
#endif
    TestSuite::Msg mm;
    mm << "num allocs: " << raft_server::get_stat_counter("num_buffer_allocs")
       << std::endl
       << "amount of allocs: " << raft_server::get_stat_counter("amount_buffer_allocs")
       << " bytes" << std::endl
       << "num active buffers: " << raft_server::get_stat_counter("num_active_buffers")
       << std::endl
       << "amount of active buffers: "
       << raft_server::get_stat_counter("amount_active_buffers") << " bytes" << std::endl;

    return 0;
}

