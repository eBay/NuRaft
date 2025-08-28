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

namespace custom_quorum_test {

int auto_quorum_size_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";

    RaftAsioPkg s1(1, s1_addr);
    std::shared_ptr<RaftAsioPkg> s2 = std::make_shared<RaftAsioPkg>(2, s2_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, s2.get()};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    // Set custom term counter, and enable auto quorum size mode.
    auto custom_inc_term = [](uint64_t cur_term) -> uint64_t {
        return (cur_term / 10) + 10;
    };
    s1.raftServer->set_inc_term_func(custom_inc_term);
    s2->raftServer->set_inc_term_func(custom_inc_term);

    raft_params params = s1.raftServer->get_current_params();
    params.auto_adjust_quorum_for_small_cluster_ = true;
    s1.raftServer->update_params(params);
    s2->raftServer->update_params(params);

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2->raftServer->get_leader());

    // Replication.
    for (size_t ii=0; ii<10; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");
    uint64_t committed_idx = s1.raftServer->get_committed_log_idx();

    // State machine should be identical.
    CHK_OK( s2->getTestSm()->isSame( *s1.getTestSm() ) );

    // Shutdown S2.
    s2->raftServer->shutdown();
    s2.reset();

    TestSuite::sleep_ms( RaftAsioPkg::HEARTBEAT_MS * 30,
                         "wait for quorum adjust" );

    // More replication.
    for (size_t ii=10; ii<11; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }

    // Replication should succeed: committed index should be moved forward.
    TestSuite::sleep_sec(1, "wait for replication");
    CHK_EQ( committed_idx + 1,
            s1.raftServer->get_committed_log_idx() );

    // Restart S2.
    _msg("launching S2 again\n");
    RaftAsioPkg s2_new(2, s2_addr);
    CHK_Z( launch_servers({&s2_new}, false) );
    TestSuite::sleep_sec(1, "wait for S2 ready");
    CHK_EQ( committed_idx + 1,
            s2_new.raftServer->get_committed_log_idx() );

    // More replication.
    for (size_t ii=11; ii<12; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }

    // Both of them should have the same commit number.
    TestSuite::sleep_sec(1, "wait for replication");
    CHK_EQ( committed_idx + 2,
            s1.raftServer->get_committed_log_idx() );
    CHK_EQ( committed_idx + 2,
            s2_new.raftServer->get_committed_log_idx() );

    s1.raftServer->shutdown();
    s2_new.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int auto_quorum_size_election_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";

    std::shared_ptr<RaftAsioPkg> s1 = std::make_shared<RaftAsioPkg>(1, s1_addr);
    std::shared_ptr<RaftAsioPkg> s2 = std::make_shared<RaftAsioPkg>(2, s2_addr);
    std::vector<RaftAsioPkg*> pkgs = {s1.get(), s2.get()};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    // Set custom term counter, and enable auto quorum size mode.
    auto custom_inc_term = [](uint64_t cur_term) -> uint64_t {
        return (cur_term / 10) + 10;
    };
    s1->raftServer->set_inc_term_func(custom_inc_term);
    s2->raftServer->set_inc_term_func(custom_inc_term);

    raft_params params = s1->raftServer->get_current_params();
    params.auto_adjust_quorum_for_small_cluster_ = true;
    s1->raftServer->update_params(params);
    s2->raftServer->update_params(params);

    CHK_TRUE( s1->raftServer->is_leader() );
    CHK_EQ(1, s1->raftServer->get_leader());
    CHK_EQ(1, s2->raftServer->get_leader());

    // Replication.
    for (size_t ii=0; ii<10; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1->raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");

    // State machine should be identical.
    CHK_OK( s2->getTestSm()->isSame( *s1->getTestSm() ) );

    // Shutdown S1.
    s1->raftServer->shutdown();
    s1.reset();

    // Wait for adjust quorum and self election.
    TestSuite::sleep_ms( RaftAsioPkg::HEARTBEAT_MS * 50,
                         "wait for quorum adjust" );

    // S2 should be a leader.
    CHK_TRUE( s2->raftServer->is_leader() );
    CHK_EQ(2, s2->raftServer->get_leader());
    uint64_t committed_idx = s2->raftServer->get_committed_log_idx();

    // More replication.
    for (size_t ii=10; ii<11; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s2->raftServer->append_entries( {msg} );
    }

    // Replication should succeed: committed index should be moved forward.
    TestSuite::sleep_sec(1, "wait for replication");
    CHK_EQ( committed_idx + 1,
            s2->raftServer->get_committed_log_idx() );

    // Restart S1.
    _msg("launching S1 again\n");
    RaftAsioPkg s1_new(1, s1_addr);
    CHK_Z( launch_servers({&s1_new}, false) );
    TestSuite::sleep_sec(1, "wait for S2 ready");
    CHK_EQ( committed_idx + 1,
            s1_new.raftServer->get_committed_log_idx() );

    // S2 should remain as a leader.
    CHK_TRUE( s2->raftServer->is_leader() );
    CHK_EQ(2, s1_new.raftServer->get_leader());
    CHK_EQ(2, s2->raftServer->get_leader());

    // More replication.
    for (size_t ii=11; ii<12; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s2->raftServer->append_entries( {msg} );
    }

    // Both of them should have the same commit number.
    TestSuite::sleep_sec(1, "wait for replication");
    CHK_EQ( committed_idx + 2,
            s1_new.raftServer->get_committed_log_idx() );
    CHK_EQ( committed_idx + 2,
            s2->raftServer->get_committed_log_idx() );

    s2->raftServer->shutdown();
    s1_new.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int full_consensus_test() {
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

    // Set async & full consensus mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.use_full_consensus_among_healthy_members_ = true;
        pp->raftServer->update_params(param);
    }

    // Stop S3.
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "stop S3");

    // Remember the commit index.
    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();

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

    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");

    // No request should have been committed.
    CHK_EQ(commit_idx, s1.raftServer->get_committed_log_idx());

    // Wait more so that the leader can tolerate not responding peer.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 3, "wait for not responding peer");
    uint64_t new_commit_idx = s1.raftServer->get_committed_log_idx();
    CHK_GT(new_commit_idx, commit_idx);

    // More replication.
    do_async_append();
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");
    // They should be committed immediately.
    CHK_GT(s1.raftServer->get_committed_log_idx(), new_commit_idx);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int self_mark_down_test() {
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

    // Set async & full consensus mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.use_full_consensus_among_healthy_members_ = true;
        pp->raftServer->update_params(param);
    }

    // S3: self mark down.
    s3.raftServer->set_self_mark_down(true);

    // Wait 2 heartbeats to make sure leader receives the flag.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 2, "wait for self mark down");

    // Stop S3.
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "stop S3");

    // Remember the commit index.
    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();

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
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");
    // They should be committed immediately.
    CHK_GT(s1.raftServer->get_committed_log_idx(), commit_idx);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int stop_term_incr_restart_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    std::atomic<bool> chk_first_msg(false);
    std::atomic<bool> first_msg_rcvd(false);
    EventAwaiter first_msg_ea;
    bool part_of_full_consensus_test = true;
    raft_server::init_options i_opt;
    i_opt.raft_callback_ = [&](cb_func::Type type, cb_func::Param* param)
        -> cb_func::ReturnCode {
        if (!chk_first_msg.load() || param->myId != 3) {
            return cb_func::ReturnCode::Ok;
        }

        // Check the very first message received after restart.
        // At the moment it receives this message,
        // S3 should NOT be part of the full consensus.
        first_msg_rcvd = true;
        if (type == cb_func::ReceivedAppendEntriesReq) {
            TestSuite::_msgt("first message received\n");
            part_of_full_consensus_test =
                s3.raftServer->is_part_of_full_consensus();
            chk_first_msg = false;
            first_msg_rcvd = true;
            first_msg_ea.invoke();
        }
        return cb_func::ReturnCode::Ok;
    };

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false, false, true, i_opt, 1000) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async & full consensus mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.use_full_consensus_among_healthy_members_ = true;
        pp->raftServer->update_params(param);
    }

    // S3: self mark down.
    s3.raftServer->set_self_mark_down(true);

    // Wait 2 heartbeats to make sure leader receives the flag.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 2, "wait for self mark down");

    // Stop S3.
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "stop S3");

    // Remember the commit index.
    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();

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
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");
    // They should be committed immediately.
    CHK_GT(s1.raftServer->get_committed_log_idx(), commit_idx);

    // Change leader to increase the term.
    s1.raftServer->yield_leadership();
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 5, "Changing leader");
    CHK_TRUE(s2.raftServer->is_leader());

    // S3: remove mark down flag.
    s3.raftServer->set_self_mark_down(false);
    chk_first_msg = true;
    s3.restartServer(nullptr, false, false, i_opt);
    TestSuite::sleep_sec(1, "restarting S3");
    first_msg_ea.wait_ms(3000);
    CHK_TRUE(first_msg_rcvd.load());

    // full consensus test for the very first message should not succeed.
    CHK_FALSE(part_of_full_consensus_test);

    // After some time,  it should be the part of consensus.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 2, "wait for sync-up");
    CHK_TRUE( s3.raftServer->is_part_of_full_consensus() );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int mark_down_by_log_index_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    std::atomic<bool> refuse_request(false);
    raft_server::init_options i_opt;
    i_opt.raft_callback_ = [&](cb_func::Type type, cb_func::Param* param)
        -> cb_func::ReturnCode {
        if (type != cb_func::Type::GotAppendEntryReqFromLeader ||
            !param || param->myId != 3) {
            return cb_func::ReturnCode::Ok;
        }
        if (refuse_request) {
            return cb_func::ReturnCode::ReturnNull;
        }
        return cb_func::ReturnCode::Ok;
    };

    _msg("launching asio-raft servers\n");
    // Do not sleep.
    CHK_Z( launch_servers(pkgs, false, false, true, i_opt, 0) );

    for (auto& pp: pkgs) {
        // Before initialization, this should return false.
        CHK_FALSE(pp->raftServer->is_part_of_full_consensus());
    }

    TestSuite::sleep_sec(1);

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async & full consensus mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.max_append_size_ = 5;
        param.reserved_log_items_ = 100;
        param.use_full_consensus_among_healthy_members_ = true;
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
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");

    // They should be committed immediately.
    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();
    CHK_EQ(commit_idx, s2.raftServer->get_committed_log_idx());
    CHK_EQ(commit_idx, s3.raftServer->get_committed_log_idx());

    // Let S3 refuse requests.
    _msg("S3 will refuse requests\n");
    refuse_request = true;

    // Wait for S3 to be excluded.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 5, "wait for not responding peer");

    // Do another append.
    do_async_append();
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");

    // S1 and S2 should have the same commit index, but S3 should not.
    commit_idx = s1.raftServer->get_committed_log_idx();
    CHK_EQ(commit_idx, s2.raftServer->get_committed_log_idx());
    CHK_GT(commit_idx, s3.raftServer->get_committed_log_idx());

    // S3 should perceive that it is not the part of quorum.
    CHK_FALSE(s3.raftServer->is_part_of_full_consensus());

    // Now let the traffic go through S3.
    _msg("S3 will accept requests again\n");
    refuse_request = false;
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 2, "wait for replication");

    // S3 should be synchronized now.
    CHK_EQ(commit_idx, s3.raftServer->get_committed_log_idx());

    // S3 should perceive that it is now part of the quorum.
    CHK_TRUE(s3.raftServer->is_part_of_full_consensus());

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int mark_down_after_leader_election_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    // Test phase:
    //   0: normal
    //   1: S2 and S3 are isolated from S1.
    //   2: S1 is isolated from S2 and S3.
    std::atomic<int> test_phase(0);

    raft_server::init_options i_opt;
    EventAwaiter ea_new_leader;
    bool new_leader_elected = false;
    i_opt.raft_callback_ = [&](cb_func::Type type, cb_func::Param* param)
        -> cb_func::ReturnCode {

        switch (test_phase.load()) {
        case 0:
            // Normal phase, do nothing.
            return cb_func::ReturnCode::Ok;
        case 1:
            if (type == cb_func::Type::BecomeLeader &&
                param && param->myId != 1) {
                // S2 or S3 becomes a leader after refusing requests.
                new_leader_elected = true;
                test_phase = 2;
                ea_new_leader.invoke();
            }

            // S2 and S3 refuse requests from S1.
            if (type == cb_func::Type::ReceivedAppendEntriesReq &&
                param && param->peerId == 1) {
                return cb_func::ReturnCode::ReturnNull;
            }
            break;
        case 2:
            // S1 refuses requests from S2 and S3.
            if (type == cb_func::Type::ReceivedAppendEntriesReq &&
                param && param->myId == 1 &&
                (param->peerId == 2 || param->peerId == 3)) {
                return cb_func::ReturnCode::ReturnNull;
            }
            break;
        default:
            // Unknown phase, do nothing.
            break;
        }

        return cb_func::ReturnCode::Ok;
    };

    _msg("launching asio-raft servers\n");
    // Do not sleep.
    CHK_Z( launch_servers(pkgs, false, false, true, i_opt, 0) );

    for (auto& pp: pkgs) {
        // Before initialization, this should return false.
        CHK_FALSE(pp->raftServer->is_part_of_full_consensus());
    }

    TestSuite::sleep_sec(1);

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async & full consensus mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.max_append_size_ = 5;
        param.reserved_log_items_ = 100;
        param.use_full_consensus_among_healthy_members_ = true;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    const size_t NUM = 10;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    std::list<ulong> idx_list;
    std::mutex idx_list_lock;
    auto do_async_append = [&](RaftAsioPkg& ss, size_t num) {
        handlers.clear();
        idx_list.clear();
        for (size_t ii = 0; ii < num; ++ii) {
            std::string test_msg = "test" + std::to_string(ii);
            ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
            msg->put(test_msg);
            ptr< cmd_result< ptr<buffer> > > ret =
                ss.raftServer->append_entries( {msg} );

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
    do_async_append(s1, NUM);
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");

    // They should be committed immediately.
    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();
    CHK_EQ(commit_idx, s2.raftServer->get_committed_log_idx());
    CHK_EQ(commit_idx, s3.raftServer->get_committed_log_idx());

    // Let S2 and S3 refuse requests. It will cause a leader election.
    _msg("S2 and S3 will refuse requests\n");
    test_phase = true;

    _msg("waiting for a new leader to be elected\n");
    ea_new_leader.wait_ms(2000);
    CHK_TRUE(new_leader_elected);

    // Right after the leader election, S1 should recognize that it is not part of
    // the full consensus.
    CHK_FALSE(s1.raftServer->is_part_of_full_consensus());

    // S2 and S3 should recognize themselves as part of the full consensus.
    RaftAsioPkg* new_leader = nullptr;
    RaftAsioPkg* follower = nullptr;
    for (auto& ss: {&s2, &s3}) {
        if (ss->raftServer->is_leader()) {
            // Leader: a majority is reachable, so it should be part of consensus
            //         (i.e., guaranteed that the state machine has the latest data),
            //         although it cannot accept further writes until S1 is marked down.
            CHK_TRUE(ss->raftServer->is_part_of_full_consensus());
            TestSuite::_msg("leader: %d\n", ss->raftServer->get_id());
            new_leader = ss;
        } else {
            // Follower: since election timer's lower bound is greater than
            //           full consensus expiry for follower, it should recognize
            //           itself as not part of the full consensus.
            CHK_FALSE(ss->raftServer->is_part_of_full_consensus());
            TestSuite::_msg("follower: %d\n", ss->raftServer->get_id());
            follower = ss;
        }
    }

    commit_idx = new_leader->raftServer->get_committed_log_idx();

    // Append more messages.
    do_async_append(*new_leader, 1);
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");

    // They can't be committed due to S1.
    CHK_EQ(commit_idx, new_leader->raftServer->get_committed_log_idx());
    CHK_EQ(commit_idx, follower->raftServer->get_committed_log_idx());

    // Wait more so that S1 can be marked down.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 4, "wait for S1 mark down");

    // After marking down S1, the new leader should be able to commit.
    CHK_SM(commit_idx, new_leader->raftServer->get_committed_log_idx());
    CHK_SM(commit_idx, follower->raftServer->get_committed_log_idx());

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int mark_down_without_advancing_heartbeats(bool streaming_mode) {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    // Test phase:
    //   0: normal
    //   1: S3 drops the response of append_entries request after processing it.
    std::atomic<int> test_phase(0);

    raft_server::init_options i_opt;
    i_opt.raft_callback_ = [&](cb_func::Type type, cb_func::Param* param)
        -> cb_func::ReturnCode {

        switch (test_phase.load()) {
        case 1:
            if (type == cb_func::SentAppendEntriesResp &&
                param && param->myId == 3) {
                // S3 drops the response of append_entries request after processing it.
                return cb_func::ReturnCode::ReturnNull;
            }
        case 0:
        default:
            // Normal phase, do nothing.
            return cb_func::ReturnCode::Ok;
        }
        return cb_func::ReturnCode::Ok;
    };

    _msg("launching asio-raft servers\n");
    // Do not sleep.
    CHK_Z( launch_servers(pkgs, false, false, true, i_opt, 0,
                          (streaming_mode ? 10 : 0) ) );

    for (auto& pp: pkgs) {
        // Before initialization, this should return false.
        CHK_FALSE(pp->raftServer->is_part_of_full_consensus());
    }

    TestSuite::sleep_sec(1);

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async & full consensus mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.max_append_size_ = 5;
        param.reserved_log_items_ = 100;
        param.use_full_consensus_among_healthy_members_ = true;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    const size_t NUM = 10;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    std::list<ulong> idx_list;
    std::mutex idx_list_lock;
    auto do_async_append = [&](RaftAsioPkg& ss, size_t num) {
        handlers.clear();
        idx_list.clear();
        for (size_t ii = 0; ii < num; ++ii) {
            std::string test_msg = "test" + std::to_string(ii);
            ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
            msg->put(test_msg);
            ptr< cmd_result< ptr<buffer> > > ret =
                ss.raftServer->append_entries( {msg} );

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
    do_async_append(s1, NUM);
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");

    // They should be committed immediately.
    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();
    CHK_EQ(commit_idx, s2.raftServer->get_committed_log_idx());
    CHK_EQ(commit_idx, s3.raftServer->get_committed_log_idx());

    // Let S3 drops the response.
    TestSuite::_msgt("S3's responses will be dropped\n");
    test_phase = 1;

    // Append more messages.
    do_async_append(s1, NUM);

    // Send enough number of heartbeats.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 5, "wait for replication");

    // S3 should perceive that it is not part of the full consensus,
    // even with live heartbeats.
    CHK_FALSE(s3.raftServer->is_part_of_full_consensus());

    // S3 should be excluded, thus S1 and S2's commit index should be advanced.
    CHK_GT(s1.raftServer->get_committed_log_idx(), commit_idx);
    CHK_GT(s2.raftServer->get_committed_log_idx(), commit_idx);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int slow_heartbeats(bool streaming_mode) {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    // Test phase:
    //   0: normal
    //   1: S3 puts delay (5*H) before handling append_entries request.
    //   2: After handling append_entries, S3 shouldn't be part of the full consensus,
    //      and it should perceive that.
    //   3: back to normal.
    std::atomic<int> test_phase(0);
    bool s3_chk = false;

    raft_server::init_options i_opt;
    i_opt.raft_callback_ = [&](cb_func::Type type, cb_func::Param* param)
        -> cb_func::ReturnCode {

        switch (test_phase.load()) {
        case 1:
            if (type == cb_func::ReceivedAppendEntriesReq &&
                param && param->myId == 3) {
                TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 8,
                                    "S3 will delay handling append_entries request");
                test_phase = 2;
                return cb_func::ReturnCode::Ok;
            }
        case 2:
            if (type == cb_func::SentAppendEntriesResp &&
                param && param->myId == 3) {
                // After handling append_entries, S3 shouldn't be part of
                // the full consensus, and S3 should perceive that.
                s3_chk = s3.raftServer->is_part_of_full_consensus();
                test_phase = 3;
                return cb_func::ReturnCode::Ok;
            }
        case 0:
        default:
            // Normal phase, do nothing.
            return cb_func::ReturnCode::Ok;
        }
        return cb_func::ReturnCode::Ok;
    };

    _msg("launching asio-raft servers\n");
    // Do not sleep.
    CHK_Z( launch_servers(pkgs, false, false, true, i_opt, 0,
                          (streaming_mode ? 10 : 0) ) );

    for (auto& pp: pkgs) {
        // Before initialization, this should return false.
        CHK_FALSE(pp->raftServer->is_part_of_full_consensus());
    }

    TestSuite::sleep_sec(1);

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    // Set async & full consensus mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.max_append_size_ = 5;
        param.reserved_log_items_ = 100;
        param.use_full_consensus_among_healthy_members_ = true;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    const size_t NUM = 10;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    std::list<ulong> idx_list;
    std::mutex idx_list_lock;
    auto do_async_append = [&](RaftAsioPkg& ss, size_t num) {
        handlers.clear();
        idx_list.clear();
        for (size_t ii = 0; ii < num; ++ii) {
            std::string test_msg = "test" + std::to_string(ii);
            ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
            msg->put(test_msg);
            ptr< cmd_result< ptr<buffer> > > ret =
                ss.raftServer->append_entries( {msg} );

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
    do_async_append(s1, NUM);
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");

    // They should be committed immediately.
    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();
    CHK_EQ(commit_idx, s2.raftServer->get_committed_log_idx());
    CHK_EQ(commit_idx, s3.raftServer->get_committed_log_idx());

    // Let S3 drops the response.
    TestSuite::_msgt("heartbeats to S3 will get delayed\n");
    test_phase = 1;

    // Send enough number of heartbeats.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 5, "wait for heartbeat delay");

    // Append more messages, should be able to commit, as S3 is excluded.
    do_async_append(s1, NUM);
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");

    // S1 and S3's commit index should be advanced, but S3 does not.
    CHK_GT(s1.raftServer->get_committed_log_idx(), commit_idx);
    CHK_GT(s2.raftServer->get_committed_log_idx(), commit_idx);
    CHK_EQ(s3.raftServer->get_committed_log_idx(), commit_idx);

    // Wait more for S3.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 3, "wait for S3's response");

    // S3 should perceive that it is not part of the full consensus.
    CHK_FALSE(s3_chk);

    // Wait a couple of heartbeats more, and S3 should be included again.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 2,
                        "wait for S3 to be part of quorum again");

    CHK_TRUE(s3.raftServer->is_part_of_full_consensus());

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int sm_commit_watcher_test() {
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

    // Set async mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    timer_helper tt(3 * 1000 * 1000); // 3 seconds
    uint64_t msg_idx = 0;
    _msg("appending and verifying state machine data\n");
    while (!tt.timeout()) {
        std::string test_msg = "test" + std::to_string(msg_idx++);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 4);
        buffer_serializer bs(msg);
        bs.put_str(test_msg);
        s1.raftServer->append_entries( {msg} );
        raft_server::req_ext_params ext_params;
        uint64_t cur_log_idx = 0;
        ext_params.after_precommit_ = [&](const raft_server::req_ext_cb_params& p) {
            cur_log_idx = p.log_idx;
        };
        s1.raftServer->append_entries_ext({msg}, ext_params);

        for (auto& ss: {&s3, &s2, &s1}) {
            auto ret = ss->raftServer->wait_for_state_machine_commit(cur_log_idx);
            EventAwaiter ea;
            ret->when_ready([&](cmd_result<bool, ptr<std::exception>>& cmd_res,
                                ptr<std::exception>& err)
                {
                    ea.invoke();
                });
            ea.wait_ms(1000);
            auto data_buf = ss->getTestSm()->getData(cur_log_idx);
            buffer_serializer bs2(data_buf);
            std::string data_str = bs2.get_str();
            CHK_EQ(test_msg, data_str);
        }
    }

    // Wait for future commit.
    {
        uint64_t target_idx = s1.raftServer->get_committed_log_idx() + 1;

        // Test multiple watchers for the same index.
        auto ret1 = s3.raftServer->wait_for_state_machine_commit(target_idx);
        auto ret2 = s3.raftServer->wait_for_state_machine_commit(target_idx);

        EventAwaiter ea1, ea2;
        bool is_ready1 = false, is_ready2 = false;
        ret1->when_ready([&](cmd_result<bool, ptr<std::exception>>& cmd_res,
                            ptr<std::exception>& err)
            {
                ea1.invoke();
                is_ready1 = true;
            });
        ret2->when_ready([&](cmd_result<bool, ptr<std::exception>>& cmd_res,
                            ptr<std::exception>& err)
            {
                ea2.invoke();
                is_ready2 = true;
            });

        std::string test_msg = "test" + std::to_string(msg_idx++);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 4);
        buffer_serializer bs(msg);
        bs.put_str(test_msg);
        s1.raftServer->append_entries( {msg} );

        ea1.wait_ms(1000);
        ea2.wait_ms(1000);
        CHK_TRUE(is_ready1);
        CHK_TRUE(is_ready2);

        auto data_buf = s3.getTestSm()->getData(target_idx);
        buffer_serializer bs2(data_buf);
        std::string data_str = bs2.get_str();
        CHK_EQ(test_msg, data_str);
    }

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int custom_commit_condition_test() {
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

    // Set async & full consensus mode.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Stop S3.
    s3.raftServer->shutdown();
    s3.stopAsio();
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 5, "stop S3");

    // Remember the commit index.
    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();

    // Set custom quorum set: {S1, S3}.
    s1.getTestSm()->setServersForCommit({1, 3});

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

    // No request should have been committed, as S1 cannot reach quorum due to S3.
    CHK_EQ(commit_idx, s1.raftServer->get_committed_log_idx());

    // Restart S3.
    raft_params new_params = s1.raftServer->get_current_params();
    s3.restartServer(&new_params);
    TestSuite::sleep_ms(500, "restarting S3");

    // More replication.
    do_async_append();
    TestSuite::sleep_sec(1, "wait for replication");
    // They should be committed immediately.
    CHK_GT(s1.raftServer->get_committed_log_idx(), commit_idx);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

}  // namespace custom_quorum_test;
using namespace custom_quorum_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    ts.doTest( "auto quorum size test",
               auto_quorum_size_test );

    ts.doTest( "auto quorum size for election test",
               auto_quorum_size_election_test );

    ts.doTest( "full consensus test",
               full_consensus_test );

    ts.doTest( "self mark down test",
               self_mark_down_test );

    ts.doTest( "stop, term increment, restart test",
               stop_term_incr_restart_test );

    ts.doTest( "mark down by log index test",
               mark_down_by_log_index_test );

    ts.doTest( "mark down after leader election test",
               mark_down_after_leader_election_test );

    ts.doTest( "mark down without advancing heartbeats test",
               mark_down_without_advancing_heartbeats,
               TestRange<bool>({false, true}) );

    ts.doTest( "slow heartbeats test",
               slow_heartbeats,
               TestRange<bool>({false, true}) );

    ts.doTest( "sm commit watcher test",
               sm_commit_watcher_test );

    ts.doTest( "custom commit condition test",
               custom_commit_condition_test );

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
