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
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 2, "wait for not responding peer");
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
    CHK_Z( launch_servers(pkgs, false, false, true, i_opt) );

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
    CHK_EQ(commit_idx, s2.raftServer->get_last_log_idx());
    CHK_EQ(commit_idx, s3.raftServer->get_last_log_idx());

    // Let S3 refuse requests.
    _msg("S3 will refuse requests\n");
    refuse_request = true;

    // Wait for S3 to be excluded.
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 4, "wait for not responding peer");

    // Do another append.
    do_async_append();
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 1, "wait for replication");

    // S1 and S2 should have the same commit index, but S3 should not.
    commit_idx = s1.raftServer->get_committed_log_idx();
    CHK_EQ(commit_idx, s2.raftServer->get_last_log_idx());
    CHK_GT(commit_idx, s3.raftServer->get_last_log_idx());

    // S3 should perceive that it is not the part of quorum.
    CHK_FALSE(s3.raftServer->is_part_of_full_consensus());

    // Now let the traffic go through S3.
    _msg("S3 will accept requests again\n");
    refuse_request = false;
    TestSuite::sleep_ms(RaftAsioPkg::HEARTBEAT_MS * 2, "wait for replication");

    // S3 should be synchronized now.
    CHK_EQ(commit_idx, s3.raftServer->get_last_log_idx());

    // S3 should perceive that it is now part of the quorum.
    CHK_TRUE(s3.raftServer->is_part_of_full_consensus());

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

    ts.doTest( "mark down by log index test",
               mark_down_by_log_index_test );

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
