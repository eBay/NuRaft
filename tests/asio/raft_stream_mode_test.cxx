/************************************************************************
Copyright 2017-present eBay Inc.

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
#include "raft_package_asio.hxx"
#include "fake_network.hxx"
#include "raft_package_fake.hxx"

#include "event_awaiter.hxx"
#include "raft_params.hxx"
#include "test_common.h"

using namespace nuraft;
using namespace raft_functional_common;

namespace stream_functional_test {

int launch_asio_servers(const std::vector<RaftAsioPkg*>& pkgs,
                        bool enable_ssl,
                        bool use_global_asio = false,
                        bool use_bg_snapshot_io = true,
                        const raft_server::init_options& opt = raft_server::init_options()) {
    size_t num_srvs = pkgs.size();
    CHK_GT(num_srvs, 0);

    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        pp->initServer(enable_ssl, use_global_asio, use_bg_snapshot_io, opt, true);
        raft_params param = pp->raftServer->get_current_params();
        param.max_log_gap_in_stream_ = 500;
        pp->raftServer->update_params(param);
    }
    // Wait longer than upper timeout.
    TestSuite::sleep_sec(1);
    return 0;
}

int make_asio_group(const std::vector<RaftAsioPkg*>& pkgs) {
    size_t num_srvs = pkgs.size();
    CHK_GT(num_srvs, 0);

    RaftAsioPkg* leader = pkgs[0];

    for (size_t ii = 1; ii < num_srvs; ++ii) {
        RaftAsioPkg* ff = pkgs[ii];

        // Add to leader.
        leader->raftServer->add_srv( *(ff->getTestMgr()->get_srv_config()) );

        // Wait longer than upper timeout.
        TestSuite::sleep_sec(1);
    }
    return 0;
}

static void async_handler(std::list<ulong>* idx_list,
                          std::mutex* idx_list_lock,
                          ptr<buffer>& result,
                          ptr<std::exception>& err) {
    result->pos(0);
    ulong idx = result->get_ulong();
    if (idx_list) {
        std::lock_guard<std::mutex> l(*idx_list_lock);
        idx_list->push_back(idx);
    }
}

int append_log_in_non_stream(const std::vector<RaftPkg*>& pkgs,
                             RaftPkg& primary, 
                             size_t num) {
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii=0; ii<num; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            primary.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );

        handlers.push_back(ret);
    }
    // Pre-commit and commit for S1.
    primary.fNet->execReqResp();
    primary.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // All handlers should be OK.
    for (auto& entry: handlers) {
        CHK_TRUE( entry->has_result() );
        CHK_EQ( cmd_result_code::OK, entry->get_result_code() );
    }
    handlers.clear();

    // Commit for S2
    primary.fNet->execReqResp();
    return 0;
}

int drain_pending_reqs_queue(const std::vector<RaftPkg*>& pkgs,
                             const RaftPkg& primary,
                             std::string& secondary_endpoint,
                             int expected_delivery,
                             int max_delivery = 100,
                             const std::string& specified_endpoint = std::string()) {
    int delivery_count = 0;
    while(primary.fNet->getNumPendingReqs(secondary_endpoint) > 0) {
        primary.fNet->execReqResp(specified_endpoint);
        delivery_count++;
        CHK_GTEQ( max_delivery, delivery_count );
    }
    _msg("processed %zu requests to drain the queue\n", delivery_count);
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( expected_delivery, delivery_count );
    return 0;
}

int append_log_in_stream_without_delivery(const RaftPkg& primary,
                                          std::string& secondary_endpoint, 
                                          size_t num,
                                          size_t expected_pending_reqs) {
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii=0; ii<num; ++ii) {
        std::string test_msg = "stream test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            primary.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );

        handlers.push_back(ret);
    }
    CHK_EQ( expected_pending_reqs, 
            primary.fNet->getNumPendingReqs(secondary_endpoint) );
    return 0;
}

int activate_stream(RaftPkg& primary, 
                    std::string& secondary_endpoint) {
    std::string enable_msg = "enable stream";
    ptr<buffer> enable_buffer = buffer::alloc(enable_msg.size() + 1);
    enable_buffer->put(enable_msg);
    ptr< cmd_result< ptr<buffer> > > enable_ret =
        primary.raftServer->append_entries( {enable_buffer} );
    CHK_TRUE( enable_ret->get_accepted() );
    primary.fNet->execReqResp();
    primary.fNet->execReqResp();
    CHK_Z( primary.fNet->getNumPendingReqs(secondary_endpoint) );
    return 0;
}

void update_stream_params(const std::vector<RaftPkg*>& pkgs,
                          int max_log_gap_in_stream) {
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.max_log_gap_in_stream_ = max_log_gap_in_stream;
        pp->raftServer->update_params(param);
    }
}

void update_max_fly_bytes_params(const std::vector<RaftPkg*>& pkgs,
                          int max_bytes_in_flight_in_stream) {
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.max_bytes_in_flight_in_stream_ = max_bytes_in_flight_in_stream;
        pp->raftServer->update_params(param);
    }
}

int stream_basic_function_test() {
    reset_log_files();

    std::string s1_addr = "tcp://127.0.0.1:20010";
    std::string s2_addr = "tcp://127.0.0.1:20020";
    std::string s3_addr = "tcp://127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_asio_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_asio_group(pkgs) );

    // Set async.
    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    const size_t NUM = 100;
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

int enable_and_disable_stream_mode_test() {
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

    // Append 10 logs in non-stream mode
    CHK_Z( append_log_in_non_stream(pkgs, s1, 10) );
    CHK_Z( s1.fNet->getNumPendingReqs(s2_addr) );

    // Set stream mode params
    update_stream_params(pkgs, 500);

    // Append 1 log to enable stream
    CHK_Z( activate_stream(s1, s2_addr) );

    // Append 10 logs in stream mode, pending reqs > 1
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 10, 10) );

    // Disable stream mode
    update_stream_params(pkgs, 0);

    // Send one more log, it get the busy lock
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 1, 11) );

    // Send one more log, it can't get the busy lock
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 1, 11) );

    // S1 and S2 should have the same data.
    // 11 pending requests, 1 for commit and skipped entry, 1 for final commit
    // 13 requests prove that the lock works correctly
    CHK_Z( drain_pending_reqs_queue(pkgs, s1, s2_addr, 13) );
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // Re-enable stream mode
    update_stream_params(pkgs, 500);
    CHK_Z( activate_stream(s1, s2_addr) );
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 10, 10) );
    // 10 pending requests, 10 commit requests
    CHK_Z( drain_pending_reqs_queue(pkgs, s1, s2_addr, 20) );
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();
    return 0;
}

int activate_and_deactivate_stream_mode_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        // avoid send snapshot twice
        param.reserved_log_items_ = 5;
        pp->raftServer->update_params(param);
    }

    // Set stream mode params
    update_stream_params(pkgs, 500);

    // Append 1 log to enable stream
    CHK_Z( activate_stream(s1, s2_addr) );

    // Append 10 logs in stream mode, pending reqs > 1
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 10, 10) );
    // 10 pending requests, 10 commit requests
    CHK_Z( drain_pending_reqs_queue(pkgs, s1, s2_addr, 20) );

    // Remove s2 and then add back
    s1.dbgLog(" --- remove S2---");
    s1.raftServer->remove_srv( s2.getTestMgr()->get_srv_config()->get_id() );

    // Leave req/resp.
    s1.fNet->execReqResp();
    // Leave done, notify to peers.
    s1.fNet->execReqResp();
    // Notify new commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // add S2 back to leader.
    s1.dbgLog(" --- add back S2---");
    s1.raftServer->add_srv( *(s2.getTestMgr()->get_srv_config()) );
    s1.fNet->execReqResp();
    // Send the entire snapshot.
    do {
        s1.fNet->execReqResp();
    } while (s2.raftServer->is_receiving_snapshot());
    // Commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_Z( s1.fNet->getNumPendingReqs(s2_addr) );

    // Append two logs, in non-stream mode, it only generate 1 reqs
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 1, 1) );
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 1, 1) );

    // Activate stream mode
    // 3 reqs: log 16, log 17 + commit 16, commit 17
    CHK_Z( drain_pending_reqs_queue(pkgs, s1, s2_addr, 3) );

    // Append 10 logs in stream mode, pending reqs > 1
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 10, 10) );
    // 10 pending requests, 10 commit requests
    CHK_Z( drain_pending_reqs_queue(pkgs, s1, s2_addr, 20) );
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();

    f_base->destroy();
    return 0;
}

int stream_mode_base_throttling_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Set stream mode params
    int max_gap = 500;
    update_stream_params(pkgs, max_gap);

    // Append 1 log to enable stream
    CHK_Z( activate_stream(s1, s2_addr) );

    // Append number of max gap logs in stream mode
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, max_gap, max_gap) );

    // Send one more log, pending reqs should not increase
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 1, max_gap) );
    
    // Need 2 * max_gap, because every append log need one commit request
    int expected_delivery = 2 * max_gap + 1;
    CHK_Z( drain_pending_reqs_queue(pkgs, s1, s2_addr,
                                    expected_delivery, expected_delivery) );
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();

    f_base->destroy();
    return 0;
}

int stream_mode_flying_bytes_throttling_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Set stream mode and max flying bytes params
    // idx 0-9, size is 13, idx 10-99, size is 14
    int max_bytes_in_flight = 13 * 9;
    update_stream_params(pkgs, 500);
    update_max_fly_bytes_params(pkgs, max_bytes_in_flight);

    // Append 1 log to enable stream
    CHK_Z( activate_stream(s1, s2_addr) );

    // Append number of max gap logs in stream mode
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 10, 10) );

    // Send one more log, pending reqs should not increase
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 1, 10) );

    // Need 2 * max_gap, because every append log need one commit request
    int expected_delivery = 2 * 10 + 1;
    CHK_Z( drain_pending_reqs_queue(pkgs, s1, s2_addr,
                                    expected_delivery, expected_delivery) );
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();

    f_base->destroy();
    return 0;
}

int snapshot_transmission_in_stream_mode() {
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

    // Append 10 logs in non-stream mode
    CHK_Z( append_log_in_non_stream(pkgs, s1, 10) );
    CHK_Z( s1.fNet->getNumPendingReqs(s2_addr) );
    
    // Set stream mode params
    update_stream_params(pkgs, 500);

    // Append 1 log to enable stream
    CHK_Z( activate_stream(s1, s2_addr) );

    // Append 10 logs in stream mode
    CHK_Z( append_log_in_stream_without_delivery(s1, s2_addr, 10, 10) );

    // Send it to S2 only, S3 will be lagging behind.
    // 10 pending requests, 10 commit requests
    CHK_Z( drain_pending_reqs_queue(pkgs, s1, s2_addr, 20, 100, "S2") );

    // Remember the current commit index.
    uint64_t committed_index = s1.raftServer->get_committed_log_idx();

    // Create a manual snapshot.
    ulong log_idx = s1.raftServer->create_snapshot();
    CHK_EQ( committed_index, log_idx );
    CHK_EQ( log_idx, s1.raftServer->get_last_snapshot_idx() );

    // Make req to S3 failed, S3 exits stream mode
    s1.fNet->makeReqFailAll("S3");

    // Trigger heartbeat to S3, it will initiate snapshot transmission.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp();

    // Send the entire snapshot.
    do {
        s1.fNet->execReqResp();
    } while (s3.raftServer->is_receiving_snapshot());

    s1.fNet->execReqResp(); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.
    CHK_Z(s1.fNet->getNumPendingReqs(s3_addr));

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    CHK_EQ( committed_index, s3.getTestSm()->last_snapshot()->get_last_log_idx() );

    // There shouldn't be any open snapshot ctx.
    CHK_Z( s1.getTestSm()->getNumOpenedUserCtxs() );

    // Append two logs, it only generate 1 reqs for S3
    CHK_Z( append_log_in_stream_without_delivery(s1, s3_addr, 1, 1) );
    CHK_Z( append_log_in_stream_without_delivery(s1, s3_addr, 1, 1) );

    // Peer in stream mode will generate more requests, so drain S2.
    // Enable stream mode for S3.
    // 2 pending requests, 2 commit requests
    CHK_Z( drain_pending_reqs_queue(pkgs, s1, s2_addr, 4) );
    CHK_Z( s1.fNet->getNumPendingReqs(s2_addr) );
    CHK_Z( s1.fNet->getNumPendingReqs(s3_addr) );

    // Append 10 logs in stream mode again
    // 10 pending requests, 10 commit requests
    CHK_Z( append_log_in_stream_without_delivery(s1, s3_addr, 10, 10) );
    CHK_Z( drain_pending_reqs_queue(pkgs, s1, s3_addr, 20) );

    // State machine should still be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();
    return 0;
}

}  // namespace stream_functional_test;
using namespace stream_functional_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;
    // with asio
    ts.doTest( "stream basic function test",
               stream_basic_function_test );
    
    // with fake network
    // Enable and disable stream mode by runtime config
    ts.doTest( "enable and disable stream mode test",
               enable_and_disable_stream_mode_test );

    // Streaming activation and deactivation
    ts.doTest( "activate and deactivate stream mode test",
               activate_and_deactivate_stream_mode_test );

    // Throttling test
    ts.doTest( "stream mode base throttling test",
               stream_mode_base_throttling_test );

    ts.doTest( "stream mode flying bytes throttling test",
               stream_mode_flying_bytes_throttling_test );

    // Snapshot transmission in stream mode
    ts.doTest( "snapshot transmission in stream mode",
               snapshot_transmission_in_stream_mode );

    return 0;
}