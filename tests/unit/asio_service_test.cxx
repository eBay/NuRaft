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

#include "raft_package_asio.hxx"

#include "event_awaiter.h"
#include "test_common.h"

#include <unordered_map>

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

namespace asio_service_test {

int launch_servers(const std::vector<RaftAsioPkg*>& pkgs,
                   bool enable_ssl)
{
    size_t num_srvs = pkgs.size();
    CHK_GT(num_srvs, 0);

    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        pp->initServer(enable_ssl);
    }
    // Wait longer than upper timeout.
    TestSuite::sleep_sec(1);
    return 0;
}

int make_group(const std::vector<RaftAsioPkg*>& pkgs) {
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

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int leader_election_test() {
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

static bool dbg_print_ctx = false;
static std::unordered_map<std::string, std::string> req_map;
static std::unordered_map<std::string, std::string> resp_map;
static std::mutex req_map_lock;
static std::mutex resp_map_lock;

std::string test_write_req_meta( std::atomic<size_t>* count,
                                 const asio_service::meta_cb_params& params )
{
    static std::mutex lock;
    std::lock_guard<std::mutex> l(lock);

    char key[256];
    sprintf(key, "%2d, %2d -> %2d, %4zu",
            params.msg_type_, params.src_id_, params.dst_id_,
            (size_t)params.log_idx_);

    std::string value = "req_" + std::to_string( std::rand() );
    {   std::lock_guard<std::mutex> l(req_map_lock);
        req_map[key] = value;
    }

    if (dbg_print_ctx) {
        _msg("%10s %s %20s\n", "write req", key, value.c_str());
    }

    if (count) (*count)++;
    return value;
}

bool test_read_req_meta( std::atomic<size_t>* count,
                         const asio_service::meta_cb_params& params,
                         const std::string& meta )
{
    static std::mutex lock;

    std::lock_guard<std::mutex> l(lock);

    char key[256];
    sprintf(key, "%2d, %2d -> %2d, %4zu",
            params.msg_type_, params.src_id_, params.dst_id_,
            (size_t)params.log_idx_);

    if (dbg_print_ctx) {
        _msg("%10s %s %20s\n", "read req", key, meta.c_str());
    }

    std::string META;
    {   std::lock_guard<std::mutex> l(req_map_lock);
        META = req_map[key];
    }
    if (META != meta) {
        CHK_EQ( META, meta );
        return false;
    }
    if (count) (*count)++;
    return true;
}

std::string test_write_resp_meta( std::atomic<size_t>* count,
                                  const asio_service::meta_cb_params& params )
{
    static std::mutex lock;
    std::lock_guard<std::mutex> l(lock);

    char key[256];
    sprintf(key, "%2d, %2d -> %2d, %4zu",
            params.msg_type_, params.src_id_, params.dst_id_,
            (size_t)params.log_idx_);

    std::string value = "resp_" + std::to_string( std::rand() );
    {   std::lock_guard<std::mutex> l(resp_map_lock);
        resp_map[key] = value;
    }

    if (dbg_print_ctx) {
        _msg("%10s %s %20s\n", "write resp", key, value.c_str());
    }

    if (count) (*count)++;
    return value;
}

bool test_read_resp_meta( std::atomic<size_t>* count,
                          const asio_service::meta_cb_params& params,
                          const std::string& meta )
{
    static std::mutex lock;
    std::lock_guard<std::mutex> l(lock);

    char key[256];
    sprintf(key, "%2d, %2d -> %2d, %4zu",
            params.msg_type_, params.src_id_, params.dst_id_,
            (size_t)params.log_idx_);

    if (dbg_print_ctx) {
        _msg("%10s %s %20s\n", "read resp", key, meta.c_str());
    }

    std::string META;
    {   std::lock_guard<std::mutex> l(resp_map_lock);
        META = resp_map[key];
    }
    if (META != meta) {
        CHK_EQ( META, meta );
        return false;
    }
    if (count) (*count)++;
    return true;
}

int message_meta_test() {
    reset_log_files();

    std::string s1_addr = "127.0.0.1:20010";
    std::string s2_addr = "127.0.0.1:20020";
    std::string s3_addr = "127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    std::atomic<size_t> read_req_cb_count(0);
    std::atomic<size_t> write_req_cb_count(0);
    std::atomic<size_t> read_resp_cb_count(0);
    std::atomic<size_t> write_resp_cb_count(0);

    _msg("launching asio-raft servers with meta callback\n");
    for (RaftAsioPkg* rr: pkgs) {
        rr->setMetaCallback
            ( std::bind( test_read_req_meta,
                         &read_req_cb_count,
                         std::placeholders::_1,
                         std::placeholders::_2 ),
              std::bind( test_write_req_meta,
                         &write_req_cb_count,
                         std::placeholders::_1 ),
              std::bind( test_read_resp_meta,
                         &read_resp_cb_count,
                         std::placeholders::_1,
                         std::placeholders::_2 ),
              std::bind( test_write_resp_meta,
                         &write_resp_cb_count,
                         std::placeholders::_1 ),
              true );
    }
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());

    TestSuite::sleep_sec(1, "wait for Raft group ready");

    for (size_t ii=0; ii<10; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }

    TestSuite::sleep_sec(1, "wait for replication");

    // Callback functions for meta should have been called.
    CHK_GT(read_req_cb_count.load(), 0);
    CHK_GT(write_req_cb_count.load(), 0);
    CHK_GT(read_resp_cb_count.load(), 0);
    CHK_GT(write_resp_cb_count.load(), 0);
    _msg( "read req callback %zu, write req callback %zu\n",
          read_req_cb_count.load(), write_req_cb_count.load() );
    _msg( "read resp callback %zu, write resp callback %zu\n",
          read_resp_cb_count.load(), write_resp_cb_count.load() );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}


std::string test_write_empty_meta( std::atomic<size_t>* count,
                                   const asio_service::meta_cb_params& params )
{
    if (count) (*count)++;
    return std::string();
}

bool test_read_empty_meta( std::atomic<size_t>* count,
                           const asio_service::meta_cb_params& params,
                           const std::string& meta )
{
    static std::mutex lock;
    std::lock_guard<std::mutex> l(lock);

    CHK_EQ( std::string(), meta );

    if (count) (*count)++;
    return true;
}

int empty_meta_test(bool always_invoke_cb) {
    reset_log_files();

    std::string s1_addr = "127.0.0.1:20010";
    std::string s2_addr = "127.0.0.1:20020";
    std::string s3_addr = "127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    std::atomic<size_t> read_req_cb_count(0);
    std::atomic<size_t> write_req_cb_count(0);
    std::atomic<size_t> read_resp_cb_count(0);
    std::atomic<size_t> write_resp_cb_count(0);

    _msg("launching asio-raft servers with meta callback\n");
    for (RaftAsioPkg* rr: pkgs) {
        rr->setMetaCallback
            ( std::bind( test_read_empty_meta,
                         &read_req_cb_count,
                         std::placeholders::_1,
                         std::placeholders::_2 ),
              std::bind( test_write_empty_meta,
                         &write_req_cb_count,
                         std::placeholders::_1 ),
              std::bind( test_read_empty_meta,
                         &read_resp_cb_count,
                         std::placeholders::_1,
                         std::placeholders::_2 ),
              std::bind( test_write_empty_meta,
                         &write_resp_cb_count,
                         std::placeholders::_1 ),
              always_invoke_cb );
    }
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());

    TestSuite::sleep_sec(1, "wait for Raft group ready");

    for (size_t ii=0; ii<10; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }

    TestSuite::sleep_sec(1, "wait for replication");

    if (always_invoke_cb) {
        // Callback functions for meta should have been called.
        CHK_GT(read_req_cb_count.load(), 0);
        CHK_GT(read_resp_cb_count.load(), 0);
    } else {
        // Callback will not be invoked on empty meta, should be 0.
        CHK_Z(read_req_cb_count.load());
        CHK_Z(read_resp_cb_count.load());
    }
    CHK_GT(write_req_cb_count, 0);
    CHK_GT(write_resp_cb_count, 0);
    _msg( "read req callback %zu, write req callback %zu\n",
          read_req_cb_count.load(), write_req_cb_count.load() );
    _msg( "read resp callback %zu, write resp callback %zu\n",
          read_resp_cb_count.load(), write_resp_cb_count.load() );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

static void async_handler(std::list<ulong>* idx_list,
                          ptr<buffer>& result,
                          ptr<std::exception>& err)
{
    result->pos(0);
    ulong idx = result->get_ulong();
    if (idx_list) {
        idx_list->push_back(idx);
    }
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
        ptr<raft_params> param = pp->raftServer->get_current_params();
        param->return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii=0; ii<10; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );
        handlers.push_back(ret);
    }
    TestSuite::sleep_sec(1, "replication");

    // Now all async handlers should have result.
    std::list<ulong> idx_list;
    for (auto& entry: handlers) {
        ptr< cmd_result< ptr<buffer> > > result = entry;
        cmd_result< ptr<buffer> >::handler_type my_handler =
            std::bind( async_handler,
                       &idx_list,
                       std::placeholders::_1,
                       std::placeholders::_2 );
        result->when_ready( my_handler );
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

}  // namespace asio_service_test;
using namespace asio_service_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    ts.doTest( "make group test",
               make_group_test );

    ts.doTest( "leader election test",
               leader_election_test );

#if defined(__linux__) || defined(__APPLE__)
    ts.doTest( "ssl test",
               ssl_test );
#endif

    ts.doTest( "message meta test",
               message_meta_test );

    ts.doTest( "empty meta test",
               empty_meta_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "async append handler test",
               async_append_handler_test );

    return 0;
}

