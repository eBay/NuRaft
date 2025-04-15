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
#include "in_memory_log_store.hxx"
#include "raft_package_asio.hxx"
#include "asio_test_common.hxx"

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

namespace req_resp_meta_test {

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
    snprintf(key, 256, "%2d, %2d -> %2d, %4zu",
             params.msg_type_, params.src_id_, params.dst_id_,
             (size_t)params.log_idx_);

    std::string value = "req_" + std::to_string( std::rand() );
    {   std::lock_guard<std::mutex> l(req_map_lock);
        req_map[key] = value;
    }

    if (dbg_print_ctx) {
        _msg("%10s %s %20s\n", "write req", key, value.c_str());
    }

    // `req_` should be given, while `resp_` should be null.
    auto chk_req_resp = [&]() {
        CHK_NONNULL(params.req_);
        CHK_NULL(params.resp_);
        return 0;
    };
    if (chk_req_resp() != 0) return std::string();

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
    snprintf(key, 256, "%2d, %2d -> %2d, %4zu",
             params.msg_type_, params.src_id_, params.dst_id_,
             (size_t)params.log_idx_);

    if (dbg_print_ctx) {
        _msg("%10s %s %20s\n", "read req", key, meta.c_str());
    }

    // `req_` should be given, while `resp_` should be null.
    auto chk_req_resp = [&]() {
        CHK_NONNULL(params.req_);
        CHK_NULL(params.resp_);
        return 0;
    };
    if (chk_req_resp() != 0) return false;

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
    snprintf(key, 256, "%2d, %2d -> %2d, %4zu",
             params.msg_type_, params.src_id_, params.dst_id_,
             (size_t)params.log_idx_);

    std::string value = "resp_" + std::to_string( std::rand() );
    {   std::lock_guard<std::mutex> l(resp_map_lock);
        resp_map[key] = value;
    }

    if (dbg_print_ctx) {
        _msg("%10s %s %20s\n", "write resp", key, value.c_str());
    }

    // `req_` and `resp_` should be given.
    auto chk_req_resp = [&]() {
        CHK_NONNULL(params.req_);
        CHK_NONNULL(params.resp_);
        return 0;
    };
    if (chk_req_resp() != 0) return std::string();

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
    snprintf(key, 256, "%2d, %2d -> %2d, %4zu",
             params.msg_type_, params.src_id_, params.dst_id_,
             (size_t)params.log_idx_);

    if (dbg_print_ctx) {
        _msg("%10s %s %20s\n", "read resp", key, meta.c_str());
    }

    // `req_` and `resp_` should be given.
    auto chk_req_resp = [&]() {
        CHK_NONNULL(params.req_);
        CHK_NONNULL(params.resp_);
        return 0;
    };
    if (chk_req_resp() != 0) return false;

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

int message_meta_test(bool crc_on_entire_message) {
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

        rr->setCrcOnEntireMessage(crc_on_entire_message);
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

bool test_read_meta_random_denial( std::atomic<bool>* start_denial,
                                   const asio_service::meta_cb_params& params,
                                   const std::string& meta )
{
    if ( !(start_denial->load()) ) return true;

    int r = std::rand();
    if (r % 25 == 0) return false;
    return true;
}

int message_meta_random_denial_test() {
    reset_log_files();

    std::string s1_addr = "127.0.0.1:20010";
    std::string s2_addr = "127.0.0.1:20020";
    std::string s3_addr = "127.0.0.1:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    std::atomic<size_t> write_req_cb_count(0);
    std::atomic<size_t> write_resp_cb_count(0);
    std::atomic<bool> start_denial(false);

    _msg("launching asio-raft servers with meta callback\n");
    for (RaftAsioPkg* rr: pkgs) {
        rr->setMetaCallback
            ( std::bind( test_read_meta_random_denial,
                         &start_denial,
                         std::placeholders::_1,
                         std::placeholders::_2 ),
              std::bind( test_write_req_meta,
                         &write_req_cb_count,
                         std::placeholders::_1 ),
              std::bind( test_read_meta_random_denial,
                         &start_denial,
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

    TestSuite::sleep_sec(1, "wait for Raft group ready");

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());

    start_denial = true;

    for (size_t ii=0; ii<100; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }

    TestSuite::sleep_sec(5, "wait for random denial");

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

int response_hint_test(bool with_meta) {
    reset_log_files();

    std::string s1_addr = "localhost:20010";
    std::string s2_addr = "localhost:20020";
    std::string s3_addr = "localhost:20030";

    RaftAsioPkg s1(1, s1_addr);
    RaftAsioPkg s2(2, s2_addr);
    RaftAsioPkg s3(3, s3_addr);
    std::vector<RaftAsioPkg*> pkgs = {&s1, &s2, &s3};

    _msg("launching asio-raft servers %s\n",
         with_meta ? "(with meta)" : "");
    std::atomic<size_t> read_req_cb_count(0);
    std::atomic<size_t> write_req_cb_count(0);
    std::atomic<size_t> read_resp_cb_count(0);
    std::atomic<size_t> write_resp_cb_count(0);
    for (RaftAsioPkg* ee: pkgs) {
        if (with_meta) {
            ee->setMetaCallback
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
    }
    CHK_Z( launch_servers(pkgs, false) );

    _msg("enable batch size hint with positive value\n");
    for (RaftAsioPkg* ee: pkgs) {
        ee->getTestSm()->set_next_batch_size_hint_in_bytes(1);
    }

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ(1, s1.raftServer->get_leader());
    CHK_EQ(1, s2.raftServer->get_leader());
    CHK_EQ(1, s3.raftServer->get_leader());
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    const size_t NUM = 100;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string msg_str = std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    _msg("disable batch size hint\n");
    for (RaftAsioPkg* ee: pkgs) {
        ee->getTestSm()->set_next_batch_size_hint_in_bytes(0);
    }

    for (size_t ii=0; ii<NUM; ++ii) {
        std::string msg_str = "2nd_" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication");

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    _msg("enable batch size hint with negative value\n");
    for (RaftAsioPkg* ee: pkgs) {
        ee->getTestSm()->set_next_batch_size_hint_in_bytes(-1);
    }

    TestSuite::sleep_sec(1, "wait peer's hint size info refreshed in leader side");

    // With negative hint size, append_entries will timeout due to
    // raft server can not commit. Set timeout to a small value.
    raft_params params = s1.raftServer->get_current_params();
    params.with_client_req_timeout(1000);
    s1.raftServer->update_params(params);

    for (size_t ii=0; ii<3; ++ii) {
        std::string msg_str = "3rd_" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(sizeof(uint32_t) + msg_str.size());
        buffer_serializer bs(msg);
        bs.put_str(msg_str);
        s1.raftServer->append_entries( {msg} );
    }
    TestSuite::sleep_sec(1, "wait for replication but actually no replication happen");

    // State machine should be identical. All are not committed.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    if (with_meta) {
        // Callback functions for meta should have been called.
        CHK_GT(read_req_cb_count.load(), 0);
        CHK_GT(write_req_cb_count.load(), 0);
        CHK_GT(read_resp_cb_count.load(), 0);
        CHK_GT(write_resp_cb_count.load(), 0);
        _msg( "read req callback %zu, write req callback %zu\n",
              read_req_cb_count.load(), write_req_cb_count.load() );
        _msg( "read resp callback %zu, write resp callback %zu\n",
              read_resp_cb_count.load(), write_resp_cb_count.load() );
    }

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

}  // namespace req_resp_meta_test;
using namespace req_resp_meta_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    ts.doTest( "message meta test",
               message_meta_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "empty meta test",
               empty_meta_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "message meta random denial test",
               message_meta_random_denial_test );

    ts.doTest( "response hint test",
               response_hint_test,
               TestRange<bool>( {false, true} ) );


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

