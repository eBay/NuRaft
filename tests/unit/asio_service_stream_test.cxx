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

#include "buffer_serializer.hxx"
#include "in_memory_log_store.hxx"
#include "raft_package_asio.hxx"

#include "event_awaiter.hxx"
#include "test_common.h"

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

namespace asio_service_stream_test {
const std::string TEST_MSG = "stream-test-msg-str";

class stream_statistic {
public:
    stream_statistic() 
        : resp_log_index_(0)
        , msg_mismatch_(false)
        , reqs_out_of_order_(false)
        , next_log_index_(1)
        , sent_log_index_(0)
        , num_messages_sent_(0)
        , error_req_count_(0)
    {
        open_logs();
    }

    int wait_for_responses(int timeout_ms = 3000) {
        TestSuite::_msg("wait for responses (up to %d ms)\n", timeout_ms);
        ea_.wait_ms(timeout_ms);
        CHK_EQ(next_log_index_.load(), num_messages_sent_.load() + 1);
        return 0;
    }

    void wait_for_receiving_requests(int timeout_ms = 1000) {
        TestSuite::_msg("wait for receiving requests (up to %d ms)\n", timeout_ms);
        req_ea_.wait_ms(timeout_ms);
    }

    void open_logs() {
        std::string server_log_file_name = "./srv" + std::to_string(1) + ".log";
        server_logger_ = cs_new<logger_wrapper>(server_log_file_name);
        std::string client_log_file_name = "./srv" + std::to_string(2) + ".log";
        client_logger_ = cs_new<logger_wrapper>(client_log_file_name);
    }

    void reset() {
        // reset log
        reset_log_files();
        open_logs();

        // reset data
        resp_log_index_.store(0);
        msg_mismatch_.store(false);
        reqs_out_of_order_.store(false);
        next_log_index_.store(1);

        sent_log_index_ = 0;
        num_messages_sent_.store(0);
        error_req_count_.store(0);
        req_ea_.reset();
        ea_.reset();
    }

    // server
    std::atomic<ulong> resp_log_index_;
    std::atomic<bool> msg_mismatch_;
    std::atomic<bool> reqs_out_of_order_;
    
    // when server receives requests, it will be invoked 
    EventAwaiter req_ea_;

    // client
    std::atomic<ulong> next_log_index_;
    ulong sent_log_index_;
    std::atomic<ulong> num_messages_sent_;
    std::atomic<ulong> error_req_count_;
    EventAwaiter ea_;

    // log
    ptr<logger_wrapper> server_logger_;
    ptr<logger_wrapper> client_logger_;
};

static stream_statistic stream_stat;

class stream_msg_handler : public nuraft::msg_handler {
public:
    stream_msg_handler(context* ctx,
                        const init_options& opt)
        : msg_handler(ctx, opt)
        {}

    ptr<resp_msg> process_req(req_msg& req, const req_ext_params& ext_params) {
        ptr<resp_msg> resp = cs_new<resp_msg>(state_->get_term(),
                                                msg_type::append_entries_response,
                                                id_,
                                                req.get_src());
        SimpleLogger* ll = stream_stat.server_logger_->getLogger();
         _log_info(ll, "req log idx: %ld, current resp log idx: %ld", 
         req.get_last_log_idx(), stream_stat.resp_log_index_.load());
        if (req.get_last_log_idx() == stream_stat.resp_log_index_.load()) {
            stream_stat.resp_log_index_++;
            resp->accept(stream_stat.resp_log_index_.load());
            ptr<buffer> buf = req.log_entries().at(0)->get_buf_ptr();
            buf->pos(0);
            std::string buf_str = buf->get_str();
            if (buf_str != TEST_MSG) {
                SimpleLogger* ll = stream_stat.server_logger_->getLogger();
                _log_info(ll, "resp str: %s", buf_str.c_str());
                stream_stat.msg_mismatch_.store(true);
            }
        } else {
            stream_stat.reqs_out_of_order_.store(true);
        }

        stream_stat.req_ea_.invoke();
        return resp;
    }
};

class stream_server {
public:
    stream_server(int id, 
                    int port)
        : my_id_(id)
        , port_(port)
    {}

    void stop_server() {
        if (my_listener_) {
            my_listener_->stop();
            my_listener_->shutdown();
        }

        if (asio_svc_) {
            asio_svc_->stop();
            size_t count = 0;
            while (asio_svc_->get_active_workers() && count < 500) {
                // 10ms per tick.
                timer_helper::sleep_ms(10);
                count++;
            }
        }

        if (my_msg_handler_) {
            my_msg_handler_->shutdown();
        }
    }

    void init_server() {
        ptr<logger> logger = stream_stat.server_logger_;
        // opts
        asio_service::options asio_opt;
        asio_opt.thread_pool_size_  = 2;
        asio_opt.replicate_log_timestamp_ = false;
        asio_opt.streaming_mode_ = true;
        std::string endpoint = "localhost:"+std::to_string(port_);
        asio_svc_ = cs_new<asio_service>(asio_opt, stream_stat.server_logger_);

        // server
        s_mgr_ = cs_new<TestMgr>(my_id_, endpoint);
        sm_ = cs_new<TestSm>( stream_stat.server_logger_->getLogger() );
        ptr<delayed_task_scheduler> scheduler = asio_svc_;
        ptr<rpc_client_factory> rpc_cli_factory = asio_svc_;
        
        my_listener_ = asio_svc_->create_rpc_listener(port_, logger);

        raft_params params;
        context* ctx( new context( s_mgr_, sm_, my_listener_, logger,
                    rpc_cli_factory, scheduler, params ) );
        const raft_server::init_options& opt = raft_server::init_options();
        my_msg_handler_ = cs_new<stream_msg_handler>(ctx, opt);
        ptr<msg_handler> handler = my_msg_handler_;
        my_listener_->listen(handler);
    }

    int my_id_;
    int port_;
    ptr<state_mgr> s_mgr_;
    ptr<state_machine> sm_;
    ptr<asio_service> asio_svc_;
    ptr<rpc_listener> my_listener_;
    ptr<stream_msg_handler> my_msg_handler_;
};

class stream_client {
public:
    stream_client(int id, 
                    int port)
        : my_id_(id)
        , port_(port)
    {
        asio_service::options asio_opt;
        asio_opt.thread_pool_size_  = 2;
        asio_opt.replicate_log_timestamp_ = false;
        asio_opt.streaming_mode_ = true;
        asio_svc_ = cs_new<asio_service>(asio_opt, stream_stat.client_logger_);

        std::string endpoint = "localhost:"+std::to_string(port_);
        client_ = asio_svc_->create_client(endpoint);
    }

    void send_req(int count, int timeout_ms=0) {
        ptr<buffer> msg = buffer::alloc(TEST_MSG.size() + 1);
        msg->put(TEST_MSG);

        TestSuite::Progress pp(count, "sending req");

        while (count > 0) {
            ptr<req_msg> req(cs_new<req_msg>(
                1, msg_type::append_entries_request, 1, my_id_,
                1, stream_stat.sent_log_index_, 1));

            ptr<log_entry> log(cs_new<log_entry>(0, msg, log_val_type::app_log));
            req->log_entries().push_back(log);

            rpc_handler h = (rpc_handler)std::bind(
                &stream_client::handle_result,
                this,
                req,
                std::placeholders::_1,
                std::placeholders::_2);
            client_->send(req, h, timeout_ms);
            stream_stat.sent_log_index_++;
            pp.update(stream_stat.sent_log_index_);
            count--;
        }
        pp.done();
        stream_stat.num_messages_sent_.store(stream_stat.sent_log_index_);
    }

    void handle_result(ptr<req_msg>& req,
                        ptr<resp_msg>& resp,
                        ptr<rpc_exception>& err)
    {
        if (err) {
            stream_stat.error_req_count_++;
            stream_stat.next_log_index_++;
            SimpleLogger* ll = stream_stat.client_logger_->getLogger();
            _log_info(ll, "handle result err: %s, error_count: %ld, next log idx: %ld",
            err->what(), stream_stat.error_req_count_.load(), 
            stream_stat.next_log_index_.load());
        } else {
            if (resp->get_next_idx() == stream_stat.next_log_index_.load()) {
                stream_stat.next_log_index_++;
            } else {
                SimpleLogger* ll = stream_stat.client_logger_->getLogger();
                _log_info(ll, "resp log index not match, resp: %ld, current: %ld",
                resp->get_next_idx(), stream_stat.next_log_index_.load());
            }
        }

        if (stream_stat.next_log_index_ == stream_stat.num_messages_sent_ + 1) {
            stream_stat.ea_.invoke();
        }
    }

    void stop() {
        if (asio_svc_) {
            asio_svc_->stop();
            size_t count = 0;
            while (asio_svc_->get_active_workers() && count < 500) {
                // 10ms per tick.
                timer_helper::sleep_ms(10);
                count++;
            }
        }
    }

    int my_id_;
    int port_;
    ptr<asio_service> asio_svc_;
    ptr<rpc_client> client_;
};

int stream_server_happy_path_test() {
    stream_stat.reset();

    stream_server s(1, 20010);
    s.init_server();
    // send request
    int count = 1000;
    stream_client client(2, 20010);
    client.send_req(count);

    // check req
    CHK_Z(stream_stat.wait_for_responses());
    CHK_EQ(count, stream_stat.resp_log_index_.load());
    CHK_EQ(count, stream_stat.next_log_index_ - 1);
    CHK_FALSE(stream_stat.msg_mismatch_.load());

    // stop
    client.stop();
    s.stop_server();
    TestSuite::sleep_sec(1, "shutting down");
    SimpleLogger::shutdown();
    return 0;
}

int client_send_to_wrong_endpoint_test() {
    stream_stat.reset();

    stream_server s(1, 20010);
    s.init_server();
    // send request
    int count = 1000;
    stream_client client(2, 20011);
    client.send_req(count);
    
    // check req if finish
    CHK_Z(stream_stat.wait_for_responses());
    CHK_EQ(count, stream_stat.error_req_count_);
    CHK_EQ(count, stream_stat.next_log_index_ - 1);
    CHK_FALSE(stream_stat.msg_mismatch_.load());

    // stop
    client.stop();
    s.stop_server();
    TestSuite::sleep_sec(1, "shutting down");
    SimpleLogger::shutdown();
    return 0;
}

int client_close_after_sending_test() {
    stream_stat.reset();

    stream_server s(1, 20010);
    s.init_server();
    // send request
    int count = 1000;
    {
        stream_client client(2, 20010);
        stream_stat.wait_for_receiving_requests();
        client.send_req(count);
        client.stop();
    }

    // check req if finish
    CHK_Z(stream_stat.wait_for_responses());
    CHK_TRUE(stream_stat.error_req_count_.load() > 0);
    CHK_EQ(count, stream_stat.next_log_index_.load() - 1);
    CHK_FALSE(stream_stat.msg_mismatch_.load());
    CHK_FALSE(stream_stat.reqs_out_of_order_.load());

    // stop
    s.stop_server();
    TestSuite::sleep_sec(1, "shutting down");
    SimpleLogger::shutdown();
    return 0;
}

int server_timeout_test() {
    stream_stat.reset();

    stream_server s(1, 20010);
    s.init_server();
    // send request
    int count = 1000;
    stream_client client(2, 20010);
    client.send_req(count, 2000);
    stream_stat.wait_for_receiving_requests();
    
    // shutdown
    s.stop_server();

    // check req if finish
    CHK_Z(stream_stat.wait_for_responses());
    CHK_TRUE(stream_stat.error_req_count_.load() > 0);
    CHK_EQ(count, stream_stat.next_log_index_.load() - 1);
    CHK_FALSE(stream_stat.msg_mismatch_.load());
    CHK_FALSE(stream_stat.reqs_out_of_order_.load());

    client.stop();
    TestSuite::sleep_sec(1, "shutting down");
    SimpleLogger::shutdown();
    return 0;
}

int server_close_after_sending_test() {
    stream_stat.reset();

    stream_server* s = new stream_server(1, 20010);
    s->init_server();
    // send request
    int count = 1000;
    stream_client client(2, 20010);
    client.send_req(count);
    stream_stat.wait_for_receiving_requests();
    
    // shutdown
    s->stop_server();
    TestSuite::sleep_sec(1, "server shutting down");
    delete s;

    // check req if finish
    CHK_Z(stream_stat.wait_for_responses());
    CHK_TRUE(stream_stat.error_req_count_.load() > 0);
    CHK_EQ(count, stream_stat.next_log_index_.load() - 1);
    CHK_FALSE(stream_stat.msg_mismatch_.load());
    CHK_FALSE(stream_stat.reqs_out_of_order_.load());

    client.stop();
    TestSuite::sleep_sec(1, "client shutting down");
    SimpleLogger::shutdown();
    return 0;
}
};

using namespace asio_service_stream_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);
    ts.options.printTestMessage = true;

    ts.doTest("stream server happy path test",
              stream_server_happy_path_test);
    ts.doTest("client send msg to wrong endpoint test",
              client_send_to_wrong_endpoint_test);
    ts.doTest("client close after sending test",
              client_close_after_sending_test);
    ts.doTest("server timeout test",
              server_timeout_test);
    ts.doTest("server close after sending test",
              server_close_after_sending_test);
    return 0;
}
