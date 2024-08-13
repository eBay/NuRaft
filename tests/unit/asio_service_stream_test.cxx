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

#include "buffer_serializer.hxx"
#include "debugging_options.hxx"
#include "in_memory_log_store.hxx"
#include "raft_package_asio.hxx"

#include "event_awaiter.hxx"
#include "test_common.h"

#include <unordered_map>

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

namespace asio_service_stream_test {
    const std::string test_msg = "stream-test-msg-str";

    class stream_msg_handler : public nuraft::msg_handler {
        public:
        stream_msg_handler(context* ctx, const init_options& opt, ptr<logger_wrapper> log_wrapper) : 
        msg_handler(ctx, opt),
        my_log_wrapper(log_wrapper),
        streamed_log_index(0)
        {}

        ptr<resp_msg> process_req(req_msg& req,
                                const req_ext_params& ext_params) 
        {
            ptr<resp_msg> resp = cs_new<resp_msg>( state_->get_term(),
                                        msg_type::append_entries_response,
                                        id_,
                                        req.get_src(),
                                        log_store_->next_slot() );
            if (req.get_last_log_idx() == streamed_log_index) {
                streamed_log_index++;
                resp->accept(streamed_log_index.load());
                ptr<buffer> buf = req.log_entries().at(0)->get_buf_ptr();
                buf->pos(0);
                std::string buf_str = buf->get_str();
                if (buf_str != test_msg) {
                    SimpleLogger* ll = my_log_wrapper->getLogger();
                    _log_info(ll, "resp str: %s", buf_str.c_str());
                    msg_mismatch.store(true);
                }
            } else {
                SimpleLogger* ll = my_log_wrapper->getLogger();
                _log_info(ll, "req log index not match, req: %ld, current: %ld", 
                req.get_last_log_idx(), streamed_log_index.load());
            }
            return resp;
        }

        ptr<logger_wrapper> my_log_wrapper;
        std::atomic<ulong> streamed_log_index;
        std::atomic<bool> msg_mismatch;
    };

    class stream_server {
        public:
        stream_server(int id, int port) : 
        my_id(id),
        port(port),
        response_log_index(1)

        {
            init_server();
        }

        void send_req(int count) {
            ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
            msg->put(test_msg);

            while(count > 0) {
                ptr<req_msg> req ( cs_new<req_msg>
                ( 1, msg_type::append_entries_request, 1, my_id,
                1, send_log_index, 1 ) );

                ptr<log_entry> log( cs_new<log_entry>
                ( 0, msg, log_val_type::app_log ) );
                req->log_entries().push_back(log);

                rpc_handler h = (rpc_handler)std::bind
                    ( &stream_server::handle_result,
                        this,
                        req,
                        std::placeholders::_1,
                        std::placeholders::_2 );
                my_client->send(req, h);
                send_log_index++;
                count--;
            }
        }

        void handle_result(ptr<req_msg>& req, ptr<resp_msg>& resp, ptr<rpc_exception>& err) {
            if (resp->get_next_idx() == get_next_log_index()) {
                response_log_index++;
            } else {
                SimpleLogger* ll = my_log_wrapper->getLogger();
                _log_info(ll, "resp log index not match, resp: %ld, current: %ld", 
                resp->get_next_idx(), get_next_log_index());
            }
        }

        void stop_server() {
            if (my_listener) {
                my_listener->stop();
                my_listener->shutdown();
            }

            if (asio_svc) {
                asio_svc->stop();
                size_t count = 0;
                while (asio_svc->get_active_workers() && count < 500) {
                    // 10ms per tick.
                    timer_helper::sleep_ms(10);
                    count++;
                }
            }
        }

        ulong get_resp_log_index() {
            return my_msg_handler->streamed_log_index;
        }

        bool is_msg_mismatch() {
            return my_msg_handler->msg_mismatch;
        }

        ulong get_next_log_index() {
            return response_log_index;
        }

        private:
        int my_id;
        int port;
        std::atomic<ulong> response_log_index;
        ulong send_log_index = 0;
        ptr<asio_service> asio_svc;
        ptr<rpc_client> my_client;
        ptr<rpc_listener> my_listener;
        ptr<logger_wrapper> my_log_wrapper;
        ptr<logger> my_log;
        ptr<stream_msg_handler> my_msg_handler;

        void init_server() {
            std::string log_file_name = "./srv" + std::to_string(my_id) + ".log";
            my_log_wrapper = cs_new<logger_wrapper>(log_file_name);
            my_log = my_log_wrapper;

            // opts
            asio_service::options asio_opt;
            asio_opt.thread_pool_size_  = 2;
            asio_opt.replicate_log_timestamp_ = false;
            asio_opt.streaming_mode_ = true;
            asio_svc = cs_new<asio_service>(asio_opt, my_log);

            // client
            std::string endpoint = "localhost:"+std::to_string(port);
            my_client = asio_svc->create_client(endpoint);

            // server
            ptr<state_mgr> s_mgr = cs_new<TestMgr>(my_id, endpoint);
            ptr<state_machine> sm = cs_new<TestSm>( my_log_wrapper->getLogger() );
            ptr<delayed_task_scheduler> scheduler = asio_svc;
            ptr<rpc_client_factory> rpc_cli_factory = asio_svc;
            my_listener = asio_svc->create_rpc_listener(port, my_log);

            raft_params params;
            context* ctx( new context( s_mgr, sm, my_listener, my_log,
                        rpc_cli_factory, scheduler, params ) );
            const raft_server::init_options& opt = raft_server::init_options();
            my_msg_handler = cs_new<stream_msg_handler>(ctx, opt, my_log_wrapper);
            ptr<msg_handler> handler = my_msg_handler;
            my_listener->listen(handler);
        }
    };

    int init_stream_server() {
        reset_log_files();

        stream_server s(1, 20010);
        // send request
        int count = 1000;
        s.send_req(count);

        // check req
        TestSuite::sleep_ms(1000, "wait for sending req");
        CHK_EQ(count, s.get_resp_log_index());
        CHK_EQ(count, s.get_next_log_index() - 1);
        CHK_FALSE(s.is_msg_mismatch());

        // stop
        s.stop_server();
        TestSuite::sleep_sec(1, "shutting down");
        SimpleLogger::shutdown();
        return 0;
    }
};

using namespace asio_service_stream_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);
    ts.options.printTestMessage = true;

    ts.doTest( "init_stream_server",
               init_stream_server );
    return 0;
}