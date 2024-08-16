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

    class stream_msg_handler : public nuraft::msg_handler {
    public:
        stream_msg_handler(context* ctx,
                           const init_options& opt,
                           ptr<logger_wrapper> log_wrapper)
            : msg_handler(ctx, opt)
            , my_log_wrapper_(log_wrapper)
            , streamed_log_index(0)
            , msg_mismatch(false)
            {}

        ptr<resp_msg> process_req(req_msg& req, const req_ext_params& ext_params) {
            ptr<resp_msg> resp = cs_new<resp_msg>(state_->get_term(),
                                                  msg_type::append_entries_response,
                                                  id_,
                                                  req.get_src());
            if (req.get_last_log_idx() == streamed_log_index) {
                streamed_log_index++;
                resp->accept(streamed_log_index.load());
                ptr<buffer> buf = req.log_entries().at(0)->get_buf_ptr();
                buf->pos(0);
                std::string buf_str = buf->get_str();
                if (buf_str != TEST_MSG) {
                    SimpleLogger* ll = my_log_wrapper_->getLogger();
                    _log_info(ll, "resp str: %s", buf_str.c_str());
                    msg_mismatch.store(true);
                }
            } else {
                SimpleLogger* ll = my_log_wrapper_->getLogger();
                _log_info(ll, "req log index not match, req: %ld, current: %ld",
                req.get_last_log_idx(), streamed_log_index.load());
            }
            return resp;
        }

        ptr<logger_wrapper> my_log_wrapper_;
        std::atomic<ulong> streamed_log_index;
        std::atomic<bool> msg_mismatch;
    };

    class stream_server {
    public:
        stream_server(int id, int port)
            : my_id_(id)
            , port_(port)
            , next_log_index_(1)
        {
            init_server();
        }

        void send_req(int count) {
            ptr<buffer> msg = buffer::alloc(TEST_MSG.size() + 1);
            msg->put(TEST_MSG);

            TestSuite::Progress pp(count, "sending req");

            while (count > 0) {
                ptr<req_msg> req(cs_new<req_msg>(
                    1, msg_type::append_entries_request, 1, my_id_,
                    1, sent_log_index_, 1));

                ptr<log_entry> log(cs_new<log_entry>(0, msg, log_val_type::app_log));
                req->log_entries().push_back(log);

                rpc_handler h = (rpc_handler)std::bind(
                    &stream_server::handle_result,
                    this,
                    req,
                    std::placeholders::_1,
                    std::placeholders::_2);
                my_client_->send(req, h);
                sent_log_index_++;
                pp.update(sent_log_index_);
                count--;
            }
            pp.done();
            num_messages_sent_= sent_log_index_;
        }

        void handle_result(ptr<req_msg>& req,
                           ptr<resp_msg>& resp,
                           ptr<rpc_exception>& err)
        {
            if (resp->get_next_idx() == get_next_log_index()) {
                next_log_index_++;
            } else {
                SimpleLogger* ll = my_log_wrapper_->getLogger();
                _log_info(ll, "resp log index not match, resp: %ld, current: %ld",
                resp->get_next_idx(), get_next_log_index());
            }
            if (next_log_index_ == num_messages_sent_ + 1) {
                ea.invoke();
            }
        }

        bool waiting_for_responses(int timeout_ms = 3000) {
            TestSuite::_msg("wait for responses (up to %d ms)\n", timeout_ms);
            ea.wait_ms(timeout_ms);
            return (next_log_index_ == num_messages_sent_ + 1);
        }

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
        }

        ulong get_resp_log_index() {
            return my_msg_handler_->streamed_log_index;
        }

        bool is_msg_mismatch() {
            return my_msg_handler_->msg_mismatch;
        }

        ulong get_next_log_index() {
            return next_log_index_;
        }

    private:
        int my_id_;
        int port_;
        std::atomic<ulong> next_log_index_;
        ulong sent_log_index_ = 0;
        ptr<asio_service> asio_svc_;
        ptr<rpc_client> my_client_;
        ptr<rpc_listener> my_listener_;
        ptr<logger_wrapper> my_log_wrapper_;
        ptr<logger> my_log_;
        ptr<stream_msg_handler> my_msg_handler_;
        size_t num_messages_sent_ = 0;
        EventAwaiter ea;

        void init_server() {
            std::string log_file_name = "./srv" + std::to_string(my_id_) + ".log";
            my_log_wrapper_ = cs_new<logger_wrapper>(log_file_name);
            my_log_ = my_log_wrapper_;

            // opts
            asio_service::options asio_opt;
            asio_opt.thread_pool_size_  = 2;
            asio_opt.replicate_log_timestamp_ = false;
            asio_opt.streaming_mode_ = true;
            asio_svc_ = cs_new<asio_service>(asio_opt, my_log_);

            // client
            std::string endpoint = "localhost:"+std::to_string(port_);
            my_client_ = asio_svc_->create_client(endpoint);

            // server
            ptr<state_mgr> s_mgr = cs_new<TestMgr>(my_id_, endpoint);
            ptr<state_machine> sm = cs_new<TestSm>( my_log_wrapper_->getLogger() );
            ptr<delayed_task_scheduler> scheduler = asio_svc_;
            ptr<rpc_client_factory> rpc_cli_factory = asio_svc_;
            my_listener_ = asio_svc_->create_rpc_listener(port_, my_log_);

            raft_params params;
            context* ctx( new context( s_mgr, sm, my_listener_, my_log_,
                        rpc_cli_factory, scheduler, params ) );
            const raft_server::init_options& opt = raft_server::init_options();
            my_msg_handler_ = cs_new<stream_msg_handler>(ctx, opt, my_log_wrapper_);
            ptr<msg_handler> handler = my_msg_handler_;
            my_listener_->listen(handler);
        }
    };

    int stream_server_happy_path_test() {
        reset_log_files();

        stream_server s(1, 20010);
        // send request
        int count = 1000;
        s.send_req(count);

        // check req
        CHK_TRUE(s.waiting_for_responses());
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

    ts.doTest("stream server happy path test",
              stream_server_happy_path_test);
    return 0;
}
