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

#include "launcher.hxx"

// LCOV_EXCL_START

namespace nuraft {

raft_launcher::raft_launcher()
    : asio_svc_(nullptr)
    , asio_listener_(nullptr)
    , raft_instance_(nullptr)
    {}

ptr<raft_server> raft_launcher::init(ptr<state_machine> sm,
                                     ptr<state_mgr> smgr,
                                     ptr<logger> lg,
                                     int port_number,
                                     const asio_service::options& asio_options,
                                     const raft_params& params_given)
{
    asio_svc_ = cs_new<asio_service>(asio_options, lg);
    asio_listener_ = asio_svc_->create_rpc_listener(port_number, lg);
    ptr<delayed_task_scheduler> scheduler = asio_svc_;
    ptr<rpc_client_factory> rpc_cli_factory = asio_svc_;

    context* ctx = new context( smgr,
                                sm,
                                asio_listener_,
                                lg,
                                rpc_cli_factory,
                                scheduler,
                                params_given );
    raft_instance_ = cs_new<raft_server>(ctx);
    asio_listener_->listen( raft_instance_ );
    return raft_instance_;
}

bool raft_launcher::shutdown(size_t time_limit_sec) {
    if (!raft_instance_) return false;

    raft_instance_->shutdown();
    raft_instance_.reset();

    if (asio_listener_) {
        asio_listener_->stop();
        asio_listener_->shutdown();
    }
    if (asio_svc_) {
        asio_svc_->stop();
        size_t count = 0;
        while ( asio_svc_->get_active_workers() &&
                count < time_limit_sec * 100 ) {
            // 10ms per tick.
            timer_helper::sleep_ms(10);
            count++;
        }
    }
    if (asio_svc_->get_active_workers()) return false;
    return true;
}

}

// LCOV_EXCL_STOP

