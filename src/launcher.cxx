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
    , dispatcher_()
    , logger_()
    , servers_()
    , shared_port_mode_(false)
    {}

ptr<raft_server> raft_launcher::init(ptr<state_machine> sm,
                                     ptr<state_mgr> smgr,
                                     ptr<logger> lg,
                                     int port_number,
                                     const asio_service::options& asio_options,
                                     const raft_params& params_given,
                                     const raft_server::init_options& opt)
{
    asio_svc_ = cs_new<asio_service>(asio_options, lg);
    asio_listener_ = asio_svc_->create_rpc_listener(port_number, lg);
    if (!asio_listener_) return nullptr;

    ptr<delayed_task_scheduler> scheduler = asio_svc_;
    ptr<rpc_client_factory> rpc_cli_factory = asio_svc_;

    context* ctx = new context( smgr,
                                sm,
                                asio_listener_,
                                lg,
                                rpc_cli_factory,
                                scheduler,
                                params_given );
    raft_instance_ = cs_new<raft_server>(ctx, opt);
    asio_listener_->listen( raft_instance_ );
    return raft_instance_;
}

bool raft_launcher::init_shared_port(int port_number,
                                     ptr<logger> lg,
                                     const asio_service::options& asio_options)
{
    if (shared_port_mode_) {
        // Already initialized in shared port mode
        return false;
    }
    if (asio_svc_ || asio_listener_ || raft_instance_) {
        // Already initialized in legacy mode
        return false;
    }

    asio_svc_ = cs_new<asio_service>(asio_options, lg);
    asio_listener_ = asio_svc_->create_rpc_listener(port_number, lg);
    if (!asio_listener_) return false;

    // Create dispatcher for routing requests to different groups
    dispatcher_ = cs_new<raft_group_dispatcher>();

    // Set dispatcher to listener
    asio_listener_->set_dispatcher(dispatcher_);

    // Note: listen() will be called in add_group() when the first group is added

    // Save logger for later use in add_group
    logger_ = lg;
    shared_port_mode_ = true;

    return true;
}

int raft_launcher::add_group(int32 group_id,
                             ptr<state_machine> sm,
                             ptr<state_mgr> smgr,
                             const raft_params& params,
                             const raft_server::init_options& opt)
{
    if (!shared_port_mode_) {
        // Not in shared port mode
        return -1;
    }
    if (!dispatcher_ || !asio_listener_ || !asio_svc_) {
        // Not properly initialized
        return -1;
    }

    std::lock_guard<std::mutex> l(servers_lock_);

    // Check if group_id already exists
    if (servers_.find(group_id) != servers_.end()) {
        return -1;
    }

    // Create context for this group
    ptr<delayed_task_scheduler> scheduler = asio_svc_;
    ptr<rpc_client_factory> rpc_cli_factory = asio_svc_;

    context* ctx = new context(smgr,
                                sm,
                                asio_listener_,
                                logger_,
                                rpc_cli_factory,
                                scheduler,
                                params,
                                nullptr,  // custom_global_mgr
                                group_id);  // Pass group_id to context

    // Create raft_server instance
    ptr<raft_server> server = cs_new<raft_server>(ctx, opt);
    if (!server) {
        return -1;
    }

    // Verify group_id is correctly set
    if (ctx->group_id_ != group_id) {
        // Context group_id mismatch, this shouldn't happen
        return -1;
    }

    // Register with dispatcher
    int ret = dispatcher_->register_group(group_id, server);
    if (ret != 0) {
        // Registration failed
        return -1;
    }

    // Store server
    servers_[group_id] = server;

    // If this is the first group, also set it as the handler
    // for backward compatibility and to handle group_id == 0 messages
    if (servers_.size() == 1) {
        asio_listener_->listen(server);
    }

    return 0;
}

int raft_launcher::remove_group(int32 group_id) {
    if (!shared_port_mode_) {
        return -1;
    }

    std::lock_guard<std::mutex> l(servers_lock_);

    auto it = servers_.find(group_id);
    if (it == servers_.end()) {
        return -1;
    }

    // Deregister from dispatcher
    dispatcher_->deregister_group(group_id);

    // Shutdown the server
    ptr<raft_server>& server = it->second;
    if (server) {
        server->shutdown();
        server.reset();
    }

    // Remove from map
    servers_.erase(it);

    return 0;
}

ptr<raft_server> raft_launcher::get_server(int32 group_id) {
    if (!shared_port_mode_) {
        return nullptr;
    }

    std::lock_guard<std::mutex> l(servers_lock_);

    auto it = servers_.find(group_id);
    if (it == servers_.end()) {
        return nullptr;
    }

    return it->second;
}

bool raft_launcher::shutdown(size_t time_limit_sec) {
    // Handle shared port mode
    if (shared_port_mode_) {
        // Shutdown all servers
        {
            std::lock_guard<std::mutex> l(servers_lock_);
            for (auto& entry : servers_) {
                ptr<raft_server>& server = entry.second;
                if (server) {
                    server->shutdown();
                    server.reset();
                }
            }
            servers_.clear();
        }

        // Clear dispatcher
        if (dispatcher_) {
            dispatcher_.reset();
        }

        shared_port_mode_ = false;
    } else {
        // Legacy mode
        if (!raft_instance_) return false;

        raft_instance_->shutdown();
        raft_instance_.reset();
    }

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

