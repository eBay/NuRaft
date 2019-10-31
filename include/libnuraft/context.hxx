/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/datatechnology/cornerstone

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

#ifndef _CONTEXT_HXX_
#define _CONTEXT_HXX_

#include "callback.hxx"
#include "pp_util.hxx"
#include "ptr.hxx"
#include "raft_params.hxx"

#include <memory>
#include <mutex>

namespace nuraft {

class delayed_task_scheduler;
class logger;
class rpc_client_factory;
class rpc_listener;
class state_machine;
class state_mgr;
struct context {
public:
    context( ptr<state_mgr>& mgr,
             ptr<state_machine>& m,
             ptr<rpc_listener>& listener,
             ptr<logger>& l,
             ptr<rpc_client_factory>& cli_factory,
             ptr<delayed_task_scheduler>& scheduler,
             const raft_params& params )
        : state_mgr_(mgr)
        , state_machine_(m)
        , rpc_listener_(listener)
        , logger_(l)
        , rpc_cli_factory_(cli_factory)
        , scheduler_(scheduler)
        , params_( cs_new<raft_params>(params) )
        {}

    void set_cb_func(cb_func::func_type func) {
        cb_func_ = cb_func(func);
    }

    ptr<raft_params> get_params() const {
        std::lock_guard<std::mutex> l(ctx_lock_);
        return params_;
    }

    void set_params(ptr<raft_params>& to) {
        std::lock_guard<std::mutex> l(ctx_lock_);
        params_ = to;
    }

    __nocopy__(context);

public:
    // State manager instance.
    ptr<state_mgr> state_mgr_;

    // State machine instance.
    ptr<state_machine> state_machine_;

    // RPC listener instance.
    ptr<rpc_listener> rpc_listener_;

    // System logger instance.
    ptr<logger> logger_;

    // RPC client factory.
    ptr<rpc_client_factory> rpc_cli_factory_;

    // Timer instance.
    ptr<delayed_task_scheduler> scheduler_;

    // Raft parameters.
    std::shared_ptr<raft_params> params_;

    // Callback function for hooking the operation.
    cb_func cb_func_;

    // Lock.
    mutable std::mutex ctx_lock_;
};

}

#endif //_CONTEXT_HXX_
