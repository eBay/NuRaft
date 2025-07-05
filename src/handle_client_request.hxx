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

#pragma once

#include "async.hxx"
#include "buffer.hxx"
#include "event_awaiter.hxx"
#include "internal_timer.hxx"
#include "ptr.hxx"
#include "raft_server.hxx"

namespace nuraft {

struct raft_server::commit_ret_elem {
    commit_ret_elem()
        : ret_value_(nullptr)
        , result_code_(cmd_result_code::OK)
        , async_result_(nullptr)
        , callback_invoked_(false)
        {}

    ~commit_ret_elem() {}

    ulong idx_;
    EventAwaiter awaiter_;
    timer_helper timer_;
    ptr<buffer> ret_value_;
    cmd_result_code result_code_;
    ptr< cmd_result< ptr<buffer> > > async_result_;
    bool callback_invoked_;
};

struct raft_server::sm_watcher_elem {
    sm_watcher_elem()
        : idx_(0)
        {}

    uint64_t idx_;

    /**
     * Watchers subscribing to the state machine commit of `idx_`.
     */
    std::list<ptr<cmd_result<bool>>> watchers_;
};

} // namespace nuraft;

