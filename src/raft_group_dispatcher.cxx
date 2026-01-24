/************************************************************************
Modifications Copyright 2025

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

#include "libnuraft/raft_group_dispatcher.hxx"

#include <cassert>
#include <chrono>

namespace nuraft {

raft_group_dispatcher::raft_group_dispatcher()
    : groups_()
    , lock_()
{
}

raft_group_dispatcher::~raft_group_dispatcher() {
    std::lock_guard<std::mutex> l(lock_);
    groups_.clear();
}

int raft_group_dispatcher::register_group(int32 group_id, ptr<raft_server>& server) {
    if (!server) {
        return -1;
    }

    std::lock_guard<std::mutex> l(lock_);

    // Check if group_id already exists
    if (groups_.find(group_id) != groups_.end()) {
        return -1;
    }

    // Add new group
    groups_[group_id] = group_entry(server);

    return 0;
}

int raft_group_dispatcher::deregister_group(int32 group_id) {
    std::lock_guard<std::mutex> l(lock_);

    auto it = groups_.find(group_id);
    if (it == groups_.end()) {
        return -1;
    }

    groups_.erase(it);
    return 0;
}

ptr<resp_msg> raft_group_dispatcher::dispatch(int32 group_id,
                                               req_msg& req,
                                               const raft_server::req_ext_params* ext_params)
{
    // Find the target group
    ptr<raft_server> server;
    {
        std::lock_guard<std::mutex> l(lock_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            // Group not found
            return ptr<resp_msg>();
        }

        server = it->second.server;

        // Update last activity time
        it->second.last_activity = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    // Dispatch request to the Raft server
    // Note: We release the lock before calling process_req to avoid potential deadlock
    if (ext_params) {
        return server->process_req(req, *ext_params);
    } else {
        raft_server::req_ext_params empty_params;
        return server->process_req(req, empty_params);
    }
}

bool raft_group_dispatcher::group_exists(int32 group_id) const {
    std::lock_guard<std::mutex> l(lock_);
    return groups_.find(group_id) != groups_.end();
}

size_t raft_group_dispatcher::get_group_count() const {
    std::lock_guard<std::mutex> l(lock_);
    return groups_.size();
}

std::vector<int32> raft_group_dispatcher::get_group_ids() const {
    std::lock_guard<std::mutex> l(lock_);
    std::vector<int32> ids;
    ids.reserve(groups_.size());

    for (const auto& entry : groups_) {
        ids.push_back(entry.first);
    }

    return ids;
}

} // namespace nuraft
