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

#ifndef _RAFT_GROUP_DISPATCHER_HXX_
#define _RAFT_GROUP_DISPATCHER_HXX_

#include "ptr.hxx"
#include "raft_server.hxx"
#include "req_msg.hxx"
#include "resp_msg.hxx"

#include <map>
#include <mutex>
#include <vector>

namespace nuraft {

// Forward declaration to avoid circular dependency
class raft_server;

/**
 * Raft group dispatcher for managing multiple Raft groups on a single port.
 *
 * This class enables multiple Raft groups to share the same TCP port by
 * dispatching incoming RPC requests to the appropriate Raft server based
 * on the group_id in the message header.
 */
class raft_group_dispatcher {
public:
    /**
     * Constructor
     */
    raft_group_dispatcher();

    /**
     * Destructor
     */
    virtual ~raft_group_dispatcher();

    /**
     * Register a Raft group with the dispatcher.
     *
     * @param group_id  Unique identifier for the Raft group
     * @param server    Pointer to the raft_server instance
     * @return          0 on success, -1 if group_id already exists
     */
    int register_group(int32 group_id, ptr<raft_server>& server);

    /**
     * Deregister a Raft group from the dispatcher.
     *
     * @param group_id  Identifier of the group to remove
     * @return          0 on success, -1 if group_id not found
     */
    int deregister_group(int32 group_id);

    /**
     * Dispatch an RPC request to the appropriate Raft group.
     *
     * @param group_id      Target group identifier
     * @param req           Request message
     * @param ext_params    Optional extended parameters
     * @return              Response message, or nullptr if group not found
     */
    ptr<resp_msg> dispatch(int32 group_id,
                           req_msg& req,
                           const raft_server::req_ext_params* ext_params = nullptr);

    /**
     * Check if a group is registered.
     *
     * @param group_id  Group identifier to check
     * @return          true if group exists, false otherwise
     */
    bool group_exists(int32 group_id) const;

    /**
     * Get the number of registered groups.
     *
     * @return  Number of groups
     */
    size_t get_group_count() const;

    /**
     * Get all registered group IDs.
     *
     * @return  Vector of group IDs
     */
    std::vector<int32> get_group_ids() const;

private:
    /**
     * Internal structure to track group information
     */
    struct group_entry {
        ptr<raft_server> server;
        int64_t last_activity;

        group_entry()
            : server()
            , last_activity(0)
            {}

        group_entry(ptr<raft_server> s)
            : server(s)
            , last_activity(0)
            {}
    };

    /**
     * Map of group_id to group_entry
     */
    std::map<int32, group_entry> groups_;

    /**
     * Mutex for thread-safe access to groups_ map
     */
    mutable std::mutex lock_;
};

} // namespace nuraft

#endif //_RAFT_GROUP_DISPATCHER_HXX_
