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

#pragma once

#include "nuraft.hxx"
#include "raft_group_dispatcher.hxx"
#include <map>

namespace nuraft {

/**
 * Helper class to skip the details of ASIO settings.
 */
class raft_launcher {
public:
    raft_launcher();

    /**
     * Initialize ASIO service and Raft server.
     *
     * @param sm State machine.
     * @param smgr State manager.
     * @param lg Logger.
     * @param port_number Port number.
     * @param asio_options ASIO options.
     * @param params Raft parameters.
     * @param opt Raft server init options.
     * @param raft_callback Callback function for hooking the operation.
     * @return Raft server instance.
     *         `nullptr` on any errors.
     */
    ptr<raft_server> init(ptr<state_machine> sm,
                          ptr<state_mgr> smgr,
                          ptr<logger> lg,
                          int port_number,
                          const asio_service::options& asio_options,
                          const raft_params& params,
                          const raft_server::init_options& opt = raft_server::init_options());

    /**
     * Initialize shared port mode for multiple Raft groups.
     * Creates ASIO service and listener, but does not create any raft_server.
     * Use add_group() to add Raft groups to this shared port.
     *
     * @param port_number Shared port number.
     * @param lg Logger.
     * @param asio_options ASIO options.
     * @return `true` on success, `false` on error.
     */
    bool init_shared_port(int port_number,
                          ptr<logger> lg,
                          const asio_service::options& asio_options);

    /**
     * Add a Raft group to the shared port.
     * Creates a raft_server and registers it with the dispatcher.
     *
     * @param group_id Unique group identifier.
     * @param sm State machine.
     * @param smgr State manager.
     * @param params Raft parameters.
     * @param opt Raft server init options.
     * @return 0 on success, -1 on failure (group_id already exists or creation failed).
     */
    int add_group(int32 group_id,
                  ptr<state_machine> sm,
                  ptr<state_mgr> smgr,
                  const raft_params& params,
                  const raft_server::init_options& opt = raft_server::init_options());

    /**
     * Remove a Raft group from the shared port.
     *
     * @param group_id Group identifier.
     * @return 0 on success, -1 if group_id not found.
     */
    int remove_group(int32 group_id);

    /**
     * Get the raft_server for a specific group.
     *
     * @param group_id Group identifier.
     * @return raft_server pointer, or nullptr if not found.
     */
    ptr<raft_server> get_server(int32 group_id);

    /**
     * Shutdown Raft server and ASIO service.
     * If this function is hanging even after the given timeout,
     * it will do force return.
     *
     * @param time_limit_sec Waiting timeout in seconds.
     * @return `true` on success.
     */
    bool shutdown(size_t time_limit_sec = 5);

    /**
     * Get ASIO service instance.
     *
     * @return ASIO service instance.
     */
    ptr<asio_service> get_asio_service() const { return asio_svc_; }

    /**
     * Get ASIO listener.
     *
     * @return ASIO listener.
     */
    ptr<rpc_listener> get_rpc_listener() const { return asio_listener_; }

    /**
     * Get Raft server instance (legacy mode).
     *
     * @return Raft server instance.
     */
    ptr<raft_server> get_raft_server() const { return raft_instance_; }

private:
    ptr<asio_service> asio_svc_;
    ptr<rpc_listener> asio_listener_;
    ptr<raft_server> raft_instance_;

    // Shared port mode members
    ptr<raft_group_dispatcher> dispatcher_;
    ptr<logger> logger_;
    std::map<int32, ptr<raft_server>> servers_;
    bool shared_port_mode_;
    std::mutex servers_lock_;
};

}

