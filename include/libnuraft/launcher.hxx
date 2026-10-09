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
     *
     * This method initializes the ASIO service and creates a TCP listener on the
     * specified port, but does NOT create any raft_server instances. It sets up
     * the infrastructure needed for port sharing, including a dispatcher that
     * routes incoming requests to the appropriate Raft group based on group_id.
     *
     * After calling this method, use init_with_group_id() to create individual
     * Raft groups. Each group will share the same port but have its own state
     * machine, state manager, and logger.
     *
     * Important: This method cannot be called if the launcher has already been
     * initialized with the init() method (legacy mode). Once initialized in
     * shared port mode, only init_with_group_id() can be used to add groups.
     *
     * @param port_number Shared port number for all Raft groups.
     * @param lg Logger for the launcher (not for individual groups).
     * @param asio_options ASIO service options. For port sharing, consider
     *        setting header_version_ to 1 to enable extended header format.
     * @return `true` on success, `false` on error (e.g., already initialized).
     */
    bool init_shared_port(int port_number,
                          ptr<logger> lg,
                          const asio_service::options& asio_options);


    /**
     * Initialize a Raft group with its own resources (for port sharing mode).
     *
     * This method MUST be called after init_shared_port(). It creates a new
     * Raft group with its own raft_server instance, state machine, state manager,
     * and logger. Multiple groups can be added to the same shared port.
     *
     * Each group operates independently but shares the network port. Incoming
     * requests are routed to the appropriate group based on the group_id field
     * in the message header.
     *
     * Key features:
     * - Each group has its own state machine and state manager
     * - Each group has its own logger for independent logging
     * - Groups are isolated from each other (no shared state)
     * - The first group added will start the listener
     *
     * Typical usage:
     *   raft_launcher launcher;
     *   launcher.init_shared_port(20000, logger, asio_opts);
     *
     *   // Add group 1
     *   auto server1 = launcher.init_with_group_id(1, sm1, smgr1, logger1, params1);
     *
     *   // Add group 2
     *   auto server2 = launcher.init_with_group_id(2, sm2, smgr2, logger2, params2);
     *
     * @param group_id Unique group identifier (must be > 0).
     * @param sm State machine for this group.
     * @param smgr State manager for this group.
     * @param lg Logger for this group (each group should have its own logger).
     * @param params Raft parameters for this group.
     * @param opt Raft server initialization options.
     * @return Raft server instance on success, nullptr on failure.
     *         Failure reasons: not in shared port mode, group_id already exists,
     *         or required parameters are null.
     */
    ptr<raft_server> init_with_group_id(int32 group_id,
                                         ptr<state_machine> sm,
                                         ptr<state_mgr> smgr,
                                         ptr<logger> lg,
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

