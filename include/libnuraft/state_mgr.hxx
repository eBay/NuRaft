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

#ifndef _STATE_MGR_HXX_
#define _STATE_MGR_HXX_

#include "pp_util.hxx"

namespace nuraft {

class cluster_config;
class log_store;
class srv_state;
class state_mgr {
    __interface_body__(state_mgr);

public:
    /**
     * Load the last saved cluster config.
     * This function will be invoked on initialization of
     * Raft server.
     *
     * Even at the very first initialization, it should
     * return proper initial cluster config, not `nullptr`.
     * The initial cluster config must include the server itself.
     *
     * @return Cluster config.
     */
    virtual ptr<cluster_config> load_config() = 0;

    /**
     * Save given cluster config.
     *
     * @param config Cluster config to save.
     */
    virtual void save_config(const cluster_config& config) = 0;

    /**
     * Save given server state.
     *
     * @param state Server state to save.
     */
    virtual void save_state(const srv_state& state) = 0;

    /**
     * Load the last saved server state.
     * This function will be invoked on initialization of
     * Raft server
     *
     * At the very first initialization, it should return
     * `nullptr`.
     *
     * @param Server state.
     */
    virtual ptr<srv_state> read_state() = 0;

    /**
     * Get instance of user-defined Raft log store.
     *
     * @param Raft log store instance.
     */
    virtual ptr<log_store> load_log_store() = 0;

    /**
     * Get ID of this Raft server.
     *
     * @return Server ID.
     */
    virtual int32 server_id() = 0;

    /**
     * System exit handler. This function will be invoked on
     * abnormal termination of Raft server.
     *
     * @param exit_code Error code.
     */
    virtual void system_exit(const int exit_code) = 0;
};

}

#endif //_STATE_MGR_HXX_
