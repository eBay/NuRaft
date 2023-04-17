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

#pragma once

#include "raft_server.hxx"

namespace nuraft {

/**
 * Class to handle the internal feature of `raft_server`.
 * Any internal class that wants to call `process_req` API should inherit this class.
 * It should not be used externally.
 */
class raft_server_handler {
protected:
    /**
     * Call `process_req` of the given raft server.
     *
     * @param srv `raft_server` instance.
     * @param req Request.
     * @param ext_params Extended parameters.
     * @return std::shared_ptr<resp_msg> Response.
     */
    static std::shared_ptr< resp_msg >
    process_req(raft_server* srv, req_msg& req,
                const raft_server::req_ext_params& ext_params = raft_server::req_ext_params()) {
        return srv->process_req(req, ext_params);
    }
};

} // namespace nuraft
