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
     * @return ptr<resp_msg> Response.
     */
    static ptr<resp_msg> process_req(raft_server* srv,
                                     req_msg& req,
                                     const raft_server::req_ext_params& ext_params =
                                         raft_server::req_ext_params()) {
        return srv->process_req(req, ext_params);
    }
};

}
