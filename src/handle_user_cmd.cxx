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

#include "raft_server.hxx"

#include "cluster_config.hxx"
#include "context.hxx"
#include "rpc_cli_factory.hxx"
#include "tracer.hxx"

#include <cassert>
#include <sstream>

namespace nuraft {

ptr< cmd_result< ptr<buffer> > > raft_server::add_srv(const srv_config& srv)
{
    ptr<buffer> buf(srv.serialize());
    ptr<log_entry> log( cs_new<log_entry>
                        ( 0, buf, log_val_type::cluster_server ) );
    ptr<req_msg> req = cs_new<req_msg>
                       ( (ulong)0, msg_type::add_server_request, 0, 0,
                         (ulong)0, (ulong)0, (ulong)0 );
    req->log_entries().push_back(log);
    return send_msg_to_leader(req);
}

ptr< cmd_result< ptr<buffer> > > raft_server::remove_srv(const int srv_id)
{
    ptr<buffer> buf(buffer::alloc(sz_int));
    buf->put(srv_id);
    buf->pos(0);
    ptr<log_entry> log(cs_new<log_entry>(0, buf, log_val_type::cluster_server));
    ptr<req_msg> req = cs_new<req_msg>
                       ( (ulong)0, msg_type::remove_server_request, 0, 0,
                         (ulong)0, (ulong)0, (ulong)0 );
    req->log_entries().push_back(log);
    return send_msg_to_leader(req);
}

ptr< cmd_result< ptr<buffer> > > raft_server::append_entries
                                 ( const std::vector< ptr<buffer> >& logs )
{
    if (logs.size() == 0) {
        ptr<buffer> result(nullptr);
        p_in("return null as log size is zero\n");
        return cs_new< cmd_result< ptr<buffer> > >(result);
    }

    ptr<req_msg> req = cs_new<req_msg>
                       ( (ulong)0, msg_type::client_request, 0, 0,
                         (ulong)0, (ulong)0, (ulong)0 ) ;
    for (auto it = logs.begin(); it != logs.end(); ++it) {
        ptr<buffer> buf = *it;
        // Just in case when user forgot to reset the position.
        buf->pos(0);
        ptr<log_entry> log( cs_new<log_entry>
                            ( 0, *it, log_val_type::app_log ) );
        req->log_entries().push_back(log);
    }

    return send_msg_to_leader(req);
}

ptr< cmd_result< ptr<buffer> > > raft_server::send_msg_to_leader(ptr<req_msg>& req)
{
    int32 leader_id = leader_;
    ptr<buffer> result = nullptr;
    if (leader_id == -1) {
        p_in("return null as leader does not exist in the current group");
        ptr< cmd_result< ptr<buffer> > > ret =
            cs_new< cmd_result< ptr<buffer> > >(result);
        ret->set_result_code( cmd_result_code::NOT_LEADER );
        return ret;
    }

    if (leader_id == id_) {
        ptr<resp_msg> resp = process_req(*req);
        if (!resp) {
            p_in("server returns null");
            ptr< cmd_result< ptr<buffer> > > ret =
                cs_new< cmd_result< ptr<buffer> > >(result);
            ret->set_result_code( cmd_result_code::BAD_REQUEST );
            return ret;
        }

        ptr< cmd_result< ptr<buffer> > > ret = nullptr;
        if (resp->has_cb()) {
            // Blocking mode:
            //   If callback function exists, get new response message
            //   from the callback function.
            resp = resp->call_cb(resp);
        }

        if (resp->get_accepted()) {
            result = resp->get_ctx();

            if (resp->has_async_cb()) {
                // Async handler mode (only when accepted):
                //   Get handler (async_result) from the callback.
                ret = resp->call_async_cb();
            }
        }
        if (result) {
            result->pos(0);
        }

        if (!ret) {
            // In blocking mode,
            // we already have result when we reach here.
            ret = cs_new< cmd_result< ptr<buffer> > >
                  ( result, resp->get_accepted() );
            ret->set_result_code( resp->get_result_code() );
        }
        return ret;
    }
    if (!ctx_->get_params()->auto_forwarding_) {
        // Auto-forwarding is disabled, return error.
        ptr< cmd_result< ptr<buffer> > > ret =
            cs_new< cmd_result< ptr<buffer> > >(result);
        ret->set_result_code( cmd_result_code::NOT_LEADER );
        return ret;
    }

// LCOV_EXCL_START
    // Otherwise: re-direct request to the current leader
    //            (not recommended).
    ptr<cluster_config> c_conf = get_config();
    ptr<rpc_client> rpc_cli;
    {
        auto_lock(rpc_clients_lock_);
        auto itor = rpc_clients_.find(leader_id);
        if (itor == rpc_clients_.end()) {
            ptr<srv_config> srv_conf = c_conf->get_server(leader_id);
            if (!srv_conf) {
                return cs_new< cmd_result< ptr<buffer> > >(result);
            }

            rpc_cli = ctx_->rpc_cli_factory_->create_client
                      ( srv_conf->get_endpoint() );
            rpc_clients_.insert( std::make_pair(leader_id, rpc_cli) );
        } else {
            rpc_cli = itor->second;
        }
    }

    if (!rpc_cli) {
        return cs_new< cmd_result< ptr<buffer> > >(result);
    }

    ptr< cmd_result< ptr<buffer> > >
        presult( cs_new< cmd_result< ptr<buffer> > >() );

    rpc_handler handler = [presult]
                          ( ptr<resp_msg>& resp,
                            ptr<rpc_exception>& err ) -> void
    {
        ptr<buffer> resp_ctx(nullptr);
        ptr<std::exception> perr;
        if (err) {
            perr = err;
        } else {
            if (resp->get_accepted()) {
                resp_ctx = resp->get_ctx();
                presult->accept();
            }
        }

        presult->set_result(resp_ctx, perr);
    };
    rpc_cli->send(req, handler);

    ptr<raft_params> params = ctx_->get_params();
    if (params->return_method_ == raft_params::blocking) {
        presult->get();
    }

    return presult;
// LCOV_EXCL_STOP
}

} // namespace nuraft;

