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
#include "event_awaiter.h"
#include "rpc_cli_factory.hxx"
#include "tracer.hxx"

#include <cassert>
#include <sstream>

namespace nuraft {

struct raft_server::auto_fwd_pkg {
    /**
     * Available RPC clients.
     */
    std::list<ptr<rpc_client>> rpc_client_idle_;

    /**
     * RPC clients in use.
     */
    std::unordered_set<ptr<rpc_client>> rpc_client_in_use_;

    /**
     * Mutex.
     */
    std::mutex lock_;

    /**
     * Event awaiter.
     */
    EventAwaiter ea_;
};

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

    // Otherwise: re-direct request to the current leader
    //            (not recommended).
    ptr<raft_params> params = ctx_->get_params();
    size_t max_conns =
        std::max(1, params->auto_forwarding_max_connections_);
    bool is_blocking_mode = (params->return_method_ == raft_params::blocking);

    ptr<cluster_config> c_conf = get_config();
    ptr<rpc_client> rpc_cli;
    ptr<auto_fwd_pkg> cur_pkg = nullptr;
    {
        auto_lock(rpc_clients_lock_);
        auto entry = auto_fwd_pkgs_.find(leader_id);
        if (entry == auto_fwd_pkgs_.end()) {
            cur_pkg = cs_new<auto_fwd_pkg>();
            auto_fwd_pkgs_[leader_id] = cur_pkg;
            p_tr("auto forwarding pkg for leader %d not found, created %p",
                 leader_id, cur_pkg.get());
        } else {
            cur_pkg = entry->second;
            p_tr("auto forwarding pkg for leader %d exists %p",
                 leader_id, cur_pkg.get());
        }
    }

    // Find available `rpc_cli`.
    do {
        std::unique_lock<std::mutex> l(cur_pkg->lock_);
        // Get an idle connection.
        auto e_rpc = cur_pkg->rpc_client_idle_.begin();
        if (e_rpc == cur_pkg->rpc_client_idle_.end()) {
            // Idle connection doesn't exist,
            // check the total number of connections.
            p_tr( "no connection available, idle %zu, in-use %zu, max %zu",
                   cur_pkg->rpc_client_idle_.size(),
                   cur_pkg->rpc_client_in_use_.size(),
                   max_conns );
            if ( cur_pkg->rpc_client_idle_.size() +
                 cur_pkg->rpc_client_in_use_.size() < max_conns ) {
                // We can create more connections.
                ptr<srv_config> srv_conf = c_conf->get_server(leader_id);
                rpc_cli = ctx_->rpc_cli_factory_->create_client
                          ( srv_conf->get_endpoint() );
                cur_pkg->rpc_client_in_use_.insert(rpc_cli);
                p_tr("created a new connection %p", rpc_cli.get());

            } else {
                // Already reached the max, wait for idle connection.
                if (is_blocking_mode) {
                    // Blocking mode, sleep here.
                    l.unlock();
                    p_tr("reached max connection, wait");
                    cur_pkg->ea_.wait_ms(params->client_req_timeout_);
                    cur_pkg->ea_.reset();
                    p_tr("wake up, find an available connection");
                    continue;

                } else {
                    // Async mode, put it into the queue, and return immediately.
                    auto_fwd_req_resp req_resp_pair;
                    req_resp_pair.req = req;
                    req_resp_pair.resp = cs_new< cmd_result< ptr<buffer> > >();

                    auto_lock(auto_fwd_reqs_lock_);
                    auto_fwd_reqs_.push_back(req_resp_pair);
                    p_tr("reached max connection, put into the queue, %zu elems",
                         auto_fwd_reqs_.size());
                    return req_resp_pair.resp;
                }
            }
        } else {
            // Put the idle connection to in-use list.
            rpc_cli = *e_rpc;
            if (rpc_cli->is_abandoned()) {
                // Abandoned connection, need to reconnect.
                ptr<srv_config> srv_conf = c_conf->get_server(leader_id);
                rpc_cli = ctx_->rpc_cli_factory_->create_client
                          ( srv_conf->get_endpoint() );
            }
            cur_pkg->rpc_client_idle_.pop_front();
            cur_pkg->rpc_client_in_use_.insert(rpc_cli);
            p_tr("idle connection %p", rpc_cli.get());
        }
        break;
    } while(true);

    if (!rpc_cli) {
        return cs_new< cmd_result< ptr<buffer> > >(result);
    }

    ptr< cmd_result< ptr<buffer> > >
        presult( cs_new< cmd_result< ptr<buffer> > >() );

    rpc_handler handler = std::bind( &raft_server::auto_fwd_resp_handler,
                                     this,
                                     presult,
                                     cur_pkg,
                                     rpc_cli,
                                     std::placeholders::_1,
                                     std::placeholders::_2 );
    rpc_cli->send(req, handler, params->auto_forwarding_req_timeout_);

    if (params->return_method_ == raft_params::blocking) {
        presult->get();
    }

    return presult;
}

void raft_server::auto_fwd_release_rpc_cli( ptr<auto_fwd_pkg> cur_pkg,
                                            ptr<rpc_client> rpc_cli )
{
    ptr<raft_params> params = ctx_->get_params();
    size_t max_conns =
        std::max(1, params->auto_forwarding_max_connections_);
    bool is_blocking_mode = (params->return_method_ == raft_params::blocking);

    auto put_back_to_idle_list = [&cur_pkg, &rpc_cli, max_conns, this]() {
        cur_pkg->rpc_client_in_use_.erase(rpc_cli);
        cur_pkg->rpc_client_idle_.push_front(rpc_cli);
        p_tr( "release connection %p, idle %zu, in-use %zu, max %zu",
              rpc_cli.get(),
              cur_pkg->rpc_client_idle_.size(),
              cur_pkg->rpc_client_in_use_.size(),
              max_conns );
    };

    std::unique_lock<std::mutex> l(cur_pkg->lock_);
    if (is_blocking_mode) {
        // Blocking mode, put the connection back to idle list,
        // and wake up the sleeping thread.
        put_back_to_idle_list();
        cur_pkg->ea_.invoke();

    } else {
        // Async mode, send the request in the queue.
        std::unique_lock<std::mutex> ll(auto_fwd_reqs_lock_);
        if (!auto_fwd_reqs_.empty()) {
            auto_fwd_req_resp entry = *auto_fwd_reqs_.begin();
            auto_fwd_reqs_.pop_front();
            p_tr( "found waiting request in the queue, remaining elems %zu",
                  auto_fwd_reqs_.size() );
            ll.unlock();

            rpc_handler handler = std::bind( &raft_server::auto_fwd_resp_handler,
                                             this,
                                             entry.resp,
                                             cur_pkg,
                                             rpc_cli,
                                             std::placeholders::_1,
                                             std::placeholders::_2 );

            // Should be unlocked before calling `send`, as resp handler can be
            // invoked in the same thread in case of error.
            l.unlock();
            rpc_cli->send(entry.req, handler, params->auto_forwarding_req_timeout_);

        } else {
            // If no request is waiting, put the connection back to idle list.
            ll.unlock();
            put_back_to_idle_list();
        }
    }
}

void raft_server::auto_fwd_resp_handler( ptr<cmd_result<ptr<buffer>>> presult,
                                         ptr<auto_fwd_pkg> cur_pkg,
                                         ptr<rpc_client> rpc_cli,
                                         ptr<resp_msg>& resp,
                                         ptr<rpc_exception>& err )
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
    auto_fwd_release_rpc_cli(cur_pkg, rpc_cli);
}

void raft_server::cleanup_auto_fwd_pkgs() {
    auto_lock(rpc_clients_lock_);
    for (auto& entry: auto_fwd_pkgs_) {
        ptr<auto_fwd_pkg> pkg = entry.second;
        pkg->ea_.invoke();
        auto_lock(pkg->lock_);
        p_in("srv %d, in-use %zu, idle %zu",
             entry.first,
             pkg->rpc_client_in_use_.size(),
             pkg->rpc_client_idle_.size());
        for (auto& ee: pkg->rpc_client_in_use_) {
            p_tr("use count %zu", ee.use_count());
        }
        for (auto& ee: pkg->rpc_client_idle_) {
            p_tr("use count %zu", ee.use_count());
        }
        pkg->rpc_client_idle_.clear();
        pkg->rpc_client_in_use_.clear();
    }
    auto_fwd_pkgs_.clear();
}

} // namespace nuraft;

