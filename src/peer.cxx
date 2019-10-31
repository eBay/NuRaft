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

#include "peer.hxx"

#include "tracer.hxx"

namespace nuraft {

void peer::send_req( ptr<peer> myself,
                     ptr<req_msg>& req,
                     rpc_handler& handler )
{
    if (abandoned_) {
        p_er("peer %d has been shut down, cannot send request",
             config_->get_id());
        return;
    }

    if (req) {
        p_tr("send req %d -> %d, type %s",
             req->get_src(),
             req->get_dst(),
             msg_type_to_string( req->get_type() ).c_str() );
    }

    ptr<rpc_result> pending = cs_new<rpc_result>(handler);
    ptr<rpc_client> rpc_local = nullptr;
    {   std::lock_guard<std::mutex> l(rpc_protector_);
        rpc_local = rpc_;
    }
    rpc_handler h = (rpc_handler)std::bind
                    ( &peer::handle_rpc_result,
                      this,
                      myself,
                      rpc_local,
                      req,
                      pending,
                      std::placeholders::_1,
                      std::placeholders::_2 );
    if (rpc_local) {
        rpc_local->send(req, h);

    } else {
        // Nothing has been sent, immediately free it
        // to serve next operation.
        p_tr("rpc local is null");
        set_free();
    }
}

// WARNING:
//   We should have the shared pointer of itself (`myself`)
//   and pointer to RPC client (`my_rpc_client`),
//   for the case when
//     1) this peer is removed before this callback function is invoked. OR
//     2) RPC client has been reset and re-connected.
void peer::handle_rpc_result( ptr<peer> myself,
                              ptr<rpc_client> my_rpc_client,
                              ptr<req_msg>& req,
                              ptr<rpc_result>& pending_result,
                              ptr<resp_msg>& resp,
                              ptr<rpc_exception>& err )
{
    if (req) {
        p_tr( "resp of req %d -> %d, type %s, %s",
              req->get_src(),
              req->get_dst(),
              msg_type_to_string( req->get_type() ).c_str(),
              (err) ? err->what() : "OK" );
    }

    if (err == nilptr) {
        // Succeeded.
        if ( req->get_type() == msg_type::append_entries_request ||
             req->get_type() == msg_type::install_snapshot_request ) {
            set_free();
        }

        reset_active_timer();
        resume_hb_speed();
        ptr<rpc_exception> no_except;
        pending_result->set_result(resp, no_except);

        reconn_backoff_.reset();
        reconn_backoff_.set_duration_ms(1);

    } else {
        // Failed.

        // NOTE: Explicit failure is also treated as an activity
        //       of that connection.
        reset_active_timer();
        slow_down_hb();
        ptr<resp_msg> no_resp;
        pending_result->set_result(no_resp, err);

        // Destroy this connection, we MUST NOT re-use existing socket.
        // Next append operation will create a new one.
        {   std::lock_guard<std::mutex> l(rpc_protector_);
            if (rpc_.get() == my_rpc_client.get()) {
                rpc_.reset();
                if ( req->get_type() == msg_type::append_entries_request ||
                     req->get_type() == msg_type::install_snapshot_request ) {
                    set_free();
                }

            } else {
                // WARNING (MONSTOR-9378):
                //   RPC client has been reset before this request returns
                //   error. Those two are different instances and we
                //   SHOULD NOT reset the new one.
                p_wn( "[EDGE CASE] RPC for %d has been reset before "
                      "returning error: current %p, from parameter %p",
                      config_->get_id(),
                      rpc_.get(),
                      my_rpc_client.get() );
            }
        }
    }
}

void peer::recreate_rpc(ptr<srv_config>& config,
                        context& ctx)
{
    if (abandoned_) return;

    ptr<rpc_client_factory> factory = nullptr;
    {   std::lock_guard<std::mutex> l(ctx.ctx_lock_);
        factory = ctx.rpc_cli_factory_;
    }
    if (!factory) return;

    std::lock_guard<std::mutex> l(rpc_protector_);

    // To avoid too frequent reconnection attempt,
    // we use exponential backoff (x2) from 1 ms to heartbeat interval.
    if (reconn_backoff_.timeout()) {
        reconn_backoff_.reset();
        size_t new_duration_ms = reconn_backoff_.get_duration_us() / 1000;
        new_duration_ms = std::min( hb_interval_, (int32)new_duration_ms * 2 );
        if (!new_duration_ms) new_duration_ms = 1;
        reconn_backoff_.set_duration_ms(new_duration_ms);

        rpc_ = factory->create_client(config->get_endpoint());
        p_tr("reconnect peer %zu", config_->get_id());

    } else {
        p_tr("skip reconnect this time");
    }
}

void peer::shutdown() {
    // Should set the flag to block all incoming requests.
    abandoned_ = true;

    // Cut off all shared pointers related to ASIO and Raft server.
    scheduler_.reset();
    {   // To guarantee atomic reset
        // (race between send_req()).
        std::lock_guard<std::mutex> l(rpc_protector_);
        rpc_.reset();
    }
    hb_task_.reset();
}

} // namespace nuraft;

