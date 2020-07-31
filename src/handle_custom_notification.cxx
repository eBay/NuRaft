/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.

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

#include "handle_custom_notification.hxx"

#include "buffer_serializer.hxx"
#include "callback.hxx"
#include "error_code.hxx"
#include "peer.hxx"
#include "raft_server.hxx"
#include "req_msg.hxx"
#include "resp_msg.hxx"
#include "tracer.hxx"

#include <cassert>
#include <cstring>

namespace nuraft {

// --- custom_notification_msg ---

ptr<custom_notification_msg> custom_notification_msg::deserialize(buffer& buf) {
    ptr<custom_notification_msg> ret = cs_new<custom_notification_msg>();

    buffer_serializer bs(buf);
    uint8_t version = bs.get_u8();
    (void)version;
    ret->type_ = static_cast<custom_notification_msg::type>(bs.get_u8());

    size_t buf_len = 0;
    void* ptr = bs.get_bytes(buf_len);

    if (buf_len) {
        ret->ctx_ = buffer::alloc(buf_len);
        memcpy(ret->ctx_->data_begin(), ptr, buf_len);
    } else {
        ret->ctx_ = nullptr;
    }

    return ret;
}

ptr<buffer> custom_notification_msg::serialize() const {
    //   << Format >>
    // version          1 byte
    // type             1 byte
    // ctx length (X)   4 bytes
    // ctx              X bytes

    const uint8_t CURRENT_VERSION = 0x0;

    size_t len = sizeof(uint8_t) +
                 sizeof(uint8_t) +
                 sizeof(uint32_t) +
                 ( (ctx_) ? ctx_->size() : 0 );

    ptr<buffer> ret = buffer::alloc(len);
    buffer_serializer bs(ret);
    bs.put_u8(CURRENT_VERSION);
    bs.put_u8(type_);
    if (ctx_) {
        bs.put_bytes(ctx_->data_begin(), ctx_->size());
    } else {
        bs.put_u32(0);
    }

    return ret;
}


// --- out_of_log_msg ---

ptr<out_of_log_msg> out_of_log_msg::deserialize(buffer& buf) {
    ptr<out_of_log_msg> ret = cs_new<out_of_log_msg>();

    buffer_serializer bs(buf);
    uint8_t version = bs.get_u8();
    (void)version;
    ret->start_idx_of_leader_ = bs.get_u64();
    return ret;
}

ptr<buffer> out_of_log_msg::serialize() const {
    //   << Format >>
    // version                      1 byte
    // start log index of leader    8 bytes
    size_t len = sizeof(uint8_t) + sizeof(ulong);
    ptr<buffer> ret = buffer::alloc(len);

    const uint8_t CURRENT_VERSION = 0x0;
    buffer_serializer bs(ret);
    bs.put_u8(CURRENT_VERSION);
    bs.put_u64(start_idx_of_leader_);
    return ret;
}


// --- force_vote_msg ---

ptr<force_vote_msg> force_vote_msg::deserialize(buffer& buf) {
    ptr<force_vote_msg> ret = cs_new<force_vote_msg>();
    buffer_serializer bs(buf);
    uint8_t version = bs.get_u8();
    (void)version;
    return ret;
}

ptr<buffer> force_vote_msg::serialize() const {
    //   << Format >>
    // version                      1 byte
    // ... to be added ...

    size_t len = sizeof(uint8_t);
    ptr<buffer> ret = buffer::alloc(len);

    const uint8_t CURRENT_VERSION = 0x0;
    buffer_serializer bs(ret);
    bs.put_u8(CURRENT_VERSION);
    return ret;
}


// --- handlers ---

ptr<resp_msg> raft_server::handle_custom_notification_req(req_msg& req) {
    ptr<resp_msg> resp = cs_new<resp_msg>( state_->get_term(),
                                           msg_type::custom_notification_response,
                                           id_,
                                           req.get_src(),
                                           log_store_->next_slot() );
    resp->accept(log_store_->next_slot());

    std::vector< ptr<log_entry> >& log_entries = req.log_entries();
    if (!log_entries.size()) {
        // Empty message, just return.
        return resp;
    }

    ptr<log_entry> msg_le = log_entries[0];
    ptr<buffer> buf = msg_le->get_buf_ptr();
    if (!buf) return resp;

    ptr<custom_notification_msg> msg = custom_notification_msg::deserialize(*buf);

    switch (msg->type_) {
    case custom_notification_msg::out_of_log_range_warning: {
        return handle_out_of_log_msg(req, msg, resp);
    }
    case custom_notification_msg::leadership_takeover: {
        return handle_leadership_takeover(req, msg, resp);
    }
    case custom_notification_msg::request_resignation: {
        return handle_resignation_request(req, msg, resp);
    }
    default:
        break;
    }

    return resp;
}

ptr<resp_msg> raft_server::handle_out_of_log_msg(req_msg& req,
                                                 ptr<custom_notification_msg> msg,
                                                 ptr<resp_msg> resp)
{
    static timer_helper msg_timer(5000000);
    int log_lv = msg_timer.timeout_and_reset() ? L_WARN : L_TRACE;

    // As it is a special form of heartbeat, need to update term.
    update_term(req.get_term());

    out_of_log_range_ = true;

    ptr<out_of_log_msg> ool_msg = out_of_log_msg::deserialize(*msg->ctx_);
    p_lv(log_lv, "this node is out of log range. leader's start index: %zu, "
         "my last index: %zu",
         ool_msg->start_idx_of_leader_,
         log_store_->next_slot() - 1);

    // Should restart election timer to avoid initiating false vote.
    if ( req.get_term() == state_->get_term() &&
         role_ == srv_role::follower ) {
        restart_election_timer();
    }

    cb_func::Param param(id_, leader_);
    cb_func::OutOfLogRangeWarningArgs args(ool_msg->start_idx_of_leader_);
    param.ctx = &args;
    ctx_->cb_func_.call(cb_func::OutOfLogRangeWarning, &param);

    return resp;
}

ptr<resp_msg> raft_server::handle_leadership_takeover
                           ( req_msg& req,
                             ptr<custom_notification_msg> msg,
                             ptr<resp_msg> resp )
{
    if (is_leader()) {
        p_er("got leadership takeover request from peer %d, "
             "I'm already a leader", req.get_src());
        return resp;
    }
    p_in("[LEADERSHIP TAKEOVER] got request");

    // Initiate force vote (ignoring priority).
    initiate_vote(true);
    return resp;
}

ptr<resp_msg> raft_server::handle_resignation_request
                           ( req_msg& req,
                             ptr<custom_notification_msg> msg,
                             ptr<resp_msg> resp )
{
    if (!is_leader()) {
        p_er("got resignation request from peer %d, "
             "but I'm not a leader", req.get_src());
        return resp;
    }
    p_in("[RESIGNATION REQUEST] got request");

    yield_leadership(false, req.get_src());
    return resp;
}

void raft_server::handle_custom_notification_resp(resp_msg& resp) {
    if (!resp.get_accepted()) return;

    peer_itor it = peers_.find(resp.get_src());
    if (it == peers_.end()) {
        p_in("the response is from an unknown peer %d", resp.get_src());
        return;
    }
    ptr<peer> p = it->second;

    p->set_next_log_idx(resp.get_next_idx());
}

} // namespace nuraft;

