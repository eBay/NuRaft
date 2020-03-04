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
#include "event_awaiter.h"
#include "peer.hxx"
#include "tracer.hxx"

#include <cassert>
#include <list>
#include <sstream>

namespace nuraft {

void raft_server::set_priority(const int srv_id, const int new_priority)
{
    recur_lock(lock_);

    // Do nothing if not a leader.
    if (id_ != leader_) {
        p_in("Got set_priority request but I'm not a leader: my ID %d, leader %d",
             id_, leader_.load());

        if (!is_leader_alive()) {
            p_wn("No live leader now, broadcast priority change");
            broadcast_priority_change(srv_id, new_priority);
        }
        return;
    }

    if (id_ == srv_id && new_priority == 0) {
        // Step down.
        // Even though current leader (myself) can send append_entries()
        // request, it cannot commit the priority change as it will
        // immediately yield its leadership.
        // So in this case, boradcast this change.
        broadcast_priority_change(srv_id, new_priority);
    }

    // Clone current cluster config.
    ptr<cluster_config> cur_config = get_config();

    // NOTE: Need to honor uncommitted config,
    //       refer to comment in `sync_log_to_new_srv()`
    if (uncommitted_config_) {
        p_in("uncommitted config exists at log %zu, prev log %zu",
             uncommitted_config_->get_log_idx(),
             uncommitted_config_->get_prev_log_idx());
        cur_config = uncommitted_config_;
    }

    ptr<buffer> enc_conf = cur_config->serialize();
    ptr<cluster_config> cloned_config = cluster_config::deserialize(*enc_conf);

    std::list<ptr<srv_config>>& s_confs = cloned_config->get_servers();

    for (auto& entry: s_confs) {
        srv_config* s_conf = entry.get();
        if (s_conf->get_id() == srv_id) {
            p_in("Change server %d priority %d -> %d",
                 srv_id, s_conf->get_priority(), new_priority);
            s_conf->set_priority(new_priority);
        }
    }

    // Create a log for new configuration, it should be replicated.
    cloned_config->set_log_idx(log_store_->next_slot());
    ptr<buffer> new_conf_buf(cloned_config->serialize());
    ptr<log_entry> entry( cs_new<log_entry>
                          ( state_->get_term(),
                            new_conf_buf,
                            log_val_type::conf ) );

    config_changing_ = true;
    uncommitted_config_ = cloned_config;

    store_log_entry(entry);
    request_append_entries();
}

void raft_server::broadcast_priority_change(const int srv_id,
                                            const int new_priority)
{
    if (srv_id == id_) {
        my_priority_ = new_priority;
    }
    ptr<cluster_config> cur_config = get_config();
    for (auto& entry: cur_config->get_servers()) {
        srv_config* s_conf = entry.get();
        if (s_conf->get_id() == srv_id) {
            p_in("Change server %d priority %d -> %d",
                 srv_id, s_conf->get_priority(), new_priority);
            s_conf->set_priority(new_priority);
        }
    }

    // If there is no live leader now,
    // broadcast this request to all peers.
    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        ptr<req_msg> req( cs_new<req_msg>
                          ( state_->get_term(),
                            msg_type::priority_change_request,
                            id_,
                            it->second->get_id(),
                            term_for_log(log_store_->next_slot() - 1),
                            log_store_->next_slot() - 1,
                            quick_commit_index_.load() ) );

        // ID + priority
        ptr<buffer> buf = buffer::alloc(sz_int * 2);
        buf->pos(0);
        buf->put((int32)srv_id);
        buf->put((int32)new_priority);
        buf->pos(0);
        ptr<log_entry> le = cs_new<log_entry>(state_->get_term(), buf);

        std::vector< ptr<log_entry> >& v = req->log_entries();
        v.push_back(le);

        ptr<peer> pp = it->second;
        if (pp->make_busy()) {
            pp->send_req(pp, req, resp_handler_);
        } else {
            p_er("peer %d is currently busy, cannot send request",
                 pp->get_id());
        }
    }
}

ptr<resp_msg> raft_server::handle_priority_change_req(req_msg& req) {
    // NOTE: now this function is protected by lock.
    ptr<resp_msg> resp
        ( cs_new<resp_msg>
          ( req.get_term(),
            msg_type::priority_change_response,
            id_,
            req.get_src() ) );

    std::vector< ptr<log_entry> >& v = req.log_entries();
    if (!v.size()) {
        p_wn("no log entry");
        return resp;
    }
    if (v[0]->is_buf_null()) {
        p_wn("empty buffer");
        return resp;
    }

    buffer& buf = v[0]->get_buf();
    buf.pos(0);
    if (buf.size() < sz_int * 2) {
        p_wn("wrong buffer size: %zu", buf.size());
        return resp;
    }

    int32 t_id = buf.get_int();
    int32 t_priority = buf.get_int();

    if (t_id == id_) {
        my_priority_ = t_priority;
    }

    ptr<cluster_config> c_conf = get_config();
    for (auto& entry: c_conf->get_servers()) {
        srv_config* s_conf = entry.get();
        if ( s_conf->get_id() == t_id ) {
            resp->accept(log_store_->next_slot());
            p_in("change peer %d priority: %d -> %d",
                 t_id, s_conf->get_priority(), t_priority);
            s_conf->set_priority(t_priority);
            return resp;
        }
    }
    p_wn("cannot find peer %d", t_id);

    return resp;
}

void raft_server::handle_priority_change_resp(resp_msg& resp) {
    p_in("got response from peer %d: %s",
         resp.get_src(),
         resp.get_accepted() ? "success" : "fail");
}

void raft_server::decay_target_priority() {
    // Gap should be bigger than 10.
    int gap = std::max((int)10, target_priority_ / 5);

    // Should be bigger than 0.
    int32 prev_priority = target_priority_;
    target_priority_ = std::max(1, target_priority_ - gap);
    p_in("[PRIORITY] decay, target %d -> %d, mine %d",
         prev_priority, target_priority_, my_priority_);

    // Once `target_priority_` becomes 1,
    // `priority_change_timer_` starts ticking.
    if (prev_priority > 1) priority_change_timer_.reset();
}

void raft_server::update_target_priority() {
    // Get max priority among all peers, including myself.
    int32 max_priority = my_priority_;
    for (auto& entry: peers_) {
        peer* peer_elem = entry.second.get();
        const srv_config& s_conf = peer_elem->get_config();
        int32 cur_priority = s_conf.get_priority();
        max_priority = std::max(max_priority, cur_priority);
    }
    if (max_priority > 0) {
        target_priority_ = max_priority;
    } else {
        target_priority_ = srv_config::INIT_PRIORITY;
    }
    priority_change_timer_.reset();

    hb_alive_ = true;
    pre_vote_.reset(state_->get_term());
    p_tr("(update) new target priority: %d", target_priority_);
}

} // namespace nuraft;

