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

#ifndef _CLUSTER_CONFIG_HXX_
#define _CLUSTER_CONFIG_HXX_

#include "buffer_serializer.hxx"
#include "srv_config.hxx"

#include <list>
#include <memory>
#include <vector>

namespace nuraft {

// WARNING:
//   Whenever adding a new field to this class,
//   below places should manually copy that new field.
//    - reset peer info
//    - add new server
//    - remove server
class cluster_config {
public:
    cluster_config(uint64_t log_idx = 0L, uint64_t prev_log_idx = 0L, bool _ec = false)
        : log_idx_(log_idx)
        , prev_log_idx_(prev_log_idx)
        , async_replication_(_ec)
        , servers_() {}

    ~cluster_config() {}

    __nocopy__(cluster_config);

public:
    static std::shared_ptr<cluster_config> deserialize(buffer& buf);

    static std::shared_ptr<cluster_config> deserialize(buffer_serializer& buf);

    uint64_t get_log_idx() const { return log_idx_; }

    void set_log_idx(uint64_t log_idx) {
        prev_log_idx_ = log_idx_;
        log_idx_ = log_idx;
    }

    uint64_t get_prev_log_idx() const { return prev_log_idx_; }

    std::list<std::shared_ptr<srv_config>> const& get_servers() const { return servers_; }

    std::list<std::shared_ptr<srv_config>>& get_servers() { return servers_; }

    std::shared_ptr<srv_config> get_server(int id) const {
        for (auto& entry: servers_) {
            const std::shared_ptr<srv_config>& srv = entry;
            if (srv->get_id() == id) {
                return srv;
            }
        }

        return std::shared_ptr<srv_config>();
    }

    bool is_async_replication() const { return async_replication_; }

    void set_async_replication(bool flag) { async_replication_ = flag; }

    std::string get_user_ctx() const { return user_ctx_; }

    void set_user_ctx(const std::string& src) { user_ctx_ = src; }

    std::shared_ptr<buffer> serialize() const;

private:
    // Log index number of current config.
    uint64_t log_idx_;

    // Log index number of previous config.
    uint64_t prev_log_idx_;

    // `true` if asynchronous replication mode is on.
    bool async_replication_;

    // Custom config data given by user.
    std::string user_ctx_;

    // List of servers.
    std::list<std::shared_ptr<srv_config>> servers_;
};

} // namespace nuraft

#endif //_CLUSTER_CONFIG_HXX_
