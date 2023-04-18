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

#include "in_memory_log_store.hxx"

#include "nuraft.hxx"

namespace nuraft {

class inmem_state_mgr : public state_mgr {
public:
    inmem_state_mgr(int srv_id, const std::string& endpoint)
        : my_id_(srv_id)
        , my_endpoint_(endpoint)
        , cur_log_store_(std::make_shared<inmem_log_store>()) {
        my_srv_config_ = std::make_shared<srv_config>(srv_id, endpoint);

        // Initial cluster config: contains only one server (myself).
        saved_config_ = std::make_shared<cluster_config>();
        saved_config_->get_servers().push_back(my_srv_config_);
    }

    ~inmem_state_mgr() {}

    std::shared_ptr<cluster_config> load_config() {
        // Just return in-memory data in this example.
        // May require reading from disk here, if it has been written to disk.
        return saved_config_;
    }

    void save_config(const cluster_config& config) {
        // Just keep in memory in this example.
        // Need to write to disk here, if want to make it durable.
        std::shared_ptr<buffer> buf = config.serialize();
        saved_config_ = cluster_config::deserialize(*buf);
    }

    void save_state(const srv_state& state) {
        // Just keep in memory in this example.
        // Need to write to disk here, if want to make it durable.
        std::shared_ptr<buffer> buf = state.serialize();
        saved_state_ = srv_state::deserialize(*buf);
    }

    std::shared_ptr<srv_state> read_state() {
        // Just return in-memory data in this example.
        // May require reading from disk here, if it has been written to disk.
        return saved_state_;
    }

    std::shared_ptr<log_store> load_log_store() { return cur_log_store_; }

    int32_t server_id() override { return my_id_; }

    void system_exit(const int exit_code) {}

    std::shared_ptr<srv_config> get_srv_config() const { return my_srv_config_; }

private:
    int my_id_;
    std::string my_endpoint_;
    std::shared_ptr<inmem_log_store> cur_log_store_;
    std::shared_ptr<srv_config> my_srv_config_;
    std::shared_ptr<cluster_config> saved_config_;
    std::shared_ptr<srv_state> saved_state_;
};

} // namespace nuraft
