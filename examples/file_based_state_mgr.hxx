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

#include "file_based_log_store.hxx"

#include "nuraft.hxx"
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>

namespace nuraft {

/**
 * Simple file-based persistent state manager implementation.
 *
 * This is a naive implementation that persists cluster configuration and
 * server state to files in a specified directory.
 *
 * This is intended as a minimal example to demonstrate how to make a state
 * manager persistent. For production use, consider using more robust
 * serialization and storage mechanisms.
 */
class file_based_state_mgr: public state_mgr {
public:
    file_based_state_mgr(int srv_id,
                        const std::string& endpoint,
                        const std::string& state_dir)
        : my_id_(srv_id)
        , my_endpoint_(endpoint)
        , state_dir_(state_dir)
        , cur_log_store_( cs_new<file_based_log_store>(state_dir, srv_id) )
    {
        // Log to stdout since logger not yet available
        std::cout << "[TRACE][file_based_state_mgr_ctor] Creating state manager for server "
                  << srv_id << " at " << endpoint << ", state dir: " << state_dir << std::endl;

        // Create state directory if it doesn't exist
        mkdir(state_dir.c_str(), 0755);

        my_srv_config_ = cs_new<srv_config>( srv_id, endpoint );

        // Try to load existing state from disk
        saved_config_ = load_config();
        if (!saved_config_) {
            // No existing config, create initial one
            saved_config_ = cs_new<cluster_config>();
            saved_config_->get_servers().push_back(my_srv_config_);
            save_config(*saved_config_);
            std::cout << "[TRACE][file_based_state_mgr_ctor] Initial cluster config created with 1 server" << std::endl;
        } else {
            std::cout << "[TRACE][file_based_state_mgr_ctor] Loaded existing cluster config with "
                      << saved_config_->get_servers().size() << " servers" << std::endl;
        }

        // Try to load existing server state from disk
        saved_state_ = read_state();
    }

    ~file_based_state_mgr() {}

    ptr<cluster_config> load_config() {
        // Try to read from disk
        std::string config_path = get_config_path();
        std::ifstream infile(config_path, std::ios::binary | std::ios::ate);
        if (!infile.is_open()) {
            // File doesn't exist yet, return nullptr
            return nullptr;
        }

        try {
            std::streamsize size = infile.tellg();
            infile.seekg(0, std::ios::beg);

            ptr<buffer> buf = buffer::alloc(size);
            infile.read(reinterpret_cast<char*>(buf->data_begin()), size);
            infile.close();

            std::cout << "[TRACE][file_based_state_mgr] Loaded cluster config from disk" << std::endl;
            return cluster_config::deserialize(*buf);
        } catch (...) {
            std::cerr << "[ERROR][file_based_state_mgr] Failed to load cluster config from disk" << std::endl;
            return nullptr;
        }
    }

    void save_config(const cluster_config& config) {
        // Write to disk
        std::string config_path = get_config_path();
        std::ofstream outfile(config_path, std::ios::binary);
        if (!outfile.is_open()) {
            std::cerr << "[ERROR][file_based_state_mgr] Failed to open config file for writing: "
                      << config_path << std::endl;
            return;
        }

        try {
            ptr<buffer> buf = config.serialize();
            outfile.write(reinterpret_cast<const char*>(buf->data_begin()), buf->size());
            outfile.close();

            // Also keep in memory
            saved_config_ = cluster_config::deserialize(*buf);
            std::cout << "[TRACE][file_based_state_mgr] Saved cluster config to disk" << std::endl;
        } catch (...) {
            std::cerr << "[ERROR][file_based_state_mgr] Failed to save cluster config to disk" << std::endl;
        }
    }

    void save_state(const srv_state& state) {
        // Write to disk
        std::string state_path = get_state_path();
        std::ofstream outfile(state_path, std::ios::binary);
        if (!outfile.is_open()) {
            std::cerr << "[ERROR][file_based_state_mgr] Failed to open state file for writing: "
                      << state_path << std::endl;
            return;
        }

        try {
            ptr<buffer> buf = state.serialize();
            outfile.write(reinterpret_cast<const char*>(buf->data_begin()), buf->size());
            outfile.close();

            // Also keep in memory
            saved_state_ = srv_state::deserialize(*buf);
            std::cout << "[TRACE][file_based_state_mgr] Saved server state to disk" << std::endl;
        } catch (...) {
            std::cerr << "[ERROR][file_based_state_mgr] Failed to save server state to disk" << std::endl;
        }
    }

    ptr<srv_state> read_state() {
        // Try to read from disk
        std::string state_path = get_state_path();
        std::ifstream infile(state_path, std::ios::binary | std::ios::ate);
        if (!infile.is_open()) {
            // File doesn't exist yet, return default state
            return cs_new<srv_state>();
        }

        try {
            std::streamsize size = infile.tellg();
            infile.seekg(0, std::ios::beg);

            ptr<buffer> buf = buffer::alloc(size);
            infile.read(reinterpret_cast<char*>(buf->data_begin()), size);
            infile.close();

            std::cout << "[TRACE][file_based_state_mgr] Loaded server state from disk" << std::endl;
            return srv_state::deserialize(*buf);
        } catch (...) {
            std::cerr << "[ERROR][file_based_state_mgr] Failed to load server state from disk" << std::endl;
            return cs_new<srv_state>();
        }
    }

    ptr<log_store> load_log_store() {
        return cur_log_store_;
    }

    int32 server_id() {
        return my_id_;
    }

    void system_exit(const int exit_code) {
    }

    ptr<srv_config> get_srv_config() const { return my_srv_config_; }

private:
    std::string get_config_path() const {
        return state_dir_ + "/server_" + std::to_string(my_id_) + "_cluster.config";
    }

    std::string get_state_path() const {
        return state_dir_ + "/server_" + std::to_string(my_id_) + "_state.bin";
    }

private:
    int my_id_;
    std::string my_endpoint_;
    std::string state_dir_;
    ptr<file_based_log_store> cur_log_store_;
    ptr<srv_config> my_srv_config_;
    ptr<cluster_config> saved_config_;
    ptr<srv_state> saved_state_;
};

}
