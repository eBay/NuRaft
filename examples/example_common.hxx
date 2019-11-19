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

using namespace nuraft;

using raft_result = cmd_result< ptr<buffer> >;

struct server_stuff {
    server_stuff()
        : server_id_(1)
        , addr_("localhost")
        , port_(25000)
        , raft_logger_(nullptr)
        , sm_(nullptr)
        , smgr_(nullptr)
        , raft_instance_(nullptr)
        {}

    void reset() {
        raft_logger_.reset();
        sm_.reset();
        smgr_.reset();
        raft_instance_.reset();
    }

    // Server ID.
    int server_id_;

    // Server address.
    std::string addr_;

    // Server port.
    int port_;

    // Endpoint: `<addr>:<port>`.
    std::string endpoint_;

    // Logger.
    ptr<logger> raft_logger_;

    // State machine.
    ptr<state_machine> sm_;

    // State manager.
    ptr<state_mgr> smgr_;

    // Raft launcher.
    raft_launcher launcher_;

    // Raft server instance.
    ptr<raft_server> raft_instance_;
};
static server_stuff stuff;


void add_server(const std::string& cmd,
                const std::vector<std::string>& tokens)
{
    if (tokens.size() < 3) {
        std::cout << "too few arguments" << std::endl;
        return;
    }

    int server_id_to_add = atoi(tokens[1].c_str());
    if ( !server_id_to_add || server_id_to_add == stuff.server_id_ ) {
        std::cout << "wrong server id: " << server_id_to_add << std::endl;
        return;
    }

    std::string endpoint_to_add = tokens[2];
    srv_config srv_conf_to_add( server_id_to_add, endpoint_to_add );
    ptr<raft_result> ret = stuff.raft_instance_->add_srv(srv_conf_to_add);
    if (!ret->get_accepted()) {
        std::cout << "failed to add server: "
                  << ret->get_result_code() << std::endl;
        return;
    }
    std::cout << "async request is in progress (check with `list` command)"
              << std::endl;
}

void server_list(const std::string& cmd,
                 const std::vector<std::string>& tokens)
{
    std::vector< ptr<srv_config> > configs;
    stuff.raft_instance_->get_srv_config_all(configs);

    int leader_id = stuff.raft_instance_->get_leader();

    for (auto& entry: configs) {
        ptr<srv_config>& srv = entry;
        std::cout
            << "server id " << srv->get_id()
            << ": " << srv->get_endpoint();
        if (srv->get_id() == leader_id) {
            std::cout << " (LEADER)";
        }
        std::cout << std::endl;
    }
}

bool do_cmd(const std::vector<std::string>& tokens);

std::vector<std::string> tokenize(const char* str, char c = ' ') {
    std::vector<std::string> tokens;
    do {
        const char *begin = str;
        while(*str != c && *str) str++;
        if (begin != str) tokens.push_back( std::string(begin, str) );
    } while (0 != *str++);

    return tokens;
}

void loop() {
    char cmd[1000];
    std::string prompt = "calc " + std::to_string(stuff.server_id_) + "> ";
    while (true) {
#if defined(__linux__) || defined(__APPLE__)
        std::cout << _CLM_GREEN << prompt << _CLM_END;
#else
        std::cout << prompt;
#endif
        std::cin.getline(cmd, 1000);

        std::vector<std::string> tokens = tokenize(cmd);
        bool cont = do_cmd(tokens);
        if (!cont) break;
    }
}

void init_raft(ptr<state_machine> sm_instance) {
    // Logger.
    std::string log_file_name = "./srv" +
                                std::to_string( stuff.server_id_ ) +
                                ".log";
    ptr<logger_wrapper> log_wrap = cs_new<logger_wrapper>( log_file_name, 4 );
    stuff.raft_logger_ = log_wrap;

    // State machine.
    stuff.smgr_ = cs_new<inmem_state_mgr>( stuff.server_id_,
                                           stuff.endpoint_ );
    // State manager.
    stuff.sm_ = sm_instance;

    // ASIO options.
    asio_service::options asio_opt;
    asio_opt.thread_pool_size_ = 4;

    // Raft parameters.
    raft_params params;
#if defined(WIN32) || defined(_WIN32)
    // heartbeat: 1 sec, election timeout: 2 - 4 sec.
    params.heart_beat_interval_ = 1000;
    params.election_timeout_lower_bound_ = 2000;
    params.election_timeout_upper_bound_ = 4000;
#else
    // heartbeat: 100 ms, election timeout: 200 - 400 ms.
    params.heart_beat_interval_ = 100;
    params.election_timeout_lower_bound_ = 200;
    params.election_timeout_upper_bound_ = 400;
#endif
    // Upto 5 logs will be preserved ahead the last snapshot.
    params.reserved_log_items_ = 5;
    // Snapshot will be created for every 5 log appends.
    params.snapshot_distance_ = 5;
    // Client timeout: 3000 ms.
    params.client_req_timeout_ = 3000;
    // According to this method, `append_log` function
    // should be handled differently.
    params.return_method_ = CALL_TYPE;

    // Initialize Raft server.
    stuff.raft_instance_ = stuff.launcher_.init(stuff.sm_,
                                                stuff.smgr_,
                                                stuff.raft_logger_,
                                                stuff.port_,
                                                asio_opt,
                                                params);
    if (!stuff.raft_instance_) {
        std::cerr << "Failed to initialize launcher (see the message "
                     "in the log file)." << std::endl;
        log_wrap.reset();
        exit(-1);
    }

    // Wait until Raft server is ready (upto 5 seconds).
    const size_t MAX_TRY = 20;
    std::cout << "init Raft instance ";
    for (size_t ii=0; ii<MAX_TRY; ++ii) {
        if (stuff.raft_instance_->is_initialized()) {
            std::cout << " done" << std::endl;
            return;
        }
        std::cout << ".";
        fflush(stdout);
        TestSuite::sleep_ms(250);
    }
    std::cout << " FAILED" << std::endl;
    log_wrap.reset();
    exit(-1);
}

void usage(int argc, char** argv) {
    std::stringstream ss;
    ss << "Usage: \n";
    ss << "    " << argv[0] << " <server id> <IP address and port>";
    ss << std::endl;

    std::cout << ss.str();
    exit(0);
}

void set_server_info(int argc, char** argv) {
    // Get server ID.
    stuff.server_id_ = atoi(argv[1]);
    if (stuff.server_id_ < 1) {
        std::cerr << "wrong server id (should be >= 1): "
                  << stuff.server_id_ << std::endl;
        usage(argc, argv);
    }

    // Get server address and port.
    std::string str = argv[2];
    size_t pos = str.rfind(":");
    if (pos == std::string::npos) {
        std::cerr << "wrong endpoint: " << str << std::endl;
        usage(argc, argv);
    }

    stuff.port_ = atoi(str.substr(pos + 1).c_str());
    if (stuff.port_ < 1000) {
        std::cerr << "wrong port (should be >= 1000): "
                  << stuff.port_ << std::endl;
        usage(argc, argv);
    }

    stuff.addr_ = str.substr(0, pos);
    stuff.endpoint_ = stuff.addr_ + ":" + std::to_string(stuff.port_);
}

