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

#include "raft_functional_common.hxx"
#include "internal_timer.hxx"

#include "nuraft.hxx"

#if defined(__linux__) || defined(__APPLE__)
    #include <unistd.h>
#endif

using namespace nuraft;
using namespace raft_functional_common;

class RaftAsioPkg {
public:
    static const int HEARTBEAT_MS = 100;

    using READ_META_FUNC = std::function
                               < bool( const asio_service::meta_cb_params&,
                                       const std::string& ) >;

    using WRITE_META_FUNC = std::function
                               < std::string(const asio_service::meta_cb_params&) >;


    RaftAsioPkg(int srv_id,
                const std::string& endpoint)
        : myId(srv_id)
        , myEndpoint(endpoint)
        , sMgr(nullptr)
        , sm(nullptr)
        , asioSvc(nullptr)
        , asioListener(nullptr)
        , raftServer(nullptr)
        , readReqMeta(nullptr)
        , writeReqMeta(nullptr)
        , alwaysInvokeCb(true)
        , myLogWrapper(nullptr)
        , myLog(nullptr)
        {}

    ~RaftAsioPkg() {
    }

    void setMetaCallback( READ_META_FUNC read_req_meta,
                          WRITE_META_FUNC write_req_meta,
                          READ_META_FUNC read_resp_meta,
                          WRITE_META_FUNC write_resp_meta,
                          bool always_invoke_cb )
    {
        readReqMeta = read_req_meta;
        writeReqMeta = write_req_meta;
        readRespMeta = read_resp_meta;
        writeRespMeta = write_resp_meta;
        alwaysInvokeCb = always_invoke_cb;
    }

    static bool verifySn(const std::string& sn) {
        // Check if `CN=localhost` exists.
        size_t pos = sn.find("CN=");
        if (pos == std::string::npos) return false;
        if (sn.substr(pos+3, 9) == "localhost") return true;
        return false;
    }

    void initServer(bool enable_ssl = false,
                    bool use_global_asio = false,
                    const raft_server::init_options& opt = raft_server::init_options()) {
        std::string log_file_name = "./srv" + std::to_string(myId) + ".log";
        myLogWrapper = cs_new<logger_wrapper>(log_file_name);
        myLog = myLogWrapper;

        sMgr = cs_new<TestMgr>(myId, myEndpoint);
        sm = cs_new<TestSm>( myLogWrapper->getLogger() );

        asio_service::options asio_opt;
        asio_opt.thread_pool_size_  = 4;
        if (enable_ssl) {
            asio_opt.enable_ssl_        = enable_ssl;
            asio_opt.verify_sn_         = RaftAsioPkg::verifySn;
            asio_opt.server_cert_file_  = "./cert.pem";
            asio_opt.root_cert_file_    = "./cert.pem"; // self-signed.
            asio_opt.server_key_file_   = "./key.pem";
        }

        if (readReqMeta) asio_opt.read_req_meta_ = readReqMeta;
        if (writeReqMeta) asio_opt.write_req_meta_ = writeReqMeta;

        if (readRespMeta) asio_opt.read_resp_meta_ = readRespMeta;
        if (writeRespMeta) asio_opt.write_resp_meta_ = writeRespMeta;

        asio_opt.invoke_req_cb_on_empty_meta_ = alwaysInvokeCb;
        asio_opt.invoke_resp_cb_on_empty_meta_ = alwaysInvokeCb;

        asioSvc = use_global_asio
                  ? nuraft_global_mgr::init_asio_service(asio_opt, myLog)
                  : cs_new<asio_service>(asio_opt, myLog);

        int raft_port = 20000 + myId * 10;
        ptr<rpc_listener> listener
                          ( asioSvc->create_rpc_listener(raft_port, myLog) );
        ptr<delayed_task_scheduler> scheduler = asioSvc;
        ptr<rpc_client_factory> rpc_cli_factory = asioSvc;

        raft_params params;
        params.with_hb_interval(HEARTBEAT_MS);
        params.with_election_timeout_lower(HEARTBEAT_MS * 2);
        params.with_election_timeout_upper(HEARTBEAT_MS * 4);
        params.with_reserved_log_items(10);
        params.with_snapshot_enabled(5);
        params.with_client_req_timeout(10000);
        context* ctx( new context( sMgr, sm, listener, myLog,
                                   rpc_cli_factory, scheduler, params ) );
        raftServer = cs_new<raft_server>(ctx, opt);

        // Listen.
        asioListener = listener;
        asioListener->listen(raftServer);
    }

    /**
     * Re-init Raft server without changing internal data including state machine.
     */
    void restartServer(
            raft_params* custom_params = nullptr,
            bool enable_ssl = false,
            bool use_global_asio = false,
            const raft_server::init_options& opt = raft_server::init_options()) {
        asio_service::options asio_opt;
        asio_opt.thread_pool_size_  = 4;
        if (enable_ssl) {
            asio_opt.enable_ssl_        = enable_ssl;
            asio_opt.verify_sn_         = RaftAsioPkg::verifySn;
            asio_opt.server_cert_file_  = "./cert.pem";
            asio_opt.root_cert_file_    = "./cert.pem"; // self-signed.
            asio_opt.server_key_file_   = "./key.pem";
        }

        asioSvc = use_global_asio
                  ? nuraft_global_mgr::init_asio_service(asio_opt, myLog)
                  : cs_new<asio_service>(asio_opt, myLog);

        int raft_port = 20000 + myId * 10;
        ptr<rpc_listener> listener
                          ( asioSvc->create_rpc_listener(raft_port, myLog) );
        ptr<delayed_task_scheduler> scheduler = asioSvc;
        ptr<rpc_client_factory> rpc_cli_factory = asioSvc;

        raft_params params;
        if (custom_params) {
            params = *custom_params;
        } else {
            params.with_hb_interval(HEARTBEAT_MS);
            params.with_election_timeout_lower(HEARTBEAT_MS * 2);
            params.with_election_timeout_upper(HEARTBEAT_MS * 4);
            params.with_reserved_log_items(10);
            params.with_snapshot_enabled(5);
            params.with_client_req_timeout(10000);
        }
        context* ctx( new context( sMgr, sm, listener, myLog,
                                   rpc_cli_factory, scheduler, params ) );
        raftServer = cs_new<raft_server>(ctx, opt);

        // Listen.
        asioListener = listener;
        asioListener->listen(raftServer);
    }

    void stopAsio() {
        if (asioListener) {
            asioListener->stop();
            asioListener->shutdown();
        }
        if (asioSvc) {
            asioSvc->stop();
            size_t count = 0;
            while (asioSvc->get_active_workers() && count < 500) {
                // 10ms per tick.
                timer_helper::sleep_ms(10);
                count++;
            }
        }
    }

    TestMgr* getTestMgr() const {
        return static_cast<TestMgr*>(sMgr.get());
    }

    TestSm* getTestSm() const {
        return static_cast<TestSm*>(sm.get());
    }

    int myId;
    std::string myEndpoint;

    ptr<state_mgr> sMgr;
    ptr<state_machine> sm;

    ptr<asio_service> asioSvc;
    ptr<rpc_listener> asioListener;

    ptr<raft_server> raftServer;

    // Callback function to read Raft request metadata.
    READ_META_FUNC readReqMeta;

    // Callback function to write Raft request metadata.
    WRITE_META_FUNC writeReqMeta;

    // Callback function to read Raft response metadata.
    READ_META_FUNC readRespMeta;

    // Callback function to write Raft response metadata.
    WRITE_META_FUNC writeRespMeta;

    bool alwaysInvokeCb;

    ptr<logger_wrapper> myLogWrapper;
    ptr<logger> myLog;
};

