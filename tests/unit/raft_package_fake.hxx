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

using namespace nuraft;
using namespace raft_functional_common;

static size_t COMMIT_TIME_MS = 50;

class RaftPkg {
public:
    RaftPkg(ptr<FakeNetworkBase>& f_base,
            int srv_id,
            const std::string& endpoint)
        : myId(srv_id)
        , myEndpoint(endpoint)
        , fBase(f_base)
        , fNet(nullptr)
        , fTimer(nullptr)
        , sMgr(nullptr)
        , sm(nullptr)
        , myLogWrapper(nullptr)
        , myLog(nullptr)
        , listener(nullptr)
        , rpcCliFactory(nullptr)
        , scheduler(nullptr)
        , ctx(nullptr)
        , raftServer(nullptr)
        {}

    ~RaftPkg() {
        if (myLogWrapper) myLogWrapper->destroy();
        if (fNet) fNet->shutdown();
    }

    void initServer(raft_params* given_params = nullptr,
                    const raft_server::init_options opt =
                        raft_server::init_options(),
                    cb_func::func_type raft_callback = nullptr)
    {
        fNet = cs_new<FakeNetwork>( myEndpoint, fBase );
        fBase->addNetwork(fNet);

        fTimer = cs_new<FakeTimer>( myEndpoint, fBase->getLogger() );
        sMgr = cs_new<TestMgr>(myId, myEndpoint);
        sm = cs_new<TestSm>( fBase->getLogger() );

        std::string log_file_name = "./srv" + std::to_string(myId) + ".log";
        myLogWrapper = cs_new<logger_wrapper>(log_file_name);
        myLog = myLogWrapper;

        listener = fNet;
        rpcCliFactory = fNet;
        scheduler = fTimer;

        if (!given_params) {
            params.with_election_timeout_lower(0);
            params.with_election_timeout_upper(10000);
            params.with_hb_interval(5000);
            params.with_client_req_timeout(1000000);
            params.with_reserved_log_items(0);
            params.with_snapshot_enabled(5);
            params.with_log_sync_stopping_gap(1);
        } else {
            params = *given_params;
        }
        // For deterministic test, we should not use BG thread.
        params.use_bg_thread_for_urgent_commit_ = false;

        ctx = new context( sMgr, sm, listener, myLog,
                           rpcCliFactory, scheduler, params );
        if (raft_callback) {
            ctx->set_cb_func(raft_callback);
        }
        raftServer = cs_new<raft_server>(ctx, opt);
    }

    void free() {
        // WARNING:
        //   Due to circular reference base <-> net,
        //   should cut off it here.
        fBase->removeNetwork(myEndpoint);
    }

    TestMgr* getTestMgr() const {
        return static_cast<TestMgr*>(sMgr.get());
    }

    TestSm* getTestSm() const {
        return static_cast<TestSm*>(sm.get());
    }

    void dbgLog(const std::string& msg) {
        SimpleLogger* ll = fBase->getLogger();
        _s_info(ll) << msg;
    }

    int myId;
    std::string myEndpoint;
    ptr<FakeNetworkBase> fBase;
    ptr<FakeNetwork> fNet;
    ptr<FakeTimer> fTimer;
    ptr<state_mgr> sMgr;
    ptr<state_machine> sm;
    ptr<logger_wrapper> myLogWrapper;
    ptr<logger> myLog;
    ptr<rpc_listener> listener;
    ptr<rpc_client_factory> rpcCliFactory;
    ptr<delayed_task_scheduler> scheduler;
    raft_params params;
    context* ctx;
    ptr<raft_server> raftServer;
};

static INT_UNUSED launch_servers(const std::vector<RaftPkg*>& pkgs,
                                 raft_params* custom_params = nullptr) {
    size_t num_srvs = pkgs.size();
    CHK_GT(num_srvs, 0);

    for (size_t ii = 0; ii < num_srvs; ++ii) {
        RaftPkg* ff = pkgs[ii];
        ff->initServer(custom_params);
        ff->fNet->listen(ff->raftServer);
        ff->fTimer->invoke( timer_task_type::election_timer );
    }
    return 0;
}

static INT_UNUSED make_group(const std::vector<RaftPkg*>& pkgs) {
    size_t num_srvs = pkgs.size();
    CHK_GT(num_srvs, 0);

    RaftPkg* leader = pkgs[0];

    for (size_t ii = 1; ii < num_srvs; ++ii) {
        RaftPkg* ff = pkgs[ii];

        // Add to leader.
        leader->raftServer->add_srv( *(ff->getTestMgr()->get_srv_config()) );

        // Join req/resp.
        leader->fNet->execReqResp();
        // Add new server, notify existing peers.
        // After getting response, it will make configuration commit.
        leader->fNet->execReqResp();
        // Notify new commit.
        leader->fNet->execReqResp();
        // Wait for bg commit for configuration change.
        TestSuite::sleep_ms(COMMIT_TIME_MS);

        // Now heartbeat to new node is enabled.

        // Heartbeat.
        leader->fTimer->invoke( timer_task_type::heartbeat_timer );
        // Heartbeat req/resp, to finish the catch-up phase.
        leader->fNet->execReqResp();
        // Need one-more req/resp.
        leader->fNet->execReqResp();
        // Wait for bg commit for new node.
        TestSuite::sleep_ms(COMMIT_TIME_MS);
    }
    return 0;
}

static VOID_UNUSED print_stats(const std::vector<RaftPkg*>& pkgs) {
    for (auto& entry: pkgs) {
        RaftPkg* pkg = entry;
        _msg( "%s\n", pkg->myEndpoint.c_str() );
        for (auto& e2: pkgs) {
            RaftPkg* dst = e2;
            if (dst == pkg) continue;
            _msg("  to %s: %zu reqs %zu resps remaining\n",
                 dst->myEndpoint.c_str(),
                 pkg->fNet->getNumPendingReqs(dst->myEndpoint),
                 pkg->fNet->getNumPendingResps(dst->myEndpoint));
        }
        _msg( "  %zu remaining timer tasks\n",
              pkg->fTimer->getNumPendingTasks() );
    }
}

