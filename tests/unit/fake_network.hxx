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

#include "nuraft.hxx"

#include <map>
#include <unordered_map>

class SimpleLogger;

namespace nuraft {

class FakeClient;
class FakeNetworkBase;
class FakeNetwork
    : public rpc_client_factory
    , public rpc_listener
    , public std::enable_shared_from_this<FakeNetwork>
{
public:
    FakeNetwork(const std::string& _endpoint,
                ptr<FakeNetworkBase>& _base);

    struct ReqPkg {
        ReqPkg(ptr<req_msg>& _req, rpc_handler& _when_done)
            : req(_req), whenDone(_when_done)
            {}
        ptr<req_msg> req;
        rpc_handler whenDone;
    };

    struct RespPkg {
        RespPkg(ptr<resp_msg>& _resp, rpc_handler& _when_done)
            : resp(_resp), whenDone(_when_done)
            {}
        ptr<resp_msg> resp;
        rpc_handler whenDone;
    };

    FakeNetworkBase* getBase() const { return base.get(); }

    std::string getEndpoint() const { return myEndpoint; }

    ptr<rpc_client> create_client(const std::string& endpoint);

    void listen(ptr<msg_handler>& handler);

    ptr<resp_msg> gotMsg(ptr<req_msg>& msg);

    bool execReqResp(const std::string& endpoint = std::string());

    ptr<FakeClient> findClient(const std::string& endpoint);

    bool delieverReqTo(const std::string& endpoint,
                       bool random_order = false);

    void delieverAllTo(const std::string& endpoint);

    bool makeReqFail(const std::string& endpoint,
                     bool random_order = false);

    void makeReqFailAll(const std::string& endpoint);

    bool handleRespFrom(const std::string& endpoint,
                        bool random_order = false);

    void handleAllFrom(const std::string& endpoint);

    size_t getNumPendingReqs(const std::string& endpoint);

    size_t getNumPendingResps(const std::string& endpoint);

    void goesOffline() { online = false; }

    void goesOnline() { online =  true; }

    bool isOnline() const { return online; }

    void stop();

    void shutdown();

private:
    std::string myEndpoint;
    ptr<FakeNetworkBase> base;
    ptr<msg_handler> handler;
    std::unordered_map< std::string, ptr<FakeClient> > clients;
    std::mutex clientsLock;
    std::list< ptr<FakeClient> > staleClients;
    bool online;
};

class FakeNetworkBase {
public:
    FakeNetworkBase();

    ~FakeNetworkBase() { destroy(); }

    void destroy();

    void addNetwork(ptr<FakeNetwork>& net);

    void removeNetwork(const std::string& endpoint);

    FakeNetwork* findNetwork(const std::string& endpoint);

    SimpleLogger* getLogger() const { return myLog; }

private:
    // <endpoint, network instance>
    std::map<std::string, ptr<FakeNetwork>> nets;

    SimpleLogger* myLog;
};

class FakeClient : public rpc_client {
    friend class FakeNetwork;
public:
    FakeClient(FakeNetwork* mother,
               FakeNetwork* dst);

    ~FakeClient();

    void send(ptr<req_msg>& req, rpc_handler& when_done);

    void dropPackets();

    bool isDstOnline();

    uint64_t get_id() const;

private:
    uint64_t myId;
    FakeNetwork* motherNet;
    FakeNetwork* dstNet;
    std::list<FakeNetwork::ReqPkg> pendingReqs;
    std::list<FakeNetwork::RespPkg> pendingResps;
};

class FakeTimer : public delayed_task_scheduler {
public:
    FakeTimer(const std::string& endpoint,
              SimpleLogger* logger = nullptr);

    void schedule(ptr<delayed_task>& task, int32 milliseconds);

    void cancel(ptr<delayed_task>& task);

    void invoke(int type);

    size_t getNumPendingTasks(int type = -1);

private:
    void cancel_impl(ptr<delayed_task>& task);

    std::string myEndpoint;

    std::mutex tasksLock;

    std::list< ptr<delayed_task> > tasks;

    SimpleLogger* myLog;
};

}  // namespace nuraft;

