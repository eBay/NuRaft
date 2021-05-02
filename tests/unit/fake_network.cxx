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

#include "fake_network.hxx"
#include "strfmt.hxx"

#include "logger.h"

#include <cassert>

namespace nuraft {

// === FakeNetworkBase

FakeNetworkBase::FakeNetworkBase() {
    myLog = new SimpleLogger("./base.log", 1024, 32*1024*1024, 10);
    myLog->setLogLevel(6);
    myLog->setDispLevel(-1);
    myLog->setCrashDumpPath("./", true);
    myLog->start();
}

void FakeNetworkBase::destroy() {
    if (myLog) {
        myLog->flushAll();
        myLog->stop();
        delete myLog;
        myLog = nullptr;
    }
    nets.clear();
    SimpleLogger::shutdown();
}

void FakeNetworkBase::addNetwork(ptr<FakeNetwork>& net) {
    nets[net->getEndpoint()] = net;
}

void FakeNetworkBase::removeNetwork(const std::string& endpoint) {
    auto entry = nets.find(endpoint);
    if (entry == nets.end()) return;
    nets.erase(entry);
}

FakeNetwork* FakeNetworkBase::findNetwork(const std::string& endpoint) {
    auto entry = nets.find(endpoint);
    if (entry == nets.end()) return nullptr;
    return (entry->second).get();
}


// === FakeNetwork

FakeNetwork::FakeNetwork(const std::string& _endpoint,
                         ptr<FakeNetworkBase>& _base)
    : myEndpoint(_endpoint)
    , base(_base)
    , handler(nullptr)
    , online(true)
{}

ptr<rpc_client> FakeNetwork::create_client(const std::string& endpoint) {
    FakeNetwork* dst_net = base->findNetwork(endpoint);
    if (!dst_net) return nullptr;

    std::lock_guard<std::mutex> ll(clientsLock);
    auto entry = clients.find(endpoint);
    if (entry != clients.end()) {
        // Already exists, move it to garbage list as it will be
        // replaced below.
        staleClients.push_back(entry->second);
        clients.erase(entry);
    }

    ptr<FakeClient> ret = cs_new<FakeClient>(this, dst_net);
    clients[endpoint] = ret;
    return ret;
}

void FakeNetwork::listen(ptr<msg_handler>& _handler) {
    handler = _handler;
}

ptr<resp_msg> FakeNetwork::gotMsg(ptr<req_msg>& msg) {
    ptr<resp_msg> resp = handler->process_req(*msg);
    return resp;
}

bool FakeNetwork::execReqResp(const std::string& endpoint) {
    if (endpoint.empty()) {
        // Do the same thing to all.

        std::map< std::string, ptr<FakeClient> > clients_clone;

        // WARNING:
        //   As a result of processing req or resp, client re-connection
        //   may happen and previous connection will be removed. To avoid
        //   `FakeClient` is being removed in the middle of processing,
        //   we should maintain the copy of the list to keep their
        //   reference counter greater than 0.
        {   std::lock_guard<std::mutex> ll(clientsLock);
            clients_clone = clients;
        }
        for (auto& entry: clients_clone) {
            const std::string& cur_endpoint = entry.first;
            bool ret = delieverReqTo(cur_endpoint);
            if (!ret) continue;
        }

        // Same as above.
        {   std::lock_guard<std::mutex> ll(clientsLock);
            clients_clone = clients;
        }
        for (auto& entry: clients_clone) {
            const std::string& cur_endpoint = entry.first;
            bool ret = handleRespFrom(cur_endpoint);
            if (!ret) continue;
        }
        return true;
    }

    bool ret = delieverReqTo(endpoint);
    if (!ret) return ret;

    ret = handleRespFrom(endpoint);
    return ret;
}

ptr<FakeClient> FakeNetwork::findClient(const std::string& endpoint) {
    std::lock_guard<std::mutex> ll(clientsLock);
    auto entry = clients.find(endpoint);
    if (entry == clients.end()) return nullptr;
    return entry->second;
}

bool FakeNetwork::delieverReqTo(const std::string& endpoint,
                                bool random_order)
{
    // this:                    source (sending request)
    // conn->dstNet (endpoint): destination (sending response)
    ptr<FakeClient> conn = findClient(endpoint);

    // If destination is offline, make failure.
    if (!conn->isDstOnline()) return makeReqFail(endpoint, random_order);

    auto pkg_entry = conn->pendingReqs.begin();
    if (pkg_entry == conn->pendingReqs.end()) return false;

    ReqPkg& pkg = *pkg_entry;

    SimpleLogger* ll = base->getLogger();
    _log_info(ll, "[BEGIN] send/process %s -> %s, %s",
              myEndpoint.c_str(), endpoint.c_str(),
              msg_type_to_string( pkg.req->get_type() ).c_str() );

    ptr<resp_msg> resp = conn->dstNet->gotMsg( pkg.req );
    _log_info(ll, "[END] send/process %s -> %s, %s",
              myEndpoint.c_str(), endpoint.c_str(),
              msg_type_to_string( pkg.req->get_type() ).c_str() );

    conn->pendingResps.push_back( FakeNetwork::RespPkg(resp, pkg.whenDone) );
    conn->pendingReqs.erase(pkg_entry);
    return true;
}

void FakeNetwork::delieverAllTo(const std::string& endpoint) {
    while (delieverReqTo(endpoint));
}

bool FakeNetwork::makeReqFail(const std::string& endpoint,
                              bool random_order)
{
    // this:                    source (sending request)
    // conn->dstNet (endpoint): destination (sending response)
    ptr<FakeClient> conn = findClient(endpoint);

    auto pkg_entry = conn->pendingReqs.begin();
    if (pkg_entry == conn->pendingReqs.end()) return false;

    ReqPkg& pkg = *pkg_entry;

    SimpleLogger* ll = base->getLogger();
    _log_info(ll, "[BEGIN] make request %s -> %s failed, %s",
              myEndpoint.c_str(), endpoint.c_str(),
              msg_type_to_string( pkg.req->get_type() ).c_str() );

    ptr<resp_msg> rsp; // empty.
    ptr<rpc_exception> exp
        ( cs_new<rpc_exception>
          ( sstrfmt( "failed to send request to peer %d" )
                   .fmt( pkg.req->get_dst() ),
            pkg.req ) );
    pkg.whenDone( rsp, exp );

    _log_info(ll, "[END] make request %s -> %s failed, %s",
              myEndpoint.c_str(), endpoint.c_str(),
              msg_type_to_string( pkg.req->get_type() ).c_str() );

    conn->pendingReqs.erase(pkg_entry);
    return true;
}

void FakeNetwork::makeReqFailAll(const std::string& endpoint) {
    while (makeReqFail(endpoint));
}

bool FakeNetwork::handleRespFrom(const std::string& endpoint,
                                 bool random_order)
{
    // this:        source (sending request)
    // endpoint:    destination (sending response)
    ptr<FakeClient> conn = findClient(endpoint);

    auto pkg_entry = conn->pendingResps.begin();
    if (pkg_entry == conn->pendingResps.end()) return false;

    SimpleLogger* ll = base->getLogger();

    // Copy shared pointer for the case of reconnection,
    // as it drops all resps.
    RespPkg pkg = *pkg_entry;
    _log_info(ll, "[BEGIN] deliver response %s -> %s, %s",
              endpoint.c_str(), myEndpoint.c_str(),
              msg_type_to_string( pkg.resp->get_type() ).c_str() );

    ptr<rpc_exception> exp;
    pkg.whenDone( pkg.resp, exp );

    _log_info(ll, "[END] deliver response %s -> %s, %s",
              endpoint.c_str(), myEndpoint.c_str(),
              msg_type_to_string( pkg.resp->get_type() ).c_str() );

    conn->pendingResps.erase(pkg_entry);

    for (auto& entry: staleClients) {
        // If client re-connection happened, there will be
        // stale clients. Drop all packets of them.
        ptr<FakeClient>& cc = entry;
        cc->dropPackets();
    }
    staleClients.clear();

    return true;
}

void FakeNetwork::handleAllFrom(const std::string& endpoint) {
    while (handleRespFrom(endpoint));
}

size_t FakeNetwork::getNumPendingReqs(const std::string& endpoint) {
    ptr<FakeClient> conn = findClient(endpoint);
    if (!conn) return 0;
    return conn->pendingReqs.size();
}

size_t FakeNetwork::getNumPendingResps(const std::string& endpoint) {
    ptr<FakeClient> conn = findClient(endpoint);
    if (!conn) return 0;
    return conn->pendingResps.size();
}

void FakeNetwork::stop() {
    handler = nullptr;
}

void FakeNetwork::shutdown() {
    std::lock_guard<std::mutex> ll(clientsLock);
    for (auto& entry: clients) {
        ptr<FakeClient>& cc = entry.second;
        cc->dropPackets();
    }
    clients.clear();
}


// === FakeClient
static std::atomic<uint64_t> fake_client_id_counter(1);

FakeClient::FakeClient(FakeNetwork* mother,
                       FakeNetwork* dst)
    : myId(fake_client_id_counter.fetch_add(1))
    , motherNet(mother)
    , dstNet(dst)
{}

FakeClient::~FakeClient() {}

void FakeClient::send(ptr<req_msg>& req,
                      rpc_handler& when_done,
                      uint64_t /*send_timeout_ms*/)
{
    SimpleLogger* ll = motherNet->getBase()->getLogger();
    _log_info(ll, "got request %s -> %s, %s",
              motherNet->getEndpoint().c_str(),
              dstNet->getEndpoint().c_str(),
              msg_type_to_string( req->get_type() ).c_str() );
    pendingReqs.push_back( FakeNetwork::ReqPkg(req, when_done) );
}

void FakeClient::dropPackets() {
    pendingReqs.clear();
    pendingResps.clear();
}

bool FakeClient::isDstOnline() {
    if (!dstNet) return false;
    return dstNet->isOnline();
}

uint64_t FakeClient::get_id() const {
    return myId;
}

bool FakeClient::is_abandoned() const {
    return false;
}


// === FakeTimer

FakeTimer::FakeTimer(const std::string& endpoint,
                     SimpleLogger* logger)
    : myEndpoint(endpoint)
    , myLog(logger)
    {}

void FakeTimer::schedule(ptr<delayed_task>& task, int32 milliseconds) {
    std::lock_guard<std::mutex> l(tasksLock);
    _log_info( myLog, " --- schedule timer for %s %d %p ---",
               myEndpoint.c_str(), task->get_type(), task.get() );
    task->reset();
    tasks.push_back(task);
}

void FakeTimer::cancel(ptr<delayed_task>& task) {
    cancel_impl(task);
}

void FakeTimer::invoke(int type) {
    _log_info( myLog, " --- invoke timer tasks for %s %d ---",
               myEndpoint.c_str(), type);

    std::list< ptr<delayed_task> > tasks_to_invoke;
    {   std::lock_guard<std::mutex> l(tasksLock);
        auto entry = tasks.begin();
        while (entry != tasks.end()) {
            ptr<delayed_task> cur_task = *entry;
            if (cur_task->get_type() == type) {
                entry = tasks.erase(entry);
                tasks_to_invoke.push_back(cur_task);
            } else {
                entry++;
            }
        }
    }

    for (auto& entry: tasks_to_invoke) {
        ptr<delayed_task>& cur_task = entry;
        cur_task->execute();
    }
}

size_t FakeTimer::getNumPendingTasks(int type) {
    std::lock_guard<std::mutex> l(tasksLock);

    if (type < 0) {
        // Count all.
        return tasks.size();
    }

    size_t count = 0;
    for (auto& entry: tasks) {
        ptr<delayed_task>& cur_task = entry;
        if (cur_task->get_type() == type) count++;
    }
    return count;
}

void FakeTimer::cancel_impl(ptr<delayed_task>& task) {
    std::lock_guard<std::mutex> l(tasksLock);
    auto entry = tasks.begin();
    while (entry != tasks.end()) {
        ptr<delayed_task> cur_task = *entry;
        if (cur_task.get() == task.get()) {
            entry = tasks.erase(entry);
            cur_task->cancel();
            _log_info( myLog, " --- cancel timer for %s %d %p ---",
                       myEndpoint.c_str(),
                       cur_task->get_type(),
                       cur_task.get() );

        } else {
            entry++;
        }
    }
}

}  // namespace nuraft;

