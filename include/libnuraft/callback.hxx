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

#ifndef _CALLBACK_H_
#define _CALLBACK_H_

#include <functional>
#include <string>

namespace nuraft {

class cb_func {
public:
    enum Type {
        /**
         * Got request from peer or client.
         * ctx: pointer to request.
         */
        ProcessReq = 1,

        /**
         * Got append entry response from peer.
         * ctx: pointer to new matched index number.
         */
        GotAppendEntryRespFromPeer = 2,

        /**
         * Appended logs and executed pre-commit locally.
         * Happens on leader only.
         * ctx: pointer to last log number.
         */
        AppendLogs = 3,

        /**
         * Heartbeat timer wakes up.
         * Happens on leader only.
         * ctx: pointer to last log number.
         */
        HeartBeat = 4,

        /**
         * Joined a cluster.
         * Happens on follower only.
         * ctx: pointer to cluster config.
         */
        JoinedCluster = 5,

        /**
         * Became a leader.
         * ctx: null.
         */
        BecomeLeader = 6,

        /**
         * Request append entries to followers.
         * Happens on leader only.
         */
        RequestAppendEntries = 7,

        /**
         * Save snapshot chunk or object, in receiver's side.
         * ctx: snapshot_sync_req.
         */
        SaveSnapshot = 8,

        /**
         * Committed a new config.
         * ctx: pointer to log index of new config.
         */
        NewConfig = 9,

        /**
         * Removed from a cluster.
         * Happens on follower only.
         * ctx: null.
         */
        RemovedFromCluster = 10,

        /**
         * Became a follower.
         * ctx: null.
         */
        BecomeFollower = 11,

        /**
         * The difference of committed log index between the follower and the
         * master became smaller than a user-specified threshold.
         * Happens on follower only.
         * ctx: null.
         */
        BecomeFresh = 12,

        /**
         * The difference of committed log index between the follower and the
         * master became larger than a user-specified threshold.
         * Happens on follwer only.
         * ctx: null
         */
        BecomeStale = 13,

        /**
         * Got append entry request from leader.
         * It will be invoked only for acceptable logs.
         * ctx: pointer to request.
         */
        GotAppendEntryReqFromLeader = 14,

        /**
         * This node is out of log range, which means that
         * leader has no valid log or snapshot to send for this node.
         * ctx: pointer to `OutOfLogRangeWarningArgs`.
         */
        OutOfLogRangeWarning = 15,

        /**
         * New connection is established.
         * Mostly this event happens in below cases:
         *   1) Leader sends message to follower, then follower will fire
         *      this event.
         *   2) Candidate sends vote request to peer, then the peer (receiver)
         *      will fire this event.
         * ctx: pointer to `ConnectionArgs`.
         */
        ConnectionOpened = 16,

        /**
         * Connection is closed.
         * ctx: pointer to `ConnectionArgs`.
         */
        ConnectionClosed = 17,

        /**
         * Invoked when a session receives a message from the valid leader
         * first time. This callback is preceded by `ConnectionOpened`
         * event.
         * ctx: pointer to `ConnectionArgs`.
         */
        NewSessionFromLeader = 18,

        /**
         * Executed a log in the state machine.
         * ctx: pointer to the log index.
         */
        StateMachineExecution = 19,
    };

    struct Param {
        Param(int32_t my_id = -1,
              int32_t leader_id = -1,
              int32_t peer_id = -1,
              void* _ctx = nullptr)
            : myId(my_id)
            , leaderId(leader_id)
            , peerId(peer_id)
            , ctx(_ctx)
            {}
        int32_t myId;
        int32_t leaderId;
        int32_t peerId;
        void* ctx;
    };

    enum ReturnCode {
        Ok = 0,
        ReturnNull = -1,
    };

    struct OutOfLogRangeWarningArgs {
        OutOfLogRangeWarningArgs(uint64_t x = 0) : startIdxOfLeader(x) {}
        uint64_t startIdxOfLeader;
    };

    struct ConnectionArgs {
        ConnectionArgs(uint64_t id = 0,
                       const std::string& addr = std::string(),
                       uint32_t p = 0,
                       int32_t srv_id = -1,
                       bool is_leader = false)
            : sessionId(id), address(addr), port(p)
            , srvId(srv_id), isLeader(is_leader) {}
        /**
         * ID of session.
         */
        uint64_t sessionId;

        /**
         * Endpoint address.
         */
        std::string address;

        /**
         * Endpoint port.
         */
        uint32_t port;

        /**
         * Endpoint server ID if given.
         */
        int32_t srvId;

        /**
         * `true` if the endpoint server is leader.
         */
        bool isLeader;
    };

    using func_type = std::function<ReturnCode(Type, Param*)>;

    cb_func() : func(nullptr) {}

    cb_func(func_type _func) : func(_func) {}

    ReturnCode call(Type type, Param* param) {
        if (func) {
            return func(type, param);
        }
        return Ok;
    }

private:
    func_type func;
};

}

#endif //_CALLBACK_H_
