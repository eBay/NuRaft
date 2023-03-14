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

#ifndef _MESSAGE_TYPE_HXX_
#define _MESSAGE_TYPE_HXX_

#include <string>

namespace nuraft {

#include "attr_unused.hxx"

// NOTE:
//   need to change `msg_type_to_string()` as well
//   whenever modify this enum.
enum msg_type {
    request_vote_request            = 1,
    request_vote_response           = 2,
    append_entries_request          = 3,
    append_entries_response         = 4,
    client_request                  = 5,
    add_server_request              = 6,
    add_server_response             = 7,
    remove_server_request           = 8,
    remove_server_response          = 9,
    sync_log_request                = 10,
    sync_log_response               = 11,
    join_cluster_request            = 12,
    join_cluster_response           = 13,
    leave_cluster_request           = 14,
    leave_cluster_response          = 15,
    install_snapshot_request        = 16,
    install_snapshot_response       = 17,
    ping_request                    = 18,
    ping_response                   = 19,
    pre_vote_request                = 20,
    pre_vote_response               = 21,
    other_request                   = 22,
    other_response                  = 23,
    priority_change_request         = 24,
    priority_change_response        = 25,
    reconnect_request               = 26,
    reconnect_response              = 27,
    custom_notification_request     = 28,
    custom_notification_response    = 29,
};

static bool ATTR_UNUSED is_valid_msg(msg_type type) {
    if ( type >= request_vote_request &&
         type <= other_response ) {
        return true;
    }
    return false;
}

// for tracing and debugging
static std::string ATTR_UNUSED msg_type_to_string(msg_type type)
{
    switch (type) {
    case request_vote_request:          return "request_vote_request";
    case request_vote_response:         return "request_vote_response";
    case append_entries_request:        return "append_entries_request";
    case append_entries_response:       return "append_entries_response";
    case client_request:                return "client_request";
    case add_server_request:            return "add_server_request";
    case add_server_response:           return "add_server_response";
    case remove_server_request:         return "remove_server_request";
    case remove_server_response:        return "remove_server_response";
    case sync_log_request:              return "sync_log_request";
    case sync_log_response:             return "sync_log_response";
    case join_cluster_request:          return "join_cluster_request";
    case join_cluster_response:         return "join_cluster_response";
    case leave_cluster_request:         return "leave_cluster_request";
    case leave_cluster_response:        return "leave_cluster_response";
    case install_snapshot_request:      return "install_snapshot_request";
    case install_snapshot_response:     return "install_snapshot_response";
    case ping_request:                  return "ping_request";
    case ping_response:                 return "ping_response";
    case pre_vote_request:              return "pre_vote_request";
    case pre_vote_response:             return "pre_vote_response";
    case other_request:                 return "other_request";
    case other_response:                return "other_response";
    case priority_change_request:       return "priority_change_request";
    case priority_change_response:      return "priority_change_response";
    case reconnect_request:             return "reconnect_request";
    case reconnect_response:            return "reconnect_response";
    case custom_notification_request:   return "custom_notification_request";
    case custom_notification_response:  return "custom_notification_response";
    default:
        return "unknown (" + std::to_string(static_cast<int>(type)) + ")";
    }
}

}

#endif //_MESSAGE_TYPE_HXX_
