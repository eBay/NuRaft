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

#ifndef _ERROR_CODE_HXX_
#define _ERROR_CODE_HXX_

namespace nuraft {

enum raft_err {
    ok = 0,
    error = -1,
    N2_leader_receive_AppendEntriesRequest = -2,
    N3_removed_from_cluster = -3,
    N4_leader_election_timeout = -4,
    N5_unexpected_msg_response = -5,
    N6_no_snapshot_found = -6,
    N7_no_config_at_idx_one = -7,
    N8_peer_last_log_idx_too_large = -8,
    N9_receive_unknown_request = -9,
    N10_leader_receive_InstallSnapshotRequest = -10,
    N11_not_follower_for_snapshot = -11,
    N12_apply_snapshot_failed = -12,
    N13_snapshot_install_failed = -13,
    N14_null_snapshot_sync_ctx = -14,
    N15_unexpected_response_msg_type = -15,
    N16_snapshot_for_peer_not_found = -16,
    N17_empty_snapshot = -17,
    N18_partial_snapshot_block = -18,
    N19_bad_log_idx_for_term = -19,
    N20_background_commit_err = -20,
    N21_log_flush_failed = -21,
    N22_unrecoverable_isolation = -22,
    N23_precommit_order_inversion = -23,
};

extern const char * raft_err_msg[];

}

#endif //_ERROR_CODE_HXX_

