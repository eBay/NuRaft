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

namespace nuraft {

const char * raft_err_msg[] = {
    "OK",
    "N1: Error",
    "N2: Leader receive AppendEntriesRequest from another leader with same term.",
    "N3: Remove this server from cluster and step down.",
    "N4: Illegal state: leader shouldn't encounter election timeout.",
    "N5: Unexpected message for response.",
    "N6: No snapshot could be found while no config can be found in committed logs.",
    "N7: There must be a configuration at log index one.",
    "N8: Peer's lastLogIndex is too large.",
    "N9: Receive an unknown request type.",
    "N10: Leader receive InstallSnapshotRequest from another leader with same term.",
    "N11: Not follower for applying a snapshot.",
    "N12: Failed to apply the snapshot after log compacted.",
    "N13: Failed to handle snapshot installation due to system errors.",
    "N14: SnapshotSyncContext must not be null.",
    "N15: Received an unexpected response message type.",
    "N16: Failed to find a snapshot for peer.",
    "N17: Empty snapshot.",
    "N18: Partial snapshot block read.",
    "N19: Bad log_idx for retrieving the term value.",
    "N20: Background committing thread encounter err.",
    "N21: Log store flush failed.",
    "N22: This node does not get messages from leader, while the others do.",
    "N23: Commit is invoked before pre-commit, order inversion happened."
};

} // namespace nuraft;

