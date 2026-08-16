/************************************************************************
Regression test for Bug 8: set_priority() must respect config_changing_ guard

set_priority() in handle_priority.cxx previously modified the cluster config
by appending a ConfigEntry to the log WITHOUT checking the config_changing_
flag. Every other config-modifying path (handle_add_srv_req,
handle_rm_srv_req) checks this flag before proceeding. Without the guard,
a priority change could create a concurrent config change while another
config change (including the BecomeLeader config entry) was still
uncommitted, violating the one-config-change-at-a-time invariant.

This test verifies the fix: when config_changing_ is true (due to an
uncommitted BecomeLeader config entry), set_priority() must return IGNORED
and must NOT append a second config entry to the log.

Reproduction strategy:
1. Set up a 3-node cluster
2. Trigger a leader election so become_leader() appends a ConfigEntry
3. Before that ConfigEntry is committed, call set_priority() on the leader
4. Verify set_priority() is rejected and the log still has only ONE
   uncommitted ConfigEntry
************************************************************************/

#include "debugging_options.hxx"
#include "fake_network.hxx"
#include "raft_package_fake.hxx"

#include "event_awaiter.hxx"
#include "raft_params.hxx"
#include "test_common.h"

#include <cinttypes>
#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

using raft_result = cmd_result< ptr<buffer> >;

namespace bug8_repro {

int set_priority_config_guard_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // At this point S1 is the leader and all configs are committed.
    CHK_TRUE( s1.raftServer->is_leader() );

    // Record the last committed log index before the election.
    uint64_t last_idx_before = s1.raftServer->get_last_log_idx();
    uint64_t committed_idx_before = s1.raftServer->get_committed_log_idx();
    _msg("Before election: last_log_idx=%" PRIu64 ", committed_log_idx=%" PRIu64 "\n",
         last_idx_before, committed_idx_before);

    // All log entries up to last_idx should be committed.
    CHK_EQ(last_idx_before, committed_idx_before);

    // --- Trigger leader election: S3 becomes the new leader ---

    // Trigger election timer of S2 (will fail, but sets up pre-vote state).
    s2.dbgLog(" --- invoke election timer of S2 ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote requests, rejected by S1 and S3.
    s2.fNet->execReqResp();

    // Trigger election timer of S3.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Send pre-vote requests. Rejected by S1, accepted by S2.
    // As part of response handling, S3 initiates actual vote.
    s3.fNet->execReqResp();
    // Send vote requests. S3 wins the election.
    // become_leader() is called, which:
    //   1. Appends a ConfigEntry to the log
    //   2. Sets config_changing_ = true
    //   3. Calls request_append_entries() (queues network messages)
    s3.fNet->execReqResp();

    // At this point S3 is the leader with an UNCOMMITTED config entry.
    CHK_TRUE( s3.raftServer->is_leader() );

    uint64_t leader_config_idx = s3.raftServer->get_log_idx_at_becoming_leader();
    _msg("S3 became leader, BecomeLeader config at log index %" PRIu64 "\n",
         leader_config_idx);
    CHK_GT(leader_config_idx, 0);

    // Verify the BecomeLeader config entry is NOT yet committed.
    uint64_t committed_after_election = s3.raftServer->get_committed_log_idx();
    _msg("After election: committed_log_idx=%" PRIu64 ", leader_config_idx=%" PRIu64 "\n",
         committed_after_election, leader_config_idx);
    CHK_GT(leader_config_idx, committed_after_election);

    // Count config entries in the log BEFORE set_priority.
    ptr<log_store> log = s3.raftServer->get_log_store();
    uint64_t last_log_idx = s3.raftServer->get_last_log_idx();
    int config_entries_before = 0;
    for (uint64_t i = committed_after_election + 1; i <= last_log_idx; i++) {
        ptr<log_entry> le = log->entry_at(i);
        if (le->get_val_type() == log_val_type::conf) {
            config_entries_before++;
            _msg("  Uncommitted config entry at index %" PRIu64 " (before set_priority)\n", i);
        }
    }
    _msg("Uncommitted config entries before set_priority: %d\n", config_entries_before);
    CHK_EQ(1, config_entries_before);  // Just the BecomeLeader config.

    // --- Call set_priority() while config_changing_ is true ---
    // With the fix, set_priority should check config_changing_ and return
    // IGNORED instead of appending another config entry.
    _msg("Calling set_priority(2, 100) on the new leader S3...\n");
    auto result = s3.raftServer->set_priority(2, 100);
    _msg("set_priority returned: %s\n",
         (result == raft_server::PrioritySetResult::SET) ? "SET" :
         (result == raft_server::PrioritySetResult::BROADCAST) ? "BROADCAST" : "IGNORED");

    // FIX VERIFIED: set_priority must be rejected because config_changing_
    // is true. Before the fix, it would return SET and append a second
    // config entry, violating the one-config-change-at-a-time invariant.
    CHK_EQ(raft_server::PrioritySetResult::IGNORED, result);

    // Verify the log still has only ONE uncommitted config entry.
    uint64_t last_log_idx_after = s3.raftServer->get_last_log_idx();
    int config_entries_after = 0;
    for (uint64_t i = committed_after_election + 1; i <= last_log_idx_after; i++) {
        ptr<log_entry> le = log->entry_at(i);
        if (le->get_val_type() == log_val_type::conf) {
            config_entries_after++;
            _msg("  Uncommitted config entry at index %" PRIu64 " (after set_priority)\n", i);
        }
    }
    _msg("Uncommitted config entries after set_priority: %d\n", config_entries_after);

    // One-config-change-at-a-time invariant holds: still only one
    // uncommitted config entry (the BecomeLeader config).
    CHK_EQ(1, config_entries_after);

    _msg("\n=== FIX VERIFIED ===\n");
    _msg("set_priority() correctly checks config_changing_ and returns IGNORED\n");
    _msg("when another config change is in progress. The log still has only\n");
    _msg("one uncommitted config entry at index %" PRIu64 ".\n", leader_config_idx);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

}  // namespace bug8_repro

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    // Disable reconnection timer for deterministic test.
    debugging_options::get_instance().disable_reconn_backoff_ = true;

    ts.doTest( "set_priority config_changing guard (Bug 8)",
               bug8_repro::set_priority_config_guard_test );

    return 0;
}
