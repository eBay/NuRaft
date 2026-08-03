/************************************************************************
Copyright 2017-present eBay Inc.
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

#include "buffer_serializer.hxx"
#include "debugging_options.hxx"
#include "in_memory_log_store.hxx"
#include "raft_package_asio.hxx"
#include "asio_test_common.hxx"

#include "event_awaiter.hxx"
#include "test_common.h"

#ifdef USE_BOOST_ASIO
    #include <boost/asio.hpp>
    using namespace boost;
    using asio_error_code = system::error_code;
#else
    #include <asio.hpp>
    using asio_error_code = asio::error_code;
#endif

#include <unordered_map>

#include <stdio.h>

// Fake network/timer harness for the deterministic incident-replay tests.
#include "raft_package_fake.hxx"

using namespace nuraft;
using namespace raft_functional_common;

namespace priority_based_election_test {

/************************************************************************
 * Deterministic replay of the 2026-07-29 production incident:
 * 3-tier cluster (priorities 3/2/1), leader powered off, and a
 * priority-1 node won the election. Scaled to 5 nodes with the fake
 * network/timer harness so every message and timer event is explicit:
 *   S1: priority 3 (leader; powered off)
 *   S2: priority 3 (surviving top-tier node -- expected winner)
 *   S3, S4: priority 2 (mid tier)
 *   S5: priority 1 (low tier -- must never win)
 *
 * Test 1 (stale_hb_prevote_grant_test): replays the stale `hb_alive_`
 *   pre-vote denials. Fails without the `hb_alive_decision` fix.
 * Test 2 (tiered_decay_excludes_low_priority_test): replays the decay
 *   collapse. Fails without the next-highest-priority decay: the old
 *   arithmetic decay lets the priority-1 node become leader, which is
 *   the incident outcome.
 ************************************************************************/


// Election timeout lower bound applied to the FOLLOWERS (S3-S5) after
// group formation, via update_params(). It defines the freshness window
// of the PR's pre-vote staleness check.
//
// NOTE: all servers are LAUNCHED with the harness-default lower bound of
// 0, because `handle_election_timeout()` silently ignores any timer event
// that arrives sooner than the lower bound after the last timer reset --
// with a non-zero bound at launch, the fake timer's manual invocations
// would be swallowed and the group could never form. Raising the bound
// only on the followers afterwards is safe: their election timers are
// never invoked in test 1, and `handle_prevote_req()` reads the params
// live.
static const int FOLLOWER_ELECTION_LOWER_MS = 800;
static const int STALE_SLEEP_MS = FOLLOWER_ELECTION_LOWER_MS + 200;

// Build the 5-node incident topology and set tier priorities.
// On return, S1 is leader and all nodes agree on priorities {3,3,2,2,1}.
static int setup_incident_group(const std::vector<RaftPkg*>& pkgs) {
    RaftPkg* s1 = pkgs[0];

    // Harness-default params (election timeout lower bound 0).
    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Tier priorities: S1=3, S2=3, S3=2, S4=2, S5=1.
    // (The test harness's default priority is 50, so all five must be
    //  set explicitly.)
    struct PrioEntry { int id; int prio; };
    std::vector<PrioEntry> prios = { {1, 3}, {2, 3}, {3, 2}, {4, 2}, {5, 1} };
    for (auto& entry: prios) {
        s1->raftServer->set_priority(entry.id, entry.prio);
        // Config change req/resp.
        s1->fNet->execReqResp();
        // One more for commit.
        s1->fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    }

    // All nodes should agree on the tiering.
    std::vector<int> expected = {3, 3, 2, 2, 1};
    for (auto& pkg: pkgs) {
        for (size_t ii = 1; ii <= pkgs.size(); ++ii) {
            CHK_EQ( expected[ii - 1],
                    pkg->raftServer->get_srv_config(ii)->get_priority() );
        }
    }

    // Deliver one more heartbeat so that every follower's election timer
    // reset is fresh, then the caller can decide when to kill S1.
    s1->fTimer->invoke( timer_task_type::heartbeat_timer );
    s1->fNet->execReqResp();

    return 0;
}

// Incident phase 1: leader dies, the surviving top-tier node pre-votes
// immediately. Followers' own election timers have not fired.
//
//   Round 1 (fresh):  followers deny (hb still genuinely fresh) -- this
//                     holds on master AND with the PR (the staleness
//                     override must not be wide open).
//   Round 2 (stale):  after sleeping past the election timeout lower
//                     bound, followers must grant even though their own
//                     timers never fired, and S2 (priority 3) must win
//                     while target priority is still 3.
int stale_hb_prevote_grant_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    RaftPkg s4(f_base, 4, "S4");
    RaftPkg s5(f_base, 5, "S5");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3, &s4, &s5};

    CHK_Z( setup_incident_group(pkgs) );

    // Give the followers a real freshness window: only now raise their
    // election timeout lower bound (see the note on
    // FOLLOWER_ELECTION_LOWER_MS).
    for (RaftPkg* pkg: { &s3, &s4, &s5 }) {
        raft_params cur = pkg->raftServer->get_current_params();
        cur.with_election_timeout_lower(FOLLOWER_ELECTION_LOWER_MS);
        pkg->raftServer->update_params(cur);
    }

    // Refresh the followers' election timer resets with one more
    // heartbeat, then power off the leader (the incident's node 7).
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    s1.dbgLog(" --- S1 (leader, priority 3) goes offline ---");
    s1.fNet->goesOffline();

    // S2's election timer fires first (it is the only node whose timer
    // we invoke). Its pre-vote arrives at S3-S5 while their heartbeat
    // freshness window is still open.
    s2.dbgLog(" --- invoke election timer of S2 (fresh round) ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Deliver pre-vote reqs/resps; nothing further should happen.
    s2.fNet->execReqResp();
    s2.fNet->execReqResp();

    // Followers were fresh, so the round must have been denied.
    CHK_FALSE( s2.raftServer->is_leader() );

    // Now age the followers past the election timeout lower bound
    // WITHOUT firing their election timers. Their raw `hb_alive_` flag
    // remains true; only the PR's staleness override can flip the
    // decision.
    TestSuite::sleep_ms(STALE_SLEEP_MS, "aging followers' heartbeat state");

    s2.dbgLog(" --- invoke election timer of S2 (stale round) ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Pre-vote round: with the PR, S3-S5 must now grant.
    // As part of resp handling, S2 initiates the actual vote.
    s2.fNet->execReqResp();
    // Vote round: S3-S5 grant (S2's priority 3 meets their target 3).
    s2.fNet->execReqResp();

    // The surviving top-tier node must now be the leader.
    // On master this fails: S3-S5 keep denying (`hb_alive_` stale-true),
    // and S2 cannot form a pre-vote quorum this round.
    CHK_TRUE( s2.raftServer->is_leader() );
    CHK_FALSE( s5.raftServer->is_leader() );

    print_stats(pkgs);
    for (auto& pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// Incident phase 2: BOTH top-tier nodes are gone, every survivor has
// gone through one failed election round (hb dead), and decay begins.
//
//   With the PR, one decay step moves the target 3 -> 2:
//     - S5 (priority 1) stays excluded at its own gate, and even if it
//       could ask, receivers deny it at the vote check.
//     - S3 (priority 2) becomes eligible and wins.
//   On master, one decay step moves the target 3 -> 1 (gap >= 10):
//     - S5 initiates, receivers decay to 1 via the `process_req()`
//       trigger and grant, and the priority-1 node becomes leader --
//       exactly the incident outcome. The assertions below fail there.
int tiered_decay_excludes_low_priority_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    RaftPkg s4(f_base, 4, "S4");
    RaftPkg s5(f_base, 5, "S5");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3, &s4, &s5};

    CHK_Z( setup_incident_group(pkgs) );

    // Both priority-3 nodes vanish. Quorum (3) is still available via
    // S3, S4, S5, but no node matches the initial target priority 3.
    s1.dbgLog(" --- S1 and S2 (both priority 3) go offline ---");
    s1.fNet->goesOffline();
    s2.fNet->goesOffline();

    // First election timeout on every survivor: hb was alive, so no
    // decay yet; all are blocked by the priority gate (target 3), and
    // all mark their heartbeat dead. This mirrors the incident state
    // right before the decay stampede.
    for (RaftPkg* pkg: { &s3, &s4, &s5 }) {
        pkg->dbgLog(" --- first election timeout (no decay, gate holds) ---");
        pkg->fTimer->invoke( timer_task_type::election_timer );
    }
    CHK_FALSE( s3.raftServer->is_leader() );
    CHK_FALSE( s4.raftServer->is_leader() );
    CHK_FALSE( s5.raftServer->is_leader() );

    // Second timeout on the LOW-priority node first -- give the
    // incident's winner the best possible head start.
    s5.dbgLog(" --- second election timeout of S5 (decay happens) ---");
    s5.fTimer->invoke( timer_task_type::election_timer );
    // Drive whatever S5 produced. With the PR the target decayed only
    // to 2, S5 (priority 1) stays gated and sends nothing meaningful.
    // On master the target collapsed to 1 and these two rounds elect S5.
    s5.fNet->execReqResp();
    s5.fNet->execReqResp();

    // THE incident assertion: the low-tier node must not lead.
    CHK_FALSE( s5.raftServer->is_leader() );

    // Now the mid-tier node's second timeout: decay 3 -> 2 makes it
    // eligible, pre-votes are granted (everyone is hb-dead), and the
    // vote round succeeds (priority 2 meets the decayed target 2 of
    // its peers).
    s3.dbgLog(" --- second election timeout of S3 (decay -> eligible) ---");
    s3.fTimer->invoke( timer_task_type::election_timer );
    // Pre-vote round; resp handling initiates the actual vote.
    s3.fNet->execReqResp();
    // Vote round.
    s3.fNet->execReqResp();

    // A mid-tier node must win; the low-tier node must still not lead.
    CHK_TRUE( s3.raftServer->is_leader() );
    CHK_FALSE( s5.raftServer->is_leader() );

    // The elected leader's configured priority must be >= 2:
    // leadership never falls through to the bottom tier while a
    // higher tier is alive and healthy.
    int leader_id = s3.raftServer->get_leader();
    CHK_EQ( 3, leader_id );
    CHK_GTEQ( s3.raftServer->get_srv_config(leader_id)->get_priority(), 2 );

    print_stats(pkgs);
    for (auto& pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}




// ============================================================================
// KNOWN-FAILING TEST -- documents a gap in PR 661, not a regression guard.
//
// Replays the receiver-side decay stampede visible in the production logs
// (node 1, 22:18:17.176-17.195): `BECOME FOLLOWER term 253` followed
// immediately by a decay and then VOTE REQ handling. That decay was fired by
// `process_req()` (raft_server.cxx) on an incoming `request_vote_request`
// carrying a newer term while `hb_alive_` is false -- NOT by the node's own
// election timeout. PR 661 changes how FAR each decay steps (one tier), but
// keeps this trigger, so during a vote storm every competing candidate's
// request still burns one tier at each receiver: two requests within
// milliseconds and the receiver's bar is back down to 1.
//
// Invariant this test encodes:
//   A node's target priority must not fall below the tier justified by its
//   OWN failed election rounds. Here S5 (priority 1) experiences exactly ONE
//   of its own election timeouts, yet after passively receiving two vote
//   requests (term 2 from S2, term 3 from S3) its target collapses to 1 and
//   it starts its own candidacy on its very next timeout.
//
// Expected results:
//   - master:       FAIL (arithmetic decay collapses on the first trigger)
//   - PR 661 as-is: FAIL (per-tier decay, but one tier per incoming request)
//   - PASSES only once the `process_req()` decay trigger is rate-limited
//     (e.g. at most one decay per election-timeout period) or removed.
// ============================================================================
int process_req_decay_storm_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    RaftPkg s4(f_base, 4, "S4");
    RaftPkg s5(f_base, 5, "S5");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3, &s4, &s5};

    CHK_Z( setup_incident_group(pkgs) );

    // Power off the leader.
    s1.dbgLog(" --- S1 (leader, priority 3) goes offline ---");
    s1.fNet->goesOffline();

    // Every survivor experiences its FIRST (and for S5, only) election
    // timeout: hb goes dead, the priority gate holds at target 3, no decay.
    for (RaftPkg* pkg: { &s3, &s4, &s5 }) {
        pkg->dbgLog(" --- first election timeout (gate holds) ---");
        pkg->fTimer->invoke( timer_task_type::election_timer );
    }

    // --- Storm request #1: S2 (priority 3) legitimately runs for term 2. ---
    s2.dbgLog(" --- S2 initiates election (term 2) ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Deliver S2's pre-vote requests (responses stay queued for now).
    // NOTE on ordering: S2 initiates the actual vote the moment the
    // pre-vote quorum completes during response handling, and any peer
    // whose pre-vote response is still un-handled is `busy` at that
    // instant and gets NO vote request. So S5's pre-vote response must be
    // handled FIRST, before quorum completes, to ensure the term-2 vote
    // request is queued toward S5.
    CHK_TRUE( s2.fNet->delieverReqTo("S3") );
    CHK_TRUE( s2.fNet->delieverReqTo("S4") );
    CHK_TRUE( s2.fNet->delieverReqTo("S5") );
    // S5's grant first (dead = 2, below quorum; frees S5)...
    CHK_TRUE( s2.fNet->handleRespFrom("S5") );
    // ...then S3's grant completes the quorum (dead = 3): S2 initiates the
    // term-2 vote and queues vote requests to the free peers (S3, S5).
    CHK_TRUE( s2.fNet->handleRespFrom("S3") );
    // (S4's pre-vote response is intentionally left un-handled: S2 must
    //  remain a candidate forever, like the incident's failed rounds.)

    // Deliver S2's term-2 vote request to S5 and to S3, but never deliver
    // the responses back.
    // Under the PR, this is process_req() decay #1 at S5: target 3 -> 2.
    s2.dbgLog(" --- deliver S2 term-2 vote request to S5, suppress resp ---");
    CHK_TRUE( s2.fNet->delieverReqTo("S5") );
    s2.dbgLog(" --- deliver S2 term-2 vote request to S3, suppress resp ---");
    CHK_TRUE( s2.fNet->delieverReqTo("S3") );
    CHK_FALSE( s2.raftServer->is_leader() );

    // --- Storm request #2: S3 (priority 2) runs for term 3. ---
    // S3's second own timeout decays its own target and makes it eligible
    // (this part is legitimate round-cadence decay on ANY implementation).
    s3.dbgLog(" --- S3 initiates election (term 3) ---");
    s3.fTimer->invoke( timer_task_type::election_timer );
    // Same ordering care as above: S5's pre-vote response must be handled
    // before the quorum-completing one, so the term-3 vote request is
    // queued toward S5.
    CHK_TRUE( s3.fNet->delieverReqTo("S2") );
    CHK_TRUE( s3.fNet->delieverReqTo("S4") );
    CHK_TRUE( s3.fNet->delieverReqTo("S5") );
    CHK_TRUE( s3.fNet->handleRespFrom("S5") );
    CHK_TRUE( s3.fNet->handleRespFrom("S4") );
    // (S2's pre-vote response left un-handled; S3 stays a candidate.)

    // Deliver S3's term-3 vote request to S5 only, suppress the response.
    // Under the PR, this is process_req() decay #2 at S5: target 2 -> 1,
    // even though S5's own election timer has fired exactly once so far.
    s3.dbgLog(" --- deliver S3 term-3 vote request to S5, suppress resp ---");
    CHK_TRUE( s3.fNet->delieverReqTo("S5") );
    CHK_FALSE( s3.raftServer->is_leader() );

    // Sanity: S5 has not sent anything on its own up to this point.
    for (const char* ep: { "S1", "S2", "S3", "S4" }) {
        TestSuite::setInfo("pre-probe, pending reqs to %s", ep);
        CHK_Z( s5.fNet->getNumPendingReqs(ep) );
    }

    // --- THE PROBE ---
    // S5's SECOND own election timeout. Its own history justifies at most
    // one decay step (target 3 -> 2), which must keep priority 1 gated.
    // If the storm's process_req() decays are allowed to accumulate, S5's
    // target is already 1 and it will initiate a pre-vote right here --
    // the first step of the incident's free-for-all.
    s5.dbgLog(" --- second election timeout of S5 (probe) ---");
    s5.fTimer->invoke( timer_task_type::election_timer );

    // S5 must NOT have started its own candidacy: no pre-vote requests
    // may be pending toward any peer.
    // THESE ASSERTIONS FAIL on master and on PR 661 as written.
    for (const char* ep: { "S1", "S2", "S3", "S4" }) {
        TestSuite::setInfo("post-probe, pending pre-vote reqs to %s", ep);
        CHK_Z( s5.fNet->getNumPendingReqs(ep) );
    }

    print_stats(pkgs);
    for (auto& pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}


int priority_election_basic_test_internal() {
    reset_log_files();

    const size_t NUM_SERVERS = 9;
    std::vector<std::string> s_addrs;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        std::string addr = "tcp://127.0.0.1:" + std::to_string(20010 + ii * 10);
        s_addrs.push_back(addr);
    }

    std::vector<std::shared_ptr<RaftAsioPkg>> s_pkgs;
    std::vector<RaftAsioPkg*> pkgs;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        std::shared_ptr<RaftAsioPkg> pkg =
            std::make_shared<RaftAsioPkg>(ii + 1, s_addrs[ii]);
        s_pkgs.push_back(pkg);
        pkgs.push_back(pkg.get());
    }

    _msg("launching asio-raft servers\n");
    CHK_Z( launch_servers(pkgs, false) );

    _msg("organizing raft group\n");
    CHK_Z( make_group(pkgs) );
    TestSuite::sleep_sec(1, "wait for Raft group ready");

    // Adjust priority.
    // S1-3: 3
    // S4-6: 2
    // S7-9: 1
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        int32_t priority = 1;
        if (ii < 3) {
            priority = 3;
        } else if (ii < 6) {
            priority = 2;
        }
        pkgs[0]->raftServer->set_priority(ii + 1, priority);
    }
    TestSuite::sleep_sec(1, "wait for replication");

    // Stop S1.
    _msg("stopping S1\n");
    pkgs[0]->raftServer->shutdown();
    pkgs[0]->stopAsio();
    s_pkgs[0].reset();
    TestSuite::sleep_sec(1, "wait for S1 shutdown");

    // Either S2 or S3 should be elected as a leader,
    // since they have the highest priority.
    _msg("waiting for a new leader\n");
    size_t new_leader_idx = 0;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        if (ii == 0) {
            continue;
        }
        if (pkgs[ii]->raftServer->is_leader()) {
            new_leader_idx = ii;
            break;
        }
    }
    CHK_TRUE(new_leader_idx != 0);
    _msg("new leader is S%d\n", new_leader_idx + 1);
    CHK_TRUE(new_leader_idx == 1 || new_leader_idx == 2);

    // Stopping the new leader.
    _msg("stopping the new leader S%d\n", new_leader_idx + 1);
    pkgs[new_leader_idx]->raftServer->shutdown();
    pkgs[new_leader_idx]->stopAsio();
    s_pkgs[new_leader_idx].reset();
    TestSuite::sleep_sec(1, "wait for new leader shutdown");

    // Either S2 or S3 should be elected as a leader.
    _msg("waiting for a new leader\n");
    size_t new_leader_idx2 = 0;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        if (ii == 0 || ii == new_leader_idx) {
            continue;
        }
        if (pkgs[ii]->raftServer->is_leader()) {
            new_leader_idx2 = ii;
            break;
        }
    }
    CHK_TRUE(new_leader_idx2 != 0);
    _msg("new leader is S%d\n", new_leader_idx2 + 1);
    CHK_TRUE(new_leader_idx2 == 1 || new_leader_idx2 == 2);

    // Stopping the new leader.
    _msg("stopping the new leader S%d\n", new_leader_idx2 + 1);
    pkgs[new_leader_idx2]->raftServer->shutdown();
    pkgs[new_leader_idx2]->stopAsio();
    s_pkgs[new_leader_idx2].reset();
    TestSuite::sleep_sec(1, "wait for new leader shutdown");

    // Either S4, S5, or S6 should be elected as a leader.
    _msg("waiting for a new leader\n");
    size_t new_leader_idx3 = 0;
    for (size_t ii = 0; ii < NUM_SERVERS; ++ii) {
        if (ii <= 2) {
            continue;
        }
        if (pkgs[ii]->raftServer->is_leader()) {
            new_leader_idx3 = ii;
            break;
        }
    }
    CHK_TRUE(new_leader_idx3 != 0);
    _msg("new leader is S%d\n", new_leader_idx3 + 1);
    CHK_TRUE(new_leader_idx3 == 3 || new_leader_idx3 == 4 || new_leader_idx3 == 5);

    // Shutdown.
    for (size_t ii = 3; ii < NUM_SERVERS; ++ii) {
        pkgs[ii]->raftServer->shutdown();
        s_pkgs[ii].reset();
    }
    TestSuite::sleep_sec(1, "shutting down");

    SimpleLogger::shutdown();
    return 0;
}

int priority_election_basic_test() {
    // NOTE: Since priority-based election is non-deterministic,
    //       it is not 100% guaranteed that the test will pass every time.
    //
    //       If it fails, run it two additional times
    //       and verify both of them pass (prob should be > 66%).
    int ret = priority_election_basic_test_internal();
    if (ret != 0) {
        _msg("priority_election_basic_test_internal() failed, retrying...\n");
        for (size_t ii = 0; ii < 2; ++ii) {
            CHK_Z(priority_election_basic_test_internal());
        }
    }
    return ret;
}

}  // namespace priority_based_election_test;
using namespace priority_based_election_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    ts.doTest( "stale hb pre-vote grant test",
               stale_hb_prevote_grant_test );

    ts.doTest( "tiered decay excludes low priority test",
               tiered_decay_excludes_low_priority_test );

    // NOTE: expected to FAIL on PR 661 as written -- documents the
    // remaining `process_req()` decay gap. See the test's comment.
    // NOTE: known-failing on master AND on PR 661 as written; encodes the
    // invariant that the process_req() decay trigger must be rate-limited.
    // See the comment block on the test for details.
    ts.doTest( "process req decay storm test",
               process_req_decay_storm_test );

    ts.doTest( "priority election basic test",
               priority_election_basic_test );

#ifdef ENABLE_RAFT_STATS
    _msg("raft stats: ENABLED\n");
#else
    _msg("raft stats: DISABLED\n");
#endif
    TestSuite::Msg mm;
    mm << "num allocs: " << raft_server::get_stat_counter("num_buffer_allocs")
       << std::endl
       << "amount of allocs: " << raft_server::get_stat_counter("amount_buffer_allocs")
       << " bytes" << std::endl
       << "num active buffers: " << raft_server::get_stat_counter("num_active_buffers")
       << std::endl
       << "amount of active buffers: "
       << raft_server::get_stat_counter("amount_active_buffers") << " bytes" << std::endl;

    return 0;
}