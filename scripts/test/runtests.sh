#!/bin/bash
set -e

./tests/buffer_test --abort-on-failure
./tests/serialization_test --abort-on-failure
./tests/strfmt_test --abort-on-failure
./tests/stat_mgr_test --abort-on-failure
./tests/raft_server_test --abort-on-failure
./tests/snapshot_test --abort-on-failure
./tests/leader_election_test --abort-on-failure
./tests/learner_new_joiner_test --abort-on-failure
./tests/failure_test --abort-on-failure

ASIO_TESTS=(
    "./tests/timer_test"
    "./tests/asio_service_test"
    "./tests/req_resp_meta_test"
    "./tests/custom_quorum_test"
    "./tests/stream_transport_layer_test"
    "./tests/raft_stream_mode_test"
)

for test in "${ASIO_TESTS[@]}"; do
    if [ -f $test ]; then
        $test --abort-on-failure
    fi
done
