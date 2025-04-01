#!/bin/bash
set -e

./tests/buffer_test --abort-on-failure
./tests/serialization_test --abort-on-failure
if [ -f ./tests/timer_test ]; then
    ./tests/timer_test --abort-on-failure
fi
./tests/strfmt_test --abort-on-failure
./tests/stat_mgr_test --abort-on-failure
./tests/raft_server_test --abort-on-failure
./tests/new_joiner_test --abort-on-failure
./tests/failure_test --abort-on-failure
if [ -f ./tests/asio_service_test ]; then
    ./tests/asio_service_test --abort-on-failure
fi
if [ -f ./tests/asio_service_stream_test ]; then
    ./tests/asio_service_stream_test --abort-on-failure
fi
if [ -f ./tests/stream_functional_test ]; then
    ./tests/stream_functional_test --abort-on-failure
fi
