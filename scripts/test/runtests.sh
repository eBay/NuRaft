#!/bin/bash
set -e

./tests/buffer_test --abort-on-failure
./tests/serialization_test --abort-on-failure
./tests/timer_test --abort-on-failure
./tests/strfmt_test --abort-on-failure
./tests/stat_mgr_test --abort-on-failure
./tests/raft_server_test --abort-on-failure
./tests/failure_test --abort-on-failure
./tests/asio_service_test --abort-on-failure
