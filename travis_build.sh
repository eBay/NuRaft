#!/bin/bash
set -ex

rm -rf build
mkdir build
cd build
if [ "${TRAVIS_PULL_REQUEST}" = "false" ]; then
    echo "branch: ${TRAVIS_BRANCH}"
    cmake -DCMAKE_BUILD_TYPE=Debug -DCODE_COVERAGE=1 -DENABLE_RAFT_STATS=1 ../
    make -j2 raft_cov
    bash <(curl -s https://codecov.io/bash) -f raft_cov.info.cleaned
else
    echo "pull request sha: ${TRAVIS_PULL_REQUEST_SHA}"
    cmake -DCMAKE_BUILD_TYPE=Debug -DADDRESS_SANITIZER=1 -DENABLE_RAFT_STATS=1 ../
    make -j2
    ./runtests.sh
fi
