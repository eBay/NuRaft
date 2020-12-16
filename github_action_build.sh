#!/bin/bash
set -ex

rm -rf build
mkdir build
cd build

echo "head branch: ${GITHUB_HEAD_REF}"
echo "base branch: ${GITHUB_BASE_REF}"
echo "branch: ${GITHUB_REF}"

BRANCH_NAME="${GITHUB_REF#refs/heads/}"
echo "branch name: ${BRANCH_NAME}"

if [ -z "${GITHUB_HEAD_REF}" ]; then
    cmake -DCMAKE_BUILD_TYPE=Debug -DCODE_COVERAGE=1 -DENABLE_RAFT_STATS=1 ../
    make -j2 raft_cov
    bash <(curl -s https://codecov.io/bash) -f raft_cov.info.cleaned
else
    echo "pull request sha: ${GITHUB_SHA}"
    cmake -DCMAKE_BUILD_TYPE=Debug -DADDRESS_SANITIZER=1 -DENABLE_RAFT_STATS=1 ../
    make -j2
    ./runtests.sh
fi
