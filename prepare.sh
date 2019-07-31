#!/bin/bash
set -ex

. manifest.sh

PROJECT_DIR=`pwd`

cd ${PROJECT_DIR}

if [ ! -d asio ]; then
    git clone https://github.com/chriskohlhoff/asio.git ./asio
    cd asio
    git checkout ${ASIO_RELEASE}
    cd ..
fi
