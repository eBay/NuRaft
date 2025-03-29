
NuRaft
======

[![build](https://github.com/eBay/NuRaft/workflows/build/badge.svg)](https://github.com/eBay/NuRaft/actions)
[![codecov](https://codecov.io/gh/eBay/NuRaft/branch/master/graph/badge.svg)](https://codecov.io/gh/eBay/NuRaft)

Raft implementation derived from the [cornerstone](https://github.com/datatechnology/cornerstone) project, which is a very lightweight C++ implementation with minimum dependencies, originally written by [Andy Chen](https://github.com/andy-yx-chen).

New features that are not described in the [original paper](https://raft.github.io/raft.pdf), but required for the real-world use cases in eBay, have been added. We believe those features are useful for others outside eBay as well.


Features
--------
### In the original cornerstone ###
* Core Raft algorithm
    * Log replication & compaction
    * Leader election
    * Snapshot
    * Dynamic membership & configuration change
* Group commit & pipelined write
* User-defined log store & state machine support

### New features added in this project ###
* [Pre-vote protocol](docs/prevote_protocol.md)
* [Leadership expiration](docs/leadership_expiration.md)
* [Priority-based semi-deterministic leader election](docs/leader_election_priority.md)
* [Read-only member (learner)](docs/readonly_member.md)
* [Object-based logical snapshot](docs/snapshot_transmission.md)
* [Custom/separate quorum size for commit & leader election](docs/custom_quorum_size.md)
* [Asynchronous replication](docs/async_replication.md)
* [SSL/TLS support](docs/enabling_ssl.md)
* [Parallel Log Appending](docs/parallel_log_appending.md)
* [Custom Commit Policy](docs/custom_commit_policy.md)
* [Streaming Mode](docs/streaming_mode.md)

How to Build
------------
#### 1. Install `cmake` and `openssl`: ####

* Ubuntu
```sh
$ sudo apt-get install cmake openssl libssl-dev libz-dev
```

* OSX
```sh
$ brew install cmake
$ brew install openssl
```
* Windows
    * Download and install [CMake](https://cmake.org/download/).
    * Currently, we do not support SSL for Windows.

#### 2. Fetch [Asio](https://github.com/chriskohlhoff/asio) library: ####

##### Using git submodule

```sh
$ git submodule update --init
```

##### Other ways to fetch:

* Linux & OSX: using the bash script

```sh
$ ./prepare.sh
```

* Windows: doing it manually
  * Clone [Asio](https://github.com/chriskohlhoff/asio) `asio-1-24-0` into the project directory.

```sh
C:\NuRaft> git clone https://github.com/chriskohlhoff/asio -b asio-1-24-0
```

#### 3. Build static library, tests, and examples: ####

* Linux & OSX
```sh
$ mkdir build
$ cd build
build$ cmake ../
build$ make
```

Run unit tests
```sh
build$ ./runtests.sh
```

* Windows:
```sh
C:\NuRaft> mkdir build
C:\NuRaft> cd build
C:\NuRaft\build> cmake -G "NMake Makefiles" ..\
C:\NuRaft\build> nmake
```

You may need to run `vcvars` script first in your `build` directory. For example (it depends on how you installed MSVC):
```sh
C:\NuRaft\build> c:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvars64.bat
```

Alternative Build Method (with Conan)
----------
If you project needs to integrate with NuRaft and you build your project with Conan you might want
to have NuRaft built with Conan too. This is now supported: this project has conanfile.py which can be used
to build locally and export the package to conan cache/repository or use it in **editable** mode locally.

1. Install conan using one of the methods desribed here: https://conan.io/downloads
1. `conan build .` - builds Release configuration
1. `conan build . -s build_type=Debug` builds an alternative configuration supported by CMake
1. You can control supported build options via command line as well, ex:
    `conan build . -o build_tests=True`

How to Use
----------
Please refer to [this document](./docs/how_to_use.md).


Example Implementation
-----------------------
Please refer to [examples](./examples).


Benchmark
---------
Please refer to [tests/bench](./tests/bench).

[Quick Benchmark Results](./docs/bench_results.md)


Supported Platforms
-------------------
* Linux, OSX (extensively tested and widely used)
* Windows (built with MSVC 2019, limited testing)


Contributing to This Project
----------------------------
We welcome contributions. If you find any bugs, potential flaws and edge cases, improvements, new feature suggestions or discussions, please submit issues or pull requests.


Contact
-------
* Jung-Sang Ahn <jungsang.ahn@gmail.com>


License Information
--------------------
Copyright 2017-present eBay Inc.

Author/Developer: Jung-Sang Ahn

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


3rd Party Code
--------------
1. URL: https://github.com/datatechnology/cornerstone<br>
License: https://github.com/datatechnology/cornerstone/blob/master/LICENSE<br>
Originally licensed under the Apache 2.0 license.

2. URL: https://github.com/stbrumme/crc32<br>
Original Copyright 2011-2016 Stephan Brumme<br>
See Original ZLib License: https://github.com/stbrumme/crc32/blob/master/LICENSE

3. URL: https://github.com/greensky00/simple_logger<br>
License: https://github.com/greensky00/simple_logger/blob/master/LICENSE<br>
Originally licensed under the MIT license.

4. URL: https://github.com/greensky00/testsuite<br>
License: https://github.com/greensky00/testsuite/blob/master/LICENSE<br>
Originally licensed under the MIT license.

5. URL: https://github.com/greensky00/latency-collector<br>
License: https://github.com/greensky00/latency-collector/blob/master/LICENSE<br>
Originally licensed under the MIT license.

6. URL: https://github.com/eriwen/lcov-to-cobertura-xml/blob/master/lcov_cobertura/lcov_cobertura.py<br>
License: https://github.com/eriwen/lcov-to-cobertura-xml/blob/master/LICENSE<br>
Copyright 2011-2012 Eric Wendelin<br>
Originally licensed under the Apache 2.0 license.

7. URL: https://github.com/bilke/cmake-modules<br>
License: https://github.com/bilke/cmake-modules/blob/master/LICENSE_1_0.txt<br>
Copyright 2012-2017 Lars Bilke<br>
Originally licensed under the BSD license.
