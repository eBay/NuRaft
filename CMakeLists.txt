cmake_minimum_required(VERSION 3.5)
project(NuRaft VERSION 1.0.0 LANGUAGES CXX)

# === Build type (default: RelWithDebInfo, O2) ===========
if (NOT CMAKE_BUILD_TYPE)
    set(DEFAULT_BUILD_TYPE "RelWithDebInfo")
    #set(DEFAULT_BUILD_TYPE "Debug")

    set(BUILD_TYPE_OPTIONS
        "Choose the type of build, "
        "options are: Debug Release RelWithDebInfo MinSizeRel.")
    set(CMAKE_BUILD_TYPE ${DEFAULT_BUILD_TYPE}
        CACHE ${BUILD_TYPE_OPTIONS} FORCE)
    message(STATUS "Build type is not given, use default.")
endif ()
message(STATUS "Build type: " ${CMAKE_BUILD_TYPE})
message(STATUS "Build Install Prefix : " ${CMAKE_INSTALL_PREFIX})

set(BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR})

if (CODE_COVERAGE GREATER 0)
    set(CMAKE_BUILD_TYPE "Debug")
    include(cmake/CodeCoverage.cmake)
    message(STATUS "---- CODE COVERAGE DETECTION MODE ----")
endif()


# === Find ASIO ===
if (BOOST_INCLUDE_PATH AND BOOST_LIBRARY_PATH)
    # If Boost path (both include and library) is given,
    # use Boost's ASIO.
    message(STATUS "Boost include path: " ${BOOST_INCLUDE_PATH})
    message(STATUS "Boost library path: " ${BOOST_LIBRARY_PATH})

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_BOOST_ASIO")

    set(ASIO_INCLUDE_DIR ${BOOST_INCLUDE_PATH})
    set(LIBBOOST_SYSTEM "${BOOST_LIBRARY_PATH}/libboost_system.a")

else ()
    # If not, ASIO standalone mode.
    FIND_PATH(ASIO_INCLUDE_DIR
              NAME asio.hpp
              HINTS ${PROJECT_SOURCE_DIR}/asio/asio/include
                    $ENV{HOME}/local/include
                    /opt/local/include
                    /usr/local/include
                    /usr/include)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DASIO_STANDALONE")

endif ()

if (NOT ASIO_INCLUDE_DIR)
    message(FATAL_ERROR "Can't find ASIO header files")
else ()
    message(STATUS "ASIO include path: " ${ASIO_INCLUDE_DIR})
endif ()


# === Includes ===
include_directories(BEFORE ./)
include_directories(BEFORE ${ASIO_INCLUDE_DIR})
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/include)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/include/libnuraft)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/examples)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/examples/calculator)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/examples/echo)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/src)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/tests)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/tests/unit)
if (DEPS_PREFIX)
    message(STATUS "deps prefix: " ${DEPS_PREFIX})
    include_directories(AFTER ${DEPS_PREFIX}/include)
else()
    message(STATUS "deps prefix is not given")
endif()


# === Compiler flags ===
if (NOT WIN32)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-pessimizing-move")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
    if (APPLE)
        include_directories(BEFORE /usr/local/opt/openssl/include)
        link_directories(/usr/local/opt/openssl/lib)
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
    endif ()

else ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd5045 /wd4571 /wd4774 /wd4820 /wd5039 /wd4626 /wd4625 /wd5026 /wd5027 /wd4623 /wd4996 /wd4530 /wd4267 /wd4244 /W3")
    message(STATUS "---- WIN32 ----")
    set(DISABLE_SSL 1)
endif ()

# === Disable SSL ===
if (DISABLE_SSL GREATER 0)
    add_definitions(-DSSL_LIBRARY_NOT_FOUND=1)
    message(STATUS "---- DISABLED SSL ----")
endif ()

# === OpenSSL libraries ===
if (NOT (DISABLE_SSL GREATER 0))
    set(LIB_PATH_HINT
        ~/Library/Frameworks
        /Library/Frameworks
        $ENV{HOME}/local/lib64
        $ENV{HOME}/local/lib
        /usr/lib
        /usr/lib/x86_64-linux-gnu
        /usr/lib64
        /usr/local/lib
        /usr/local/lib64
        /opt/local/lib
        /opt/local/lib64
        /opt/csw
        /opt)
    find_path(OPENSSL_LIBRARY_PATH
              NAMES libssl.a
              PATHS ${PROJECT_SOURCE_DIR}
                    ${DEPS_PREFIX}/lib
                    ${DEPS_PREFIX}/lib64
                    /usr/local/opt/openssl/lib
                    ${LIB_PATH_HINT})

    if (NOT OPENSSL_LIBRARY_PATH)
        message(STATUS "Use system's side OpenSSL library")
        set(LIBSSL ssl)
        set(LIBCRYPTO crypto)
    else ()
        message(STATUS "Open SSL library path: ${OPENSSL_LIBRARY_PATH}/libssl.a")
        set(LIBSSL ${OPENSSL_LIBRARY_PATH}/libssl.a)
        set(LIBCRYPTO ${OPENSSL_LIBRARY_PATH}/libcrypto.a)
    endif ()
endif ()

# === Compiler options ===
if (ADDRESS_SANITIZER GREATER 0)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fuse-ld=gold")
    message(STATUS "---- ADDRESS SANITIZER IS ON ----")
endif()

if (THREAD_SANITIZER GREATER 0)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fuse-ld=gold")
    add_definitions(-DSUPPRESS_TSAN_FALSE_ALARMS=1)
    message(STATUS "---- THREAD SANITIZER IS ON ----")
endif()

if (CODE_COVERAGE GREATER 0)
    APPEND_COVERAGE_COMPILER_FLAGS()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline")
    set(COVERAGE_EXCLUDES
        '*asio/*'
        '*examples/*'
        '*tests/*'
        '*usr/*'
    )
endif()

if ( (TESTSUITE_NO_COLOR GREATER 0) OR (WIN32) )
    add_definitions(-DTESTSUITE_NO_COLOR=1)
    add_definitions(-DLOGGER_NO_COLOR=1)
    message(STATUS "---- NO ANSI COLOR ----")
endif()

if (ENABLE_RAFT_STATS GREATER 0)
    add_definitions(-DENABLE_RAFT_STATS=1)
    message(STATUS "---- ENABLED RAFT STATS ----")
endif()

# === Other shared libraries ===
if (NOT WIN32)
    set(LIBDL dl)
    set(LIBZ z)
endif()
set(LIBRARIES
    ${LIBSSL}
    ${LIBCRYPTO}
    ${LIBBOOST_SYSTEM}
    ${LIBDL}
    ${LIBZ})


# === Paths ===
set(ROOT_SRC ${PROJECT_SOURCE_DIR}/src)
set(EXAMPLES_SRC ${PROJECT_SOURCE_DIR}/examples)

# === Copy script files ===
file(COPY ${CMAKE_SOURCE_DIR}/scripts/test/runtests.sh
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# === Generate dummy self-signed cert and key for testing ===
add_custom_target(build_ssl_key)
if (NOT (DISABLE_SSL GREATER 0))
    add_custom_command(
        TARGET build_ssl_key
        PRE_BUILD
        COMMAND
        openssl req
        -new
        -newkey rsa:4096
        -days 365
        -nodes
        -x509
        -subj "/C=AB/ST=CD/L=EFG/O=ORG/CN=localhost"
        -keyout ${CMAKE_CURRENT_BINARY_DIR}/key.pem
        -out ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
        2> /dev/null
    )
endif ()

# === Source files ===
set(RAFT_CORE
    ${ROOT_SRC}/asio_service.cxx
    ${ROOT_SRC}/buffer.cxx
    ${ROOT_SRC}/buffer_serializer.cxx
    ${ROOT_SRC}/cluster_config.cxx
    ${ROOT_SRC}/crc32.cxx
    ${ROOT_SRC}/error_code.cxx
    ${ROOT_SRC}/global_mgr.cxx
    ${ROOT_SRC}/handle_append_entries.cxx
    ${ROOT_SRC}/handle_client_request.cxx
    ${ROOT_SRC}/handle_custom_notification.cxx
    ${ROOT_SRC}/handle_commit.cxx
    ${ROOT_SRC}/handle_join_leave.cxx
    ${ROOT_SRC}/handle_priority.cxx
    ${ROOT_SRC}/handle_snapshot_sync.cxx
    ${ROOT_SRC}/handle_timeout.cxx
    ${ROOT_SRC}/handle_user_cmd.cxx
    ${ROOT_SRC}/handle_vote.cxx
    ${ROOT_SRC}/launcher.cxx
    ${ROOT_SRC}/peer.cxx
    ${ROOT_SRC}/raft_server.cxx
    ${ROOT_SRC}/snapshot.cxx
    ${ROOT_SRC}/snapshot_sync_req.cxx
    ${ROOT_SRC}/srv_config.cxx
    ${ROOT_SRC}/stat_mgr.cxx
    )
add_library(RAFT_CORE_OBJ OBJECT ${RAFT_CORE})

set(STATIC_LIB_SRC
    $<TARGET_OBJECTS:RAFT_CORE_OBJ>)

# === Executables ===
set(LIBRARY_NAME "nuraft")

add_library(static_lib ${STATIC_LIB_SRC})
set_target_properties(static_lib PROPERTIES OUTPUT_NAME ${LIBRARY_NAME} CLEAN_DIRECT_OUTPUT 1)

add_library(shared_lib SHARED ${STATIC_LIB_SRC})
set_target_properties(shared_lib PROPERTIES OUTPUT_NAME ${LIBRARY_NAME} CLEAN_DIRECT_OUTPUT 1)
if (APPLE)
    target_link_libraries(shared_lib ${LIBRARIES})
endif ()

if (WIN32)
    set(LIBRARY_OUTPUT_NAME "${LIBRARY_NAME}.lib")
else ()
    set(LIBRARY_OUTPUT_NAME "lib${LIBRARY_NAME}.a")
endif ()
message(STATUS "Output library file name: ${LIBRARY_OUTPUT_NAME}")

# === Examples ===
add_subdirectory("${PROJECT_SOURCE_DIR}/examples")


# === Tests ===
add_subdirectory("${PROJECT_SOURCE_DIR}/tests")


if (CODE_COVERAGE GREATER 0)
    set(CODE_COVERAGE_DEPS
        raft_server_test
        failure_test
        asio_service_test
        buffer_test
        serialization_test
        timer_test
        strfmt_test
        stat_mgr_test
    )

    # lcov
    SETUP_TARGET_FOR_COVERAGE(
        NAME raft_cov
        EXECUTABLE ./runtests.sh
        DEPENDENCIES ${CODE_COVERAGE_DEPS}
    )
endif()


# === Install Targets ===
install(TARGETS static_lib ARCHIVE DESTINATION lib)
install(TARGETS shared_lib LIBRARY DESTINATION lib)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/libnuraft DESTINATION include)


