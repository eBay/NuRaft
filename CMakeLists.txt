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
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
    if (APPLE)
#        include_directories(BEFORE
#                /usr/local/opt/openssl/include
#                )
#        link_directories(
#                /usr/local/opt/openssl/lib
#                )
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
    endif ()

else ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd5045 /wd4571 /wd4774 /wd4820 /wd5039 /wd4626 /wd4625 /wd5026 /wd5027 /wd4623 /wd4996 /wd4530 /wd4267 /wd4244 /W3")
    message(STATUS "---- WIN32 ----")
    set(DISABLE_SSL 1)
endif ()

# === Disable SSL ===
if (DISABLE_SSL)
    add_definitions(-DSSL_LIBRARY_NOT_FOUND=1)
    message(STATUS "---- DISABLED SSL ----")
endif ()

# Use OPENSSL_LIBRARY_PATH and OPENSSL_INCLUDE_PATH for OpenSSL.
macro(NURAFT_USE_PREDEFINED_OPENSSL)
    message(STATUS "Pre-defined SSL library path: ${OPENSSL_LIBRARY_PATH}/libssl.a")
    message(STATUS "Pre-defined SSL include path: ${OPENSSL_INCLUDE_PATH}")

    set(OPENSSL_SSL_LIBRARY ${OPENSSL_LIBRARY_PATH}/libssl.a CACHE PATH "")
    set(OPENSSL_CRYPTO_LIBRARY ${OPENSSL_LIBRARY_PATH}/libcrypto.a CACHE PATH "")
    set(OPENSSL_INCLUDE_DIR ${OPENSSL_INCLUDE_PATH} CACHE PATH "")
    mark_as_advanced(OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY OPENSSL_INCLUDE_DIR)

    set(LIBSSL ${OPENSSL_SSL_LIBRARY})
    set(LIBCRYPTO ${OPENSSL_CRYPTO_LIBRARY})
endmacro()

# Search for static OpenSSL. If provided, DEPS_PREFIX is used as the root.
macro(NURAFT_USE_SYSTEM_OPENSSL)
    # FindOpenSSL does not detect OpenSSL installed by homebrew.
    if (APPLE)
        list(APPEND CMAKE_LIBRARY_PATH /opt/homebrew/opt/openssl@1.1)
        list(APPEND CMAKE_INCLUDE_PATH /opt/homebrew/opt/openssl@1.1)
    endif ()

    # This needs to be mantained for backwards compatibility.
    set(OPENSSL_ROOT_DIR ${DEPS_PREFIX})
    set(OPENSSL_USE_STATIC_LIBS TRUE)

    find_package(OpenSSL QUIET)

    if (OpenSSL_FOUND)
        message(STATUS "OpenSSL library path: ${OPENSSL_SSL_LIBRARY}")
        message(STATUS "OpenSSL include path: ${OPENSSL_INCLUDE_DIR}")

        set(LIBSSL OpenSSL::SSL)
        set(LIBCRYPTO OpenSSL::Crypto)
    else ()
        message(STATUS "OpenSSL not found")
    endif()
endmacro()

# === OpenSSL libraries ===
if (NOT DISABLE_SSL)
    # Predefined options need to be maintained for backwards compatability.
    if (OPENSSL_LIBRARY_PATH AND OPENSSL_INCLUDE_PATH)
        nuraft_use_predefined_openssl()
    else ()
        nuraft_use_system_openssl()
    endif ()

    include_directories(BEFORE ${OPENSSL_INCLUDE_DIR})
endif()

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
file(COPY ${PROJECT_SOURCE_DIR}/scripts/test/runtests.sh
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
    ${ROOT_SRC}/log_entry.cxx
    ${ROOT_SRC}/peer.cxx
    ${ROOT_SRC}/raft_server.cxx
    ${ROOT_SRC}/snapshot.cxx
    ${ROOT_SRC}/snapshot_sync_ctx.cxx
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
add_library(NuRaft::static_lib ALIAS static_lib)
set_target_properties(static_lib PROPERTIES OUTPUT_NAME ${LIBRARY_NAME} CLEAN_DIRECT_OUTPUT 1)

add_library(shared_lib SHARED ${STATIC_LIB_SRC})
add_library(NuRaft::shared_lib ALIAS shared_lib)
set_target_properties(shared_lib PROPERTIES OUTPUT_NAME ${LIBRARY_NAME} CLEAN_DIRECT_OUTPUT 1)

# Include directories are necessary for dependents to use the targets.
target_include_directories(static_lib
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_PREFIX}/include>
)

target_include_directories(static_lib
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_PREFIX}/include>
)

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
        logger_test
        asio_service_stream_test
        stream_functional_test
    )

    # lcov
    SETUP_TARGET_FOR_COVERAGE(
        NAME raft_cov
        EXECUTABLE ./runtests.sh
        DEPENDENCIES ${CODE_COVERAGE_DEPS}
    )
endif()


# === Install Targets ===
install(TARGETS shared_lib static_lib
    EXPORT nuraft-targets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/libnuraft DESTINATION include)

install(EXPORT nuraft-targets
    FILE NuRaftTargets.cmake
    NAMESPACE NuRaft::
    DESTINATION lib/cmake/NuRaft
)

include(CMakePackageConfigHelpers)
configure_package_config_file("${CMAKE_CURRENT_SOURCE_DIR}/NuRaftConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/NuRaftConfig.cmake"
    INSTALL_DESTINATION lib/cmake/NuRaft
)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/NuRaftConfig.cmake"
    DESTINATION lib/cmake/NuRaft
)
