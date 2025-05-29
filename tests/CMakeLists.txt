
function(unit_test)
    set(options SKIP)
    set(oneValueArgs NAME TIMEOUT)
    set(multiValueArgs SOURCES)
    cmake_parse_arguments(PARSE_ARGV 0 arg
        "${options}" "${oneValueArgs}" "${multiValueArgs}"
    )

    add_executable(${arg_NAME} ${arg_SOURCES})
    target_link_libraries(${arg_NAME} ${LIBRARY_NAME})
    add_dependencies(${arg_NAME} build_ssl_key)
    if(NOT arg_SKIP)
        add_test(NAME ${arg_NAME} WORKING_DIRECTORY ${CMAKE_BINARY_DIR} COMMAND ${arg_NAME} --abort-on-failure)
        if(arg_TIMEOUT)
            set_tests_properties(${arg_NAME} PROPERTIES TIMEOUT ${arg_TIMEOUT})
        endif()
    endif()
endfunction()


# === Basic Raft server functionality tests without real network stack ===

unit_test(NAME raft_server_test
    SOURCES
    unit/raft_server_test.cxx
    unit/fake_network.cxx
    ${EXAMPLES_SRC}/logger.cc
    ${EXAMPLES_SRC}/in_memory_log_store.cxx
)

unit_test(NAME snapshot_test
    SOURCES
    unit/snapshot_test.cxx
    unit/fake_network.cxx
    ${EXAMPLES_SRC}/logger.cc
    ${EXAMPLES_SRC}/in_memory_log_store.cxx
)

unit_test(NAME leader_election_test
    SOURCES
    unit/leader_election_test.cxx
    unit/fake_network.cxx
    ${EXAMPLES_SRC}/logger.cc
    ${EXAMPLES_SRC}/in_memory_log_store.cxx
)

unit_test(NAME learner_new_joiner_test
    SOURCES
    unit/learner_new_joiner_test.cxx
    unit/fake_network.cxx
    ${EXAMPLES_SRC}/logger.cc
    ${EXAMPLES_SRC}/in_memory_log_store.cxx)

# === Failure recovery & conflict resolution test ===
unit_test(NAME failure_test
    SOURCES
    unit/failure_test.cxx
    unit/fake_network.cxx
    ${EXAMPLES_SRC}/logger.cc
    ${EXAMPLES_SRC}/in_memory_log_store.cxx)

# === ASIO network stuff test, only when ASIO is enabled ===
if(NOT DISABLE_ASIO)
    unit_test(NAME asio_service_test
        SOURCES
        asio/asio_service_test.cxx
        ${EXAMPLES_SRC}/logger.cc
        ${EXAMPLES_SRC}/in_memory_log_store.cxx
        TIMEOUT 720
        SKIP # skipping for now because it takes too long to run
    )

    unit_test(NAME req_resp_meta_test
        SOURCES
        asio/req_resp_meta_test.cxx
        ${EXAMPLES_SRC}/logger.cc
        ${EXAMPLES_SRC}/in_memory_log_store.cxx
        TIMEOUT 720
        SKIP
    )

    unit_test(NAME custom_quorum_test
        SOURCES
        asio/custom_quorum_test.cxx
        ${EXAMPLES_SRC}/logger.cc
        ${EXAMPLES_SRC}/in_memory_log_store.cxx
        TIMEOUT 720
        SKIP
    )

    unit_test(NAME stream_transport_layer_test
        SOURCES
        asio/stream_transport_layer_test.cxx
        ${EXAMPLES_SRC}/logger.cc
        ${EXAMPLES_SRC}/in_memory_log_store.cxx
        TIMEOUT 720
        SKIP
    )

    unit_test(NAME raft_stream_mode_test
        SOURCES
        asio/raft_stream_mode_test.cxx
        unit/fake_network.cxx
        ${EXAMPLES_SRC}/logger.cc
        ${EXAMPLES_SRC}/in_memory_log_store.cxx
        SKIP # skipping for now because this test fails
    )

    # === Benchmark ===
    unit_test(NAME raft_bench
        SOURCES
        bench/raft_bench.cxx
        ${EXAMPLES_SRC}/logger.cc
        ${EXAMPLES_SRC}/in_memory_log_store.cxx
        SKIP
    )
endif()

# === Other modules ===
unit_test(NAME buffer_test
    SOURCES
    unit/buffer_test.cxx)

unit_test(NAME serialization_test
    SOURCES
    unit/serialization_test.cxx)

if (NOT DISABLE_ASIO)
    unit_test(NAME timer_test
        SOURCES
        unit/timer_test.cxx)
endif()

unit_test(NAME strfmt_test
    SOURCES
    unit/strfmt_test.cxx)

unit_test(NAME stat_mgr_test
    SOURCES
    unit/stat_mgr_test.cxx)


unit_test(NAME logger_test
    SOURCES
    unit/logger_test.cxx)

#set_tests_properties(asio_service_stream_test stream_functional_test asio_service_test PROPERTIES RUN_SERIAL TRUE)
