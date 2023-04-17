set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter")

# === Basic Raft server functionality test ===
add_executable(raft_server_test
               unit/raft_server_test.cxx
               unit/fake_network.cxx
	       $<TARGET_OBJECTS:in_mem_logstore>)
target_link_libraries(raft_server_test nuraft)

# === Failure recovery & conflict resolution test ===
add_executable(failure_test
               unit/failure_test.cxx
               unit/fake_network.cxx
	       $<TARGET_OBJECTS:in_mem_logstore>)
target_link_libraries(failure_test nuraft)

# === ASIO network stuff test ===
add_executable(asio_service_test
               unit/asio_service_test.cxx
	       $<TARGET_OBJECTS:in_mem_logstore>)
add_dependencies(asio_service_test build_ssl_key)
target_link_libraries(asio_service_test nuraft)

# === Benchmark ===
add_executable(raft_bench
               bench/raft_bench.cxx
	       $<TARGET_OBJECTS:in_mem_logstore>)
target_link_libraries(raft_bench nuraft)


# === Other modules ===
add_executable(buffer_test
	       unit/buffer_test.cxx)
target_link_libraries(buffer_test nuraft)

add_executable(serialization_test
               unit/serialization_test.cxx)
target_link_libraries(serialization_test nuraft)

add_executable(timer_test
               unit/timer_test.cxx)
target_link_libraries(timer_test nuraft)

add_executable(strfmt_test
               unit/strfmt_test.cxx)
target_link_libraries(strfmt_test nuraft)

add_executable(stat_mgr_test
               unit/stat_mgr_test.cxx)
target_link_libraries(stat_mgr_test nuraft)

