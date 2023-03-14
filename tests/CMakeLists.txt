# === Basic Raft server functionality test ===
add_executable(raft_server_test
               unit/raft_server_test.cxx
               unit/fake_network.cxx
               ${EXAMPLES_SRC}/logger.cc
               ${EXAMPLES_SRC}/in_memory_log_store.cxx)
add_dependencies(raft_server_test
                 static_lib)
target_link_libraries(raft_server_test
                      ${BUILD_DIR}/${LIBRARY_OUTPUT_NAME})

# === Failure recovery & conflict resolution test ===
add_executable(failure_test
               unit/failure_test.cxx
               unit/fake_network.cxx
               ${EXAMPLES_SRC}/logger.cc
               ${EXAMPLES_SRC}/in_memory_log_store.cxx)
add_dependencies(failure_test
                 static_lib)
target_link_libraries(failure_test
                      ${BUILD_DIR}/${LIBRARY_OUTPUT_NAME})

# === ASIO network stuff test ===
add_executable(asio_service_test
               unit/asio_service_test.cxx
               ${EXAMPLES_SRC}/logger.cc
               ${EXAMPLES_SRC}/in_memory_log_store.cxx)
add_dependencies(asio_service_test
                 static_lib
                 build_ssl_key)
target_link_libraries(asio_service_test
                      ${BUILD_DIR}/${LIBRARY_OUTPUT_NAME}
                      ${LIBRARIES})

# === Benchmark ===
add_executable(raft_bench
               bench/raft_bench.cxx
               ${EXAMPLES_SRC}/logger.cc
               ${EXAMPLES_SRC}/in_memory_log_store.cxx)
add_dependencies(raft_bench
                 static_lib)
target_link_libraries(raft_bench
                      ${BUILD_DIR}/${LIBRARY_OUTPUT_NAME}
                      ${LIBRARIES})


# === Other modules ===
add_executable(buffer_test
               unit/buffer_test.cxx)
add_dependencies(buffer_test
                 static_lib)
target_link_libraries(buffer_test
                      ${BUILD_DIR}/${LIBRARY_OUTPUT_NAME})

add_executable(serialization_test
               unit/serialization_test.cxx)
add_dependencies(serialization_test
                 static_lib)
target_link_libraries(serialization_test
                      ${BUILD_DIR}/${LIBRARY_OUTPUT_NAME})

add_executable(timer_test
               unit/timer_test.cxx)
add_dependencies(timer_test
                 static_lib)
target_link_libraries(timer_test
                      ${BUILD_DIR}/${LIBRARY_OUTPUT_NAME}
                      ${LIBRARIES})

add_executable(strfmt_test
               unit/strfmt_test.cxx)
add_dependencies(strfmt_test
                 static_lib)
target_link_libraries(strfmt_test
                      ${BUILD_DIR}/${LIBRARY_OUTPUT_NAME})

add_executable(stat_mgr_test
               unit/stat_mgr_test.cxx)
add_dependencies(stat_mgr_test
                 static_lib)
target_link_libraries(stat_mgr_test
                      ${BUILD_DIR}/${LIBRARY_OUTPUT_NAME})

