cmake_minimum_required(VERSION 3.11)

# === Example replicated calculator ===
add_executable(calc_server
               calculator/calc_server.cxx
               logger.cc
               in_memory_log_store.cxx)
target_link_libraries(calc_server nuraft)

add_executable(echo_server
               echo/echo_server.cxx
               logger.cc
               in_memory_log_store.cxx)
target_link_libraries(echo_server nuraft)

add_executable(quick_start
               quick_start.cxx
               logger.cc
               in_memory_log_store.cxx)
target_link_libraries(quick_start nuraft)

