set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-extra -Wno-error")

    add_library(in_mem_logstore OBJECT) target_sources(
        in_mem_logstore PRIVATE in_memory_log_store.cxx logger.cc)
        target_link_libraries(in_mem_logstore ${BUILD_DIR}
                              / ${LIBRARY_OUTPUT_NAME} ${LIBRARIES})

#== = Example replicated calculator == =
            add_executable(calc_server calculator / calc_server.cxx $ < TARGET_OBJECTS
                           : in_mem_logstore >) add_dependencies(calc_server static_lib)
                target_link_libraries(calc_server ${BUILD_DIR}
                                      / ${LIBRARY_OUTPUT_NAME} ${LIBRARIES})

                    add_executable(echo_server echo / echo_server.cxx $ < TARGET_OBJECTS
                                   : in_mem_logstore
                                         >) add_dependencies(echo_server static_lib)
                        target_link_libraries(echo_server ${BUILD_DIR}
                                              / ${LIBRARY_OUTPUT_NAME} ${LIBRARIES})

                            add_executable(quick_start quick_start
                                               .cxx $<TARGET_OBJECTS : in_mem_logstore>)
                                add_dependencies(quick_start static_lib)
                                    target_link_libraries(quick_start ${BUILD_DIR}
                                                          / ${LIBRARY_OUTPUT_NAME} ${
                                                              LIBRARIES})
