cmake_minimum_required (VERSION 2.8.11)
project (id-allocator)

set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules")

set (CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}
     "-std=c++11")

add_subdirectory (src)
