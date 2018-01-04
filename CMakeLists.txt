cmake_minimum_required (VERSION 2.8.11)
project (lmdb-allocator)

set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules")
find_package (LMDB REQUIRED)

set (CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}
     "-std=c++11")

add_subdirectory (src)
