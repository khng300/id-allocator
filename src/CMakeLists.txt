include_directories (include)
set (SRC
     allocator.cc
     example.cc
     data_store.cc
     index_store.cc)

add_executable (lmdb-allocator-example ${SRC})
target_link_libraries (lmdb-allocator-example ${LMDB_LIBRARIES})
