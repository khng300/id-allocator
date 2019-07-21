set (LMDB_SOURCE_DIR ${PROJECT_SOURCE_DIR}/third_party/lmdb/libraries/liblmdb)

#include_directories (${LMDB_INCLUDE_DIR})
include_directories (${LMDB_SOURCE_DIR})
include_directories (include)

set (SRC
     ${LMDB_SOURCE_DIR}/mdb.c
	 ${LMDB_SOURCE_DIR}/midl.c
     allocator.cc
     example.cc
     data_store.cc
     index_store.cc)

add_executable (lmdb-allocator-example ${SRC})
#target_link_libraries (lmdb-allocator-example ${LMDB_LIBRARIES})
