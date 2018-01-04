# - Try to find LMDB
# Once done this will define
#  LMDB_FOUND - System has LMDB
#  LMDB_INCLUDE_DIRS - The LMDB include directories
#  LMDB_LIBRARIES - The libraries needed to use LMDB
#  LMDB_LIBRARY_DIRS - The directories containing LMDB library

find_package (PkgConfig REQUIRED)
pkg_search_module (LMDB REQUIRED lmdb)
