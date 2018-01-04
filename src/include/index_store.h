#ifndef __INDEX_STORE_H__
#define __INDEX_STORE_H__

#include <lmdbxx/lmdb++.h>

#include <string>

#include "allocator.h"

//
// Index Store interface
//
struct IndexStore {
  // Open/create the index store in the environment
  IndexStore(lmdb::env& env, Allocator& allocator);
  // Close the index store
  ~IndexStore() noexcept;

  // Check if #index exists
  // If #index does not exist, false is returned.
  bool IndexExist(lmdb::txn& txn, const std::string& index);

  // Get data with #index from index store
  // If #index does not exist, false is returned.
  bool GetIndex(lmdb::txn& txn, const std::string& index, std::string& data);

  // Set data with #index in index store
  void SetIndex(lmdb::txn& txn,
                const std::string& index,
                const std::string& data);

  // Delete data with #index from data store
  void DeleteIndex(lmdb::txn& txn, const std::string& index);

 private:
  // dbi of the allocator
  lmdb::dbi dbi;
  // The allocator we are going to use
  Allocator& allocator;
};

#endif  // __INDEX_STORE_H__