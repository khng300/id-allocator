#ifndef __DATA_STORE_H__
#define __DATA_STORE_H__

#include <lmdbxx/lmdb++.h>

#include <string>

#include "allocator.h"

//
// Data Store interface
//
struct DataStore {
  // Open/create the data store in the environment
  DataStore(lmdb::env& env, Allocator& allocator);
  // Close the data store
  ~DataStore() noexcept;

  // Check if object with #id exists
  // If #id does not exist, false is returned.
  bool IdExist(lmdb::txn& txn, object_id_t id);
  // Get data with #id from data store
  // If #id does not exist, false is returned.
  bool GetData(lmdb::txn& txn, object_id_t id, std::string& data);
  // Set data with #id in data store
  void SetData(lmdb::txn& txn,
               object_id_t id,
               const std::string& data);
  // Delete data with #id from data store
  void DeleteData(lmdb::txn& txn, object_id_t id);

 private:
  // dbi of the allocator
  lmdb::dbi dbi;
  // The allocator we are going to use
  Allocator& allocator;
};

#endif  // __DATA_STORE_H__