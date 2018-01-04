#include "data_store.h"

#include <iostream>

// Name of the database
static const char* database_name = "DataStore";

DataStore::DataStore(lmdb::env& env, Allocator& allocator) try
    : dbi(0),
      allocator(allocator) {
  lmdb::txn txn = lmdb::txn::begin(env);
  dbi = lmdb::dbi::open(txn, database_name, MDB_CREATE | MDB_INTEGERKEY);
  txn.commit();
} catch (const lmdb::error& e) {
  std::cout << e.what();
  throw;
}

DataStore::~DataStore() noexcept {}

bool DataStore::IdExist(lmdb::txn& txn, object_id_t id) {
  lmdb::val val_id(&id, sizeof(object_id_t));
  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  if (!cursor.get(val_id, MDB_SET))
    return false;
  return true;
}

bool DataStore::GetData(lmdb::txn& txn, object_id_t id, std::string& data) {
  lmdb::val val_id(&id, sizeof(object_id_t)), val_data;
  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  if (!cursor.get(val_id, MDB_SET_KEY))
    return false;
  data.assign(val_id.data(), val_id.size());
  return true;
}

void DataStore::SetData(lmdb::txn &txn, object_id_t id, const std::string &data) {
  lmdb::val val_id(&id, sizeof(object_id_t));
  lmdb::val val_data(data);
  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  cursor.put(val_id, val_data, 0);
}

void DataStore::DeleteData(lmdb::txn &txn, object_id_t id) {
  lmdb::val val_id(&id, sizeof(object_id_t));
  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  if (!cursor.get(val_id, MDB_SET))
    return;
  cursor.del();
}