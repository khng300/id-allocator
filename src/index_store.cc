#include "index_store.h"

#include <iostream>
#include <list>
#include <memory>

//
// The supporting long key in database like LMDB (LevelDB supports that already)
// is to chop it into smaller components, with the use of pointers to track its
// parents. Such that, the keys will be organized in a hierarchial manner.
//

// Name of the database
static const char* database_name = "IndexStore";

// The maximum number of ID allowed
static constexpr uint64_t max_parent_id = (uint64_t)-1;

// Maximum size of the content of a single key component
static uint32_t max_part_size = 128;

//
// Format of an index entry
//
struct IndexEntry {
  // ID of Parent IndexEntry
  // For the first part of indice it is equal to #max_parent_id
  uint64_t parent_id;
  // ID of this IndexEntry
  uint64_t id;
  // Reference counter of this IndexEntry
  uint64_t refcount;
  // The size of this key partition
  uint32_t part_size;
  // Whether this IndexEntry marks the end of a key
  uint8_t is_leaf;
  // The content of this key partition
  char part[];
};

//
// Index Store Comparator
//
// 1. Parent ID
// 2. Part of Key Content (component)
//
static int IndexStoreCompare(const MDB_val* a, const MDB_val* b) {
  IndexEntry* ie_a = static_cast<IndexEntry*>(a->mv_data);
  IndexEntry* ie_b = static_cast<IndexEntry*>(b->mv_data);
  if (ie_a->parent_id != ie_b->parent_id) {
    return (ie_a->parent_id < ie_a->parent_id) ? -1 : 1;
  }

  int r;
  r = memcmp(ie_a->part, ie_b->part,
             std::min(ie_a->part_size, ie_b->part_size));
  if (!r) {
    if (ie_a->part_size < ie_b->part_size)
      return -1;
    return ie_a->part_size > ie_b->part_size;
  }
  return r;
}

//
// Initialize the index store
//
IndexStore::IndexStore(lmdb::env& env, Allocator& allocator) try
    : dbi(0),
      allocator(allocator) {
  lmdb::txn txn = lmdb::txn::begin(env);
  dbi = lmdb::dbi::open(txn, database_name, MDB_CREATE);
  dbi.set_compare(txn, IndexStoreCompare);
  txn.commit();
} catch (const lmdb::error& e) {
  std::cout << e.what();
  throw;
}

//
// Uninitialize the index store
//
IndexStore::~IndexStore() noexcept {}

static std::list<std::string> ChopIndex(const std::string& index) {
  size_t off = 0;
  std::list<std::string> entry_list;
  while (off < index.size()) {
    uint32_t part_size = (index.size() - off < max_part_size)
                             ? index.size() - off
                             : max_part_size;
    std::string entry_buffer(sizeof(IndexEntry) + part_size, 0);
    IndexEntry* e = reinterpret_cast<IndexEntry*>(&entry_buffer[0]);
    e->part_size = part_size;
    memcpy(e->part, index.data() + off, part_size);
    entry_list.push_back(entry_buffer);
    off += part_size;
  }

  return entry_list;
}

bool IndexStore::IndexExist(lmdb::txn& txn, const std::string& index) {
  uint64_t parent_id = max_parent_id;
  std::list<std::string> entry_list = ChopIndex(index);
  size_t i = 0, n = entry_list.size();

  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  // First use #max_parent_id for the first component
  for (std::string& str : entry_list) {
    IndexEntry* e = reinterpret_cast<IndexEntry*>(&str[0]);
    e->parent_id = parent_id;

    // Search in the database for the component.
    lmdb::val val_index(str);
    if (!cursor.get(val_index, MDB_SET_KEY) ||
        (i == n - 1 && !val_index.data<IndexEntry>()->is_leaf)) {
      // If we do not find the intermediate index entry, or we find the last
      // entry's #is_leaf set to 0, that implies we did not find corresponding
      // index.
      return false;
    }
    parent_id = val_index.data<IndexEntry>()->id;

    i++;
  }
  return true;
}

bool IndexStore::GetIndex(lmdb::txn& txn,
                          const std::string& index,
                          std::string& data) {
  uint64_t parent_id = max_parent_id;
  std::list<std::string> entry_list = ChopIndex(index);
  size_t i = 0, n = entry_list.size();

  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  // First use #max_parent_id for the first component
  for (std::string& str : entry_list) {
    IndexEntry* e = reinterpret_cast<IndexEntry*>(&str[0]);
    e->parent_id = parent_id;

    // Search in the database for the component.
    lmdb::val val_index(str);
    if (!cursor.get(val_index, MDB_SET_KEY) ||
        (i == n - 1 && !val_index.data<IndexEntry>()->is_leaf)) {
      // If we do not find the intermediate index entry, or we find the last
      // entry's #is_leaf set to 0, that implies we did not find corresponding
      // index.
      return false;
    }
    parent_id = val_index.data<IndexEntry>()->id;

    i++;
  }

  // Retrieve data right at the current index
  lmdb::val val_index, val_data;
  if (!cursor.get(val_index, val_data, MDB_GET_CURRENT))
    return false;
  data.assign(val_data.data(), val_data.size());
  return true;
}

void IndexStore::SetIndex(lmdb::txn& txn,
                          const std::string& index,
                          const std::string& data) {
  uint64_t parent_id = max_parent_id;
  std::list<std::string> entry_list = ChopIndex(index);
  size_t i = 0, n = entry_list.size();

  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  // First use #max_parent_id for the first component
  for (std::string& str : entry_list) {
    IndexEntry* e = reinterpret_cast<IndexEntry*>(&str[0]);
    e->parent_id = parent_id;

    // Search in the database for the component.
    lmdb::val val_index(str), val_data;
    if (!cursor.get(val_index, val_data, MDB_SET_KEY)) {
      val_index = {str};
      // If we do not find the intermediate index entry, we need to insert
      // one into the index store.
      allocator.IdAllocate(txn, &e->id);
      e->refcount = 1;
      if (i == n - 1) {
        e->is_leaf = true;
        val_data = {data};
      } else {
        e->is_leaf = false;
      }

      // Update the index store
      cursor.put(val_index, val_data);
    } else {
      // If we get an index entry, we update the properties of the
      // prepared index entry (no need for content of the index entry).
      memcpy(e, val_index.data(), sizeof(IndexEntry));
      val_index = {str};
      e->refcount++;
      if (i == n - 1) {
        e->is_leaf = true;
        val_data = {data};
      }

      // Update the index store
      cursor.del();
      cursor.put(val_index, val_data);
    }
    parent_id = e->id;

    i++;
  }
}

void IndexStore::DeleteIndex(lmdb::txn& txn, const std::string& index) {
  uint64_t parent_id = max_parent_id;
  std::list<std::string> entry_list = ChopIndex(index);
  size_t i = 0, n = entry_list.size();

  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  // First use #max_parent_id for the first component
  for (std::string& str : entry_list) {
    IndexEntry* e = reinterpret_cast<IndexEntry*>(&str[0]);
    e->parent_id = parent_id;

    // Search in the database for the index entry.
    lmdb::val val_index(str);
    if (!cursor.get(val_index, MDB_SET_KEY) ||
        (i == n - 1 && !val_index.data<IndexEntry>()->is_leaf)) {
      // If we do not find the intermediate index entry, or we find the last
      // entry's #is_leaf set to 0, that implies we did not find corresponding
      // index.
      return;
    }
    // Update the properties of index entry in #entry_list for later deletion
    memcpy(e, val_index.data(), sizeof(IndexEntry));

    parent_id = e->id;
    i++;
  }

  // Now starts to delete the key from the index store
  i = 0;
  for (std::string& str : entry_list) {
    lmdb::val val_index(str), val_data;
    // Seek to the index entry we found
    if (cursor.get(val_index, val_data, MDB_SET)) {
      IndexEntry* e = reinterpret_cast<IndexEntry*>(&str[0]);

      // Prepare to update the index entry by first deleting it, since we
      // have no way to overwrite the content of the index entry.
      cursor.del();
      if (--e->refcount) {
        // The index entry is still in-use by other keys.
        if (i == n - 1) {
          // Since the key using this index entry as leaf index entry is
          // gone, we remove the data this index entry contains.
          e->is_leaf = false;
          cursor.put(val_index);
        } else
          // The index entry we are going to delete is the intermediate
          // index entry for the specified key. So we have to put whatever
          // data there back. (Some of the other keys may reference this
          // index entry as its leaf index entry)
          cursor.put(val_index, val_data);
      } else {
        // No one is using the index entry, so we simply delete it.
        allocator.IdFree(txn, e->id);
      }
    }
    i++;
  }
}