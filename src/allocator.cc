#include "allocator.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>

//
// The maximal length of an extent allowed
//
static constexpr uint64_t maximum_length = UINT64_MAX;

// Name of the database
static const char *database_name = "Allocator";

//
// Extent representing a range of free ID
//
struct FreeIdExtent {
  // Starting ID that is free
  object_id_t id;
  // Length of the extent
  object_id_t length;
};

//
// Free Extent Comparator
//
static int AllocatorCompare(const MDB_val *a, const MDB_val *b) {
  FreeIdExtent *a_ext = static_cast<FreeIdExtent *>(a->mv_data);
  FreeIdExtent *b_ext = static_cast<FreeIdExtent *>(b->mv_data);
  assert(a->mv_size == b->mv_size && a->mv_size == sizeof(FreeIdExtent));

  if (a_ext->id != b_ext->id)
    return (a_ext->id < b_ext->id) ? -1 : 1;
  return 0;
}

//
// Open/create the allocator in the environment
//
Allocator::Allocator(lmdb::env &env) try : dbi(0) {
  lmdb::txn txn = lmdb::txn::begin(env);
  try {
    dbi = lmdb::dbi::open(txn, database_name, 0);
  } catch (lmdb::not_found_error &) {
    FreeIdExtent ext{0, maximum_length};
    lmdb::val key(&ext, sizeof(FreeIdExtent));
    lmdb::val data{};
    dbi = lmdb::dbi::open(txn, database_name, MDB_CREATE);
    dbi.put(txn, key, data);
  }
  dbi.set_compare(txn, AllocatorCompare);
  txn.commit();
} catch (const lmdb::error &e) {
  std::cout << e.what();
  throw;
}

Allocator::~Allocator() noexcept {}

//
// Allocate an ID from the free extent database
//
// Currently the routine is very simple - it looks up the first extent in the
// free ID database and return the starting ID of the found extent to the
// caller.
//
optional<std::pair<object_id_t, object_id_t>>
Allocator::IdAllocate(lmdb::txn &txn, object_id_t len) {
  lmdb::val val_ext;
  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  if (!cursor.get(val_ext, MDB_FIRST))
    return {};

  FreeIdExtent ext = *val_ext.data<FreeIdExtent>();
  object_id_t alloc_id = ext.id;
  object_id_t alloc_id_len = std::min(ext.length, len);
  cursor.del();
  ext.id += alloc_id_len;
  ext.length -= alloc_id_len;
  if (ext.length) {
    val_ext = lmdb::val(&ext, sizeof(FreeIdExtent));
    cursor.put(val_ext);
  }

  return {{alloc_id, alloc_id_len}};
}

//
// Check if the two extents are consecutive (providing that #a must be smaller
// than #b)
//
static inline bool AllocatorCheckConsecutive(const FreeIdExtent *a,
                                             const FreeIdExtent *b) {
  return a->id + a->length == b->id;
}

//
// Check if two extents overlap
//
static inline bool AllocatorCheckExtentOverlap(const FreeIdExtent &Extent,
                                               const FreeIdExtent &NewExtent) {
  return NewExtent.id <= Extent.id + Extent.length - 1 &&
         Extent.id <= NewExtent.id + NewExtent.length - 1;
}

//
// Free an ID to the free extent database
//
// Double free of an ID is prohibited.
//
void Allocator::IdFree(lmdb::txn &txn, object_id_t id, object_id_t len) {
  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  FreeIdExtent ext{id, 0};
  lmdb::val val_ext(&ext, sizeof(FreeIdExtent));
  bool allocator_full = false;
  // First find an extent with its id greater than #id
  bool found = cursor.get(val_ext, MDB_SET_RANGE);
  if (!found) {
    found = cursor.get(val_ext, MDB_LAST);
    if (!found)
      allocator_full = true;
  }

  // #NewExtent is the extent to be inserted into the database
  FreeIdExtent new_ext{id, len};
  if (!allocator_full) {
    // There is at least one free extent presented in the database
    ext = *val_ext.data<FreeIdExtent>();
    // Sanity check - the range to be freed must not be in database
    assert(!AllocatorCheckExtentOverlap(lookup_ext, new_ext));
    if (id > ext.id) {
      // Check if we can merge the extent smaller than #NewExtent
      if (AllocatorCheckConsecutive(&ext, &new_ext)) {
        new_ext.id = ext.id;
        new_ext.length += ext.length;
        cursor.del();
      }
      // We don't need to check the next extent in this case, as we can only
      // reach there if there is no more extent greater than #ID (Recall that we
      // failed the first lookup)
    } else {
      // Check if we can merge the extent greater than #NewExtent
      if (AllocatorCheckConsecutive(&new_ext, &ext)) {
        new_ext.length += ext.length;
        cursor.del();
      }

      // Check if merging with extents preceding #NewExtent is possible
      found = cursor.get(val_ext, MDB_PREV);
      if (found) {
        ext = *val_ext.data<FreeIdExtent>();
        // Sanity check - the range to be freed must not be in database
        assert(!AllocatorCheckExtentOverlap(ext, new_ext));
        // Check if we can merge the extent less than #NewExtent
        if (AllocatorCheckConsecutive(&ext, &new_ext)) {
          new_ext.id = ext.id;
          new_ext.length += ext.length;
          cursor.del();
        }
      }
    }
  }
  // Insert the resulting new extent
  cursor.put(lmdb::val(&new_ext, sizeof(FreeIdExtent)), lmdb::val());
}

#if 0

void Allocator::IdFree(lmdb::txn &txn, object_id_t id, object_id_t len) {
  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  FreeIdExtent Extent(ID, 0);
  lmdb::Val ExtentKey(&Extent.ID), ExtentData;
  bool AllocatorFull = false;
  // First find an free extent with its ID greater than #ID
  auto Err = Cursor.get(ExtentKey, ExtentData, MDB_SET_RANGE);
  if (Err) {
    Err = lmdb::filterDBError(std::move(Err), MDB_NOTFOUND);
    if (Err)
      return Err;
    // Get the last free extent in the database instead
    Err = Cursor.get(ExtentKey, ExtentData, MDB_LAST);
    if (Err) {
      Err = lmdb::filterDBError(std::move(Err), MDB_NOTFOUND);
      if (Err)
        return Err;
      AllocatorFull = true;
    }
  }

  // #NewExtent is the extent to be inserted into the database
  FreeIdExtent NewExtent(ID, Length);
  if (!AllocatorFull) {
    // There is at least one free extent presented in the database
    Extent.ID = *ExtentKey.data<RecordID>();
    Extent.Length = *ExtentData.data<RecordID>();
    // Sanity check - the range to be freed must not be in database
    assert(!AllocatorCheckExtentOverlap(Extent, NewExtent));
    if (ID > Extent.ID) {
      // Check if we can merge the extent smaller than #NewExtent
      if (AllocatorCheckConsecutive(Extent, NewExtent)) {
        NewExtent.ID = Extent.ID;
        NewExtent.Length += Extent.Length;
        Err = Cursor.del();
        if (Err)
          return Err;
      }
      // We don't need to check the next extent in this case, as we can only
      // reach there if there is no more extent greater than #ID (Recall that we
      // failed the first lookup)
    } else {
      // Check if we can merge the extent greater than #NewExtent
      if (AllocatorCheckConsecutive(NewExtent, Extent)) {
        NewExtent.Length += Extent.Length;
        Err = Cursor.del();
        if (Err)
          return Err;
      }

      // Check if merging with extents preceding #NewExtent is possible
      Err = Cursor.get(ExtentKey, ExtentData, MDB_PREV);
      if (Err) {
        Err = lmdb::filterDBError(std::move(Err), MDB_NOTFOUND);
        if (Err)
          return Err;
      } else {
        Extent.ID = *ExtentKey.data<RecordID>();
        Extent.Length = *ExtentData.data<RecordID>();
        // Sanity check - the range to be freed must not be in database
        assert(!AllocatorCheckExtentOverlap(Extent, NewExtent));
        // Check if we can merge the extent less than #NewExtent
        if (AllocatorCheckConsecutive(Extent, NewExtent)) {
          NewExtent.ID = Extent.ID;
          NewExtent.Length += Extent.Length;
          Err = Cursor.del();
          if (Err)
            return Err;
        }
      }
    }
  }
  // Insert the resulting new extent
  ExtentKey.assign(&NewExtent.ID, sizeof(RecordID));
  ExtentData.assign(&NewExtent.Length, sizeof(RecordID));
  return Cursor.put(ExtentKey, ExtentData, 0);
}

#endif