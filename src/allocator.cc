#include <allocator.h>

#include <cassert>
#include <cstdint>
#include <iostream>

//
// The maximal length of an extent allowed
//
static constexpr uint64_t maximum_length = UINT64_MAX;

// Name of the database
static const char* database_name = "Allocator";

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
static int AllocatorCompare(const MDB_val* a, const MDB_val* b) {
  FreeIdExtent* a_ext = static_cast<FreeIdExtent*>(a->mv_data);
  FreeIdExtent* b_ext = static_cast<FreeIdExtent*>(b->mv_data);
  assert(a->mv_size == b->mv_size && a->mv_size == sizeof(FreeIdExtent));

  if (a_ext->id != b_ext->id)
    return (a_ext->id < b_ext->id) ? -1 : 1;
  return 0;
}

//
// Open/create the allocator in the environment
//
Allocator::Allocator(lmdb::env& env) try : dbi(0) {
  lmdb::txn txn = lmdb::txn::begin(env);
  try {
    dbi = lmdb::dbi::open(txn, database_name, 0);
  } catch (lmdb::not_found_error& e) {
    FreeIdExtent ext = {.id = 0, .length = maximum_length};
    lmdb::val key(&ext, sizeof(FreeIdExtent));
    lmdb::val data{};
    dbi = lmdb::dbi::open(txn, database_name, MDB_CREATE);
    dbi.put(txn, key, data);
  }
  txn.commit();
} catch (const lmdb::error& e) {
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
bool Allocator::IdAllocate(lmdb::txn& txn, object_id_t* const id) {
  lmdb::val val_ext;
  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  if (!cursor.get(val_ext, MDB_FIRST))
    return false;

  FreeIdExtent ext = *val_ext.data<FreeIdExtent>();
  object_id_t id_got = ext.id++;
  cursor.del();
  if (--ext.length) {
    val_ext = lmdb::val(&ext, sizeof(FreeIdExtent));
    cursor.put(val_ext);
  }

  *id = id_got;
  return true;
}

//
// Check if the two extents are consecutive (providing that #a must be smaller
// than #b)
//
static inline bool AllocatorCheckConsecutive(const FreeIdExtent* a,
                                             const FreeIdExtent* b) {
  return a->id + a->length == b->id;
}

//
// Check if an ID is in the range of an extent
//
static inline bool AllocatorCheckInRange(const FreeIdExtent* ext,
                                         object_id_t id) {
  return id >= ext->id && id < ext->id + ext->length;
}

//
// Free an ID to the free extent database
//
// Double free of an ID is prohibited.
//
void Allocator::IdFree(lmdb::txn& txn, object_id_t id) {
  FreeIdExtent lookup_ext = {.id = id, .length = 0};
  lmdb::val val_ext_found(&lookup_ext, sizeof(FreeIdExtent));
  lmdb::cursor cursor = lmdb::cursor::open(txn, dbi);
  // First find an extent with its id greater than #id
  bool found = cursor.get(val_ext_found, MDB_SET_RANGE);

  // Sanity check - #id must not exist in the free ID database
  assert(!found ||
         !AllocatorCheckInRange(val_ext_found.data<FreeIdExtent>(), id));
  if (!found) {
    found = cursor.get(val_ext_found, MDB_LAST);
    // Sanity check - #id must not exist in the free ID database
    assert(!AllocatorCheckInRange(val_ext_found.data<FreeIdExtent>(), id));
  }

  FreeIdExtent new_ext = {.id = id, .length = 1};
  if (found && id > val_ext_found.data<FreeIdExtent>()->id) {
    //
    // Check if we can merge the extent smaller than #new_ext
    //
    if (AllocatorCheckConsecutive(val_ext_found.data<FreeIdExtent>(),
                                  &new_ext)) {
      new_ext.id = val_ext_found.data<FreeIdExtent>()->id;
      new_ext.length += val_ext_found.data<FreeIdExtent>()->length;
      cursor.del();
    }
    //
    // We don't need to check the next extent in this case, as we can only
    // reach there if there is no more extent greater than #id (Recall that we
    // failed the first lookup)
    //
  } else if (found) {
    //
    // Check if we can merge the extent greater than #new_ext
    //
    if (AllocatorCheckConsecutive(&new_ext,
                                  val_ext_found.data<FreeIdExtent>())) {
      new_ext.length += val_ext_found.data<FreeIdExtent>()->length;
      cursor.del();
    }
    found = cursor.get(val_ext_found, MDB_PREV);
    // Sanity check - #id must not exist in the free ID database
    assert(!found ||
           !AllocatorCheckInRange(val_ext_found.data<FreeIdExtent>(), id));
    //
    // Check if we can merge the extent less than #new_ext
    //
    if (found && AllocatorCheckConsecutive(val_ext_found.data<FreeIdExtent>(),
                                           &new_ext)) {
      new_ext.id = val_ext_found.data<FreeIdExtent>()->id;
      new_ext.length += val_ext_found.data<FreeIdExtent>()->length;
      cursor.del();
    }
  }

  // Insert the resulting new extent
  lmdb::val val_new_ext = lmdb::val(&new_ext, sizeof(FreeIdExtent));
  cursor.put(val_new_ext);
}