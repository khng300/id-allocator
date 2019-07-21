#ifndef __ALLOCATOR_H__
#define __ALLOCATOR_H__

#include "lmdbxx/lmdb++.h"
#include "optional.hpp"

using std::experimental::optional;

//
// Type of id
//
typedef uint64_t object_id_t;

//
// Allocator interface
//
struct Allocator {
  // Open/create the allocator in the environment
  Allocator(lmdb::env &env);
  // Close the allocator
  ~Allocator() noexcept;

  // Allocate an ID
  optional<std::pair<object_id_t, object_id_t>> IdAllocate(lmdb::txn &txn,
                                                           object_id_t len);

  // Free an ID
  void IdFree(lmdb::txn &txn, object_id_t id, object_id_t len);

private:
  // dbi of the allocator
  lmdb::dbi dbi;
};

#endif // __ALLOCATOR_H__