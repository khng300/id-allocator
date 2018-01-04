#ifndef __ALLOCATOR_H__
#define __ALLOCATOR_H__

#include <lmdbxx/lmdb++.h>

//
// Type of id
//
typedef uint64_t object_id_t;

//
// Allocator interface
//
struct Allocator {
  // Open/create the allocator in the environment
  Allocator(lmdb::env& env);
  // Close the allocator
  ~Allocator() noexcept;

  // Allocate an ID
  bool IdAllocate(lmdb::txn& txn, object_id_t* const id);
  // Free an ID
  void IdFree(lmdb::txn& txn, object_id_t id);

 private:
  // dbi of the allocator
  lmdb::dbi dbi;
};

#endif // __ALLOCATOR_H__