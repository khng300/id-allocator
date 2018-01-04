#include <allocator.h>
#include <index_store.h>

#include <cstdlib>
#include <iostream>
#include <set>

const object_id_t max_id = 1000;

int main() {
  lmdb::env env = lmdb::env::create();
  try {
    std::set<object_id_t> got_ids;
    env.set_max_dbs(3);
    env.set_mapsize(1ull * 1024 * 1024 * 1024 * 1024); // 1TiB max. mapsize
    env.open("example.mdb");
    
    Allocator allocator(env);
    IndexStore index_store(env, allocator);
    lmdb::txn txn = lmdb::txn::begin(env);
    for (object_id_t i = 0; i <= max_id; ++i) {
      object_id_t id_got;
      if (allocator.IdAllocate(txn, &id_got)) {
	      got_ids.insert(id_got);
        std::cout << "Got ID: " << id_got << std::endl;
      }
      if (!((i + 1) % 100)) {
        txn.commit();
        txn = lmdb::txn::begin(env);
      }
    }
    txn.commit();
#if 1
    txn = lmdb::txn::begin(env);
    object_id_t i = 0;
    for (object_id_t id : got_ids) {
      allocator.IdFree(txn, id);
      std::cout << "Freed ID: " << id << std::endl;
      if (!((i + 1) % 100)) {
        txn.commit();
        txn = lmdb::txn::begin(env);
      }
      ++i;
    }
    txn.commit();
#endif
  } catch (const lmdb::error& e) {
    std::cout << "Failure: " << e.what() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
