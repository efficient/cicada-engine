#pragma once
#ifndef MICA_TRANSACTION_BTREE_INDEX_IMPL_PREFETCH_H_
#define MICA_TRANSACTION_BTREE_INDEX_IMPL_PREFETCH_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool HasValue, class Key, class Compare>
void BTreeIndex<StaticConfig, HasValue, Key, Compare>::prefetch(
    Transaction* tx, const Key& key) {
  // Prefetching is not meaningfull in a tree.
  (void)tx;
  (void)key;
}
}
}

#endif
