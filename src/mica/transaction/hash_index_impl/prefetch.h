#pragma once
#ifndef MICA_TRANSACTION_HASH_INDEX_IMPL_PREFETCH_H_
#define MICA_TRANSACTION_HASH_INDEX_IMPL_PREFETCH_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool UniqueKey, class Key, class Hash,
          class KeyEqual>
void HashIndex<StaticConfig, UniqueKey, Key, Hash, KeyEqual>::prefetch(
    Transaction* tx, const Key& key) {
  Timing t(tx->context()->timing_stack(), &Stats::index_read);

  auto bkt_id = get_bucket_id(key);

  RowAccessHandlePeekOnly rah(tx);
  rah.prefetch_row(idx_tbl_, 0, bkt_id, 0, sizeof(Bucket));
}
}
}

#endif
