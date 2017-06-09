#pragma once
#ifndef MICA_TRANSACTION_HASH_INDEX_IMPL_BUCKET_H_
#define MICA_TRANSACTION_HASH_INDEX_IMPL_BUCKET_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool UniqueKey, class Key, class Hash,
          class KeyEqual>
uint64_t HashIndex<StaticConfig, UniqueKey, Key, Hash,
                   KeyEqual>::get_bucket_id(const Key& key) const {
  // Constant from CityHash.
  return (hash_(key) * 0x9ddfea08eb382d69ULL) & bucket_count_mask_;
  // return (key * 0x9ddfea08eb382d69ULL) % bucket_count_;
}
}
}

#endif