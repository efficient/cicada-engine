#pragma once
#ifndef MICA_TRANSACTION_HASH_INDEX_IMPL_LOOKUP_H_
#define MICA_TRANSACTION_HASH_INDEX_IMPL_LOOKUP_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool UniqueKey, class Key, class Hash,
          class KeyEqual>
template <typename Func>
uint64_t HashIndex<StaticConfig, UniqueKey, Key, Hash, KeyEqual>::lookup(
    Transaction* tx, const Key& key, bool skip_validation, const Func& func) {
  Timing t(tx->context()->timing_stack(), &Stats::index_read);

  uint64_t chain_len;

  if (StaticConfig::kCollectProcessingStats) chain_len = 0;

  uint64_t found = 0;

  const Bucket* bkt;
  auto bkt_id = get_bucket_id(key);

  while (true) {
    if (StaticConfig::kCollectProcessingStats) chain_len++;

    if (skip_validation) {
      RowAccessHandlePeekOnly rah(tx);
      if (!rah.peek_row(idx_tbl_, 0, bkt_id, false, false, false))
        return kHaveToAbort;
      bkt = reinterpret_cast<const Bucket*>(rah.cdata());
    } else {
      RowAccessHandle rah(tx);
      if (!rah.peek_row(idx_tbl_, 0, bkt_id, true, true, false) ||
          !rah.read_row(data_copier_))
        return kHaveToAbort;
      bkt = reinterpret_cast<const Bucket*>(rah.cdata());
    }

    for (size_t j = 0; j < Bucket::kBucketSize; j++) {
      // printf("HashIndex::lookup() key=%" PRIu64 " bucket_key=%" PRIu64
      //        " value=%" PRIu64 "\n",
      //        key, bkt->keys[i], bkt->values[i]);
      if (!key_equal_(bkt->keys[j], key)) continue;

      if (bkt->values[j] == kNullRowID) continue;

      auto value = bkt->values[j];

      if (StaticConfig::kCollectProcessingStats) {
        if (tx->context()->stats().max_hash_index_chain_len < chain_len)
          tx->context()->stats().max_hash_index_chain_len = chain_len;
      }

      found++;
      if (!func(bkt->keys[j], value)) return found;
      if (UniqueKey) {
        // There will be no matching key.
        return found;
      }

      // To omit checking j if the bucket has only one item.
      if (Bucket::kBucketSize == 1) break;
    }

    bkt_id = bkt->next;
    if (bkt_id == kNullRowID) {
      if (StaticConfig::kCollectProcessingStats) {
        if (tx->context()->stats().max_hash_index_chain_len < chain_len)
          tx->context()->stats().max_hash_index_chain_len = chain_len;
      }
      return found;
    }
  }
}
}
}

#endif
