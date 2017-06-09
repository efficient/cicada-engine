#pragma once
#ifndef MICA_TRANSACTION_HASH_INDEX_IMPL_INIT_H_
#define MICA_TRANSACTION_HASH_INDEX_IMPL_INIT_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool UniqueKey, class Key, class Hash,
          class KeyEqual>
HashIndex<StaticConfig, UniqueKey, Key, Hash, KeyEqual>::HashIndex(
    DB<StaticConfig>* db, Table<StaticConfig>* main_tbl,
    Table<StaticConfig>* idx_tbl, uint64_t expected_num_rows, const Hash& hash,
    const KeyEqual& key_equal)
    : db_(db),
      main_tbl_(main_tbl),
      idx_tbl_(idx_tbl),
      expected_num_rows_(expected_num_rows),
      hash_(hash),
      key_equal_(key_equal) {
  static_assert(std::is_trivially_copyable<Key>::value,
                "trivially copyable keys required");

  // bucket_count_ = expected_num_rows * 11 / 10 /
  //                 Bucket::kBucketSize;  // 10% provisioning
  bucket_count_ =
      expected_num_rows * 12 / 10 / Bucket::kBucketSize;  // 20% provisioning
  // bucket_count_ = expected_num_rows * 15 / 10 /
  //                 Bucket::kBucketSize;  // 50% provisioning

  bucket_count_ = ::mica::util::next_power_of_two(bucket_count_);
  bucket_count_mask_ = bucket_count_ - 1;
}

template <class StaticConfig, bool UniqueKey, class Key, class Hash,
          class KeyEqual>
bool HashIndex<StaticConfig, UniqueKey, Key, Hash, KeyEqual>::init(
    Transaction* tx) {
  Timing t(tx->context()->timing_stack(), &Stats::index_write);

  const uint64_t kBatchSize = 16;
  for (uint64_t i = 0; i < bucket_count_; i++) {
    if (i % kBatchSize == 0) {
      bool ret = tx->begin();
      if (!ret) return false;
    }

    RowAccessHandle rah(tx);
    if (!rah.new_row(idx_tbl_, 0, Transaction::kNewRowID, true, kDataSize)) {
      printf("failed to insert buckets\n");
      return false;
    }
    if (rah.row_id() != i) {
      printf("failed to insert buckets\n");
      return false;
    }

    auto new_bkt = reinterpret_cast<Bucket*>(rah.data());
    for (uint64_t j = 0; j < Bucket::kBucketSize; j++)
      new_bkt->values[j] = kNullRowID;
    new_bkt->next = kNullRowID;

    if (i % kBatchSize == kBatchSize - 1 || i == bucket_count_ - 1) {
      if (!tx->commit()) {
        printf("failed to insert buckets\n");
        return false;
      }
    }
  }
  // printf("HashIndex::init()\n");
  return true;
}
}
}

#endif
