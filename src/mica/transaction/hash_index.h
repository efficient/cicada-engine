#pragma once
#ifndef MICA_TRANSACTION_HASH_INDEX_H_
#define MICA_TRANSACTION_HASH_INDEX_H_

#include "mica/common.h"
#include "mica/util/type_traits.h"

namespace mica {
namespace transaction {
template <class StaticConfig, bool UniqueKey, class Key,
          class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>>
class HashIndex;

template <class StaticConfig, bool UniqueKey, class Key,
          class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>>
class HashIndexBucketCopier {
 public:
  typedef HashIndex<StaticConfig, UniqueKey, Key, Hash, KeyEqual> HashIndexT;
  typedef typename HashIndexT::Bucket Bucket;

  bool operator()(uint16_t cf_id, RowVersion<StaticConfig>* dest,
                  const RowVersion<StaticConfig>* src) const {
    (void)cf_id;
    if (dest->data_size == 0) return true;

    auto dest_bucket = reinterpret_cast<Bucket*>(dest->data);
    auto src_bucket = reinterpret_cast<const Bucket*>(src->data);

    dest_bucket->next = src_bucket->next;

    ::mica::util::memcpy(dest_bucket->keys, src_bucket->keys,
                         sizeof(Bucket::keys));
    if (Bucket::kBucketSize == 1) {
      for (size_t i = 0; i < Bucket::kBucketSize; i++)
        dest_bucket->values[i] = src_bucket->values[i];
    } else {
      ::mica::util::memcpy(dest_bucket->values, src_bucket->values,
                           sizeof(Bucket::values));
    }
    return true;
  }
};

template <class StaticConfig, bool UniqueKey, class Key, class Hash,
          class KeyEqual>
class HashIndex {
 public:
  typedef HashIndexBucketCopier<StaticConfig, UniqueKey, Key, Hash, KeyEqual>
      DataCopier;

  typedef typename StaticConfig::Timing Timing;
  typedef ::mica::transaction::RowAccessHandle<StaticConfig> RowAccessHandle;
  typedef ::mica::transaction::RowAccessHandlePeekOnly<StaticConfig>
      RowAccessHandlePeekOnly;
  typedef ::mica::transaction::Transaction<StaticConfig> Transaction;

  struct Bucket {
    // static constexpr size_t kBucketSize = 13;	// (216 - 8) / 16
    // static constexpr size_t kBucketSize = 9;	// (152 - 8) / 16
    // static constexpr size_t kBucketSize = 5;	// (88 - 8) / 16
    static constexpr size_t kBucketSize = 1;  // (24 - 8) / 16

    uint64_t next;

    Key keys[kBucketSize];
    uint64_t values[kBucketSize];
  };
  static constexpr uint64_t kDataSize = sizeof(Bucket);

  static constexpr uint64_t kNullRowID = static_cast<uint64_t>(-1);

  static constexpr uint64_t kHaveToAbort = static_cast<uint64_t>(-1);

  // hash_index_impl/init.h
  HashIndex(DB<StaticConfig>* db, Table<StaticConfig>* main_tbl,
            Table<StaticConfig>* idx_tbl, uint64_t expected_num_rows,
            const Hash& hash = Hash(), const KeyEqual& key_equal = KeyEqual());

  bool init(Transaction* tx);

  // hash_index_impl/insert.h
  uint64_t insert(Transaction* tx, const Key& key, uint64_t value);

  // hash_index_impl/remove.h
  uint64_t remove(Transaction* tx, const Key& key, uint64_t value);

  // hash_index_impl/lookup.h
  template <typename Func>
  uint64_t lookup(Transaction* tx, const Key& key, bool skip_validation,
                  const Func& func);

  // hash_index_impl/prefetch.h
  void prefetch(Transaction* tx, const Key& key);

  Table<StaticConfig>* main_table() { return main_tbl_; }
  const Table<StaticConfig>* main_table() const { return main_tbl_; }

  Table<StaticConfig>* index_table() { return idx_tbl_; }
  const Table<StaticConfig>* index_table() const { return idx_tbl_; }

  uint64_t expected_num_rows() const { return expected_num_rows_; }

 private:
  DB<StaticConfig>* db_;
  Table<StaticConfig>* main_tbl_;
  Table<StaticConfig>* idx_tbl_;
  uint64_t expected_num_rows_;
  Hash hash_;
  KeyEqual key_equal_;

  DataCopier data_copier_;

  uint64_t bucket_count_;
  uint64_t bucket_count_mask_;

  // hash_index_impl/bucket.h
  uint64_t get_bucket_id(const Key& key) const;
};
}
}

#include "hash_index_impl/init.h"
#include "hash_index_impl/bucket.h"
#include "hash_index_impl/insert.h"
#include "hash_index_impl/remove.h"
#include "hash_index_impl/lookup.h"
#include "hash_index_impl/prefetch.h"

#endif
