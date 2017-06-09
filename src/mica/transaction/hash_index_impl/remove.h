#pragma once
#ifndef MICA_TRANSACTION_HASH_INDEX_IMPL_REMOVE_H_
#define MICA_TRANSACTION_HASH_INDEX_IMPL_REMOVE_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool UniqueKey, class Key, class Hash,
          class KeyEqual>
uint64_t HashIndex<StaticConfig, UniqueKey, Key, Hash, KeyEqual>::remove(
    Transaction* tx, const Key& key, uint64_t value) {
  Timing t(tx->context()->timing_stack(), &Stats::index_write);

  auto bkt_id = get_bucket_id(key);
  RowAccessHandle rah(tx);
  RowAccessHandle rah_prev(tx);

  if (!rah.peek_row(idx_tbl_, 0, bkt_id, true, true, false) ||
      !rah.read_row(data_copier_))
    return kHaveToAbort;
  // printf("HashIndex::remove() 1\n");
  auto cbkt = reinterpret_cast<const Bucket*>(rah.cdata());

  // Find the existing key.
  uint64_t existing_key_j = Bucket::kBucketSize;
  while (true) {
    for (uint64_t j = 0; j < Bucket::kBucketSize; j++)
      if (cbkt->values[j] == value && key_equal_(cbkt->keys[j], key)) {
        existing_key_j = j;
        break;
      }
    if (existing_key_j != Bucket::kBucketSize) break;

    if (cbkt->next == kNullRowID) break;
    bkt_id = cbkt->next;

    rah_prev = rah;
    rah.reset();
    if (!rah.peek_row(idx_tbl_, 0, bkt_id, true, true, false) ||
        !rah.read_row(data_copier_))
      return kHaveToAbort;
    // printf("HashIndex::remove() 2\n");
    cbkt = reinterpret_cast<const Bucket*>(rah.cdata());
  }

  // No existing key found.
  if (existing_key_j == Bucket::kBucketSize) return 0;

  // If this is not the last bucket in the chain, find a key from the last bucket to fill the slot of this deleted key.

  if (cbkt->next != kNullRowID) {
    if (!rah.write_row(kDataSize, data_copier_)) return kHaveToAbort;
    auto filled_in_bkt = reinterpret_cast<Bucket*>(rah.data());

    while (cbkt->next != kNullRowID) {
      bkt_id = cbkt->next;

      rah_prev = rah;
      rah.reset();
      if (!rah.peek_row(idx_tbl_, 0, bkt_id, true, true, false) ||
          !rah.read_row(data_copier_))
        return kHaveToAbort;
      cbkt = reinterpret_cast<const Bucket*>(rah.cdata());
    }

    uint64_t last_j = Bucket::kBucketSize;
    for (uint64_t j = 0; j < Bucket::kBucketSize; j++)
      if (cbkt->values[j] != kNullRowID) {
        if (last_j == Bucket::kBucketSize) {
          last_j = j;
          break;
        }
      }
    assert(last_j != Bucket::kBucketSize);

    // Fill the slot.
    filled_in_bkt->keys[existing_key_j] = cbkt->keys[last_j];
    filled_in_bkt->values[existing_key_j] = cbkt->values[last_j];

    // Pretend that we deleted some key from the last bucket.
    existing_key_j = last_j;
  }

  // Delete the last bucket if we have removed the last key from it.
  bool will_delete_bucket;
  if (!rah_prev)
    will_delete_bucket = false;
  else {
    will_delete_bucket = true;
    for (uint64_t j = 0; j < Bucket::kBucketSize; j++)
      if (cbkt->values[j] != kNullRowID) {
        if (j == existing_key_j)
          continue;
        else {
          will_delete_bucket = false;
          break;
        }
      }
  }

  if (!will_delete_bucket) {
    // Simply make the slot as empty.
    if (!rah.write_row(kDataSize, data_copier_)) return kHaveToAbort;
    auto bkt = reinterpret_cast<Bucket*>(rah.data());
    bkt->values[existing_key_j] = kNullRowID;
  } else {
    // Unlink this bucket from the previous bucket.
    if (!rah_prev.write_row(0, data_copier_)) return kHaveToAbort;
    auto prev_bkt = reinterpret_cast<Bucket*>(rah.data());

    prev_bkt->next = kNullRowID;

    // Delete this bucket.
    if (!rah.delete_row()) return kHaveToAbort;
  }

  return 1;
}
}
}

#endif
