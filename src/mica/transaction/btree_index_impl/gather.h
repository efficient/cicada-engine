#pragma once
#ifndef MICA_TRANSACTION_BTREE_INDEX_IMPL_MERGE_H_
#define MICA_TRANSACTION_BTREE_INDEX_IMPL_MERGE_H_

namespace mica {
namespace transaction {
// Note that link pointers are not handled by copy/gather/scatter.

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename NodeT>
void BTreeIndex<StaticConfig, HasValue, Key, Compare>::copy(
    NodeT* dest, const InternalNode* left) {
  if (kUseIndirection)
    ::mica::util::memcpy(dest->indir, left->indir,
                         sizeof(uint8_t) * left->count);
  // for (size_t i = 0; i < left->count; i++) dest->keys[i] = left->keys[i];
  ::mica::util::memcpy(dest->keys, left->keys,
                       sizeof(Key) * static_cast<size_t>(left->count));
  ::mica::util::memcpy(
      dest->child_row_ids, left->child_row_ids,
      sizeof(uint64_t) * (static_cast<size_t>(left->count) + 1));

  dest->count = left->count;
  dest->min_key = left->min_key;
  dest->max_key = left->max_key;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename NodeT>
void BTreeIndex<StaticConfig, HasValue, Key, Compare>::copy(
    NodeT* dest, const LeafNode* left) {
  if (kUseIndirection)
    ::mica::util::memcpy(dest->indir, left->indir,
                         sizeof(uint8_t) * left->count);
  // for (size_t i = 0; i < left->count; i++) dest->keys[i] = left->keys[i];
  ::mica::util::memcpy(dest->keys, left->keys,
                       sizeof(Key) * static_cast<size_t>(left->count));
  if (HasValue)
    ::mica::util::memcpy(dest->values, left->values,
                         sizeof(uint64_t) * static_cast<size_t>(left->count));

  dest->count = left->count;
  dest->min_key = left->min_key;
  dest->max_key = left->max_key;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename NodeT>
void BTreeIndex<StaticConfig, HasValue, Key, Compare>::gather(
    NodeT* dest, const InternalNode* left, const InternalNode* right) {
  if (kUseIndirection)
    ::mica::util::memcpy(dest->indir, left->indir,
                         sizeof(uint8_t) * left->count);
  // for (size_t i = 0; i < left->count; i++) dest->keys[i] = left->keys[i];
  ::mica::util::memcpy(dest->keys, left->keys,
                       sizeof(Key) * static_cast<size_t>(left->count));
  ::mica::util::memcpy(
      dest->child_row_ids, left->child_row_ids,
      sizeof(uint64_t) * (static_cast<size_t>(left->count) + 1));

  if (kUseIndirection) {
    dest->indir[left->count] = left->count;
    for (size_t i = 0; i < right->count; i++)
      dest->indir[static_cast<size_t>(left->count) + 1 + i] =
          static_cast<uint8_t>(static_cast<size_t>(left->count) + 1 +
                               static_cast<size_t>(right->indir[i]));
  }
  dest->keys[left->count] = left->max_key;
  // for (size_t i = 0; i < right->count; i++)
  //   dest->keys[static_cast<size_t>(left->count) + 1 + i] = right->keys[i];
  ::mica::util::memcpy(dest->keys + static_cast<size_t>(left->count) + 1,
                       right->keys,
                       sizeof(Key) * static_cast<size_t>(right->count));
  ::mica::util::memcpy(
      dest->child_row_ids + static_cast<size_t>(left->count) + 1,
      right->child_row_ids,
      sizeof(uint64_t) * (static_cast<size_t>(right->count) + 1));

  dest->count = static_cast<uint8_t>(static_cast<size_t>(left->count) + 1 +
                                     static_cast<size_t>(right->count));
  dest->min_key = left->min_key;
  dest->max_key = right->max_key;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename NodeT>
void BTreeIndex<StaticConfig, HasValue, Key, Compare>::gather(
    NodeT* dest, const LeafNode* left, const LeafNode* right) {
  if (kUseIndirection)
    ::mica::util::memcpy(dest->indir, left->indir,
                         sizeof(uint8_t) * left->count);
  // for (size_t i = 0; i < left->count; i++) dest->keys[i] = left->keys[i];
  ::mica::util::memcpy(dest->keys, left->keys,
                       sizeof(Key) * static_cast<size_t>(left->count));
  if (HasValue)
    ::mica::util::memcpy(dest->values, left->values,
                         sizeof(uint64_t) * (static_cast<size_t>(left->count)));

  if (kUseIndirection) {
    for (size_t i = 0; i < right->count; i++)
      dest->indir[static_cast<size_t>(left->count) + i] =
          static_cast<uint8_t>(static_cast<size_t>(left->count) +
                               static_cast<size_t>(right->indir[i]));
  }
  // for (size_t i = 0; i < right->count; i++)
  //   dest->keys[static_cast<size_t>(left->count) + i] = right->keys[i];
  ::mica::util::memcpy(dest->keys + static_cast<size_t>(left->count),
                       right->keys,
                       sizeof(Key) * static_cast<size_t>(right->count));
  if (HasValue)
    ::mica::util::memcpy(
        dest->values + static_cast<size_t>(left->count), right->values,
        sizeof(uint64_t) * (static_cast<size_t>(right->count)));

  dest->count = static_cast<uint8_t>(static_cast<size_t>(left->count) +
                                     static_cast<size_t>(right->count));
  dest->min_key = left->min_key;
  dest->max_key = right->max_key;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename NodeT>
void BTreeIndex<StaticConfig, HasValue, Key, Compare>::scatter(
    InternalNode* left, InternalNode* right, const NodeT* src,
    uint64_t new_left_count) {
  assert(src->count >= new_left_count + 1);
  size_t new_right_count = static_cast<size_t>(src->count) - new_left_count - 1;

  if (kUseIndirection) {
    left->child_row_ids[0] = src->child_row_id(0);
    for (size_t i = 0; i < new_left_count; i++) {
      left->indir[i] = static_cast<uint8_t>(i);
      left->keys[i] = src->key(i);
      left->child_row_ids[i + 1] = src->child_row_id(i + 1);
    }

    right->child_row_ids[0] = src->child_row_id(new_left_count + 1);
    for (size_t i = 0; i < new_right_count; i++) {
      right->indir[i] = static_cast<uint8_t>(i);
      right->keys[i] = src->key(new_left_count + 1 + i);
      right->child_row_ids[i + 1] =
          src->child_row_id(new_left_count + 1 + i + 1);
    }
  } else {
    // for (size_t i = 0; i < new_left_count; i++) left->keys[i] = src->keys[i];
    ::mica::util::memcpy(left->keys, src->keys, sizeof(Key) * new_left_count);
    ::mica::util::memcpy(left->child_row_ids, src->child_row_ids,
                         sizeof(uint64_t) * (new_left_count + 1));

    // for (size_t i = 0; i < new_right_count; i++)
    //   right->keys[i] = src->keys[new_left_count + 1 + i];
    ::mica::util::memcpy(right->keys, src->keys + new_left_count + 1,
                         sizeof(Key) * new_right_count);
    ::mica::util::memcpy(right->child_row_ids,
                         src->child_row_ids + new_left_count + 1,
                         sizeof(uint64_t) * (new_right_count + 1));
  }

  left->min_key = src->min_key;
  left->max_key = src->key(new_left_count);

  right->min_key = src->key(new_left_count);
  right->max_key = src->max_key;

  left->count = static_cast<uint8_t>(new_left_count);
  right->count = static_cast<uint8_t>(new_right_count);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename NodeT>
void BTreeIndex<StaticConfig, HasValue, Key, Compare>::scatter(
    LeafNode* left, LeafNode* right, const NodeT* src,
    uint64_t new_left_count) {
  assert(src->count >= new_left_count);
  size_t new_right_count = static_cast<size_t>(src->count) - new_left_count;

  assert(new_right_count != 0);

  if (kUseIndirection) {
    for (size_t i = 0; i < new_left_count; i++) {
      left->indir[i] = static_cast<uint8_t>(i);
      left->keys[i] = src->key(i);
      if (HasValue) left->values[i] = src->value(i);
    }

    for (size_t i = 0; i < new_right_count; i++) {
      right->indir[i] = static_cast<uint8_t>(i);
      right->keys[i] = src->key(new_left_count + i);
      if (HasValue) right->values[i] = src->value(new_left_count + i);
    }
  } else {
    // for (size_t i = 0; i < new_left_count; i++) left->keys[i] = src->keys[i];
    ::mica::util::memcpy(left->keys, src->keys, sizeof(Key) * new_left_count);
    if (HasValue)
      ::mica::util::memcpy(left->values, src->values,
                           sizeof(uint64_t) * new_left_count);

    // for (size_t i = 0; i < new_right_count; i++)
    //   right->keys[i] = src->keys[new_left_count + i];
    ::mica::util::memcpy(right->keys, src->keys + new_left_count,
                         sizeof(Key) * new_right_count);
    if (HasValue)
      ::mica::util::memcpy(right->values, src->values + new_left_count,
                           sizeof(uint64_t) * new_right_count);
  }

  left->min_key = src->min_key;
  left->max_key = src->key(new_left_count);

  right->min_key = src->key(new_left_count);
  right->max_key = src->max_key;

  left->count = static_cast<uint8_t>(new_left_count);
  right->count = static_cast<uint8_t>(new_right_count);
}
}
}

#endif
