#pragma once
#ifndef MICA_TRANSACTION_BTREE_INDEX_IMPL_FIXUP_H_
#define MICA_TRANSACTION_BTREE_INDEX_IMPL_FIXUP_H_

namespace mica {
namespace transaction {
// We exploit the fact that a leaf node's min key never becomes smaller;
// we only need to take the next pointer as in B-link-tree.

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <bool RightOpen, bool RightExclusive, typename RowAccessHandleT>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::fixup_internal(
    RowAccessHandleT& rah, const Node*& node_b, const Key& key) const {
  auto node = as_internal(node_b);

  if (RightOpen) (void)key;

  // Ensure key < node->max_key.
  uint64_t fixup_len = 0;
  while (node->next != kNullRowID) {
    if (RightOpen)
      ;
    else if (RightExclusive) {
      if (comp_le(key, node->max_key)) break;
    } else {
      if (comp_lt(key, node->max_key)) break;
    }

    if (kPrefetchNode) prefetch_row(rah, node->next);

    rah.reset();
    node_b = get_node(rah, node->next);
    if (!node_b) return false;

    node = as_internal(node_b);

    if ((kVerbose & VerboseFlag::kFixup)) fixup_len++;
  }

  if (!comp_le(node->min_key, key)) return false;

  if ((kVerbose & VerboseFlag::kFixup))
    if (fixup_len > 100)
      printf("BTreeIndex::fixup_internal(): key=%" PRIu64 " fixup_len=%" PRIu64
             "\n",
             key_info(key), fixup_len);

  return true;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <bool RightOpen, bool RightExclusive, typename RowAccessHandleT>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::fixup_leaf(
    RowAccessHandleT& rah, const Node*& node_b, const Key& key) const {
  auto node = as_leaf(node_b);

  // Ensure key < node->max_key.
  uint64_t fixup_len = 0;

  if (!RightOpen) {
    while (node->prev != kNullRowID) {
      if (RightExclusive) {
        if (comp_lt(node->min_key, key)) break;
      } else {
        if (comp_le(node->min_key, key)) break;
      }

      if (kPrefetchNode) prefetch_row(rah, node->prev);

      rah.reset();
      node_b = get_node(rah, node->prev);
      if (!node_b) return false;

      node = as_leaf(node_b);

      if ((kVerbose & VerboseFlag::kFixup)) fixup_len++;
    }
  }

  while (node->next != kNullRowID) {
    if (RightOpen)
      ;
    else if (RightExclusive) {
      if (comp_le(key, node->max_key)) break;
    } else {
      if (comp_lt(key, node->max_key)) break;
    }

    if (kPrefetchNode) prefetch_row(rah, node->next);

    rah.reset();
    node_b = get_node(rah, node->next);
    if (!node_b) return false;

    node = as_leaf(node_b);

    if ((kVerbose & VerboseFlag::kFixup)) fixup_len++;
  }

  if (!comp_le(node->min_key, key)) return false;

  if ((kVerbose & VerboseFlag::kFixup))
    if (fixup_len > 100)
      printf("BTreeIndex::fixup_leaf(): key=%" PRIu64 " fixup_len=%" PRIu64
             "\n",
             key_info(key), fixup_len);

  return true;
}
}
}

#endif
