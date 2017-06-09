#pragma once
#ifndef MICA_TRANSACTION_BTREE_INDEX_IMPL_NODE_H_
#define MICA_TRANSACTION_BTREE_INDEX_IMPL_NODE_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool HasValue, class Key, class Compare>
typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::InternalNode*
BTreeIndex<StaticConfig, HasValue, Key, Compare>::make_internal_node(
    RowAccessHandle& rah) {
  if (!rah.new_row(idx_tbl_, 0, Transaction::kNewRowID, true, kDataSize))
    return nullptr;

  auto node = reinterpret_cast<InternalNode*>(rah.data());

  node->type = NodeType::kInternal;
  return node;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::LeafNode*
BTreeIndex<StaticConfig, HasValue, Key, Compare>::make_leaf_node(
    RowAccessHandle& rah) {
  if (!rah.new_row(idx_tbl_, 0, Transaction::kNewRowID, true, kDataSize))
    return nullptr;

  auto node = reinterpret_cast<LeafNode*>(rah.data());

  node->type = NodeType::kLeaf;
  return node;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::free_node(
    RowAccessHandle& rah) {
  return rah.read_row(data_copier_) && rah.write_row(0, data_copier_) &&
         rah.delete_row();
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename RowAccessHandleT>
const typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::Node*
BTreeIndex<StaticConfig, HasValue, Key, Compare>::get_node(
    RowAccessHandleT& rah, uint64_t row_id) const {
  if (!rah.peek_row(idx_tbl_, 0, row_id, true, false, false)) {
    // rah.tx()->print_version_chain(idx_tbl_, 0, row_id);
    return nullptr;
  }
  assert(rah.cdata() != nullptr);
  return reinterpret_cast<const Node*>(rah.cdata());
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <bool RightOpen, bool RightExclusive, typename RowAccessHandleT>
const typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::Node*
BTreeIndex<StaticConfig, HasValue, Key, Compare>::get_node_with_fixup(
    RowAccessHandleT& rah, uint64_t row_id, const Key& key) const {
  if (!rah.peek_row(idx_tbl_, 0, row_id, true, false, false)) return nullptr;
  auto node_b = reinterpret_cast<const Node*>(rah.cdata());
  if (is_internal(node_b)) {
    if (!fixup_internal<RightOpen, RightExclusive>(rah, node_b, key))
      return nullptr;
  } else {
    if (!fixup_leaf<RightOpen, RightExclusive>(rah, node_b, key))
      return nullptr;
  }
  return node_b;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::Node*
BTreeIndex<StaticConfig, HasValue, Key, Compare>::get_writable_node(
    RowAccessHandle& rah) {
  if (!rah.read_row(data_copier_) || !rah.write_row(kDataSize, data_copier_))
    return nullptr;
  return reinterpret_cast<Node*>(rah.data());
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename RowAccessHandleT>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::validate_read(
    RowAccessHandleT& rah) {
  return rah.read_row(data_copier_);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename RowAccessHandleT>
void BTreeIndex<StaticConfig, HasValue, Key, Compare>::prefetch_row(
    RowAccessHandleT& rah, uint64_t row_id) const {
  rah.prefetch_row(idx_tbl_, 0, row_id, 0, 0);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
void BTreeIndex<StaticConfig, HasValue, Key, Compare>::prefetch_node(
    const Node* node) const {
  // Prefetching the node content only seems to lower the throughput.
  // auto addr = reinterpret_cast<const char*>(node);
  // auto max_addr = addr + sizeof(LeafNode);
  // for (; addr < max_addr; addr += 64)
  //   __builtin_prefetch(reinterpret_cast<const void*>(addr), 0, 0);
  (void)node;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::is_internal(
    const Node* node) {
  return node->type == NodeType::kInternal;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::is_leaf(
    const Node* node) {
  return node->type == NodeType::kLeaf;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::InternalNode*
BTreeIndex<StaticConfig, HasValue, Key, Compare>::as_internal(Node* node) {
  assert(!node || is_internal(node));
  return reinterpret_cast<InternalNode*>(node);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
const typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::InternalNode*
BTreeIndex<StaticConfig, HasValue, Key, Compare>::as_internal(
    const Node* node) {
  assert(!node || is_internal(node));
  return reinterpret_cast<const InternalNode*>(node);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::LeafNode*
BTreeIndex<StaticConfig, HasValue, Key, Compare>::as_leaf(Node* node) {
  assert(!node || is_leaf(node));
  return reinterpret_cast<LeafNode*>(node);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
const typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::LeafNode*
BTreeIndex<StaticConfig, HasValue, Key, Compare>::as_leaf(const Node* node) {
  assert(!node || is_leaf(node));
  return reinterpret_cast<const LeafNode*>(node);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <bool Exclusive, typename NodeT>
int BTreeIndex<StaticConfig, HasValue, Key, Compare>::search_leftmost(
    const NodeT& node, const Key& key) const {
  // Find the smallest index j such that comp_lt(key, node->key(j)) (for Exclusive == true).
  int left = 0;
  int right = node->count;
  if (kUseBinarySearch)
    while (left + kBinarySearchThreshold <= right) {
      int mid = (left + right) >> 1;
      bool result;
      if (Exclusive)
        result = comp_lt(key, node->key(mid));
      else
        result = comp_le(key, node->key(mid));
      if (result)
        right = mid + 1;  // Inspect the left partition including mid.
      else
        left = mid + 1;  // Inspect the right partition excluding mid.
    }

  if (Exclusive) {
    for (; left < right; left++)
      if (comp_lt(key, node->key(left))) break;
  } else {
    for (; left < right; left++)
      if (comp_le(key, node->key(left))) break;
  }
  return left;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <bool Exclusive, typename NodeT>
int BTreeIndex<StaticConfig, HasValue, Key, Compare>::search_rightmost(
    const NodeT& node, const Key& key) const {
  // Find the largest index j such that comp_lt(node->key(j), key) (for Exclusive == true).
  int left = -1;
  int right = node->count - 1;
  if (kUseBinarySearch)
    while (left + kBinarySearchThreshold <= right) {
      int mid = (left + right + 1) >> 1;
      bool result;
      if (Exclusive)
        result = comp_lt(node->key(mid), key);
      else
        result = comp_le(node->key(mid), key);
      if (result)
        left = mid - 1;  // Inspect the right partition including mid.
      else
        right = mid - 1;  // Inspect the left partition excluding mid.
    }

  if (Exclusive) {
    for (; right > left; right--)
      if (comp_lt(node->key(right), key)) break;
  } else {
    for (; right > left; right--)
      if (comp_le(node->key(right), key)) break;
  }
  return right;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::key_info(
    const Key& key) {
  if (typeid(Key) == typeid(uint64_t))
    return *reinterpret_cast<const uint64_t*>(&key);
  else if (typeid(Key) == typeid(std::pair<uint64_t, uint64_t>))
    return (reinterpret_cast<const std::pair<uint64_t, uint64_t>*>(&key))
        ->first;
  else {
    // TODO: Use << operator instead of forced conversion.
    return 0;
  }
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <class RowAccessHandleT>
void BTreeIndex<StaticConfig, HasValue, Key, Compare>::dump_node(
    RowAccessHandleT& rah, const Node* node_b) {
  char buf[4096];
  char* end = buf + sizeof(buf);
  size_t rem = sizeof(buf);

  if (is_internal(node_b)) {
    auto node = as_internal(node_b);
    rem -= static_cast<size_t>(snprintf(
        end - rem, rem, "InternalNode row_id=%" PRIu64 ": ", rah.row_id()));

    if (node->next == kNullRowID)
      rem -= static_cast<size_t>(snprintf(end - rem, rem, "next=NULL\n"));
    else
      rem -= static_cast<size_t>(
          snprintf(end - rem, rem, "next=%" PRIu64 "\n", node->next));

    rem -= static_cast<size_t>(snprintf(end - rem, rem, "    "));
    rem -= static_cast<size_t>(
        snprintf(end - rem, rem, "%8" PRIu64 "  ", key_info(node->min_key)));
    for (size_t i = 0; i < node->count; i++)
      rem -= static_cast<size_t>(
          snprintf(end - rem, rem, "%8" PRIu64 "  ", key_info(node->key(i))));
    rem -= static_cast<size_t>(
        snprintf(end - rem, rem, "%8" PRIu64 "  ", key_info(node->max_key)));
    rem -= static_cast<size_t>(snprintf(end - rem, rem, "\n"));

    rem -= static_cast<size_t>(snprintf(end - rem, rem, "          "));
    for (size_t i = 0; i <= node->count; i++)
      rem -= static_cast<size_t>(
          snprintf(end - rem, rem, "%8" PRIu64 "  ", node->child_row_id(i)));
    rem -= static_cast<size_t>(snprintf(end - rem, rem, "\n"));
  } else if (is_leaf(node_b)) {
    auto node = as_leaf(node_b);
    rem -= static_cast<size_t>(snprintf(
        end - rem, rem, "LeafNode row_id=%" PRIu64 ": ", rah.row_id()));

    if (node->prev == kNullRowID)
      rem -= static_cast<size_t>(snprintf(end - rem, rem, "prev=NULL "));
    else
      rem -= static_cast<size_t>(
          snprintf(end - rem, rem, "prev=%" PRIu64 " ", node->prev));
    if (node->next == kNullRowID)
      rem -= static_cast<size_t>(snprintf(end - rem, rem, "next=NULL\n"));
    else
      rem -= static_cast<size_t>(
          snprintf(end - rem, rem, "next=%" PRIu64 "\n", node->next));

    rem -= static_cast<size_t>(
        snprintf(end - rem, rem, "%8" PRIu64 "  ", key_info(node->min_key)));
    for (size_t i = 0; i < node->count; i++)
      rem -= static_cast<size_t>(
          snprintf(end - rem, rem, "%8" PRIu64 "  ", key_info(node->key(i))));
    rem -= static_cast<size_t>(
        snprintf(end - rem, rem, "%8" PRIu64 "  ", key_info(node->max_key)));
    rem -= static_cast<size_t>(snprintf(end - rem, rem, "\n"));

    if (HasValue) {
      rem -= static_cast<size_t>(snprintf(end - rem, rem, "          "));
      for (size_t i = 0; i < node->count; i++)
        rem -= static_cast<size_t>(
            snprintf(end - rem, rem, "%8" PRIu64 "  ", node->value(i)));
      rem -= static_cast<size_t>(snprintf(end - rem, rem, "\n"));
    }
  } else
    assert(false);

  printf("%s", buf);
}
}
}
#endif
