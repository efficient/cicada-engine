#pragma once
#ifndef MICA_TRANSACTION_BTREE_INDEX_IMPL_REMOVE_H_
#define MICA_TRANSACTION_BTREE_INDEX_IMPL_REMOVE_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::remove(
    Transaction* tx, const Key& key, uint64_t value) {
  Timing t(tx->context()->timing_stack(), &Stats::index_write);

  if ((kVerbose & VerboseFlag::kRemove))
    printf("BTreeIndex::remove(): key=%" PRIu64 " value=%" PRIu64 "\n",
           key_info(key), value);

  RowAccessHandle rah_head(tx);
  auto head = as_internal(get_node(rah_head, 0));
  if (!head) return kHaveToAbort;

  RowAccessHandle rah_root(tx);
  auto root_b =
      get_node_with_fixup<false, false>(rah_root, head->child_row_id(0), key);
  if (!root_b) return kHaveToAbort;

  bool up_rebalancing_from_child = false;
  auto ret = remove_recursive(tx, rah_root, root_b, key, value,
                              &up_rebalancing_from_child);
  if (ret == kHaveToAbort) return kHaveToAbort;

  if (up_rebalancing_from_child) {
    // Get the root node without fixup because we need to ensure that this root node is consistent (i.e., a single node in the level).
    RowAccessHandle rah_root(tx);
    auto root_b = get_node(rah_root, head->child_row_id(0));
    if (!root_b) return kHaveToAbort;

    // Validate the root because we are going to read it but not write to it.
    if (!validate_read(rah_root)) return kHaveToAbort;

    // Collapse [head - root - child] into [head - child].
    if (is_internal(root_b)) {
      auto root = as_internal(root_b);
      if (root->count == 0) {
        // The root has a next node, which means that this node is not a real root node and it is unsafe to collapse.
        if (root->next != kNullRowID) return kHaveToAbort;

        if ((kVerbose & VerboseFlag::kRemove))
          printf("BTreeIndex::remove(): key=%" PRIu64 " collapsing root\n",
                 key_info(key));

        auto head = as_internal(get_writable_node(rah_head));
        if (!head) return kHaveToAbort;

        head->child_row_id(0) = root->child_row_id(0);

        if (!free_node(rah_root)) return kHaveToAbort;
      } else {
        if ((kVerbose & VerboseFlag::kRemove))
          printf("BTreeIndex::remove(): key=%" PRIu64
                 " ignoring rebalancing from root\n",
                 key_info(key));
      }
    }

    if ((kVerbose & VerboseFlag::kRemove))
      printf("BTreeIndex::remove(): new root row_id=%" PRIu64 "\n",
             head->child_row_id(0));
  }

  return ret;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::remove_recursive(
    Transaction* tx, RowAccessHandle& rah, const Node* node_b, const Key& key,
    uint64_t value, bool* up_rebalancing) {
  if ((kVerbose & VerboseFlag::kRemove))
    printf("BTreeIndex::remove_recursive(): key=%" PRIu64
           " node_row_id=%" PRIu64 "\n",
           key_info(key), rah.row_id());

  if (kPrefetchNode) prefetch_node(node_b);

  if ((kVerbose & VerboseFlag::kRemove)) dump_node(rah, node_b);

  uint64_t ret;
  if (is_internal(node_b)) {
    auto node = as_internal(node_b);

    size_t j = static_cast<size_t>(search_leftmost<true>(node, key));

    auto child_row_id = node->child_row_id(j);

    RowAccessHandle rah_child(tx);
    auto child_b =
        get_node_with_fixup<false, false>(rah_child, child_row_id, key);
    if (!child_b) return kHaveToAbort;
    child_row_id = rah_child.row_id();

    bool up_rebalancing_from_child = false;
    ret = remove_recursive(tx, rah_child, child_b, key, value,
                           &up_rebalancing_from_child);
    if (ret == kHaveToAbort) return kHaveToAbort;

    if (up_rebalancing_from_child) {
      if ((kVerbose & VerboseFlag::kRemove))
        printf("BTreeIndex::remove_recursive(): key=%" PRIu64
               " rebalancing request from child\n",
               key_info(key));

      if (!rebalance(tx, rah, node, child_row_id, up_rebalancing))
        return kHaveToAbort;
    }
  } else {
    auto node = as_leaf(node_b);

    ret = remove_item(tx, rah, node, key, value, up_rebalancing);
  }
  return ret;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::rebalance(
    Transaction* tx, RowAccessHandle& rah_parent, const InternalNode* parent_r,
    uint64_t child_row_id, bool* up_rebalancing) {
  size_t j;
  for (j = 0; j < static_cast<size_t>(parent_r->count) + 1; j++) {
    if (parent_r->child_row_id(j) == child_row_id) break;
  }

  // Could not found the child node.
  if (j == static_cast<size_t>(parent_r->count) + 1) return false;

  // Choose the child's sibling.
  size_t k;
  if (j != 0) {
    // Choose the left sibling.
    k = j;
    j--;
  } else {
    // Choose the right sibling.
    k = j + 1;
  }
  assert(j + 1 == k);

  // This node has only one child.  We are probably seeing an inconsistent state.
  if (k >= static_cast<size_t>(parent_r->count) + 1) return false;

  auto parent = as_internal(get_writable_node(rah_parent));
  if (!parent) return false;
  parent_r = parent;

  // Get a writable version of the left node.
  RowAccessHandle rah_left(tx);
  if (!get_node(rah_left, parent->child_row_id(j))) return false;
  auto left_b = get_writable_node(rah_left);
  if (!left_b) return false;

  // We do not get a writable version immediately because we might delete the right node, where we can optimize the write to have an empty version.
  RowAccessHandle rah_right(tx);
  auto right_b = get_node(rah_right, parent->child_row_id(k));
  if (!right_b) return false;

  if ((kVerbose & VerboseFlag::kRemove)) {
    printf("BTreeIndex::rebalance(): parent_row_id=%" PRIu64
           " left_row_id=%" PRIu64 " right_row_id=%" PRIu64 "\n",
           rah_parent.row_id(), rah_left.row_id(), rah_right.row_id());
    dump_node(rah_parent, parent);
    dump_node(rah_left, left_b);
    dump_node(rah_right, right_b);
  }

  bool merge;
  const Key* left_max_key;

  if (is_internal(left_b)) {
    auto left = as_internal(left_b);
    auto right = as_internal(right_b);

    // There must be no new node between two.
    if (left->next != rah_right.row_id()) return false;

    // Gather both node's data (omit copying data that will not move).
    merge = static_cast<size_t>(left->count) + 1 +
                static_cast<size_t>(right->count) <=
            InternalNode::kMaxCount;
    if (merge) {
      // Merge the right node into the left node.
      gather(left, left, right);

      left->next = right->next;

      // Delete the right node that has been merged to the left node.
      if (!free_node(rah_right)) return false;
    } else {
      auto right_b = get_writable_node(rah_right);
      if (!right_b) return false;
      auto right = as_internal(right_b);

      // Redistribute keys with the child's sibling node.
      size_t new_left_count = (static_cast<size_t>(left->count) + 1 +
                               static_cast<size_t>(right->count)) /
                                  2 -
                              1;
      InternalNodeBuffer buf;
      gather(&buf, left, right);
      scatter(left, right, &buf, new_left_count);
    }

    left_max_key = &left->max_key;
  } else {
    auto left = as_leaf(left_b);
    auto right = as_leaf(right_b);

    // There must be no new node between two.
    if (left->next != rah_right.row_id()) return false;
    // This checking is not required, but let's do it for completeness.
    if (right->prev != rah_left.row_id()) return false;

    merge =
        static_cast<size_t>(left->count) + static_cast<size_t>(right->count) <=
        LeafNode::kMaxCount;

    if (merge) {
      // Merge nodes.
      gather(left, left, right);

      left->next = right->next;

      // Fix the right node's right node's prev pointer.
      if (right->next != kNullRowID) {
        RowAccessHandle rah_right_right(tx);
        if (!get_node(rah_right_right, right->next)) return false;
        auto right_right = as_leaf(get_writable_node(rah_right_right));
        if (!right_right) return false;
        if (right_right->prev != rah_right.row_id()) return false;
        right_right->prev = rah_left.row_id();
      }

      // Delete the right node that has been merged to the left node.
      if (!free_node(rah_right)) return false;
    } else {
      auto right_b = get_writable_node(rah_right);
      if (!right_b) return false;
      auto right = as_leaf(right_b);

      // Redistribute keys with the child's sibling node.
      size_t new_left_count = (static_cast<size_t>(left->count) +
                               static_cast<size_t>(right->count)) /
                              2;

      LeafNodeBuffer buf;
      gather(&buf, left, right);
      scatter(left, right, &buf, new_left_count);
    }

    left_max_key = &left->max_key;
  }

  if (merge) {
    // Remove the split key from the parent node.
    assert(parent->count > 0);
    if (kUseIndirection) {
      if (parent->indir[j] != static_cast<size_t>(parent->count) - 1) {
        size_t fill_in;
        for (fill_in = 0; fill_in < parent->count; fill_in++) {
          if (parent->indir[fill_in] == parent->count - 1) break;
        }
        parent->keys[parent->indir[j]] = parent->keys[parent->count - 1];
        parent->child_row_ids[parent->indir[j] + 1] =
            parent->child_row_ids[parent->count - 1 + 1];
        parent->indir[fill_in] = parent->indir[j];
      }
      if (j != static_cast<size_t>(parent->count) - 1)
        ::mica::util::memmove(
            parent->indir + j, parent->indir + j + 1,
            sizeof(uint8_t) * (static_cast<size_t>(parent->count) - j - 1));
    } else {
      // Since we have merged child[j] and child[k],
      // we remove the split key between two (i.e., key[j]) and
      // the right child row ID (i.e., child_row_ids[k] == child_row_ids[j + 1]).
      // for (size_t i = j; i < static_cast<size_t>(parent->count) - 1; i++)
      //   parent->keys[i] = parent->keys[i + 1];
      ::mica::util::memmove(
          parent->keys + j, parent->keys + j + 1,
          sizeof(Key) * (static_cast<size_t>(parent->count) - j - 1));
      ::mica::util::memmove(
          parent->child_row_ids + j + 1, parent->child_row_ids + j + 2,
          sizeof(uint64_t) * (static_cast<size_t>(parent->count) - j - 1));
    }
    parent->count--;

    // This merge has made the parent node have too few keys.
    // Perform rebalancing in the grandparent node recursively.
    if (parent->count < (InternalNode::kMaxCount) / 4) *up_rebalancing = true;

    if ((kVerbose & VerboseFlag::kRemove)) dump_node(rah_parent, parent);

    if ((kVerbose & VerboseFlag::kRemove)) dump_node(rah_left, left_b);
  } else {
    // Fix the split key.
    parent->key(j) = *left_max_key;

    if ((kVerbose & VerboseFlag::kRemove)) dump_node(rah_parent, parent);

    if ((kVerbose & VerboseFlag::kRemove)) dump_node(rah_left, left_b);

    if ((kVerbose & VerboseFlag::kRemove)) dump_node(rah_right, right_b);
  }

  return true;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::remove_item(
    Transaction* tx, RowAccessHandle& rah, const LeafNode* node_r,
    const Key& key, uint64_t value, bool* up_rebalancing) {
  (void)tx;

  if ((kVerbose & VerboseFlag::kRemove))
    printf("BTreeIndex::remove_item(): key=%" PRIu64 " node_row_id=%" PRIu64
           "\n",
           key_info(key), rah.row_id());

  // Validate the leaf node because we may read it but not write to it.
  if (!validate_read(rah)) return kHaveToAbort;

  size_t j = static_cast<size_t>(search_leftmost<false>(node_r, key));

  if (j == node_r->count || comp_lt(key, node_r->key(j))) {
    // The key does not exist.
    assert(j == node_r->count || comp_ne(key, node_r->key(j)));
    if ((kVerbose & VerboseFlag::kRemove))
      printf("BTreeIndex::remove_item(): key=%" PRIu64 " does not exist\n",
             key_info(key));
    return 0;
  }

  if (HasValue && value != node_r->value(j)) {
    // The value does not match.
    return 0;
  }

  // Obtain a writable version of node.
  auto node = as_leaf(get_writable_node(rah));
  if (!node) return kHaveToAbort;
  node_r = node;

  // Remove the existing key at j.
  if (kUseIndirection) {
    if (node->indir[j] != static_cast<size_t>(node->count) - 1) {
      size_t fill_in;
      for (fill_in = 0; fill_in < node->count; fill_in++) {
        if (node->indir[fill_in] == node->count - 1) break;
      }
      node->keys[node->indir[j]] = node->keys[node->count - 1];
      if (HasValue)
        node->values[node->indir[j]] = node->values[node->count - 1];
      node->indir[fill_in] = node->indir[j];
    }
    if (j != static_cast<size_t>(node->count) - 1)
      ::mica::util::memmove(
          node->indir + j, node->indir + j + 1,
          sizeof(uint8_t) * (static_cast<size_t>(node->count) - j - 1));
  } else {
    // for (size_t i = j; i < static_cast<size_t>(node->count) - 1; i++)
    //   node->keys[i] = node->keys[i + 1];
    ::mica::util::memmove(
        node->keys + j, node->keys + j + 1,
        sizeof(Key) * (static_cast<size_t>(node->count) - j - 1));
    if (HasValue)
      ::mica::util::memmove(
          node->values + j, node->values + j + 1,
          sizeof(uint64_t) * (static_cast<size_t>(node->count) - j - 1));
  }
  node->count--;

  if ((kVerbose & VerboseFlag::kRemove)) dump_node(rah, node);

  if ((kVerbose & VerboseFlag::kRemove))
    printf("BTreeIndex::remove_item(): key=%" PRIu64 " node->count=%" PRIu8
           "\n",
           key_info(key), node->count);

  if (node->count < LeafNode::kMaxCount / 4) *up_rebalancing = true;

  return 1;
}
}
}

#endif
