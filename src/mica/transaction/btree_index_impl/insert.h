#pragma once
#ifndef MICA_TRANSACTION_BTREE_INDEX_IMPL_INSERT_H_
#define MICA_TRANSACTION_BTREE_INDEX_IMPL_INSERT_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::insert(
    Transaction* tx, const Key& key, uint64_t value) {
  Timing t(tx->context()->timing_stack(), &Stats::index_write);

  if ((kVerbose & VerboseFlag::kInsert))
    printf("BTreeIndex::insert(): key=%" PRIu64 " value=%" PRIu64 "\n",
           key_info(key), value);

  RowAccessHandle rah_head(tx);
  auto head = as_internal(get_node(rah_head, 0));
  if (!head) return kHaveToAbort;

  RowAccessHandle rah_root(tx);
  auto root_b =
      get_node_with_fixup<false, false>(rah_root, head->child_row_id(0), key);
  if (!root_b) return kHaveToAbort;

  Key up_key_from_child{};
  uint64_t up_row_id_from_child = kNullRowID;
  auto ret = insert_recursive(tx, rah_root, root_b, key, value,
                              &up_key_from_child, &up_row_id_from_child);
  if (ret == kHaveToAbort) return kHaveToAbort;

  if (up_row_id_from_child != kNullRowID) {
    if ((kVerbose & VerboseFlag::kInsert))
      printf("BTreeIndex::insert(): key=%" PRIu64
             " up_key from child; up_key = %" PRIu64 " up_row_id = %" PRIu64
             "\n",
             key_info(key), key_info(up_key_from_child), up_row_id_from_child);

    // Replace the root.
    RowAccessHandle rah_head(tx);
    if (!get_node(rah_head, 0)) return kHaveToAbort;
    auto head = as_internal(get_writable_node(rah_head));
    if (!head) return kHaveToAbort;

    RowAccessHandle rah_root(tx);
    auto root = make_internal_node(rah_root);
    if (!root) return kHaveToAbort;

    // Left child is the original root.
    // Right child is the node created by splitting the original root.
    root->count = 1;
    if (kUseIndirection) root->indir[0] = 0;
    root->key(0) = up_key_from_child;
    // Do not use a fixed-up node (previous rah_root and root_b); the root's row ID comes directly from the head node.
    root->child_row_id(0) = head->child_row_id(0);
    root->child_row_id(1) = up_row_id_from_child;
    root->next = kNullRowID;
    root->min_key = Key{};
    root->max_key = Key{};

    if ((kVerbose & VerboseFlag::kInsert)) dump_node(rah_root, root);

    head->count = 0;
    head->child_row_id(0) = rah_root.row_id();

    if ((kVerbose & VerboseFlag::kInsert))
      printf("BTreeIndex::insert(): new root node_row_id=%" PRIu64 "\n",
             head->child_row_id(0));
  }
  return ret;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::insert_recursive(
    Transaction* tx, RowAccessHandle& rah, const Node* node_b, const Key& key,
    uint64_t value, Key* up_key, uint64_t* up_row_id) {
  if ((kVerbose & VerboseFlag::kInsert))
    printf("BTreeIndex::insert_recursive(): key=%" PRIu64
           " node_row_id=%" PRIu64 "\n",
           key_info(key), rah.row_id());

  if (kPrefetchNode) prefetch_node(node_b);

  if ((kVerbose & VerboseFlag::kInsert)) dump_node(rah, node_b);

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

    Key up_key_from_child{};
    uint64_t up_row_id_from_child = kNullRowID;
    ret = insert_recursive(tx, rah_child, child_b, key, value,
                           &up_key_from_child, &up_row_id_from_child);
    if (ret == kHaveToAbort) return kHaveToAbort;

    if (up_row_id_from_child != kNullRowID) {
      if ((kVerbose & VerboseFlag::kInsert))
        printf("BTreeIndex::insert_recursive(): key=%" PRIu64
               " up_key from child; "
               "up_key=%" PRIu64 " up_row_id=%" PRIu64 "\n",
               key_info(key), key_info(up_key_from_child),
               up_row_id_from_child);

      if (!fixup_internal<false, false>(rah, node_b, up_key_from_child))
        return kHaveToAbort;
      node = as_internal(node_b);

      if (!insert_child(tx, rah, node, up_key_from_child, up_row_id_from_child,
                        up_key, up_row_id))
        return kHaveToAbort;
    }
  } else {
    auto node = as_leaf(node_b);

    ret = insert_item(tx, rah, node, key, value, up_key, up_row_id);
  }
  return ret;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::insert_child(
    Transaction* tx, RowAccessHandle& rah, const InternalNode* node_r,
    const Key& key, uint64_t child_row_id, Key* up_key, uint64_t* up_row_id) {
  if ((kVerbose & VerboseFlag::kInsert))
    printf("BTreeIndex::insert_child(): key=%" PRIu64 " node_row_id=%" PRIu64
           "\n",
           key_info(key), rah.row_id());

  // Find the insert position for the split key.
  size_t j = static_cast<size_t>(search_leftmost<true>(node_r, key));

  // Obtain a writable version of node.
  auto node = as_internal(get_writable_node(rah));
  if (!node) return false;
  node_r = node;
  (void)node_r;

  if (node->count == InternalNode::kMaxCount) {
    RowAccessHandle rah_right(tx);
    auto right = make_internal_node(rah_right);
    if (!right) return false;

    size_t new_left_count = (static_cast<size_t>(node->count) - 1) / 2;

    if ((kVerbose & VerboseFlag::kInsert))
      printf("BTreeIndex::insert_child(): key=%" PRIu64 " splitting\n",
             key_info(key));

    if ((kVerbose & VerboseFlag::kInsert)) dump_node(rah, node);

    InternalNodeBuffer buf;
    copy(&buf, node);

    // Insert the new key at j.
    if (kUseIndirection) {
      ::mica::util::memmove(
          buf.indir + j + 1, buf.indir + j,
          sizeof(uint8_t) * (static_cast<size_t>(buf.count) - j));
      buf.indir[j] = buf.count;
    } else {
      // for (size_t i = static_cast<size_t>(buf.count); i > j; i--)
      //   buf.keys[i] = buf.keys[i - 1];
      ::mica::util::memmove(buf.keys + j + 1, buf.keys + j,
                            sizeof(Key) * (static_cast<size_t>(buf.count) - j));
      ::mica::util::memmove(
          buf.child_row_ids + j + 2, buf.child_row_ids + j + 1,
          sizeof(uint64_t) * (static_cast<size_t>(buf.count) - j));
    }
    buf.key(j) = key;
    buf.child_row_id(j + 1) = child_row_id;
    buf.count++;

    // Adjust the left node size if the new child will be on the left node.
    if (j < new_left_count) new_left_count++;

    scatter(node, right, &buf, new_left_count);

    right->next = node->next;
    node->next = rah_right.row_id();

    if ((kVerbose & VerboseFlag::kInsert)) dump_node(rah, node);

    if ((kVerbose & VerboseFlag::kInsert)) dump_node(rah_right, right);

    // The last key of the left node needs to be inserted in the
    // parent node so that the right node can be found.
    // This key will not be used in this node in the future.
    *up_key = node->max_key;
    *up_row_id = rah_right.row_id();
  } else {
    // Insert the new key at j.
    if (kUseIndirection) {
      ::mica::util::memmove(
          node->indir + j + 1, node->indir + j,
          sizeof(uint8_t) * (static_cast<size_t>(node->count) - j));
      node->indir[j] = node->count;
    } else {
      // for (size_t i = static_cast<size_t>(node->count); i > j; i--)
      //   node->keys[i] = node->keys[i - 1];
      ::mica::util::memmove(
          node->keys + j + 1, node->keys + j,
          sizeof(Key) * (static_cast<size_t>(node->count) - j));
      ::mica::util::memmove(
          node->child_row_ids + j + 2, node->child_row_ids + j + 1,
          sizeof(uint64_t) * (static_cast<size_t>(node->count) - j));
    }
    node->key(j) = key;
    node->child_row_id(j + 1) = child_row_id;
    node->count++;

    if ((kVerbose & VerboseFlag::kInsert)) dump_node(rah, node);
  }
  return true;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::insert_item(
    Transaction* tx, RowAccessHandle& rah, const LeafNode* node_r,
    const Key& key, uint64_t value, Key* up_key, uint64_t* up_row_id) {
  if ((kVerbose & VerboseFlag::kInsert))
    printf("BTreeIndex::insert_item(): key=%" PRIu64 " node_row_id=%" PRIu64
           "\n",
           key_info(key), rah.row_id());

  // Validate the leaf node because we may read it but not write to it.
  if (!validate_read(rah)) return kHaveToAbort;

  size_t j = static_cast<size_t>(search_leftmost<false>(node_r, key));

  if (j != node_r->count && comp_ge(key, node_r->key(j))) {
    // i.e., node_r->key(j) == key
    assert(comp_eq(node_r->key(j), key));
    return 0;
  }

  // Obtain a writable version of node.
  auto node = as_leaf(get_writable_node(rah));
  if (!node) return kHaveToAbort;
  node_r = node;

  if ((kVerbose & VerboseFlag::kInsert))
    printf("BTreeIndex::insert_item(): key=%" PRIu64 " node->count=%" PRIu8
           "\n",
           key_info(key), node->count);

  if (node->count == LeafNode::kMaxCount) {
    // Split the full leaf node.
    RowAccessHandle rah_right(tx);
    auto right = make_leaf_node(rah_right);
    if (!right) return kHaveToAbort;

    size_t new_left_count = node->count / 2;

    if ((kVerbose & VerboseFlag::kInsert))
      printf("BTreeIndex::insert_item(): key=%" PRIu64 " splitting\n",
             key_info(key));

    LeafNodeBuffer buf;
    copy(&buf, node);

    // Insert the new key at j.
    if (kUseIndirection) {
      ::mica::util::memmove(
          buf.indir + j + 1, buf.indir + j,
          sizeof(uint8_t) * (static_cast<size_t>(buf.count) - j));
      buf.indir[j] = buf.count;
    } else {
      // for (size_t i = static_cast<size_t>(buf.count); i > j; i--)
      //   buf.keys[i] = buf.keys[i - 1];
      ::mica::util::memmove(buf.keys + j + 1, buf.keys + j,
                            sizeof(Key) * (static_cast<size_t>(buf.count) - j));
      if (HasValue)
        ::mica::util::memmove(
            buf.values + j + 1, buf.values + j,
            sizeof(uint64_t) * (static_cast<size_t>(buf.count) - j));
    }
    buf.key(j) = key;
    if (HasValue) buf.value(j) = value;
    buf.count++;

    // Adjust the left node size if the new child will be on the left node.
    if (j < new_left_count) new_left_count++;

    scatter(node, right, &buf, new_left_count);

    // Fix the doubly-linked list.
    right->next = node->next;
    node->next = rah_right.row_id();

    right->prev = rah.row_id();
    if (right->next != kNullRowID) {
      RowAccessHandle rah_right_right(tx);
      if (!get_node(rah_right_right, right->next)) return kHaveToAbort;
      auto right_right = as_leaf(get_writable_node(rah_right_right));
      if (!right_right) return kHaveToAbort;
      if (right_right->prev != rah.row_id()) return kHaveToAbort;
      right_right->prev = node->next;  // == rah_right.row_id();
    }

    if ((kVerbose & VerboseFlag::kInsert)) dump_node(rah, node);

    if ((kVerbose & VerboseFlag::kInsert)) dump_node(rah_right, right);

    // The first key of the (new) right node needs to be inserted in the
    // parent node.
    *up_key = right->key(0);
    *up_row_id = rah_right.row_id();
  } else {
    // Insert the new key at j.
    if (kUseIndirection) {
      ::mica::util::memmove(
          node->indir + j + 1, node->indir + j,
          sizeof(uint8_t) * (static_cast<size_t>(node->count) - j));
      node->indir[j] = node->count;
    } else {
      // for (size_t i = static_cast<size_t>(node->count); i > j; i--)
      //   node->keys[i] = node->keys[i - 1];
      ::mica::util::memmove(
          node->keys + j + 1, node->keys + j,
          sizeof(Key) * (static_cast<size_t>(node->count) - j));
      if (HasValue)
        ::mica::util::memmove(
            node->values + j + 1, node->values + j,
            sizeof(uint64_t) * (static_cast<size_t>(node->count) - j));
    }
    node->key(j) = key;
    if (HasValue) node->value(j) = value;
    node->count++;

    if ((kVerbose & VerboseFlag::kInsert)) dump_node(rah, node);
  }
  return 1;
}
}
}

#endif
