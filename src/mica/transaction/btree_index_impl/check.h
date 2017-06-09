#pragma once
#ifndef MICA_TRANSACTION_BTREE_INDEX_IMPL_CHECK_H_
#define MICA_TRANSACTION_BTREE_INDEX_IMPL_CHECK_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::check(
    Transaction* tx) const {
  RowAccessHandlePeekOnly rah(tx);
  auto head = as_internal(get_node(rah, 0));
  if (!head) {
    printf("BTreeIndex::check(): invalid head\n");
    // This happens when the index has been just initialized.
    // We accept this type of errors.
    return true;
  }

  RowAccessHandlePeekOnly rah_root(tx);
  auto root = get_node(rah_root, head->child_row_id(0));
  if (!root) {
    printf("BTreeIndex::check(): invalid root\n");
    return false;
  }

  return check_recursive(tx, rah_root, root, true, Key{}, Key{});
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::check_recursive(
    Transaction* tx, RowAccessHandlePeekOnly& rah, const Node* node_b,
    bool is_root, const Key& expected_min_key,
    const Key& expected_max_key) const {
  auto dump = [tx, this](auto& rah, auto& node) {
    dump_node(rah, node);
    if (is_internal(node) && as_internal(node)->next != kNullRowID) {
      RowAccessHandlePeekOnly rah_next(tx);
      auto next_b = get_node(rah_next, as_internal(node)->next);
      if (next_b) dump_node(rah_next, next_b);
    } else if (is_leaf(node) && as_leaf(node)->next != kNullRowID) {
      RowAccessHandlePeekOnly rah_next(tx);
      auto next_b = get_node(rah_next, as_leaf(node)->next);
      if (next_b) dump_node(rah_next, next_b);
    }
    if (is_leaf(node) && as_leaf(node)->prev != kNullRowID) {
      RowAccessHandlePeekOnly rah_prev(tx);
      auto prev_b = get_node(rah_prev, as_leaf(node)->prev);
      if (prev_b) dump_node(rah_prev, prev_b);
    }
    printf("\n");
  };

  if (is_internal(node_b)) {
    auto node = as_internal(node_b);

    // Check root.
    if (is_root) {
      if (!comp_eq(node->max_key, Key{})) {
        printf("BTreeIndex::check(): node_row_id=%" PRIu64
               ": invalid root->max_key(%" PRIu64 ")\n",
               rah.row_id(), key_info(node->max_key));
        dump(rah, node);
        return false;
      }
      if (node->next != kNullRowID) {
        printf("BTreeIndex::check(): node_row_id=%" PRIu64
               ": invalid root->next(%" PRIu64 ")\n",
               rah.row_id(), node->next);
        dump(rah, node);
        return false;
      }
    }

    // Check count.
    if (node->count < InternalNode::kMaxCount / 4 && !is_root) {
      printf("BTreeIndex::check(): node_row_id=%" PRIu64
             ": too small count: %" PRIu8 "\n",
             rah.row_id(), node->count);
      dump(rah, node);
      return false;
    }
    if (node->count > InternalNode::kMaxCount) {
      printf("BTreeIndex::check(): node_row_id=%" PRIu64
             ": too high count: %" PRIu8 "\n",
             rah.row_id(), node->count);
      dump(rah, node);
      return false;
    }

    // Check keys.
    if (!comp_eq(expected_min_key, node->min_key)) {
      printf("BTreeIndex::check(): node_row_id=%" PRIu64
             ": invalid key: min_key != expected_min_key(%" PRIu64 ")\n",
             rah.row_id(), key_info(expected_min_key));
      dump(rah, node);
      return false;
    }
    if (!comp_eq(expected_max_key, node->max_key)) {
      printf("BTreeIndex::check(): node_row_id=%" PRIu64
             ": invalid key: max_key != expected_max_key(%" PRIu64 ")\n",
             rah.row_id(), key_info(expected_max_key));
      dump(rah, node);
      return false;
    }

    if (node->count >= 1) {
      for (size_t i = 0; i < static_cast<size_t>(node->count) - 1; i++) {
        if (!comp_lt(node->key(i), node->key(i + 1))) {
          printf("BTreeIndex::check(): node_row_id=%" PRIu64
                 ": invalid key: keys[%zu] >= keys[%zu]\n",
                 rah.row_id(), i, i + 1);
          dump(rah, node);
          return false;
        }
      }
      // if (node->prev != kNullRowID && !comp_le(node->min_key, node->key(0))) {
      //   printf("BTreeIndex::check(): node_row_id=%" PRIu64
      //          ": invalid key: keys[0] < min_key\n",
      //          rah.row_id());
      //   dump(rah, node);
      //   return false;
      // }
      if (node->next != kNullRowID &&
          !comp_lt(node->key(node->count - 1), node->max_key)) {
        printf("BTreeIndex::check(): node_row_id=%" PRIu64
               ": invalid key: keys[%" PRIu8 "] >= max_key\n",
               rah.row_id(), node->count - 1);
        dump(rah, node);
        return false;
      }
    }

    for (size_t i = 0; i < static_cast<size_t>(node->count) + 1; i++) {
      RowAccessHandlePeekOnly rah_child(tx);
      auto child_row_id = node->child_row_id(i);
      auto child = get_node(rah_child, child_row_id);
      if (!child) {
        printf("BTreeIndex::check(): node_row_id=%" PRIu64
               ": cannot get child: child_row_id=%" PRIu64 "\n",
               rah.row_id(), child_row_id);
        dump(rah, node);
        return false;
      }

      // Check children's pointers.
      if (is_internal(child)) {
        if (i < node->count &&
            as_internal(child)->next != node->child_row_id(i + 1)) {
          printf("BTreeIndex::check(): node_row_id=%" PRIu64
                 ": invalid next: child[%zu]->next(%" PRIu64
                 ") != child[%zu]\n",
                 rah.row_id(), i, as_internal(child)->next, i + 1);
          dump(rah, node);
          return false;
        }
      } else if (is_leaf(child)) {
        if (i < node->count &&
            as_leaf(child)->next != node->child_row_id(i + 1)) {
          printf("BTreeIndex::check(): node_row_id=%" PRIu64
                 ": invalid next: child[%zu]->next(%" PRIu64
                 ") != child[%zu]\n",
                 rah.row_id(), i, as_leaf(child)->next, i + 1);
          dump(rah, node);
          return false;
        }
        if (i > 0 && as_leaf(child)->prev != node->child_row_id(i - 1)) {
          printf("BTreeIndex::check(): node_row_id=%" PRIu64
                 ": invalid prev: child[%zu]->prev(%" PRIu64
                 ") != child[%zu]\n",
                 rah.row_id(), i, as_leaf(child)->prev, i + 1);
          dump(rah, node);
          return false;
        }
      }

      // Check the child recursively.
      const Key* child_expected_min_key;
      const Key* child_expected_max_key;
      if (i == 0)
        child_expected_min_key = &expected_min_key;
      else
        child_expected_min_key = &node->key(i - 1);
      if (i == node->count)
        child_expected_max_key = &expected_max_key;
      else
        child_expected_max_key = &node->key(i);

      if (!check_recursive(tx, rah_child, child, false, *child_expected_min_key,
                           *child_expected_max_key))
        return false;
    }
  } else if (is_leaf(node_b)) {
    auto node = as_leaf(node_b);

    // Check root.
    if (is_root) {
      if (!comp_eq(node->max_key, Key{})) {
        printf("BTreeIndex::check(): node_row_id=%" PRIu64
               ": invalid root->max_key(%" PRIu64 ")\n",
               rah.row_id(), key_info(node->max_key));
        dump(rah, node);
        return false;
      }
      if (node->next != kNullRowID) {
        printf("BTreeIndex::check(): node_row_id=%" PRIu64
               ": invalid root->next(%" PRIu64 ")\n",
               rah.row_id(), node->next);
        dump(rah, node);
        return false;
      }
      if (node->prev != kNullRowID) {
        printf("BTreeIndex::check(): node_row_id=%" PRIu64
               ": invalid root->prev(%" PRIu64 ")\n",
               rah.row_id(), node->prev);
        dump(rah, node);
        return false;
      }
    }

    // Check count.
    if (node->count < LeafNode::kMaxCount / 4 && !is_root) {
      printf("BTreeIndex::check(): node_row_id=%" PRIu64
             ": too small count: %" PRIu8 "\n",
             rah.row_id(), node->count);
      dump(rah, node);
      return false;
    }
    if (node->count > LeafNode::kMaxCount) {
      printf("BTreeIndex::check(): node_row_id=%" PRIu64
             ": too high count: %" PRIu8 "\n",
             rah.row_id(), node->count);
      dump(rah, node);
      return false;
    }

    // Check keys.
    if (!comp_eq(expected_min_key, node->min_key)) {
      printf("BTreeIndex::check(): node_row_id=%" PRIu64
             ": invalid key: min_key != expected_min_key(%" PRIu64 ")\n",
             rah.row_id(), key_info(expected_min_key));
      dump(rah, node);
      return false;
    }
    if (!comp_eq(expected_max_key, node->max_key)) {
      printf("BTreeIndex::check(): node_row_id=%" PRIu64
             ": invalid key: max_key != expected_max_key(%" PRIu64 ")\n",
             rah.row_id(), key_info(expected_max_key));
      dump(rah, node);
      return false;
    }

    if (node->count >= 1) {
      for (size_t i = 0; i < static_cast<size_t>(node->count) - 1; i++) {
        if (!comp_lt(node->key(i), node->key(i + 1))) {
          printf("BTreeIndex::check(): node_row_id=%" PRIu64
                 ": invalid key: keys[%zu] >= keys[%zu]\n",
                 rah.row_id(), i, i + 1);
          dump(rah, node);
          return false;
        }
      }
      if (node->prev != kNullRowID && !comp_le(node->min_key, node->key(0))) {
        printf("BTreeIndex::check(): node_row_id=%" PRIu64
               ": invalid key: keys[0] < min_key\n",
               rah.row_id());
        dump(rah, node);
        return false;
      }
      if (node->next != kNullRowID &&
          !comp_lt(node->key(static_cast<size_t>(node->count) - 1),
                   node->max_key)) {
        printf("BTreeIndex::check(): node_row_id=%" PRIu64
               ": invalid key: keys[%zu] >= max_key\n",
               rah.row_id(), static_cast<size_t>(node->count) - 1);
        dump(rah, node);
        return false;
      }
    }
  } else {
    printf("BTreeIndex::check(): node_row_id=%" PRIu64 ": invalid node type\n",
           rah.row_id());
    return false;
  }
  return true;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::dump_tree(
    Transaction* tx) const {
  RowAccessHandlePeekOnly rah(tx);
  auto head = as_internal(get_node(rah, 0));
  if (!head) {
    printf("BTreeIndex::dump_tree(): invalid head\n");
    return false;
  }

  RowAccessHandlePeekOnly rah_root(tx);
  auto root = get_node(rah_root, head->child_row_id(0));
  if (!root) {
    printf("BTreeIndex::dump_tree(): invalid root\n");
    return false;
  }

  return dump_tree_recursive(tx, rah_root, root);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::dump_tree_recursive(
    Transaction* tx, RowAccessHandlePeekOnly& rah, const Node* node_b) const {
  dump_node(rah, node_b);
  printf("\n");

  if (is_internal(node_b)) {
    auto node = as_internal(node_b);

    for (size_t i = 0; i < static_cast<size_t>(node->count) + 1; i++) {
      RowAccessHandlePeekOnly rah_child(tx);
      auto child_row_id = node->child_row_id(i);
      auto child = get_node(rah_child, child_row_id);
      if (!child) {
        printf("BTreeIndex::dump_tree(): node_row_id=%" PRIu64
               ": cannot get child: child_row_id=%" PRIu64 "\n",
               rah.row_id(), child_row_id);
        // return false;
      } else if (!dump_tree_recursive(tx, rah_child, child))
        return false;
    }
  } else if (is_leaf(node_b)) {
    // Nothing to recursive into.
  } else {
    printf("BTreeIndex::dump_tree(): node_row_id=%" PRIu64
           ": invalid node type\n",
           rah.row_id());
    // return false;
  }
  return true;
}
}
}

#endif
