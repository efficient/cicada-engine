#pragma once
#ifndef MICA_TRANSACTION_BTREE_INDEX_IMPL_LOOKUP_H_
#define MICA_TRANSACTION_BTREE_INDEX_IMPL_LOOKUP_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename Func>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::lookup(
    Transaction* tx, const Key& key, bool skip_validation, const Func& func) {
  return lookup<BTreeRangeType::kInclusive, BTreeRangeType::kInclusive, false>(
      tx, key, key, skip_validation, func);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <BTreeRangeType LeftRangeType, BTreeRangeType RightRangeType,
          bool Reversed, typename Func>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::lookup(
    Transaction* tx, const Key& min_key, const Key& max_key,
    bool skip_validation, const Func& func) {
  Timing t(tx->context()->timing_stack(), &Stats::index_read);
  if ((kVerbose & VerboseFlag::kLookup))
    printf("BTreeIndex::lookup(): min_key=%" PRIu64 " max_key=%" PRIu64 "\n",
           key_info(min_key), key_info(max_key));

  if (skip_validation) {
    RowAccessHandlePeekOnly rah(tx);
    auto head = as_internal(get_node(rah, 0));
    if (!head) return kHaveToAbort;

    return lookup_recursive<LeftRangeType, RightRangeType, Reversed>(
        tx, rah, head, min_key, max_key, skip_validation, func);
  } else {
    RowAccessHandle rah(tx);
    auto head = as_internal(get_node(rah, 0));
    if (!head) return kHaveToAbort;

    return lookup_recursive<LeftRangeType, RightRangeType, Reversed>(
        tx, rah, head, min_key, max_key, skip_validation, func);
  }
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <BTreeRangeType LeftRangeType, BTreeRangeType RightRangeType,
          bool Reversed, typename Func, typename RowAccessHandleT>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::lookup_recursive(
    Transaction* tx, RowAccessHandleT& rah, const Node* node_b,
    const Key& min_key, const Key& max_key, bool skip_validation,
    const Func& func) {
  if ((kVerbose & VerboseFlag::kLookup))
    printf("BTreeIndex::lookup_recursive(): min_key=%" PRIu64
           " max_key=%" PRIu64 " node_row_id=%" PRIu64 "\n",
           key_info(min_key), key_info(max_key), rah.row_id());

  if (kPrefetchNode) prefetch_node(node_b);

  if ((kVerbose & VerboseFlag::kLookup)) dump_node(rah, node_b);

  if (is_internal(node_b)) {
    auto node = as_internal(node_b);

    // Search for the first matching child.
    int j;
    if (!Reversed) {
      if (LeftRangeType == BTreeRangeType::kOpen)
        j = 0;
      else
        j = search_leftmost<true>(node, min_key);
    } else {
      if (RightRangeType == BTreeRangeType::kOpen)
        j = node->count;
      else if (RightRangeType == BTreeRangeType::kInclusive)
        j = search_rightmost<false>(node, max_key) + 1;
      else /* if (RightRangeType == BTreeRangeType::kExclusive) */
        j = search_rightmost<true>(node, max_key) + 1;
    }

    auto child_row_id = node->child_row_id(j);

    RowAccessHandleT rah_child(tx);
    const Node* child_b;
    if (!Reversed) {
      if (LeftRangeType == BTreeRangeType::kOpen)
        child_b = get_node(rah_child, child_row_id);
      else
        child_b =
            get_node_with_fixup<false, false>(rah_child, child_row_id, min_key);
    } else {
      if (RightRangeType == BTreeRangeType::kOpen)
        child_b =
            get_node_with_fixup<true, false>(rah_child, child_row_id, max_key);
      else if (RightRangeType == BTreeRangeType::kInclusive) {
        child_b =
            get_node_with_fixup<false, false>(rah_child, child_row_id, max_key);
      } else /* if (RightRangeType == BTreeRangeType::kExclusive) */ {
        child_b =
            get_node_with_fixup<false, true>(rah_child, child_row_id, max_key);
      }
    }
    if (!child_b) return kHaveToAbort;

    return lookup_recursive<LeftRangeType, RightRangeType, Reversed>(
        tx, rah_child, child_b, min_key, max_key, skip_validation, func);
  } else {
    return return_range<LeftRangeType, RightRangeType, Reversed>(
        rah, node_b, min_key, max_key, skip_validation, func);
  }
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <BTreeRangeType LeftRangeType, BTreeRangeType RightRangeType,
          bool Reversed, typename Func, typename RowAccessHandleT>
uint64_t BTreeIndex<StaticConfig, HasValue, Key, Compare>::return_range(
    RowAccessHandleT& rah, const Node* node_b, const Key& min_key,
    const Key& max_key, bool skip_validation, const Func& func) {
  auto node = as_leaf(node_b);

  if (!skip_validation) {
    // We need to track the first node always.
    //
    // TODO: This validation is too conservative.  If only a single key is
    // requested on a unique index and a perfect match is found, it does not
    // require node validation because the row validation is already being done.
    // To do that optimization, we should make a specialized function for point
    // lookups on a unique index.
    if (!validate_read(rah)) return kHaveToAbort;
  }

  // Search for the first matching key.
  int j;
  if (!Reversed) {
    if (LeftRangeType == BTreeRangeType::kOpen) {
      j = 0;
      assert(node->prev == kNullRowID);
    } else if (LeftRangeType == BTreeRangeType::kInclusive) {
      j = search_leftmost<false>(node, min_key);

      assert(j - 1 < 0 || j - 1 >= node->count ||
             comp_lt(node->key(j - 1), min_key));
      assert(j < 0 || j >= node->count || comp_le(min_key, node->key(j)));
    } else /*if (LeftRangeType == BTreeRangeType::kExclusive)*/ {
      j = search_leftmost<true>(node, min_key);

      assert(j - 1 < 0 || j - 1 >= node->count ||
             comp_le(node->key(j - 1), min_key));
      assert(j < 0 || j >= node->count || comp_lt(min_key, node->key(j)));
    }
  } else {
    if (RightRangeType == BTreeRangeType::kOpen) {
      j = node->count - 1;
      assert(node->next == kNullRowID);
    } else if (RightRangeType == BTreeRangeType::kInclusive) {
      j = search_rightmost<false>(node, max_key);

      assert(j + 1 < 0 || j + 1 >= node->count ||
             comp_lt(max_key, node->key(j + 1)));
      assert(j < 0 || j >= node->count || comp_le(node->key(j + 1), max_key));
    } else /*if (RightRangeType == BTreeRangeType::kExclusive)*/ {
      j = search_rightmost<true>(node, max_key);

      assert(j + 1 < 0 || j + 1 >= node->count ||
             comp_le(max_key, node->key(j + 1)));
      assert(j < 0 || j >= node->count || comp_lt(node->key(j + 1), max_key));
    }
  }

  uint64_t found = 0;
  bool done = false;

  while (true) {
    if (!Reversed) {
      if (kPrefetchNode && node->next != kNullRowID)
        prefetch_row(rah, node->next);

      for (; j < node->count; j++) {
        bool matching;
        if (RightRangeType == BTreeRangeType::kOpen)
          matching = true;
        else if (RightRangeType == BTreeRangeType::kInclusive)
          matching = comp_le(node->key(j), max_key);
        else /*if (RightRangeType == BTreeRangeType::kExclusive)*/
          matching = comp_lt(node->key(j), max_key);

        if (matching) {
          found++;

          uint64_t value;
          if (HasValue)
            value = node->value(j);
          else
            value = 0;

          if (!func(node->key(j), value)) {
            done = true;
            break;
          }
        } else {
          done = true;
          break;
        }
      }
      if (done) break;
      if (node->next == kNullRowID) break;
    } else {
      if (kPrefetchNode && node->prev != kNullRowID)
        prefetch_row(rah, node->prev);

      for (; j >= 0; j--) {
        bool matching;
        if (LeftRangeType == BTreeRangeType::kOpen)
          matching = true;
        else if (LeftRangeType == BTreeRangeType::kInclusive)
          matching = comp_le(min_key, node->key(j));
        else /*if (LeftRangeType == BTreeRangeType::kExclusive)*/
          matching = comp_lt(min_key, node->key(j));

        if (matching) {
          found++;

          uint64_t value;
          if (HasValue)
            value = node->value(j);
          else
            value = 0;

          if (!func(node->key(j), value)) {
            done = true;
            break;
          }
        } else {
          done = true;
          break;
        }
      }
      if (done) break;
      if (node->prev == kNullRowID) break;
    }

    // TODO: Use node->min_key and node->max_key to terminate early without touching the sibling node.

    rah.reset();
    if (!Reversed)
      node = as_leaf(get_node(rah, node->next));
    else
      node = as_leaf(get_node(rah, node->prev));
    if (!node) return kHaveToAbort;

    if (!skip_validation) {
      // We need to track subsequent nodes directly used for finding matches.
      if (!validate_read(rah)) return kHaveToAbort;
    }
    if (!Reversed)
      j = 0;
    else
      j = node->count - 1;
  }

  return found;
}
}
}

#endif
