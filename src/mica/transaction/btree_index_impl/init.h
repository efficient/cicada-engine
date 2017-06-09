#pragma once
#ifndef MICA_TRANSACTION_BTREE_INDEX_IMPL_INIT_H_
#define MICA_TRANSACTION_BTREE_INDEX_IMPL_INIT_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool HasValue, class Key, class Compare>
BTreeIndex<StaticConfig, HasValue, Key, Compare>::BTreeIndex(
    DB<StaticConfig>* db, Table<StaticConfig>* main_tbl,
    Table<StaticConfig>* idx_tbl, const Compare& comp)
    : db_(db), main_tbl_(main_tbl), idx_tbl_(idx_tbl), comp_(comp) {
  static_assert(sizeof(InternalNode) <= sizeof(LeafNode), "invalid node size");
  static_assert(std::is_trivially_copyable<Key>::value,
                "trivially copyable keys required");
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BTreeIndex<StaticConfig, HasValue, Key, Compare>::init(Transaction* tx) {
  Timing t(tx->context()->timing_stack(), &Stats::index_write);

  bool ret = tx->begin();
  if (!ret) return false;

  RowAccessHandle rah_head(tx);
  auto head = make_internal_node(rah_head);
  if (!head || rah_head.row_id() != 0) {
    printf("failed to create head\n");
    return false;
  }

  RowAccessHandle rah_root(tx);
  auto root = make_leaf_node(rah_root);
  if (!root) {
    printf("failed to create root\n");
    return false;
  }

  head->count = 0;
  head->child_row_id(0) = rah_root.row_id();
  head->next = kNullRowID;
  head->min_key = Key{};
  head->max_key = Key{};

  root->count = 0;
  root->prev = kNullRowID;
  root->next = kNullRowID;
  root->min_key = Key{};
  root->max_key = Key{};

  if (!tx->commit()) {
    printf("failed to initialize a new BTreeIndex\n");
    return false;
  }
  // printf("BTreeIndex::init()\n");
  return true;
}
}
}

#endif
