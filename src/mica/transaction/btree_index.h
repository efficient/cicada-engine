#pragma once
#ifndef MICA_TRANSACTION_BTREE_INDEX_H_
#define MICA_TRANSACTION_BTREE_INDEX_H_

#include "mica/common.h"
#include "mica/util/type_traits.h"

namespace mica {
namespace transaction {
template <class StaticConfig, bool HasValue, class Key,
          class Compare = std::less<Key>>
class BTreeIndex;

template <class StaticConfig, bool HasValue, class Key,
          class Compare = std::less<Key>>
class BTreeIndexNodeCopier {
 public:
  typedef BTreeIndex<StaticConfig, HasValue, Key, Compare> BTreeIndexT;
  typedef typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::Node Node;
  typedef typename BTreeIndex<StaticConfig, HasValue, Key,
                              Compare>::InternalNode InternalNode;
  typedef typename BTreeIndex<StaticConfig, HasValue, Key, Compare>::LeafNode
      LeafNode;
  static constexpr bool kUseIndirection = BTreeIndexT::kUseIndirection;

  bool operator()(uint16_t cf_id, RowVersion<StaticConfig>* dest,
                  const RowVersion<StaticConfig>* src) const {
    (void)cf_id;
    if (dest->data_size == 0) return true;

    auto src_node_b = reinterpret_cast<const Node*>(src->data);

    if (BTreeIndexT::is_internal(src_node_b)) {
      auto src_node = BTreeIndexT::as_internal(src_node_b);
      auto dest_node = reinterpret_cast<InternalNode*>(dest->data);

      dest_node->type = src_node->type;
      dest_node->count = src_node->count;
      dest_node->next = src_node->next;
      dest_node->min_key = src_node->min_key;
      dest_node->max_key = src_node->max_key;
      if (kUseIndirection)
        ::mica::util::memcpy(dest_node->indir, src_node->indir,
                             sizeof(uint8_t) * src_node->count);
      // for (size_t i = 0; i < src_node->count; i++)
      //   dest_node->keys[i] = src_node->keys[i];
      ::mica::util::memcpy(dest_node->keys, src_node->keys,
                           sizeof(Key) * static_cast<size_t>(src_node->count));
      ::mica::util::memcpy(
          dest_node->child_row_ids, src_node->child_row_ids,
          sizeof(uint64_t) * (static_cast<size_t>(src_node->count) + 1));
    } else {
      auto src_node = BTreeIndexT::as_leaf(src_node_b);
      auto dest_node = reinterpret_cast<LeafNode*>(dest->data);

      dest_node->type = src_node->type;
      dest_node->count = src_node->count;
      dest_node->prev = src_node->prev;
      dest_node->next = src_node->next;
      dest_node->min_key = src_node->min_key;
      dest_node->max_key = src_node->max_key;
      if (kUseIndirection)
        ::mica::util::memcpy(dest_node->indir, src_node->indir,
                             sizeof(uint8_t) * src_node->count);
      // for (size_t i = 0; i < src_node->count; i++)
      //   dest_node->keys[i] = src_node->keys[i];
      ::mica::util::memcpy(dest_node->keys, src_node->keys,
                           sizeof(Key) * static_cast<size_t>(src_node->count));
      if (HasValue)
        ::mica::util::memcpy(
            dest_node->values, src_node->values,
            sizeof(uint64_t) * static_cast<size_t>(src_node->count));
    }
    return true;
  }
};

enum class BTreeRangeType {
  kOpen = 0,
  kInclusive,
  kExclusive,
};

template <class StaticConfig, bool HasValue, class Key, class Compare>
class BTreeIndex {
 public:
  typedef BTreeIndexNodeCopier<StaticConfig, HasValue, Key, Compare> DataCopier;

  typedef typename StaticConfig::Timing Timing;
  typedef ::mica::transaction::RowAccessHandle<StaticConfig> RowAccessHandle;
  typedef ::mica::transaction::RowAccessHandlePeekOnly<StaticConfig>
      RowAccessHandlePeekOnly;
  typedef ::mica::transaction::Transaction<StaticConfig> Transaction;

  enum VerboseFlag : uint64_t {
    kInsert = uint64_t(1) << 1,
    kRemove = uint64_t(1) << 2,
    kLookup = uint64_t(1) << 3,
    kFixup = uint64_t(1) << 4,
    kConsistency = uint64_t(1) << 10,
  };

  static constexpr uint64_t kVerbose = VerboseFlag::kFixup;
  // static constexpr uint64_t kVerbose =
  //     VerboseFlag::kFixup | VerboseFlag::kConsistency;
  // static constexpr uint64_t kVerbose = VerboseFlag::kInsert;
  // static constexpr uint64_t kVerbose = VerboseFlag::kRemove;
  // static constexpr uint64_t kVerbose = (uint64_t(1) << 63) - 1;

  // static constexpr bool kUseBinarySearch = false;
  static constexpr bool kUseBinarySearch = true;
  static const int kBinarySearchThreshold = 8;

  // static constexpr bool kPrefetchNode = false;
  static constexpr bool kPrefetchNode = true;

  static constexpr bool kUseIndirection = false;
  // static constexpr bool kUseIndirection = true;

  enum class NodeType : uint8_t {
    kInternal = 0,
    kLeaf,
  };

  struct Node {
    NodeType type;
    uint8_t count;
  };

  template <size_t MaxCount>
  struct InternalNodeT : public Node {
    static constexpr size_t kMaxCount = MaxCount;

    // Used for fixups.
    uint64_t next;
    // We do not keep prev pointers for internal nodes because it is only useful for determining if min_key is valid (i.e., no meaning for the smallest internal node).
    // If we used prev pointers instead of min_key, it has a performance penalty by having to track a prev node of every tracked node with a new inserted key because those two nodes are necessary to define a range for the inserted key.

    // The minimum key that can be in this node.
    Key min_key;

    // The split key between this node and the next node.
    // Any key in this node's child must be smaller than max_key.
    // This is duplicate with parent node's split key, but we need this within the node for the fixup process.
    Key max_key;

    uint8_t indir[kUseIndirection ? kMaxCount : 0];

    // keys[i]
    //   <= (any keys in the child node at child_row_ids[i + 1])
    //     < keys[i + 1]
    Key keys[kMaxCount];
    uint64_t child_row_ids[kMaxCount + 1];

    template <typename IndexType>
    Key& key(IndexType i) {
      return kUseIndirection ? keys[indir[i]] : keys[i];
    }
    template <typename IndexType>
    const Key& key(IndexType i) const {
      return kUseIndirection ? keys[indir[i]] : keys[i];
    }
    template <typename IndexType>
    uint64_t& child_row_id(IndexType i) {
      if (kUseIndirection) {
        if (i == 0)
          return child_row_ids[0];
        else
          return child_row_ids[indir[i - 1] + 1];
      } else
        return child_row_ids[i];
    }
    template <typename IndexType>
    const uint64_t& child_row_id(IndexType i) const {
      if (kUseIndirection) {
        if (i == 0)
          return child_row_ids[0];
        else
          return child_row_ids[indir[i - 1] + 1];
      } else
        return child_row_ids[i];
    }
  };

  static constexpr size_t kInternalNodeMaxCount =
      (1024 - 40 - 40) / (sizeof(Key) + sizeof(uint64_t));
  typedef InternalNodeT<kInternalNodeMaxCount> InternalNode;
  typedef InternalNodeT<kInternalNodeMaxCount * 2 + 1> InternalNodeBuffer;

  template <size_t MaxCount>
  struct LeafNodeT : public Node {
    static constexpr size_t kMaxCount = MaxCount;

    // Used for fixups and fast iteration.
    uint64_t next;
    // Used for fast reverse iteration.
    uint64_t prev;

    // The minimum key that can be in this node.
    Key min_key;

    // The split key between this node and the next node.
    // Any key in this node must be smaller than max_key.  This is duplicate
    // with parent node's split key, but we need this within the node for the
    // fixup process.
    Key max_key;

    uint8_t indir[kUseIndirection ? kMaxCount : 0];

    Key keys[kMaxCount];
    uint64_t values[HasValue ? kMaxCount : 0];

    template <typename IndexType>
    Key& key(IndexType i) {
      return kUseIndirection ? keys[indir[i]] : keys[i];
    }
    template <typename IndexType>
    const Key& key(IndexType i) const {
      return kUseIndirection ? keys[indir[i]] : keys[i];
    }
    template <typename IndexType>
    uint64_t& value(IndexType i) {
      return kUseIndirection ? values[indir[i]] : values[i];
    }
    template <typename IndexType>
    const uint64_t& value(IndexType i) const {
      return kUseIndirection ? values[indir[i]] : values[i];
    }
  };

  static constexpr size_t kLeafNodeMaxCount =
      (1024 - 40 - 40) / (sizeof(Key) + (HasValue ? sizeof(uint64_t) : 0));
  typedef LeafNodeT<kLeafNodeMaxCount> LeafNode;
  typedef LeafNodeT<kLeafNodeMaxCount * 2> LeafNodeBuffer;

  static constexpr uint64_t kDataSize =
      std::max(sizeof(InternalNode), sizeof(LeafNode));

  static constexpr uint64_t kNullRowID = static_cast<uint64_t>(-1);

  static constexpr uint64_t kHaveToAbort = static_cast<uint64_t>(-1);

  // btree_index_impl/init.h
  BTreeIndex(DB<StaticConfig>* db, Table<StaticConfig>* main_tbl,
             Table<StaticConfig>* idx_tbl, const Compare& comp = Compare());

  bool init(Transaction* tx);

  // btree_index_impl/insert.h
  uint64_t insert(Transaction* tx, const Key& key, uint64_t value);

  // btree_index_impl/remove.h
  uint64_t remove(Transaction* tx, const Key& key, uint64_t value);

  // btree_index_impl/lookup.h
  template <typename Func>
  uint64_t lookup(Transaction* tx, const Key& key, bool skip_validation,
                  const Func& func);

  template <BTreeRangeType LeftRangeType, BTreeRangeType RightRangeType,
            bool Reversed, typename Func>
  uint64_t lookup(Transaction* tx, const Key& min_key, const Key& max_key,
                  bool skip_validation, const Func& func);

  // btree_index_impl/prefetch.h
  void prefetch(Transaction* tx, const Key& key);

  // btree_index_impl/check.h
  bool check(Transaction* tx) const;
  bool dump_tree(Transaction* tx) const;

  Table<StaticConfig>* main_table() { return main_tbl_; }
  const Table<StaticConfig>* main_table() const { return main_tbl_; }

  Table<StaticConfig>* index_table() { return idx_tbl_; }
  const Table<StaticConfig>* index_table() const { return idx_tbl_; }

 private:
  DB<StaticConfig>* db_;
  Table<StaticConfig>* main_tbl_;
  Table<StaticConfig>* idx_tbl_;
  Compare comp_;

  DataCopier data_copier_;
  friend DataCopier;

  // btree_index_impl/insert.h
  uint64_t insert_recursive(Transaction* tx, RowAccessHandle& rah,
                            const Node* node_b, const Key& key, uint64_t value,
                            Key* up_key, uint64_t* up_row_id);
  bool insert_child(Transaction* tx, RowAccessHandle& rah,
                    const InternalNode* node, const Key& key,
                    uint64_t child_row_id, Key* up_key, uint64_t* up_row_id);
  uint64_t insert_item(Transaction* tx, RowAccessHandle& rah,
                       const LeafNode* node, const Key& key, uint64_t value,
                       Key* up_key, uint64_t* up_row_id);

  // btree_index_impl/remove.h
  uint64_t remove_recursive(Transaction* tx, RowAccessHandle& rah,
                            const Node* node_b, const Key& key, uint64_t value,
                            bool* up_rebalancing);
  bool rebalance(Transaction* tx, RowAccessHandle& rah_parent,
                 const InternalNode* parent, uint64_t child_row_id,
                 bool* up_rebalancing);
  uint64_t remove_item(Transaction* tx, RowAccessHandle& rah,
                       const LeafNode* node, const Key& key, uint64_t value,
                       bool* up_rebalancing);

  // btree_index_impl/lookup.h
  template <BTreeRangeType LeftRangeType, BTreeRangeType RightRangeType,
            bool Reversed, typename Func, typename RowAccessHandleT>
  uint64_t lookup_recursive(Transaction* tx, RowAccessHandleT& rah,
                            const Node* node_b, const Key& min_key,
                            const Key& max_key, bool skip_validation,
                            const Func& func);

  template <BTreeRangeType LeftRangeType, BTreeRangeType RightRangeType,
            bool Reversed, typename Func, typename RowAccessHandleT>
  uint64_t return_range(RowAccessHandleT& rah, const Node* node_b,
                        const Key& min_key, const Key& max_key,
                        bool skip_validation, const Func& func);

  // btree_index_impl/fixup.h
  template <bool RightOpen, bool RightExclusive, typename RowAccessHandleT>
  bool fixup_internal(RowAccessHandleT& rah, const Node*& node_b,
                      const Key& key) const;
  template <bool RightOpen, bool RightExclusive, typename RowAccessHandleT>
  bool fixup_leaf(RowAccessHandleT& rah, const Node*& node_b,
                  const Key& key) const;

  // btree_index_impl/gather.h
  template <typename NodeT>
  static void copy(NodeT* dest, const InternalNode* left);
  template <typename NodeT>
  static void copy(NodeT* dest, const LeafNode* left);

  template <typename NodeT>
  static void gather(NodeT* dest, const InternalNode* left,
                     const InternalNode* right);
  template <typename NodeT>
  static void gather(NodeT* dest, const LeafNode* left, const LeafNode* right);

  template <typename NodeT>
  static void scatter(InternalNode* left, InternalNode* right, const NodeT* src,
                      uint64_t new_left_count);

  template <typename NodeT>
  static void scatter(LeafNode* left, LeafNode* right, const NodeT* src,
                      uint64_t new_left_count);

  // btree_index_impl/check.h
  bool check_recursive(Transaction* tx, RowAccessHandlePeekOnly& rah,
                       const Node* node_b, bool is_root,
                       const Key& expected_min_key,
                       const Key& expected_max_key) const;
  bool dump_tree_recursive(Transaction* tx, RowAccessHandlePeekOnly& rah,
                           const Node* node_b) const;

  // btree_index_impl/node.h
  InternalNode* make_internal_node(RowAccessHandle& rah);

  LeafNode* make_leaf_node(RowAccessHandle& rah);

  bool free_node(RowAccessHandle& rah);

  template <typename RowAccessHandleT>
  const Node* get_node(RowAccessHandleT& rah, uint64_t row_id) const;

  template <bool RightOpen, bool RightExclusive, typename RowAccessHandleT>
  const Node* get_node_with_fixup(RowAccessHandleT& rah, uint64_t row_id,
                                  const Key& key) const;

  Node* get_writable_node(RowAccessHandle& rah);

  template <typename RowAccessHandleT>
  bool validate_read(RowAccessHandleT& rah);

  template <typename RowAccessHandleT>
  void prefetch_row(RowAccessHandleT& rah, uint64_t row_id) const;

  void prefetch_node(const Node* node) const;

  static bool is_internal(const Node* node);
  static bool is_leaf(const Node* node);

  static InternalNode* as_internal(Node* node);
  static const InternalNode* as_internal(const Node* node);

  static LeafNode* as_leaf(Node* node);
  static const LeafNode* as_leaf(const Node* node);

  template <bool Exclusive, typename NodeT>
  int search_leftmost(const NodeT& node, const Key& key) const;

  template <bool Exclusive, typename NodeT>
  int search_rightmost(const NodeT& node, const Key& key) const;

  bool comp_lt(const Key& key1, const Key& key2) const {
    return comp_(key1, key2);
  }
  bool comp_le(const Key& key1, const Key& key2) const {
    return !comp_(key2, key1);
  }
  bool comp_gt(const Key& key1, const Key& key2) const {
    return comp_(key2, key1);
  }
  bool comp_ge(const Key& key1, const Key& key2) const {
    return !comp_(key1, key2);
  }
  bool comp_eq(const Key& key1, const Key& key2) const {
    return comp_le(key1, key2) && comp_ge(key1, key2);
  }
  bool comp_ne(const Key& key1, const Key& key2) const {
    return comp_lt(key1, key2) || comp_gt(key1, key2);
  }

  static uint64_t key_info(const Key& key);

  template <class RowAccessHandleT>
  static void dump_node(RowAccessHandleT& rah, const Node* node_b);
};
}
}

#include "btree_index_impl/init.h"
#include "btree_index_impl/node.h"
#include "btree_index_impl/gather.h"
#include "btree_index_impl/fixup.h"
#include "btree_index_impl/insert.h"
#include "btree_index_impl/remove.h"
#include "btree_index_impl/lookup.h"
#include "btree_index_impl/prefetch.h"
#include "btree_index_impl/check.h"

#endif
