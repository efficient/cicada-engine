#pragma once
#ifndef MICA_TRANSACTION_LIST_IMPL_H_
#define MICA_TRANSACTION_LIST_IMPL_H_

namespace mica {
namespace transaction {
template <class StaticConfig>
List<StaticConfig>::List(uint64_t off, uint64_t neg_infty, uint64_t pos_infty)
    : off_(off), neg_infty_(neg_infty), pos_infty_(pos_infty) {
  if (off % sizeof(uint64_t) != 0) {
    fprintf(stderr, "error: off must be a multiple of sizeof(uint64_t)\n");
    assert(false);
    return;
  }
}

template <class StaticConfig>
const uint64_t& List<StaticConfig>::prev(const char* data) const {
  return *static_cast<uint64_t*>(data + off_);
}

template <class StaticConfig>
const uint64_t& List<StaticConfig>::next(const char* data) const {
  return *static_cast<uint64_t*>(data + off_ + sizeof(uint64_t));
}

template <class StaticConfig>
bool List<StaticConfig>::is_adjacent(const char* left,
                                     const char* right) const {
  return next(left) == prev(right);
}

template <class StaticConfig>
void List<StaticConfig>::init_infty(char* left, char* right) {
  set_prev(rah_neg_infty, kNullRowID);
  set_next(rah_neg_infty, pos_infty_);

  set_prev(rah_pos_infty, neg_infty_);
  set_next(rah_pos_infty, kNullRowID);
}

template <class StaticConfig>
void List<StaticConfig>::set_prev(char* data, uint64_t prev) {
  *static_cast<uint64_t*>(data + off_) = prev;
}

template <class StaticConfig>
void List<StaticConfig>::set_next(char* data, uint64_t next) {
  *static_cast<uint64_t*>(data + off_ + sizeof(uint64_)) = next;
}

template <class StaticConfig>
bool List<StaticConfig>::insert(uint64_t row_id, char* data, char* left,
                                char* right) {
  if (!is_adjacent(left, right)) return false;

  set_prev(data, prev(right));
  set_next(data, next(left));

  set_next(left, row_id);
  set_prev(right, row_id);

  return true;
}

template <class StaticConfig>
bool List<StaticConfig>::remove(char* data, char* left, char* right) {
  if (!is_adjacent(left, data)) return false;
  if (!is_adjacent(data, right)) return false;

  set_next(rah_left, next(data));
  set_prev(rah_right, prev(data));

  return true;
}
}
}

#endif
