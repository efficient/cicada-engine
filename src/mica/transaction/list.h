#pragma once
#ifndef MICA_TRANSACTION_LIST_H_
#define MICA_TRANSACTION_LIST_H_

#include "mica/common.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
class List {
 public:
  typedef typename StaticConfig::Timing Timing;

  static constexpr uint64_t kDataSize = sizeof(uint64_t) * 2;

  static constexpr uint64_t kNullRowID = static_cast<uint64_t>(-1);

  List(uint64_t off, uint64_t neg_infty = 0, uint64_t pos_infty = 1);

  uint64_t neg_infty() const { return neg_infty_; }
  uint64_t pos_infty() const { return pos_infty_; }

  const uint64_t& prev(const char* data) const;
  const uint64_t& next(const char* data) const;

  bool is_adjacent(const char* left, const char* right) const;

  void init_infty(char* left, char* right);

  void set_prev(char* data, uint64_t prev);
  void set_next(char* data, uint64_t next);

  bool insert(uint64_t row_id, char* data, char* left, char* right);
  bool remove(char* data, char* left, char* right);

 private:
  uint64_t off_;
  uint64_t neg_infty_;
  uint64_t pos_infty_;
};
}
}

#endif
