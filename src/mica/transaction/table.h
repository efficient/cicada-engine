#pragma once
#ifndef MICA_TRANSACTION_TABLE_H_
#define MICA_TRANSACTION_TABLE_H_

#include <vector>
#include "mica/common.h"
#include "mica/transaction/db.h"
#include "mica/transaction/row.h"
#include "mica/transaction/context.h"
#include "mica/transaction/transaction.h"
#include "mica/util/memcpy.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
class DB;

template <class StaticConfig>
class Table {
 public:
  typedef typename StaticConfig::Timestamp Timestamp;

  Table(DB<StaticConfig>* db, uint16_t cf_count,
        const uint64_t* data_size_hints);
  ~Table();

  DB<StaticConfig>* db() { return db_; }
  const DB<StaticConfig>* db() const { return db_; }

  uint16_t cf_count() const { return cf_count_; }

  uint64_t data_size_hint(uint16_t cf_id) const {
    return cf_[cf_id].data_size_hint;
  }

  uint64_t row_count() const { return row_count_; }

  uint8_t inlining(uint16_t cf_id) const { return cf_[cf_id].inlining; }

  uint16_t inlined_rv_size_cls(uint16_t cf_id) const {
    return cf_[cf_id].inlined_rv_size_cls;
  }

  bool is_valid(uint16_t cf_id, uint64_t row_id) const;

  RowHead<StaticConfig>* head(uint16_t cf_id, uint64_t row_id);

  const RowHead<StaticConfig>* head(uint16_t cf_id, uint64_t row_id) const;

  RowHead<StaticConfig>* alt_head(uint16_t cf_id, uint64_t row_id);

  RowGCInfo<StaticConfig>* gc_info(uint16_t cf_id, uint64_t row_id);

  const RowVersion<StaticConfig>* latest_rv(uint16_t cf_id,
                                            uint64_t row_id) const;

  bool allocate_rows(Context<StaticConfig>* ctx,
                     std::vector<uint64_t>& row_ids);

  bool renew_rows(Context<StaticConfig>* ctx, uint16_t cf_id,
                  uint64_t& row_id_begin, uint64_t row_id_end,
                  bool expiring_only);

  template <typename Func>
  bool scan(Transaction<StaticConfig>* tx, uint16_t cf_id, uint64_t off,
            uint64_t len, const Func& f);

  void print_table_status() const;

 private:
  DB<StaticConfig>* db_;
  uint16_t cf_count_;

  struct ColumnFamilyInfo {
    uint64_t data_size_hint;

    uint64_t rh_offset;

    uint64_t rh_size;
    uint8_t inlining;
    uint16_t inlined_rv_size_cls;
  };

  // We use only the half the first level because of shuffling.
  static constexpr uint64_t kFirstLevelWidth =
      PagePool<StaticConfig>::kPageSize / sizeof(char*) / 2;

  uint64_t total_rh_size_;
  uint64_t second_level_width_;
  uint64_t row_id_shift_;  // log_2(second_level_width)
  uint64_t row_id_mask_;   // (1 << row_id_shift) - 1

  ColumnFamilyInfo cf_[StaticConfig::kMaxColumnFamilyCount];

  char* base_root_;
  char** root_;
  uint8_t* page_numa_ids_;

  volatile uint32_t lock_ __attribute__((aligned(64)));
  uint64_t row_count_;
} __attribute__((aligned(64)));
}
}

#include "table_impl.h"

#endif
