#pragma once
#ifndef MICA_TRANSACTION_TABLE_IMPL_H_
#define MICA_TRANSACTION_TABLE_IMPL_H_

namespace mica {
namespace transaction {
template <class StaticConfig>
Table<StaticConfig>::Table(DB<StaticConfig>* db, uint16_t cf_count,
                           const uint64_t* data_size_hints)
    : db_(db), cf_count_(cf_count) {
  assert(cf_count <= StaticConfig::kMaxColumnFamilyCount);

  constexpr size_t kAlignment = 64;
  // constexpr size_t kAlignment = 32;
  // constexpr size_t kAlignment = 8;

  printf("RowHead size: %" PRIu64 " bytes\n", sizeof(RowHead<StaticConfig>));
  printf("RowVersion size: %" PRIu64 " bytes\n",
         sizeof(RowVersion<StaticConfig>));

  uint64_t rh_offset = 0;
  for (uint16_t cf_id = 0; cf_id < cf_count_; cf_id++) {
    auto& cf = cf_[cf_id];
    cf.data_size_hint = data_size_hints[cf_id];

    if (StaticConfig::kInlinedRowVersion) {
      if (cf.data_size_hint <= StaticConfig::kInlineThreshold) {
        cf.rh_size = ::mica::util::roundup<kAlignment>(
            sizeof(RowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>) +
            cf.data_size_hint);
        cf.inlining = 1;
        cf.inlined_rv_size_cls =
            SharedRowVersionPool<StaticConfig>::data_size_to_class(
                cf.data_size_hint);
      } else {
        cf.rh_size =
            ::mica::util::roundup<kAlignment>(sizeof(RowHead<StaticConfig>));
        cf.inlining = 0;
        cf.inlined_rv_size_cls = 0;
      }
    } else {
      cf.rh_size =
          ::mica::util::roundup<kAlignment>(sizeof(RowHead<StaticConfig>));
      cf.inlining = 0;
      cf.inlined_rv_size_cls = 0;
    }

    cf.rh_offset = rh_offset;
    rh_offset += cf.rh_size;

    printf("column family %" PRIu16 "\n", cf_id);

    printf("  data size hint: %" PRIu64 " bytes\n", cf.data_size_hint);
    printf("  rh size: %" PRIu64 " bytes (inlining=%s)\n", cf.rh_size,
           (cf.inlining ? "yes" : "no"));
    // if (StaticConfig::kInlinedRowVersion && cf.inlining) {
    //   RowHead<StaticConfig> h;
    //   printf(
    //       "inlined_rv offset: %" PRIu64 " bytes\n",
    //       reinterpret_cast<size_t>(&h.inlined_rv) -
    //       reinterpret_cast<size_t>(&h));
    // }
  }

  total_rh_size_ = rh_offset;

  second_level_width_ =
      PagePool<StaticConfig>::kPageSize /
      (total_rh_size_ + sizeof(RowGCInfo<StaticConfig>) * cf_count_);
  row_id_shift_ = 0;
  while ((uint64_t(1) << row_id_shift_) < second_level_width_) row_id_shift_++;
  if ((uint64_t(1) << row_id_shift_) > second_level_width_) {
    row_id_shift_--;
    second_level_width_ = uint64_t(1) << row_id_shift_;
  }
  row_id_mask_ = (uint64_t(1) << row_id_shift_) - 1;

  printf("maximum table size: %" PRIu64 " rows (%" PRIu64 " * %" PRIu64 ")\n",
         kFirstLevelWidth * second_level_width_, kFirstLevelWidth,
         second_level_width_);
  printf("\n");

  base_root_ = db_->page_pool(0)->allocate();
  if (base_root_ == nullptr) {
    printf("failed to allocate memory\n");
    return;
  }
  ::mica::util::memset(base_root_, 0, PagePool<StaticConfig>::kPageSize);

  // Attempt to shuffle the root entry a little bit to avoid hotspots in the
  // memory addresses.
  uint64_t off =
      ::mica::util::rdtsc() % (PagePool<StaticConfig>::kPageSize / 2) / 64 * 64;
  assert(off % 64 == 0);
  assert(off + PagePool<StaticConfig>::kPageSize / 2 <=
         PagePool<StaticConfig>::kPageSize);
  root_ = reinterpret_cast<char**>(base_root_ + off);

  page_numa_ids_ = reinterpret_cast<uint8_t*>(db_->page_pool(0)->allocate());

  lock_ = 0;
  row_count_ = 0;
}

template <class StaticConfig>
Table<StaticConfig>::~Table() {
  for (uint64_t i = 0; i < kFirstLevelWidth; i++)
    if (root_[i] != nullptr) db_->page_pool(page_numa_ids_[i])->free(root_[i]);

  db_->page_pool(0)->free(base_root_);
  // root_ is part of base_root_.
  db_->page_pool(0)->free(reinterpret_cast<char*>(page_numa_ids_));
}

template <class StaticConfig>
bool Table<StaticConfig>::is_valid(uint16_t cf_id, uint64_t row_id) const {
  if (row_id >= row_count_) return true;
  auto p = root_[row_id >> row_id_shift_];
  return (p == nullptr || head(cf_id, row_id)->older_rv != nullptr);
}

template <class StaticConfig>
RowHead<StaticConfig>* Table<StaticConfig>::head(uint16_t cf_id,
                                                 uint64_t row_id) {
  auto& cf = cf_[cf_id];
  auto p = root_[row_id >> row_id_shift_];
  auto h = p + (row_id & row_id_mask_) * total_rh_size_ + cf.rh_offset;

  // __builtin_prefetch(h, 0, 3);
  // if (StaticConfig::kInlinedRowVersion &&
  //     (sizeof(RowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>) +
  //      StaticConfig::kInlineThreshold) > 64 &&
  //     cf.inlining && cf.rh_size > 64)
  //   __builtin_prefetch(h + 64, 0, 3);
  // if (StaticConfig::kInlinedRowVersion &&
  //     (sizeof(RowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>) +
  //      StaticConfig::kInlineThreshold) > 128 &&
  //     cf.inlining && cf.rh_size > 128)
  //   __builtin_prefetch(h + 128, 0, 3);
  // if (StaticConfig::kInlinedRowVersion &&
  //     (sizeof(RowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>) +
  //      StaticConfig::kInlineThreshold) > 192 &&
  //     cf.inlining && cf.rh_size > 192)
  //   __builtin_prefetch(h + 192, 0, 3);

  return reinterpret_cast<RowHead<StaticConfig>*>(h);
}

template <class StaticConfig>
const RowHead<StaticConfig>* Table<StaticConfig>::head(uint16_t cf_id,
                                                       uint64_t row_id) const {
  auto& cf = cf_[cf_id];
  auto p = root_[row_id >> row_id_shift_];
  auto h = p + (row_id & row_id_mask_) * total_rh_size_ + cf.rh_offset;

  // __builtin_prefetch(h, 0, 3);
  // if (StaticConfig::kInlinedRowVersion &&
  //     (sizeof(RowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>) +
  //      StaticConfig::kInlineThreshold) > 64 &&
  //     cf.inlining && cf.rh_size > 64)
  //   __builtin_prefetch(h + 64, 0, 3);
  // if (StaticConfig::kInlinedRowVersion &&
  //     (sizeof(RowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>) +
  //      StaticConfig::kInlineThreshold) > 128 &&
  //     cf.inlining && cf.rh_size > 128)
  //   __builtin_prefetch(h + 128, 0, 3);
  // if (StaticConfig::kInlinedRowVersion &&
  //     (sizeof(RowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>) +
  //      StaticConfig::kInlineThreshold) > 192 &&
  //     cf.inlining && cf.rh_size > 192)
  //   __builtin_prefetch(h + 192, 0, 3);

  return reinterpret_cast<const RowHead<StaticConfig>*>(h);
}

template <class StaticConfig>
RowHead<StaticConfig>* Table<StaticConfig>::alt_head(uint16_t cf_id,
                                                     uint64_t row_id) {
  assert(StaticConfig::kInlineWithAltRow);

  auto alt_row_id = (row_id + 1) * 0x9ddfea08eb382d69ULL;

  auto& cf = cf_[cf_id];
  auto p = root_[row_id >> row_id_shift_];
  auto h = p + (alt_row_id & row_id_mask_) * total_rh_size_ + cf.rh_offset;

  // __builtin_prefetch(h, 0, 3);
  if (StaticConfig::kInlinedRowVersion &&
      (sizeof(RowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>) +
       StaticConfig::kInlineThreshold) > 64 &&
      cf.inlining && cf.rh_size > 64)
    __builtin_prefetch(h + 64, 0, 3);
  // if (StaticConfig::kInlinedRowVersion &&
  //     (sizeof(RowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>) +
  //      StaticConfig::kInlineThreshold) > 128 &&
  //     cf.inlining && cf.rh_size > 128)
  //   __builtin_prefetch(h + 128, 0, 3);
  // if (StaticConfig::kInlinedRowVersion &&
  //     (sizeof(RowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>) +
  //      StaticConfig::kInlineThreshold) > 192 &&
  //     cf.inlining && cf.rh_size > 192)
  //   __builtin_prefetch(h + 192, 0, 3);

  return reinterpret_cast<RowHead<StaticConfig>*>(h);
}

template <class StaticConfig>
RowGCInfo<StaticConfig>* Table<StaticConfig>::gc_info(uint16_t cf_id,
                                                      uint64_t row_id) {
  auto p = root_[row_id >> row_id_shift_];
  auto g = reinterpret_cast<RowGCInfo<StaticConfig>*>(
      p + second_level_width_ * total_rh_size_ +
      ((row_id & row_id_mask_) * cf_count_ + cf_id) *
          sizeof(RowGCInfo<StaticConfig>));
  return g;
}

template <class StaticConfig>
const RowVersion<StaticConfig>* Table<StaticConfig>::latest_rv(
    uint16_t cf_id, uint64_t row_id) const {
  auto rv = head(cf_id, row_id)->older_rv;
  while (rv != nullptr) {
    if (rv->status >= RowVersionStatus::kCommitted) break;
    rv = rv->older_rv;
  }
  return rv;
}

template <class StaticConfig>
bool Table<StaticConfig>::allocate_rows(Context<StaticConfig>* ctx,
                                        std::vector<uint64_t>& row_ids) {
  if (StaticConfig::kCollectProcessingStats) ctx->stats().insert_row_count++;

  // Allocate a new page and initialize it.
  uint8_t numa_id = ctx->numa_id_;
  char* p = nullptr;
  for (auto trial = 0; trial < db_->numa_count(); trial++) {
    p = db_->page_pool(numa_id)->allocate();
    if (p != nullptr) break;
    if (++numa_id == db_->numa_count()) numa_id = 0;
  }

  if (p == nullptr) {
    printf("failed to allocate memory\n");
    __sync_lock_release(&lock_);
    return false;
  }
  // printf("allocated %" PRIu64 " rows\n", second_level_width_);

  for (uint64_t i = 0; i < second_level_width_; i++) {
    for (uint16_t cf_id = 0; cf_id < cf_count_; cf_id++) {
      auto& cf = cf_[cf_id];
      auto h = reinterpret_cast<RowHead<StaticConfig>*>(p + i * total_rh_size_ +
                                                        cf.rh_offset);
      h->older_rv = nullptr;

      if (StaticConfig::kInlinedRowVersion && cf.inlining) {
        auto inlined_rv = h->inlined_rv;
        inlined_rv->status = RowVersionStatus::kInvalid;
        inlined_rv->numa_id =
            RowVersion<StaticConfig>::kInlinedRowVersionNUMAID;
        inlined_rv->size_cls = cf.inlined_rv_size_cls;
      }

      // The following is now done by Context::allocate_row().
      // auto g = reinterpret_cast<RowGCInfo<StaticConfig>*>(
      //     p + second_level_width_ * total_rh_size_ +
      //     i * sizeof(RowGCInfo<StaticConfig>));
      // g->gc_lock = 0;
    }
  }

  // Acquire the table lock.
  while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

  // Assign row IDs.
  uint64_t row_id = row_count_;
  if ((row_id >> row_id_shift_) == kFirstLevelWidth) {
    printf("maximum table size (%" PRIu64 " rows) reached\n",
           kFirstLevelWidth * second_level_width_);
    db_->page_pool(numa_id)->free(p);
    __sync_lock_release(&lock_);
    return false;
  }

  // Register the new page.
  root_[row_id >> row_id_shift_] = p;
  page_numa_ids_[row_id >> row_id_shift_] = numa_id;

  row_count_ += second_level_width_;

  // Release the table lock.
  __sync_lock_release(&lock_);

  for (uint64_t i = 0; i < second_level_width_; i++) {
    // Ensure that the the last entry in the row_ids is 0. Some components such
    // as HashIndex requires it.
    row_ids.push_back(row_id + second_level_width_ - 1 - i);
  }

  return true;
}

// template <class StaticConfig>
// void Table<StaticConfig>::delete_row(Context<StaticConfig>* ctx,
//                                      uint64_t row_id) {
//   if (StaticConfig::kCollectProcessingStats) ctx->stats().delete_row_count++;
//
//   assert(head(0, row_id)->older_rv == nullptr);
//
//   while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();
//
//   uint8_t numa_id = page_numa_ids_[row_id >> row_id_shift_];
//
//   free_row_ids_[numa_id].push_back(row_id);
//
//   __sync_lock_release(&lock_);
// }

template <class StaticConfig>
bool Table<StaticConfig>::renew_rows(Context<StaticConfig>* ctx, uint16_t cf_id,
                                     uint64_t& row_id_begin,
                                     uint64_t row_id_end, bool expiring_only) {
  auto& cf = cf_[cf_id];

  if (row_id_end > row_count_) row_id_end = row_count_;

  auto min_wts = db_->min_wts();

  Transaction<StaticConfig> tx(ctx);

  for (; row_id_begin < row_id_end; row_id_begin++) {
    // if (row_id_begin % 10000 == 0) printf("row_id=%" PRIu64 "\n",
    // row_id_begin);

    while (true) {
      auto rv = latest_rv(cf_id, row_id_begin);
      if (rv == nullptr) break;

      // Never renew "deleted" versions.
      if (rv->status == RowVersionStatus::kDeleted) break;

      if (rv->wts.about_to_expire(min_wts)) {
        // Renew expiring versions.
      } else if (expiring_only) {
        break;
      } else {
        if (head(cf_id, row_id_begin)->older_rv == rv) {
          if (!StaticConfig::kInlinedRowVersion || !cf.inlining) break;
          if (rv->is_inlined() || cf.inlined_rv_size_cls < rv->size_cls) break;
          // Renew non-inlined versions.
        } else {
          // Renew non-latest versions.
        }
      }

      // rts does not need checking becaue wts <= rts.
      while (true) {
        if (!tx.begin()) return false;

        // if (head(cf_id, row_id_begin)->inlined_rv->status ==
        //     RowVersionStatus::kInvalid)
        //   printf("unused inlined_rv\n");

        RowAccessHandle<StaticConfig> rah(&tx);
        if (!rah.peek_row(this, cf_id, row_id_begin, false, true, true) ||
            !rah.read_row() || !rah.write_row()) {
          // tx.abort();
          // printf("row_id=%" PRIu64 "\n", row_id_begin);
          // printf("status=%d\n", static_cast<int>(rv->status));
          // printf("inlined_rv status=%d\n",
          //        static_cast<int>(head(row_id_begin)->inlined_rv->status));
          // printf("\n");
          continue;
        }

        if (tx.commit()) break;
      }

      // Cause garbage collection ignoring StaticConfig::kMinQuiescenceInterval.
      ctx->quiescence();
      ctx->gc(false);
    }
  }

  return true;
}

template <class StaticConfig>
template <typename Func>
bool Table<StaticConfig>::scan(Transaction<StaticConfig>* tx, uint16_t cf_id,
                               uint64_t off, uint64_t len, const Func& f) {
  RowAccessHandlePeekOnly<StaticConfig> rah(tx);

  uint64_t row_count = row_count_;
  for (uint64_t row_id = 0; row_id < row_count; row_id++) {
    if (head(cf_id, row_id)->older_rv == nullptr) continue;

    if (row_id + 16 < row_count_)
      rah.prefetch_row(this, cf_id, row_id + 16, off, len);

    if (!rah.peek_row(this, cf_id, row_id, false, false, false)) return false;

    f(rah);

    rah.reset();
  }
  return true;
}

template <class StaticConfig>
void Table<StaticConfig>::print_table_status() const {
  uint64_t net_row_count = row_count_;
  for (uint16_t i = 0; i < db_->thread_count(); i++)
    net_row_count -= db_->context(i)->free_rows_[this].size();

  printf("total row count: %10" PRIu64 "\n", row_count_);
  printf("net row count:   %10" PRIu64 "\n", net_row_count);

  for (uint16_t cf_id = 0; cf_id < cf_count_; cf_id++) {
    auto& cf = cf_[cf_id];
    printf("column family %" PRIu16 "\n", cf_id);

    uint64_t inlined_pending = 0;
    uint64_t inlined_committed = 0;
    uint64_t inlined_aborted = 0;

    uint64_t inlined_unused = 0;

    uint64_t noninlined_pending = 0;
    uint64_t noninlined_committed = 0;
    uint64_t noninlined_aborted = 0;

    uint64_t chain_length = 0;

    uint64_t total_size = 0;
    uint64_t total_net_data_size = 0;

    for (uint64_t i = 0; i < row_count_; i++) {
      uint64_t net_data_size = 0;

      auto h = head(cf_id, i);
      auto rv = h->older_rv;
      while (rv != nullptr) {
        auto rv_size =
            SharedRowVersionPool<StaticConfig>::class_to_rv_size(rv->size_cls);

        if (net_data_size < rv->data_size) net_data_size = rv->data_size;

        if (StaticConfig::kInlinedRowVersion && cf.inlining &&
            rv->is_inlined()) {
          if (rv->status == RowVersionStatus::kPending)
            inlined_pending++;
          else if (rv->status >= RowVersionStatus::kCommitted)
            inlined_committed++;
          else if (rv->status == RowVersionStatus::kAborted)
            inlined_aborted++;
          else
            assert(false);
          total_size += cf.rh_size;
        } else {
          if (rv->status == RowVersionStatus::kPending)
            noninlined_pending++;
          else if (rv->status >= RowVersionStatus::kCommitted)
            noninlined_committed++;
          else if (rv->status == RowVersionStatus::kAborted)
            noninlined_aborted++;
          else
            assert(false);
          total_size += rv_size;
          chain_length++;
        }

        rv = rv->older_rv;
      }
      if (StaticConfig::kInlinedRowVersion && cf.inlining &&
          h->inlined_rv->status == RowVersionStatus::kInvalid) {
        inlined_unused++;
        total_size += cf.rh_size;
      }

      total_net_data_size += net_data_size;
    }

    printf("   i pending:    %10" PRIu64 "\n", inlined_pending);
    printf("   i committed:  %10" PRIu64 "\n", inlined_committed);
    printf("   i aborted:    %10" PRIu64 "\n", inlined_aborted);
    printf("   i unused:     %10" PRIu64 "\n", inlined_unused);
    printf("  ni pending:    %10" PRIu64 "\n", noninlined_pending);
    printf("  ni committed:  %10" PRIu64 "\n", noninlined_committed);
    printf("  ni aborted:    %10" PRIu64 "\n", noninlined_aborted);
    auto total = inlined_pending + inlined_committed + inlined_aborted +
                 inlined_unused + noninlined_pending + noninlined_committed +
                 noninlined_aborted;
    printf("  total:         %10" PRIu64 "", total);
    printf(
        " (%6.2lf%% overhead)\n",
        100. * static_cast<double>(total) / static_cast<double>(net_row_count) -
            100.);

    printf("  avg chain len: %.2lf\n", static_cast<double>(chain_length) /
                                           static_cast<double>(net_row_count));
    printf("  memory use:    %10.3lf MB\n",
           static_cast<double>(total_size) * 0.000001);
    printf("  net data:      %10.3lf MB\n",
           static_cast<double>(total_net_data_size) * 0.000001);
    printf("\n");
  }
}
}
}

#endif
