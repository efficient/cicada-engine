#pragma once
#ifndef MICA_TRANSACTION_CONTEXT_H_
#define MICA_TRANSACTION_CONTEXT_H_

#include <queue>
#include "mica/transaction/stats.h"
#include "mica/transaction/row.h"
#include "mica/transaction/table.h"
#include "mica/transaction/db.h"
#include "mica/transaction/row_version_pool.h"
#include "mica/util/memcpy.h"
#include "mica/util/rand.h"
#include "mica/util/latency.h"
// #include "mica/util/queue.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
struct RowHead;
template <class StaticConfig>
class Table;
template <class StaticConfig>
class Transaction;
template <class StaticConfig>
class DB;

template <class StaticConfig>
class Context {
 public:
  typedef typename StaticConfig::Timestamp Timestamp;
  typedef typename StaticConfig::ConcurrentTimestamp ConcurrentTimestamp;
  typedef typename StaticConfig::Timing Timing;

  Context(DB<StaticConfig>* db, uint16_t thread_id, uint8_t numa_id)
      : db_(db),
        thread_id_(thread_id),
        numa_id_(numa_id),
        backoff_rand_(static_cast<uint64_t>(thread_id)),
        timing_stack_(&stats_, db_->sw()) {
    if (StaticConfig::kPairwiseSleeping) {
      auto active_count = db_->thread_count();
      auto count = ::mica::util::lcore.lcore_count();
      auto half_count = count / 2;
      if (thread_id_ < half_count) {
        if (thread_id_ + half_count < active_count)
          pair_selector_ = 0;
        else
          pair_selector_ = 2;  // No matching hyperthread pair.
      } else
        pair_selector_ = 1;
    }

    clock_ = 0;
    clock_boost_ = 0;
    adjusted_clock_ = 0;

    next_sync_thread_id_ = 0;

    last_tsc_ = ::mica::util::rdtsc();
    last_quiescence_ = db_->sw()->now();
    last_clock_sync_ = db_->sw()->now();
  }

  ~Context() {}

  DB<StaticConfig>* db() { return db_; }

  uint64_t clock() const { return clock_; }

  Timestamp wts() const { return wts_.get(); }
  Timestamp rts() const { return rts_.get(); }

  uint16_t thread_id() const { return thread_id_; }
  uint8_t numa_id() const { return numa_id_; }

  const Stats& stats() const { return stats_; }
  Stats& stats() { return stats_; }

  const ::mica::util::Latency& inter_commit_latency() const {
    return inter_commit_latency_;
  }
  ::mica::util::Latency& inter_commit_latency() {
    return inter_commit_latency_;
  }

  const ::mica::util::Latency& commit_latency() const {
    return commit_latency_;
  }
  ::mica::util::Latency& commit_latency() { return commit_latency_; }

  const ::mica::util::Latency& abort_latency() const { return abort_latency_; }
  ::mica::util::Latency& abort_latency() { return abort_latency_; }

  const ::mica::util::Latency& ro_tx_staleness() const {
    return ro_tx_staleness_;
  }
  ::mica::util::Latency& ro_tx_staleness() { return ro_tx_staleness_; }

  TimingStack* timing_stack() { return &timing_stack_; }

  void set_clock(uint64_t ref_clock) {
    clock_ = ref_clock;
    clock_boost_ = 0;
    adjusted_clock_ = ref_clock;

    last_tsc_ = ::mica::util::rdtsc();
    last_clock_sync_ = db_->sw()->now();
  }

  void synchronize_clock() {
    uint16_t thread_id = next_sync_thread_id_;
    if (++next_sync_thread_id_ == db_->thread_count()) next_sync_thread_id_ = 0;

    if (!db_->is_active(thread_id)) return;

    auto ctx = db_->context(thread_id);

    // Update the local clock.
    update_clock();

    ::mica::util::memory_barrier();

    // Read the other thread's clock.  This clock is slightly behind because of
    // the latency.  We cannot correct this error because this is one-way
    // synchronization.
    auto clock = ctx->clock();

    // Take it if the other one is faster.
    int64_t clock_diff = static_cast<int64_t>(clock - clock_);

    if (clock_diff > 0) clock_ = clock;

    // Note that we do not reset last_clock_sync_ so that the next clock update
    // can include the latency of reading the remote clock.

    // TODO: Would it be more accurate to consider only the half of the clock
    // read latency, as if in the network I/O?
  }

  void update_clock() {
    uint64_t tsc = ::mica::util::rdtsc();

    int64_t tsc_diff = static_cast<int64_t>(tsc - last_tsc_);
    if (tsc_diff <= 0)
      tsc_diff = 1;
    else if (tsc_diff > StaticConfig::kMaxClockIncrement)
      tsc_diff = StaticConfig::kMaxClockIncrement;

    clock_ += static_cast<uint64_t>(tsc_diff);
    last_tsc_ = tsc;
  }

  Timestamp generate_timestamp(bool for_peek_only_transaction = false) {
    update_clock();

    auto adjusted_clock = clock_ + clock_boost_;
    if (static_cast<int64_t>(adjusted_clock - adjusted_clock_) <= 0)
      adjusted_clock = adjusted_clock_ + 1;
    adjusted_clock_ = adjusted_clock;

    // TODO: Obtain the era.
    const uint16_t era = 0;

    auto wts = Timestamp::make(era, adjusted_clock, thread_id_);
    wts_.write(wts);

    Timestamp rts = db_->min_wts();
    // Make sure rts < wts; we do not need to worry about collisions by
    // subtracting 1 because (1) every thread does it and (2) timestamp
    // collisions are benign for read-only transactions.
    rts.t2--;
    rts_.write(rts);

    if (for_peek_only_transaction)
      return rts;
    else
      return wts;
  }

  uint64_t allocate_row(Table<StaticConfig>* tbl) {
    auto& free_row_ids = free_rows_[tbl];
    if (free_row_ids.empty()) {
      if (!tbl->allocate_rows(this, free_row_ids))
        return static_cast<uint64_t>(-1);
    }
    auto row_id = free_row_ids.back();
    free_row_ids.pop_back();

    if (StaticConfig::kVerbose) printf("new row ID = %" PRIu64 "\n", row_id);

    // gc_ts needs to be initialized with a near-past timestamp.
    auto min_wts = db_->min_wts();
    for (uint16_t cf_id = 0; cf_id < tbl->cf_count(); cf_id++) {
      auto g = tbl->gc_info(cf_id, row_id);
      g->gc_lock = 0;
      g->gc_ts.init(min_wts);

      assert(tbl->head(cf_id, row_id)->older_rv == nullptr);
    }

    return row_id;
  }

  void deallocate_row(Table<StaticConfig>* tbl, uint64_t row_id) {
    free_rows_[tbl].push_back(row_id);

    // TODO: Defragement and return a group of rows.
  }

  template <bool NewRow>
  RowVersion<StaticConfig>* allocate_version(Table<StaticConfig>* tbl,
                                             uint16_t cf_id, uint64_t row_id,
                                             RowHead<StaticConfig>* head,
                                             uint64_t data_size) {
    auto size_cls =
        SharedRowVersionPool<StaticConfig>::data_size_to_class(data_size);

    if (StaticConfig::kInlinedRowVersion && tbl->inlining(cf_id) &&
        size_cls <= tbl->inlined_rv_size_cls(cf_id)) {
      if (!StaticConfig::kInlineWithAltRow && NewRow) {
        assert(head->inlined_rv->status == RowVersionStatus::kInvalid);
        assert(head->inlined_rv->is_inlined());
        // NewRow is guaranteed to have an inlined version available if
        // alternative rows are disabled.
        head->inlined_rv->data_size = static_cast<uint32_t>(data_size);
        return head->inlined_rv;
      } else if (head->inlined_rv->status == RowVersionStatus::kInvalid &&
                 __sync_bool_compare_and_swap(&head->inlined_rv->status,
                                              RowVersionStatus::kInvalid,
                                              RowVersionStatus::kPending)) {
        // Acquire an inlined version by contesting it.
        assert(head->inlined_rv->is_inlined());
        head->inlined_rv->data_size = static_cast<uint32_t>(data_size);
        return head->inlined_rv;
      } else if (StaticConfig::kInlineWithAltRow) {
        auto alt_head = tbl->alt_head(cf_id, row_id);
        if (alt_head->inlined_rv->status == RowVersionStatus::kInvalid &&
            __sync_bool_compare_and_swap(&alt_head->inlined_rv->status,
                                         RowVersionStatus::kInvalid,
                                         RowVersionStatus::kPending)) {
          // Acquire an inlined version at the alternative row by contesting it.
          assert(alt_head->inlined_rv->is_inlined());
          alt_head->inlined_rv->data_size = static_cast<uint32_t>(data_size);
          return alt_head->inlined_rv;
        }
      }
    }

    auto pool = db_->row_version_pool(thread_id_);
    auto rv = pool->allocate(size_cls);
    rv->data_size = static_cast<uint32_t>(data_size);
    return rv;
  }

  RowVersion<StaticConfig>* allocate_version_for_new_row(
      Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
      RowHead<StaticConfig>* head, uint64_t data_size) {
    return allocate_version<true>(tbl, cf_id, row_id, head, data_size);
  }

  RowVersion<StaticConfig>* allocate_version_for_existing_row(
      Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
      RowHead<StaticConfig>* head, uint64_t data_size) {
    return allocate_version<false>(tbl, cf_id, row_id, head, data_size);
  }

  void deallocate_version(RowVersion<StaticConfig>* rv) {
    if (StaticConfig::kInlinedRowVersion && rv->is_inlined()) {
      ::mica::util::memory_barrier();
      rv->status = RowVersionStatus::kInvalid;
    } else {
      auto pool = db_->row_version_pool(thread_id_);
      pool->deallocate(rv);
    }
  }

  void schedule_gc(/*uint64_t gc_epoch,*/ const Timestamp& wts,
                   Table<StaticConfig>* tbl, uint16_t cf_id, uint8_t deleted,
                   uint64_t row_id, RowHead<StaticConfig>* head,
                   RowVersion<StaticConfig>* write_rv);
  void check_gc();
  void gc(bool forced);

  void quiescence() { db_->quiescence(thread_id_); }
  void idle() { db_->idle(thread_id_); }

 private:
  friend class Table<StaticConfig>;
  friend class Transaction<StaticConfig>;

  DB<StaticConfig>* db_;
  uint16_t thread_id_;
  uint8_t numa_id_;

  uint16_t next_sync_thread_id_;

  uint64_t last_tsc_;
  uint64_t last_quiescence_;
  uint64_t last_clock_sync_;

  uint64_t clock_boost_;
  uint64_t adjusted_clock_;

  ::mica::util::Rand backoff_rand_;

  std::unordered_map<const Table<StaticConfig>*, std::vector<uint64_t>>
      free_rows_;

  struct GCItem {
    // uint64_t gc_epoch;
    Timestamp wts;
    Table<StaticConfig>* tbl;
    uint16_t cf_id;
    uint8_t deleted;  // Move here for better packing.
    uint64_t row_id;
    RowHead<StaticConfig>* head;
    RowVersion<StaticConfig>* write_rv;
  };
  std::queue<GCItem> gc_items_;
  // ::mica::util::SingleThreadedQueue<GCItem, StaticConfig::kMaxGCQueueSize>
  //     gc_items_;

  Stats stats_;
  TimingStack timing_stack_;
  ::mica::util::Latency inter_commit_latency_;
  ::mica::util::Latency commit_latency_;
  ::mica::util::Latency abort_latency_;
  ::mica::util::Latency ro_tx_staleness_;

  // For kPairwiseSleeping.
  uint64_t pair_selector_;

  // Frequently modified by the owner thread, but sometimes read by
  // other threads.
  ConcurrentTimestamp wts_ __attribute__((aligned(64)));
  ConcurrentTimestamp rts_;
  volatile uint64_t clock_;
} __attribute__((aligned(64)));
}
}

#include "context_gc.h"

#endif
