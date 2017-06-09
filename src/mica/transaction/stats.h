#pragma once
#ifndef MICA_TRANSACTION_STATS_H_
#define MICA_TRANSACTION_STATS_H_

#include <algorithm>
#include "mica/util/stopwatch.h"
#include "mica/util/memcpy.h"

namespace mica {
namespace transaction {
struct Stats {
  // kCollectCommitStats
  uint64_t tx_count;
  uint64_t committed_count;
  uint64_t aborted_by_get_row_count;
  uint64_t aborted_by_pre_validation_count;
  uint64_t aborted_by_deferred_row_version_insert_count;
  uint64_t aborted_by_main_validation_count;
  uint64_t aborted_by_logging_count;
  uint64_t aborted_by_application_count;

  // kCollectCommitStats
  uint64_t tx_time;
  uint64_t committed_time;
  uint64_t aborted_by_get_row_time;
  uint64_t aborted_by_pre_validation_time;
  uint64_t aborted_by_deferred_row_version_insert_time;
  uint64_t aborted_by_main_validation_time;
  uint64_t aborted_by_logging_time;
  uint64_t aborted_by_application_time;

  // ActiveTiming
  uint64_t worker;
  uint64_t timestamping;
  uint64_t alloc;
  uint64_t dealloc;
  uint64_t execution_read;
  uint64_t execution_write;
  uint64_t index_read;
  uint64_t index_write;
  uint64_t row_copy;
  uint64_t wait_for_pending;
  uint64_t sort_wset;
  uint64_t pre_validation;
  uint64_t deferred_row_insert;
  uint64_t rts_update;
  uint64_t main_validation;
  uint64_t logging;
  uint64_t write;
  uint64_t rollback;
  uint64_t gc;
  uint64_t backoff;

  // kCollectProcessingStats
  uint64_t insert_row_count;
  uint64_t delete_row_count;
  uint64_t refill_rows_count;
  uint64_t return_rows_count;
  uint64_t gc_inc_count;
  uint64_t gc_forced_count;

  // kCollectProcessingStats
  uint64_t max_read_chain_len;
  uint64_t max_write_chain_len;
  uint64_t max_write_trials;
  uint64_t max_deferred_write_chain_len;
  uint64_t max_deferred_write_trials;
  uint64_t max_hash_index_chain_len;
  uint64_t max_gc_dealloc_chain_len;

  Stats() { reset(); }

  void reset() { ::mica::util::memset(this, 0, sizeof(Stats)); }

  Stats& operator+=(const Stats& o) {
    tx_count += o.tx_count;
    committed_count += o.committed_count;
    aborted_by_get_row_count += o.aborted_by_get_row_count;
    aborted_by_pre_validation_count += o.aborted_by_pre_validation_count;
    aborted_by_deferred_row_version_insert_count +=
        o.aborted_by_deferred_row_version_insert_count;
    aborted_by_main_validation_count += o.aborted_by_main_validation_count;
    aborted_by_logging_count += o.aborted_by_logging_count;
    aborted_by_application_count += o.aborted_by_application_count;

    tx_time += o.tx_time;
    committed_time += o.committed_time;
    aborted_by_get_row_time += o.aborted_by_get_row_time;
    aborted_by_pre_validation_time += o.aborted_by_pre_validation_time;
    aborted_by_deferred_row_version_insert_time +=
        o.aborted_by_deferred_row_version_insert_time;
    aborted_by_main_validation_time += o.aborted_by_main_validation_time;
    aborted_by_logging_time += o.aborted_by_logging_time;
    aborted_by_application_time += o.aborted_by_application_time;

    worker += o.worker;
    timestamping += o.timestamping;
    alloc += o.alloc;
    dealloc += o.dealloc;
    execution_read += o.execution_read;
    execution_write += o.execution_write;
    index_read += o.index_read;
    index_write += o.index_write;
    row_copy += o.row_copy;
    wait_for_pending += o.wait_for_pending;
    sort_wset += o.sort_wset;
    pre_validation += o.pre_validation;
    deferred_row_insert += o.deferred_row_insert;
    rts_update += o.rts_update;
    main_validation += o.main_validation;
    logging += o.logging;
    write += o.write;
    rollback += o.rollback;
    gc += o.gc;
    backoff += o.backoff;

    insert_row_count += o.insert_row_count;
    delete_row_count += o.delete_row_count;
    refill_rows_count += o.refill_rows_count;
    return_rows_count += o.return_rows_count;
    gc_inc_count += o.gc_inc_count;
    gc_forced_count += o.gc_forced_count;

    max_read_chain_len = std::max(max_read_chain_len, o.max_read_chain_len);
    max_write_chain_len = std::max(max_write_chain_len, o.max_write_chain_len);
    max_write_trials = std::max(max_write_trials, o.max_write_trials);
    max_deferred_write_chain_len =
        std::max(max_deferred_write_chain_len, o.max_deferred_write_chain_len);
    max_deferred_write_trials =
        std::max(max_deferred_write_trials, o.max_deferred_write_trials);
    max_hash_index_chain_len =
        std::max(max_hash_index_chain_len, o.max_hash_index_chain_len);
    max_gc_dealloc_chain_len =
        std::max(max_gc_dealloc_chain_len, o.max_gc_dealloc_chain_len);
    return *this;
  }
} __attribute__((aligned(64)));

class TimingStack {
 public:
  typedef ::mica::util::Stopwatch Stopwatch;

  static constexpr uint16_t kMaxDepth = 16;

  TimingStack(Stats* stats, const Stopwatch* sw)
      : stats_(stats), sw_(sw), depth_(0) {
    start_ = sw_->now();
  }

 private:
  friend class ActiveTiming;

  Stats* stats_;
  const Stopwatch* sw_;

  uint64_t start_;
  uint64_t* targets_[kMaxDepth];
  uint16_t depth_;
};

class ActiveTiming {
 public:
  ActiveTiming(TimingStack* ts, uint64_t Stats::*target) : ts_(ts) {
    update();

    assert(ts_->depth_ < TimingStack::kMaxDepth);
    ts_->targets_[ts_->depth_++] = &(ts_->stats_->*target);
  }

  ~ActiveTiming() {
    update();
    ts_->depth_--;
  }

  void switch_to(uint64_t Stats::*target) {
    assert(ts_->depth_ != 0);
    update();
    ts_->targets_[ts_->depth_ - 1] = &(ts_->stats_->*target);
  }

 private:
  TimingStack* ts_;

  void update() {
    uint64_t now = ts_->sw_->now();
    if (ts_->depth_ != 0) *ts_->targets_[ts_->depth_ - 1] += now - ts_->start_;
    ts_->start_ = now;
  }
};

class DummyTiming {
 public:
  DummyTiming(TimingStack*, uint64_t Stats::*) {}
  void switch_to(uint64_t Stats::*) {}
};
}
}

#endif
