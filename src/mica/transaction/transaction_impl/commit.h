#pragma once
#ifndef MICA_TRANSACTION_TRANSACTION_IMPL_COMMIT_H_
#define MICA_TRANSACTION_TRANSACTION_IMPL_COMMIT_H_

#include <algorithm>  // For std::sort().
#include <unistd.h>

namespace mica {
namespace transaction {
template <class StaticConfig>
bool Transaction<StaticConfig>::begin(bool peek_only,
                                      const Timestamp* causally_after_ts) {
  Timing t(ctx_->timing_stack(), &Stats::timestamping);

  if (!ctx_->db_->is_active(ctx_->thread_id_)) return false;

  // while (StaticConfig::kMaxGCQueueSize - ctx_->gc_items_.size() <
  //        StaticConfig::kMaxAccessSize) {
  //   ctx_->idle();
  //   ctx_->gc(true);
  // }

  if (began_)
    abort();
  else
    began_ = true;

  begin_time_ = ctx_->db_->sw()->now();

  peek_only_ = peek_only;

  if (StaticConfig::kCollectExtraCommitStats) {
    abort_reason_target_count_ = &ctx_->stats().aborted_by_application_count;
    abort_reason_target_time_ = &ctx_->stats().aborted_by_application_time;
  }

  while (true) {
    ts_ = ctx_->generate_timestamp(peek_only);

    // TODO: We should bump the clock instead of waiting for a high timestamp.
    if (causally_after_ts != nullptr) {
      if (ts_ <= *causally_after_ts) {
        ::mica::util::pause();
        continue;
      }
    }

    if (!StaticConfig::kReserveAfterAbort) break;

    bool retry = false;
    RAH rah(this);
    for (auto& item : to_reserve_) {
      rah.reset();
      if (!peek_row(rah, item.tbl, item.cf_id, item.row_id, false,
                    item.read_hint, item.write_hint)) {
        retry = true;
        break;
      }

      // rah.access_item_->read_rv->rts.update(ts_);
    }
    if (!retry) break;
  }

  if (StaticConfig::kCollectROTXStalenessStats && peek_only) {
    auto clock_diff = ctx_->wts_.get().clock_diff(ctx_->rts_.get());
    auto diff_us = clock_diff / ctx_->db_->sw()->c_1_usec();
    ctx_->ro_tx_staleness_.update(diff_us);
  }

  if (StaticConfig::kReserveAfterAbort) to_reserve_.clear();

  access_size_ = 0;
  iset_size_ = 0;
  rset_size_ = 0;
  wset_size_ = 0;

  access_bucket_count_ = 0;

  if (StaticConfig::kVerbose) printf("begin: ts=%" PRIu64 "\n", ts_.t2);

  return true;
}

template <class StaticConfig>
void Transaction<StaticConfig>::sort_wset() {
  // Sort the write set's rows by contention level in descending order (high
  // wts to low wts).  This reduces visible row footprint by failing as soon
  // as possible before inserting more rows.
  Timestamp wts[StaticConfig::kMaxAccessSize];

  for (auto j = 0; j < wset_size_; j++) {
    auto i = wset_idx_[j];
    auto item = &accesses_[i];

    // This is very slightly faster than below.
    wts[i] = item->head->older_rv->wts;

    // wts[i] = item->read_rv->wts;
  }

  if (StaticConfig::kPartialSortSize == static_cast<uint64_t>(-1) ||
      wset_size_ <= StaticConfig::kPartialSortSize * 3) {
    // Full sort if partial sort is not requested or the array size is too
    // small.  sort() is typically faster than partial_sort() if the middle
    // offset is larger than 30--40% of the total size.
    std::sort(wset_idx_, wset_idx_ + wset_size_,
              [&wts](auto a, auto b) { return wts[a] > wts[b]; });
  } else {
    std::partial_sort(wset_idx_, wset_idx_ + StaticConfig::kPartialSortSize,
                      wset_idx_ + wset_size_,
                      [&wts](auto a, auto b) { return wts[a] > wts[b]; });
    // printf("%" PRIu16 "\n", wset_size_);
  }

  // std::sort(wset_idx_, wset_idx_ + wset_size_, [this](auto a, auto b) {
  //   return this->accesses_[a].latest_wts > this->accesses_[b].latest_wts;
  // });

  // Insertion sort.
  // for (int j = 1; j < wset_size_; j++) {
  //   auto j_idx = wset_idx_[j];
  //   auto& j_wts = wts[j_idx];
  //   int k = j - 1;
  //   while (k >= 0 && wts[wset_idx_[k]] < j_wts) {
  //     wset_idx_[k + 1] = wset_idx_[k];
  //     k--;
  //   }
  //   wset_idx_[k + 1] = j_idx;
  // }
}

template <class StaticConfig>
bool Transaction<StaticConfig>::check_version() {
  for (auto i = 0; i < access_size_; i++) {
    auto item = &accesses_[i];

    // These states do not need any validation.
    if (item->state == RowAccessState::kInvalid ||
        item->state == RowAccessState::kNew ||
        item->state == RowAccessState::kPeek)
      continue;

    auto rv = item->newer_rv->older_rv;
    if (item->write_rv == nullptr)
      locate<true, false, true>(item->newer_rv, rv);
    else
      locate<true, true, true>(item->newer_rv, rv);
    if (rv == nullptr) {
      if (StaticConfig::kReserveAfterAbort)
        reserve(item->tbl, item->cf_id, item->row_id,
                item->state == RowAccessState::kRead ||
                    item->state == RowAccessState::kReadWrite ||
                    item->state == RowAccessState::kReadDelete,
                item->state == RowAccessState::kWrite ||
                    item->state == RowAccessState::kReadWrite ||
                    item->state == RowAccessState::kDelete ||
                    item->state == RowAccessState::kReadDelete);
      return false;
    }

    if (rv != item->read_rv && (item->state == RowAccessState::kRead ||
                                item->state == RowAccessState::kReadWrite ||
                                item->state == RowAccessState::kReadDelete)) {
      if (StaticConfig::kReserveAfterAbort)
        reserve(item->tbl, item->cf_id, item->row_id, true,
                item->state == RowAccessState::kWrite ||
                    item->state == RowAccessState::kReadWrite ||
                    item->state == RowAccessState::kDelete ||
                    item->state == RowAccessState::kReadDelete);
      return false;
    }
  }
  return true;
}

template <class StaticConfig>
void Transaction<StaticConfig>::update_rts() {
  for (auto j = 0; j < rset_size_; j++) {
    auto i = rset_idx_[j];
    auto item = &accesses_[i];

    item->read_rv->rts.update(ts_);
  }
}

template <class StaticConfig>
void Transaction<StaticConfig>::write() {
  // Row changes are visible.
  for (auto j = 0; j < wset_size_; j++) {
    auto i = wset_idx_[j];
    auto item = &accesses_[i];

    if (item->state == RowAccessState::kDelete ||
        item->state == RowAccessState::kReadDelete)
      item->write_rv->status = RowVersionStatus::kDeleted;
    else
      item->write_rv->status = RowVersionStatus::kCommitted;
  }

  ::mica::util::memory_barrier();

  // auto gc_epoch = ctx_->db_->gc_epoch();

  for (auto j = 0; j < wset_size_; j++) {
    auto i = wset_idx_[j];
    auto item = &accesses_[i];

    if (item->write_rv->older_rv != nullptr) {
      uint8_t deleted = item->state == RowAccessState::kDelete ||
                        item->state == RowAccessState::kReadDelete;
      ctx_->schedule_gc(/*gc_epoch,*/ ts_, item->tbl, item->cf_id, deleted,
                        item->row_id, item->head, item->write_rv);
    }
  }
}

template <class StaticConfig>
template <class WriteFunc>
bool Transaction<StaticConfig>::commit(Result* detail,
                                       const WriteFunc& write_func) {
  Timing t(ctx_->timing_stack(), &Stats::main_validation);

  if (!began_) {
    if (detail != nullptr) *detail = Result::kInvalid;
    return false;
  }

  //if (!peek_only_) {

  if (StaticConfig::kVerbose) printf("try_to_commit: ts=%" PRIu64 "\n", ts_.t2);

  if (false) {
    // Try to amend the timestamp to meet the read timestamp requirement for the write est.
    Timestamp max_write_rts = ts_;

    for (auto i = 0; i < access_size_; i++) {
      auto item = &accesses_[i];

      if (item->state == RowAccessState::kInvalid ||
          item->state == RowAccessState::kNew ||
          item->state == RowAccessState::kPeek)
        continue;

      if (item->state == RowAccessState::kWrite ||
          item->state == RowAccessState::kReadWrite ||
          item->state == RowAccessState::kReadDelete) {
        auto rv = item->read_rv;
        auto rts = rv->rts.get();
        if (max_write_rts < rts) max_write_rts = rts;
      }
    }

    if (ts_ != max_write_rts) {
      // printf("ts=%" PRIu64 " max_write_rts=%" PRIu64 "\n", ts_.t2,
      //        max_write_rts.t2);
      // XXX: Hack to increment the timestamp and clock.
      auto c = (max_write_rts.t2 >> 8) + 1;
      ts_.t2 = (c << 8) | (ts_.t2 & 0xff);
      // printf("old clock=%" PRIu64 "\n", ctx_->adjusted_clock_);
      ctx_->adjusted_clock_ += c - ((ctx_->adjusted_clock_ << 8) >> 8);
      // printf("new clock=%" PRIu64 "\n", ctx_->adjusted_clock_);

      for (auto i = 0; i < access_size_; i++) {
        auto item = &accesses_[i];
        if (item->write_rv != nullptr) {
          item->write_rv->wts = ts_;
          item->write_rv->rts.write(ts_);
        }

        // Allow refinding the newer versions using the new timestamp.
        item->newer_rv = item->head;
      }
    }
  }

  if (consecutive_commits_ < 5) {
    if (StaticConfig::kSortWriteSetByContention) {
      t.switch_to(&Stats::sort_wset);
      if (StaticConfig::kVerbose)
        printf("sort_wset_by_contention: ts=%" PRIu64 "\n", ts_.t2);
      sort_wset();
    }

    if (StaticConfig::kPreValidation) {
      t.switch_to(&Stats::pre_validation);
      if (StaticConfig::kVerbose)
        printf("pre_validation: ts=%" PRIu64 "\n", ts_.t2);
      if (!check_version()) {
        if (StaticConfig::kCollectExtraCommitStats) {
          abort_reason_target_count_ =
              &ctx_->stats().aborted_by_pre_validation_count;
          abort_reason_target_time_ =
              &ctx_->stats().aborted_by_pre_validation_time;
        }
        abort();
        if (detail != nullptr) *detail = Result::kAbortedByPreValidation;
        return false;
      }
    }
  }

  {
    t.switch_to(&Stats::deferred_row_insert);
    if (StaticConfig::kVerbose)
      printf("deferred_version_insert: ts=%" PRIu64 "\n", ts_.t2);
    if (!insert_version_deferred()) {
      if (StaticConfig::kCollectExtraCommitStats) {
        abort_reason_target_count_ =
            &ctx_->stats().aborted_by_deferred_row_version_insert_count;
        abort_reason_target_time_ =
            &ctx_->stats().aborted_by_deferred_row_version_insert_time;
      }
      abort();
      if (detail != nullptr)
        *detail = Result::kAbortedByDeferredRowVersionInsert;
      return false;
    }
  }

  {
    t.switch_to(&Stats::rts_update);
    if (StaticConfig::kVerbose) printf("rts_update: ts=%" PRIu64 "\n", ts_.t2);
    update_rts();
  }

  {
    t.switch_to(&Stats::main_validation);
    if (StaticConfig::kVerbose)
      printf("main_validation: ts=%" PRIu64 "\n", ts_.t2);
    if (!check_version()) {
      if (StaticConfig::kCollectExtraCommitStats) {
        abort_reason_target_count_ =
            &ctx_->stats().aborted_by_main_validation_count;
        abort_reason_target_time_ =
            &ctx_->stats().aborted_by_main_validation_time;
      }
      abort();
      if (detail != nullptr) *detail = Result::kAbortedByMainValidation;
      return false;
    }
  }

  {
    t.switch_to(&Stats::logging);
    if (StaticConfig::kVerbose) printf("logging: ts=%" PRIu64 "\n", ts_.t2);
    if (!ctx_->db_->logger()->log(this)) {
      if (StaticConfig::kCollectExtraCommitStats) {
        abort_reason_target_count_ = &ctx_->stats().aborted_by_logging_count;
        abort_reason_target_time_ = &ctx_->stats().aborted_by_logging_time;
      }
      abort();
      if (detail != nullptr) *detail = Result::kAbortedByLogging;
      return false;
    }
  }

  {
    t.switch_to(&Stats::write);
    if (StaticConfig::kVerbose) printf("write: ts=%" PRIu64 "\n", ts_.t2);

    if (!write_func()) return false;

    // We must insert new rows before marking anything committed because earlier
    // committed rows may have row IDs to new rows.
    insert_row_deferred();

    write();
  }

  // }    // if (peek_only_)

  began_ = false;

  if (StaticConfig::kStragglerAvoidance) ctx_->clock_boost_ = 0;
  if (StaticConfig::kReserveAfterAbort) to_reserve_.clear();
  if (consecutive_commits_ < 100) consecutive_commits_++;

  if (StaticConfig::kCollectCommitStats) {
    auto now = ctx_->db_->sw()->now();
    auto diff = now - begin_time_;
    ctx_->stats().tx_count++;
    ctx_->stats().tx_time += diff;
    ctx_->stats().committed_count++;
    ctx_->stats().committed_time += diff;
    if (StaticConfig::kCollectExtraCommitStats)
      ctx_->commit_latency_.update(diff / ctx_->db_->sw()->c_1_usec());

    if (last_commit_time_ != 0)
      ctx_->inter_commit_latency_.update((now - last_commit_time_) /
                                         ctx_->db_->sw()->c_1_usec());
    last_commit_time_ = now;
  }

  maintenance();

  if (detail != nullptr) *detail = Result::kCommitted;
  return true;
}

template <class StaticConfig>
bool Transaction<StaticConfig>::abort(bool skip_backoff) {
  if (!began_) return false;

  Timing t(ctx_->timing_stack(), &Stats::rollback);

  if (StaticConfig::kVerbose) printf("abort: ts=%" PRIu64 "\n", ts_.t2);

  // Delete the last insert first so that we clean up any newly allocated row
  // IDs after cleaning up related versions.
  uint16_t j = iset_size_;
  while (j > 0) {
    j--;
    auto i = iset_idx_[j];
    auto item = &accesses_[i];

    assert(item->write_rv != nullptr);

    assert(!item->inserted);
    // Release rows that are never inserted or became visible (as it is a new
    // row).
    if (StaticConfig::kVerbose)
      printf("abort: ts=%" PRIu64 " deallocate: %p\n", ts_.t2, item->write_rv);

    ctx_->deallocate_version(item->write_rv);

    assert(item->read_rv == nullptr);
    assert(item->head == item->newer_rv);
    assert(item->head->older_rv == nullptr);

    // Free up row IDs for failed row inserts.
    if (item->cf_id == 0) {
      // OK to use non-atomics as no one else is accessing this row.
      ctx_->deallocate_row(item->tbl, item->row_id);
    }
  }

  // Different from inserts, we must keep the write set order to deallocate row
  // IDs correctly after deallocating all versions using that row ID during GC.
  for (auto j = 0; j < wset_size_; j++) {
    auto i = wset_idx_[j];
    auto item = &accesses_[i];

    assert(item->write_rv != nullptr);

    if (!item->inserted) {
      // Release rows that are never inserted or became visible (as it is a new
      // row).
      if (StaticConfig::kVerbose)
        printf("abort: ts=%" PRIu64 " deallocate: %p\n", ts_.t2,
               item->write_rv);

      ctx_->deallocate_version(item->write_rv);
    } else {
      // New row inserts should have been processed as a non-inserted version.
      assert(item->read_rv != nullptr);

      // Mark the (visible) version as aborted.
      item->write_rv->status = RowVersionStatus::kAborted;
    }
  }

  began_ = false;

  if (StaticConfig::kStragglerAvoidance)
    ctx_->clock_boost_ =
        static_cast<uint64_t>(StaticConfig::kStragglerAvoidanceIncrement);
  consecutive_commits_ = 0;

  if (StaticConfig::kCollectCommitStats) {
    auto diff = ctx_->db_->sw()->now() - begin_time_;
    ctx_->stats().tx_count++;
    ctx_->stats().tx_time += diff;
    if (StaticConfig::kCollectExtraCommitStats) {
      (*abort_reason_target_count_)++;
      (*abort_reason_target_time_) += diff;
    } else {
      ctx_->stats().aborted_by_main_validation_count++;
      ctx_->stats().aborted_by_main_validation_time += diff;
    }
    if (StaticConfig::kCollectExtraCommitStats)
      ctx_->abort_latency_.update(diff / ctx_->db_->sw()->c_1_usec());
  }

  maintenance();

  if (StaticConfig::kBackoff && !skip_backoff) {
    t.switch_to(&Stats::backoff);
    backoff();
  }

  return true;
}

template <class StaticConfig>
void Transaction<StaticConfig>::backoff() {
  auto max_backoff_time = ctx_->db_->backoff();

  // Ignore very small backoff time (10 cycles).
  if (max_backoff_time <= 10.) return;

  uint64_t now = ctx_->db_->sw()->now();
  uint64_t ready = now + static_cast<uint64_t>(max_backoff_time *
                                               ctx_->backoff_rand_.next_f64());

  bool use_usleep = false;
  uint64_t sleep_us;
#ifndef NDEBUG
  // Suppress a compiler warning.
  sleep_us = 0;
#endif
  if (StaticConfig::kPairwiseSleeping) {
    uint64_t us = ctx_->db_->sw()->c_1_usec();
    sleep_us = (ready - now) / us;
    if (sleep_us >= StaticConfig::kPairwiseSleepingMinTime &&
        ((now / (StaticConfig::kPairwiseSleepingSpan * us)) & 1) ==
            ctx_->pair_selector_)
      use_usleep = true;
  }
  if (use_usleep) {
    // The best is to use MONITOR/MWAIT, but we cannot do that in userspace
    // (usually).
    usleep(static_cast<useconds_t>(sleep_us));
  } else {
    while (ready > now) {
      ::mica::util::pause();
      now = ctx_->db_->sw()->now();
    }
  }
}

template <class StaticConfig>
void Transaction<StaticConfig>::maintenance() {
  assert(!began_);

  ctx_->db_->update_backoff(ctx_->thread_id_);

  // uint64_t now = ctx_->db_->sw()->now();
  uint64_t now = begin_time_;

  if (static_cast<int64_t>(now - ctx_->last_quiescence_) >
      StaticConfig::kMinQuiescenceInterval *
          static_cast<int64_t>(ctx_->db_->sw()->c_1_usec())) {
    ctx_->last_quiescence_ = now;

    ctx_->quiescence();

    ctx_->gc(false);
  }

  if (static_cast<int64_t>(now - ctx_->last_clock_sync_) >
      StaticConfig::kMinClockSyncInterval *
          static_cast<int64_t>(ctx_->db_->sw()->c_1_usec())) {
    ctx_->last_clock_sync_ = now;

    ctx_->synchronize_clock();
  }
}
}
}

#endif
