#pragma once
#ifndef MICA_TRANSACTION_CONTEXT_GC_H_
#define MICA_TRANSACTION_CONTEXT_GC_H_

namespace mica {
namespace transaction {
template <class StaticConfig>
void Context<StaticConfig>::schedule_gc(
    /*uint64_t gc_epoch,*/ const Timestamp& wts, Table<StaticConfig>* tbl,
    uint16_t cf_id, uint8_t deleted, uint64_t row_id,
    RowHead<StaticConfig>* head, RowVersion<StaticConfig>* write_rv) {
  // We need to store Timestamp separately from write_rv because write_rv may be
  // gone when we check the timestamp.

  // gc_items_.push({gc_epoch, wts, tbl, row_id, head, write_rv});
  gc_items_.push({wts, tbl, cf_id, deleted, row_id, head, write_rv});

  // if (gc_items_.full()) {
  //   fprintf(stderr, "Error: GC queue is full\n");
  //   return;
  // }
  // auto& tail = gc_items_.tail();
  // tail.wts = wts;
  // tail.tbl = tbl;
  // tail.cf_id = cf_id;
  // tail.deleted = deleted;
  // tail.row_id = row_id;
  // tail.head = head;
  // tail.write_rv = write_rv;
  // gc_items_.push_tail();

  __builtin_prefetch(tbl->gc_info(cf_id, row_id), 0, 3);
}

template <class StaticConfig>
void Context<StaticConfig>::gc(bool forced) {
  Timing t(timing_stack(), &Stats::gc);

  auto gc_row = [this](auto& ts, auto tbl, auto cf_id, auto deleted,
                       auto row_id, auto head, auto write_rv) {
    uint64_t dealloc_chain_len;
    if (StaticConfig::kCollectProcessingStats) {
      dealloc_chain_len = 0;
    }

    auto gc_info = tbl->gc_info(cf_id, row_id);

    {
      auto gc_ts = gc_info->gc_ts.get();
      // write_rv is invalid (dangling) now.
      if (gc_ts >= ts) return true;
    }

    if (!deleted) {
      if (gc_info->gc_lock == 1 ||
          __sync_lock_test_and_set(&gc_info->gc_lock, 1) == 1) {
        // Some other thread is GCing this row already.
        // Just give up on this row; maybe that thread will clean up almost
        // everything.
        return true;
      }

      // Read gc_ts again because it may have been changed before locking.
      {
        auto gc_ts = gc_info->gc_ts.get();
        if (gc_ts >= ts) {
          __sync_lock_release(&gc_info->gc_lock);
          return true;
        }
      }
    } else {
      // We do not give up locking if the GC was invoked by deletion.
      while (__sync_lock_test_and_set(&gc_info->gc_lock, 1) == 1)
        ::mica::util::pause();

      // There must be no more commits for deleted rows; we can omit the
      // checking usually.
      assert(gc_info->gc_ts.get() < ts);
    }

    if (StaticConfig::kVerbose)
      printf("gc: thread_id=%2hu min_rts=%" PRIu64 " begin prev_gc_ts=%" PRIu64
             "\n",
             thread_id_, db_->min_rts().t2, gc_info->gc_ts.get().t2);

    gc_info->gc_ts.write(ts);

    if (StaticConfig::kVerbose)
      printf("gc: thread_id=%2hu min_rts=%" PRIu64 " write_rv %p (wts=%" PRIu64
             ")\n",
             thread_id_, db_->min_rts().t2, write_rv, write_rv->wts.t2);

    assert(write_rv->status >= RowVersionStatus::kCommitted);
    assert(write_rv->wts < db_->min_rts());

    RowVersion<StaticConfig>* rv;
    bool delete_rv;

    if (write_rv->status == RowVersionStatus::kDeleted &&
        head->older_rv == write_rv) {
      // Delete the row if this row version was a "deleted" version and the row
      // has this version only.

      // This will not reclaim a deleted row if there is any aborted
      // version before a "deleted" version.
      //
      // However, with StaticConfig::kSkipPending == false, there will be no
      // newer version installed after any older pending or deleted version, so
      // it is guaranteed to have a deleted version as the first version in the
      // version list.

      // Actual row deletion (row ID deallocation) is done at bit later because
      // we are still holding a GC lock for this row.
      delete_rv = true;

      // We will deallocate this "deleted" version as well.
      rv = write_rv;
      head->older_rv = nullptr;
    } else {
      delete_rv = false;

      // Take the rest of row versions from the version chain.
      rv = write_rv->older_rv;
      write_rv->older_rv = nullptr;
    }

    // We can now release the lock because we have modified any shared data, and
    // the rest will not be visible to other threads.  Other threads can do
    // parallel GCing this row.
    __sync_lock_release(&gc_info->gc_lock);

    while (rv != nullptr) {
      // If this test fails, some bad thing is going on (accessing a GC'ed
      // row version).
      assert(rv->status != RowVersionStatus::kInvalid);
      assert(rv->wts < db_->min_rts() ||
             (delete_rv && rv->wts <= db_->min_rts()));

      if (StaticConfig::kCollectProcessingStats) dealloc_chain_len++;

      auto older_rv = rv->older_rv;
      __builtin_prefetch(older_rv, 0, 0);

      if (StaticConfig::kVerbose)
        printf("gc: thread_id=%2hu min_rts=%" PRIu64
               "   delete %p (wts=%" PRIu64 ")\n",
               thread_id_, db_->min_rts().t2, rv, rv->wts.t2);

      deallocate_version(rv);

      rv = older_rv;
    }

    if (delete_rv && cf_id == 0) {
      // Deleting the first column family implies the whole row deletion.
      deallocate_row(tbl, row_id);
    }

    if (StaticConfig::kCollectProcessingStats) {
      if (stats_.max_gc_dealloc_chain_len < dealloc_chain_len)
        stats_.max_gc_dealloc_chain_len = dealloc_chain_len;
    }

    // TODO: Reclaim aborted row versions that appear before the latest
    // committed row versions.  Maybe we can do slow scanning to reinsert any
    // committed version if it is not the first one.

    if (StaticConfig::kVerbose)
      printf("gc: thread_id=%2hu min_rts=%" PRIu64 " end\n", thread_id_,
             db_->min_rts().t2);

    return true;
  };

  // auto gc_epoch = db_->gc_epoch();
  auto min_rts = db_->min_rts();

  while (!gc_items_.empty() && /*gc_epoch - gc_items_.front().gc_epoch >= 2 &&*/
         min_rts > gc_items_.front().wts) {
    auto& item = gc_items_.front();
    if (!gc_row(item.wts, item.tbl, item.cf_id, item.deleted, item.row_id,
                item.head, item.write_rv))
      break;
    gc_items_.pop();
  }

  // while (!gc_items_.empty() && min_rts > gc_items_.head().wts) {
  //   auto& item = gc_items_.head();
  //   if (!gc_row(item.wts, item.tbl, item.cf_id, item.deleted, item.row_id,
  //               item.head, item.write_rv))
  //     break;
  //   gc_items_.pop_head();
  // }

  if (StaticConfig::kCollectProcessingStats) {
    if (forced)
      stats_.gc_forced_count++;
    else
      stats_.gc_inc_count++;
  }
}
}
}

#endif
