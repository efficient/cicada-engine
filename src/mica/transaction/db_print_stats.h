#pragma once
#ifndef MICA_TRANSACTION_DB_PRINT_STATS_H_
#define MICA_TRANSACTION_DB_PRINT_STATS_H_

namespace mica {
namespace transaction {
template <class StaticConfig>
void DB<StaticConfig>::reset_stats() {
  for (uint16_t thread_id = 0; thread_id < num_threads_; thread_id++) {
    ctxs_[thread_id]->stats().reset();
    ctxs_[thread_id]->inter_commit_latency().reset();
    ctxs_[thread_id]->commit_latency().reset();
    ctxs_[thread_id]->abort_latency().reset();
    ctxs_[thread_id]->ro_tx_staleness().reset();
  }

  last_committed_count_ = 0;

  auto now = sw_->now();
  last_backoff_update_ = now;
  last_backoff_ = backoff_;

  // Keep other backoff states.
}

template <class StaticConfig>
void DB<StaticConfig>::print_stats(double elapsed_time,
                                   double total_time) const {
  Stats stats;
  ::mica::util::Latency inter_commit_latency;
  ::mica::util::Latency commit_latency;
  ::mica::util::Latency abort_latency;
  ::mica::util::Latency ro_tx_staleness;
  for (uint16_t thread_id = 0; thread_id < num_threads_; thread_id++) {
    stats += ctxs_[thread_id]->stats();
    inter_commit_latency += ctxs_[thread_id]->inter_commit_latency();
    commit_latency += ctxs_[thread_id]->commit_latency();
    abort_latency += ctxs_[thread_id]->abort_latency();
    ro_tx_staleness += ctxs_[thread_id]->ro_tx_staleness();
  }

  printf("elapsed:                   %10.3lf sec\n", elapsed_time);
  printf("\n");

  if (StaticConfig::kCollectCommitStats) {
    printf("transactions:              %10lu (%7.3lf M/sec)\n", stats.tx_count,
           static_cast<double>(stats.tx_count) / elapsed_time / 1000000.);
    printf(
        "committed:                 %10lu (%7.3lf M/sec; %6.2lf%% "
        "attempts, "
        "%6.2lf%% "
        "time)\n",
        stats.committed_count,
        static_cast<double>(stats.committed_count) / elapsed_time / 1000000.,
        100. * static_cast<double>(stats.committed_count) /
            static_cast<double>(stats.tx_count),
        100. * static_cast<double>(stats.committed_time) /
            static_cast<double>(stats.tx_time));
    printf(
        "aborted: get_row:          %10lu (%7.3lf M/sec; %6.2lf%% "
        "attempts, "
        "%6.2lf%% time)\n",
        stats.aborted_by_get_row_count,
        static_cast<double>(stats.aborted_by_get_row_count) / elapsed_time /
            1000000.,
        100. * static_cast<double>(stats.aborted_by_get_row_count) /
            static_cast<double>(stats.tx_count),
        100. * static_cast<double>(stats.aborted_by_get_row_time) /
            static_cast<double>(stats.tx_time));
    printf(
        "aborted: pre_validation:   %10lu (%7.3lf M/sec; %6.2lf%% "
        "attempts, "
        "%6.2lf%% time)\n",
        stats.aborted_by_pre_validation_count,
        static_cast<double>(stats.aborted_by_pre_validation_count) /
            elapsed_time / 1000000.,
        100. * static_cast<double>(stats.aborted_by_pre_validation_count) /
            static_cast<double>(stats.tx_count),
        100. * static_cast<double>(stats.aborted_by_pre_validation_time) /
            static_cast<double>(stats.tx_time));
    printf(
        "aborted: d_version_insert: %10lu (%7.3lf M/sec; %6.2lf%% "
        "attempts, "
        "%6.2lf%% time)\n",
        stats.aborted_by_deferred_row_version_insert_count,
        static_cast<double>(
            stats.aborted_by_deferred_row_version_insert_count) /
            elapsed_time / 1000000.,
        100. * static_cast<double>(
                   stats.aborted_by_deferred_row_version_insert_count) /
            static_cast<double>(stats.tx_count),
        100. * static_cast<double>(
                   stats.aborted_by_deferred_row_version_insert_time) /
            static_cast<double>(stats.tx_time));
    printf(
        "aborted: main_validation:  %10lu (%7.3lf M/sec; %6.2lf%% "
        "attempts, "
        "%6.2lf%% time)\n",
        stats.aborted_by_main_validation_count,
        static_cast<double>(stats.aborted_by_main_validation_count) /
            elapsed_time / 1000000.,
        100. * static_cast<double>(stats.aborted_by_main_validation_count) /
            static_cast<double>(stats.tx_count),
        100. * static_cast<double>(stats.aborted_by_main_validation_time) /
            static_cast<double>(stats.tx_time));
    printf(
        "aborted: logging:          %10lu (%7.3lf M/sec; %6.2lf%% "
        "attempts, "
        "%6.2lf%% time)\n",
        stats.aborted_by_logging_count,
        static_cast<double>(stats.aborted_by_logging_count) / elapsed_time /
            1000000.,
        100. * static_cast<double>(stats.aborted_by_logging_count) /
            static_cast<double>(stats.tx_count),
        100. * static_cast<double>(stats.aborted_by_logging_time) /
            static_cast<double>(stats.tx_time));
    printf(
        "aborted: application:      %10lu (%7.3lf M/sec; %6.2lf%% "
        "attempts, "
        "%6.2lf%% time)\n",
        stats.aborted_by_application_count,
        static_cast<double>(stats.aborted_by_application_count) / elapsed_time /
            1000000.,
        100. * static_cast<double>(stats.aborted_by_application_count) /
            static_cast<double>(stats.tx_count),
        100. * static_cast<double>(stats.aborted_by_application_time) /
            static_cast<double>(stats.tx_time));
    printf("\n");

    printf("commit count:");
    for (uint16_t thread_id = 0; thread_id < num_threads_; thread_id++) {
      printf(" %" PRIu64 "", context(thread_id)->stats().committed_count);
    }
    printf("\n");
    printf("commit rate (%%):");
    for (uint16_t thread_id = 0; thread_id < num_threads_; thread_id++) {
      printf(" %5.1f",
             100. * static_cast<double>(
                        context(thread_id)->stats().committed_count) /
                 static_cast<double>(context(thread_id)->stats().tx_count));
    }
    printf("\n");

    printf("\n");
  }

  if (StaticConfig::kCollectCommitStats) {
    // printf("sum: %" PRIu64 " (us)\n", inter_commit_latency.sum());
    printf("inter-commit latency (us): min=%" PRIu64 ", max=%" PRIu64
           ", avg=%" PRIu64 "; 50-th=%" PRIu64 ", 95-th=%" PRIu64
           ", 99-th=%" PRIu64 ", 99.9-th=%" PRIu64 "\n",
           inter_commit_latency.min(), inter_commit_latency.max(),
           inter_commit_latency.avg(), inter_commit_latency.perc(0.50),
           inter_commit_latency.perc(0.95), inter_commit_latency.perc(0.99),
           inter_commit_latency.perc(0.999));
    if (StaticConfig::kCollectExtraCommitStats) {
      printf("      commit latency (us): min=%" PRIu64 ", max=%" PRIu64
             ", avg=%" PRIu64 "; 50-th=%" PRIu64 ", 95-th=%" PRIu64
             ", 99-th=%" PRIu64 ", 99.9-th=%" PRIu64 "\n",
             commit_latency.min(), commit_latency.max(), commit_latency.avg(),
             commit_latency.perc(0.50), commit_latency.perc(0.95),
             commit_latency.perc(0.99), commit_latency.perc(0.999));
      printf("       abort latency (us): min=%" PRIu64 ", max=%" PRIu64
             ", avg=%" PRIu64 "; 50-th=%" PRIu64 ", 95-th=%" PRIu64
             ", 99-th=%" PRIu64 ", 99.9-th=%" PRIu64 "\n",
             abort_latency.min(), abort_latency.max(), abort_latency.avg(),
             abort_latency.perc(0.50), abort_latency.perc(0.95),
             abort_latency.perc(0.99), abort_latency.perc(0.999));
    }
    printf("\n");
  }

  if (StaticConfig::kCollectROTXStalenessStats) {
    printf("     RO TX staleness (us): min=%" PRIu64 ", max=%" PRIu64
           ", avg=%" PRIu64 "; 50-th=%" PRIu64 ", 95-th=%" PRIu64
           ", 99-th=%" PRIu64 ", 99.9-th=%" PRIu64 "\n",
           ro_tx_staleness.min(), ro_tx_staleness.max(), ro_tx_staleness.avg(),
           ro_tx_staleness.perc(0.50), ro_tx_staleness.perc(0.95),
           ro_tx_staleness.perc(0.99), ro_tx_staleness.perc(0.999));
    printf("\n");
  }

  if (typeid(typename StaticConfig::Timing) ==
      typeid(::mica::transaction::ActiveTiming)) {
    double ms;
    ms = sw_->diff(stats.worker, 0) * 1000.;
    printf("worker:                   %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.timestamping, 0) * 1000.;
    printf("timestamping:             %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.alloc, 0) * 1000.;
    printf("alloc:                    %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.dealloc, 0) * 1000.;
    printf("dealloc:                  %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.execution_read, 0) * 1000.;
    printf("execution_read:           %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.execution_write, 0) * 1000.;
    printf("execution_write:          %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.index_read, 0) * 1000.;
    printf("index_read:               %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.index_write, 0) * 1000.;
    printf("index_write:              %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.row_copy, 0) * 1000.;
    printf("row_copy:                 %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.wait_for_pending, 0) * 1000.;
    printf("wait_for_pending:         %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.sort_wset, 0) * 1000.;
    printf("sort_wset:                %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.pre_validation, 0) * 1000.;
    printf("pre_validation:           %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.deferred_row_insert, 0) * 1000.;
    printf("deferred_version_insert:  %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.rts_update, 0) * 1000.;
    printf("rts_update:               %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.main_validation, 0) * 1000.;
    printf("main_validation:          %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.logging, 0) * 1000.;
    printf("logging:                  %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.write, 0) * 1000.;
    printf("write:                    %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.rollback, 0) * 1000.;
    printf("rollback:                 %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.gc, 0) * 1000.;
    printf("gc:                       %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    ms = sw_->diff(stats.backoff, 0) * 1000.;
    printf("backoff:                  %10.3lf ms (%6.2lf%%)\n", ms,
           100. * ms / (total_time * 1000.));
    printf("\n");
  }

  if (StaticConfig::kCollectProcessingStats) {
    printf("insert_row_count:             %10" PRIu64 "\n",
           stats.insert_row_count);
    printf("delete_row_count:             %10" PRIu64 "\n",
           stats.delete_row_count);
    printf("refill_rows_count:            %10" PRIu64 "\n",
           stats.refill_rows_count);
    printf("return_rows_count:            %10" PRIu64 "\n",
           stats.return_rows_count);
    printf("gc_inc_count:                 %10" PRIu64 "\n", stats.gc_inc_count);
    printf("gc_forced_count:              %10" PRIu64 "\n",
           stats.gc_forced_count);
    printf("\n");

    printf("max_read_chain_len:           %10" PRIu64 "\n",
           stats.max_read_chain_len);
    printf("max_write_chain_len:          %10" PRIu64 "\n",
           stats.max_write_chain_len);
    printf("max_write_trials:             %10" PRIu64 "\n",
           stats.max_write_trials);
    printf("max_deferred_write_chain_len: %10" PRIu64 "\n",
           stats.max_write_chain_len);
    printf("max_deferred_write_trials:    %10" PRIu64 "\n",
           stats.max_write_trials);
    printf("max_hash_index_chain_len:     %10" PRIu64 "\n",
           stats.max_hash_index_chain_len);
    printf("max_gc_dealloc_chain_len:     %10" PRIu64 "\n",
           stats.max_gc_dealloc_chain_len);
    printf("\n");
  }
}

template <class StaticConfig>
void DB<StaticConfig>::print_pool_status() const {
  for (uint16_t cls = 0; cls < SharedRowVersionPool<StaticConfig>::kClassCount;
       cls++) {
    uint64_t total_free_rows = 0;
    uint64_t total_rows = 0;

    for (uint8_t numa_id = 0; numa_id < num_numa_; numa_id++) {
      uint64_t free_rows = shared_row_version_pools_[numa_id]->free_count(cls);
      total_free_rows += free_rows;
      total_rows += shared_row_version_pools_[numa_id]->total_count(cls);
    }

    if (total_rows == 0) continue;

    for (uint16_t thread_id = 0; thread_id < num_threads_; thread_id++) {
      uint64_t free_rows = row_version_pools_[thread_id]->free_count(cls);
      total_free_rows += free_rows;
    }

    uint64_t size = SharedRowVersionPool<StaticConfig>::class_to_rv_size(cls);
    printf("row version class %" PRIu8 " (%" PRIu64 " bytes):\n", cls, size);

    printf("  in use:        %10" PRIu64 " rows     (%7.3lf GB)\n",
           total_rows - total_free_rows,
           static_cast<double>((total_rows - total_free_rows) * size) /
               1000000000.);
    printf("    free:        %10" PRIu64 " rows     (%7.3lf GB)\n",
           total_free_rows,
           static_cast<double>(total_free_rows * size) / 1000000000.);
    printf("   total:        %10" PRIu64 " rows     (%7.3lf GB)\n", total_rows,
           static_cast<double>(total_rows * size) / 1000000000.);

    for (uint8_t numa_id = 0; numa_id < num_numa_; numa_id++) {
      uint64_t free_rows = shared_row_version_pools_[numa_id]->free_count(cls);
      printf("  shared pool %" PRIu8 ": %10" PRIu64 " rows free\n", numa_id,
             free_rows);
    }
    for (uint16_t thread_id = 0; thread_id < num_threads_; thread_id++) {
      uint64_t free_rows = row_version_pools_[thread_id]->free_count(cls);
      printf("  local pool %2" PRIu16 ": %10" PRIu64 " rows free\n", thread_id,
             free_rows);
    }

    printf("\n");
  }
}
}
}

#endif
