#pragma once
#ifndef MICA_TRANSACTION_DB_H_
#define MICA_TRANSACTION_DB_H_

#include <unordered_map>
#include "mica/common.h"
#include "mica/transaction/timestamp.h"
#include "mica/alloc/hugetlbfs_shm.h"
#include "mica/transaction/page_pool.h"
#include "mica/transaction/table.h"
#include "mica/transaction/context.h"
#include "mica/transaction/transaction.h"
#include "mica/transaction/hash_index.h"
#include "mica/transaction/btree_index.h"
#include "mica/transaction/logging.h"
#include "mica/util/lcore.h"

namespace mica {
namespace transaction {
struct BasicDBConfig {
  // Perform pre-validation before making any write on the shared memory.
  // Faster when enabled.
  static constexpr bool kPreValidation = true;
  // Allow insering a new write version just after the row head, even if the
  // existing newest (and newer than this version) version has been aborted.
  // Faster when enabled.  Write-only verions are not affected by this.
  static constexpr bool kInsertNewestVersionOnly = true;
  // Sort the write set by their approximate contention level (from most
  // contended to least contended) to reduce footprint of aborted transactions.
  // Faster when enabled.
  static constexpr bool kSortWriteSetByContention = true;
  // Sort for top-k entries only.
  // static const uint64_t kPartialSortSize = static_cast<uint64_t>(-1);
  static const uint64_t kPartialSortSize = 8;
  // Adjust timestamp counter offsets automatically to allow even progress by
  // each thread. Sometimes faster when enabled.
  static constexpr bool kStragglerAvoidance = true;
  // Do not wait for a pending row version and simply abort.
  static constexpr bool kNoWaitForPending = false;
  // When kNoWaitForPending == true, skip pending versions instead of aborting.
  static constexpr bool kSkipPending = false;
  // A test feature that increment read timestamp early after an abort.
  static constexpr bool kReserveAfterAbort = false;
  // Have an inlined row version within a head.
  static constexpr bool kInlinedRowVersion = true;
  // The maximum size of the data to inline (bytes).  The overhead is 40 bytes.
  // static constexpr uint64_t kInlineThreshold = 64 - 40;
  // static constexpr uint64_t kInlineThreshold = 128 - 40;
  // static constexpr uint64_t kInlineThreshold = 192 - 40;
  static constexpr uint64_t kInlineThreshold = 256 - 40;
  // static constexpr uint64_t kInlineThreshold = 4096 - 40;
  // Use an alternative location for the inlining.
  static constexpr bool kInlineWithAltRow = false;
  // Promote a non-inlined version into an inlined version during read.
  static constexpr bool kPromoteNonInlinedVersion = true;

  // The maximum single increment of a clock (cycles).
  static constexpr int64_t kMaxClockIncrement = 10000000000000UL;  // ~1 hour

  // Backoff when the transaction has been aborted.  Requires
  // kCollectCommitStats == true.
  static constexpr bool kBackoff = true;
  // The interval of backoff time updates (us).
  static constexpr uint64_t kBackoffUpdateInterval = 5000;
  // Increment for hill climbing for backoff updates (us).
  static constexpr double kBackoffHCIncrement = 0.5;

  // The minimum backoff time (us).
  static constexpr double kBackoffMin = 0.0;
  // The maximum backoff time (us).
  static constexpr double kBackoffMax = 1000.;
  // Print the current backoff status for debugging the adaptive backoff logic.
  static constexpr bool kPrintBackoff = false;

  // Use usleep() alternatively for backoff if this thread has a pair
  // hyperthread.  It is assumed that there are 2 hyperthreads per core, and the
  // lower half and higher half match with each other.  E.g., for 56 cores, core
  // #0's pair is core #28.
  static constexpr bool kPairwiseSleeping = false;
  // The duration to alternate sleeping mode between a pair of hyperthreads
  // (us).
  static constexpr uint64_t kPairwiseSleepingSpan = 10000;
  // The minimum time to sleep using usleep() (us).
  static constexpr uint64_t kPairwiseSleepingMinTime = 2;

  // The maximum number of LCore to support.
  static constexpr size_t kMaxLCoreCount = 64;
  // The maximum number of numa nodes to support.
  static constexpr size_t kMaxNUMACount = 8;

  // The maximum number of column families.
  static constexpr uint16_t kMaxColumnFamilyCount = 8;

  // The number of RowVersion that a shared pool manages as a batch.
  static constexpr size_t kRowVersionPoolGroupSize = 1024;
  // The maximum number of RowVersion groups to keep in each local pool.
  static constexpr size_t kRowVersionPoolGroupMaxCount = 16;

  // The maximum size of the read and write set.  Both sets share the same
  // array.
  static constexpr uint16_t kMaxAccessSize = 1024;

  // The maximum size of garbage collection queue.  This must be at least 2 *
  // kMaxAccessSize + 1.
  // static constexpr size_t kMaxGCQueueSize = 4096;

  // The bucket size of the hash table for the access set.
  // Each bucket takes sizeof(uint16_t) * (kAccessBucketSize + 2) bytes.
  static constexpr uint16_t kAccessBucketSize = 6;  // 16 bytes
  // The root bucket count of the hash table for the access set.
  // static constexpr size_t kAccessBucketRootCount = 16;  // 256 bytes total
  static constexpr size_t kAccessBucketRootCount = 64;  // 1024 bytes total

  // The cycle increment for tsc offset when a transaction aborts (cycles). This
  // is now just a fixed increment for a thread that had an abort.  There is no
  // increment.
  static constexpr int64_t kStragglerAvoidanceIncrement = 2600;  // 1 us

  // The minimum interval to quiescence to increment the GC epoch (us).
  static constexpr int64_t kMinQuiescenceInterval = 10;

  // The minimum interval to synchronize the local clock with a remote clock
  // (us).
  static constexpr int64_t kMinClockSyncInterval = 100;

  // Collect commit-related statistics.  Required by kBackoff.
  static constexpr bool kCollectCommitStats = true;
  // Collect extra commit/abort latencies.
  static constexpr bool kCollectExtraCommitStats = false;
  // Collect the staleness of read-only transaction.
  static constexpr bool kCollectROTXStalenessStats = false;
  // Collect internal processing statistics (a bit slow).
  static constexpr bool kCollectProcessingStats = false;

  // Use ActiveTiming for fine-grained tracking (slow) and DummyTiming to omit
  // it.
  // typedef ::mica::transaction::ActiveTiming Timing;
  typedef ::mica::transaction::DummyTiming Timing;

  // Timestamp type.  Use CompactTimestamp for up to 9 months of consecutive
  // execution (without TSC renomalization) with up to 256 cores @ 3 GHz.  Use
  // WideTimestamp for up to 2.5 B years of consecutive execution with up to 4
  // Bi cores @ 1 THz, with an up to 10% throughput penalty and 24 bytes
  // overhead per row version (effectively no space overhead due to alignment).
  typedef ::mica::transaction::CompactTimestamp Timestamp;
  typedef ::mica::transaction::CompactConcurrentTimestamp ConcurrentTimestamp;
  // typedef ::mica::transaction::WideTimestamp Timestamp;
  // typedef ::mica::transaction::WideConcurrentTimestamp ConcurrentTimestamp;
  // typedef ::mica::transaction::CentralizedTimestamp Timestamp;
  // typedef ::mica::transaction::CentralizedConcurrentTimestamp
  // ConcurrentTimestamp;

  // The low-level memory allocator for PagePool.
  typedef ::mica::alloc::HugeTLBFS_SHM Alloc;

  // Logger.
  // template <class StaticConfig>
  // using Logger = ::mica::transaction::NullLogger<StaticConfig>;
  typedef ::mica::transaction::NullLogger<BasicDBConfig> Logger;

  // Show verbose messages.
  static constexpr bool kVerbose = false;
};

template <class StaticConfig = BasicDBConfig>
class DB {
 public:
  typedef typename StaticConfig::Timestamp Timestamp;
  typedef typename StaticConfig::ConcurrentTimestamp ConcurrentTimestamp;
  typedef typename StaticConfig::Logger Logger;
  typedef ::mica::util::Stopwatch Stopwatch;

  typedef HashIndex<StaticConfig, true, uint64_t> HashIndexUniqueU64;
  typedef HashIndex<StaticConfig, false, uint64_t> HashIndexNonuniqueU64;
  typedef BTreeIndex<StaticConfig, true, uint64_t> BTreeIndexUniqueU64;
  typedef BTreeIndex<StaticConfig, false, std::pair<uint64_t, uint64_t>>
      BTreeIndexNonuniqueU64;

  DB(PagePool<StaticConfig>** page_pools, Logger* logger, Stopwatch* sw,
     uint16_t num_threads);
  ~DB();

  PagePool<StaticConfig>* page_pool(uint8_t numa_id) {
    return page_pools_[numa_id];
  }
  const PagePool<StaticConfig>* page_pool(uint8_t numa_id) const {
    return page_pools_[numa_id];
  }

  Logger* logger() { return logger_; }
  const Logger* logger() const { return logger_; }

  const Stopwatch* sw() const { return sw_; }

  uint16_t thread_count() const { return num_threads_; }
  uint8_t numa_count() const { return num_numa_; }

  Context<StaticConfig>* context() {
    return context(static_cast<uint16_t>(::mica::util::lcore.lcore_id()));
  }
  const Context<StaticConfig>* context() const {
    return context(static_cast<uint16_t>(::mica::util::lcore.lcore_id()));
  }

  SharedRowVersionPool<StaticConfig>* shared_row_version_pool(uint8_t numa_id) {
    return shared_row_version_pools_[numa_id];
  }
  const SharedRowVersionPool<StaticConfig>* shared_row_version_pool(
      uint8_t numa_id) const {
    return shared_row_version_pools_[numa_id];
  }

  RowVersionPool<StaticConfig>* row_version_pool(uint16_t thread_id) {
    return row_version_pools_[thread_id];
  }
  const RowVersionPool<StaticConfig>* row_version_pool(
      uint16_t thread_id) const {
    return row_version_pools_[thread_id];
  }

  bool is_active(uint16_t thread_id) const { return thread_active_[thread_id]; }
  uint16_t active_thread_count() const { return active_thread_count_; }

  void activate(uint16_t thread_id);
  void deactivate(uint16_t thread_id);
  void reset_clock(uint16_t thread_id);
  void idle(uint16_t thread_id);

  Context<StaticConfig>* context(uint16_t thread_id) {
    return ctxs_[thread_id];
  }
  const Context<StaticConfig>* context(uint16_t thread_id) const {
    return ctxs_[thread_id];
  }

  bool create_table(std::string name, uint16_t cf_count,
                    const uint64_t* data_size_hints);

  Table<StaticConfig>* get_table(std::string name) { return tables_[name]; }
  const Table<StaticConfig>* get_table(std::string name) const {
    return tables_[name];
  }

  bool create_hash_index_unique_u64(std::string name,
                                    Table<StaticConfig>* main_tbl,
                                    uint64_t expected_num_rows);

  auto get_hash_index_unique_u64(std::string name) {
    return hash_idxs_unique_u64_[name];
  }
  auto get_hash_index_unique_u64(std::string name) const {
    return hash_idxs_unique_u64_[name];
  }

  bool create_hash_index_nonunique_u64(std::string name,
                                       Table<StaticConfig>* main_tbl,
                                       uint64_t expected_num_rows);

  auto get_hash_index_nonunique_u64(std::string name) {
    return hash_idxs_nonunique_u64_[name];
  }
  auto get_hash_index_nonunique_u64(std::string name) const {
    return hash_idxs_nonunique_u64_[name];
  }

  bool create_btree_index_unique_u64(std::string name,
                                     Table<StaticConfig>* main_tbl);

  auto get_btree_index_unique_u64(std::string name) {
    return btree_idxs_unique_u64_[name];
  }
  auto get_btree_index_unique_u64(std::string name) const {
    return btree_idxs_unique_u64_[name];
  }

  bool create_btree_index_nonunique_u64(std::string name,
                                        Table<StaticConfig>* main_tbl);

  auto get_btree_index_nonunique_u64(std::string name) {
    return btree_idxs_nonunique_u64_[name];
  }
  auto get_btree_index_nonunique_u64(std::string name) const {
    return btree_idxs_nonunique_u64_[name];
  }

  void quiescence(uint16_t thread_id);

  void update_backoff(uint16_t thread_id);
  double backoff() const { return backoff_; }

  void reset_backoff();

  Timestamp min_wts() const { return min_wts_.get(); }
  Timestamp min_rts() const { return min_rts_.get(); }

  // uint64_t gc_epoch() const { return gc_epoch_; }

  // db_print_stats.h
  void reset_stats();
  void print_stats(double elapsed_time, double total_time) const;

  void print_pool_status() const;

 private:
  friend class Table<StaticConfig>;

  PagePool<StaticConfig>** page_pools_;
  Logger* logger_;
  Stopwatch* sw_;

  uint16_t num_threads_;
  uint8_t num_numa_;
  Context<StaticConfig>* ctxs_[StaticConfig::kMaxLCoreCount];

  SharedRowVersionPool<StaticConfig>*
      shared_row_version_pools_[StaticConfig::kMaxNUMACount];
  RowVersionPool<StaticConfig>*
      row_version_pools_[StaticConfig::kMaxLCoreCount];

  std::unordered_map<std::string, Table<StaticConfig>*> tables_;

  std::unordered_map<std::string, HashIndexUniqueU64*> hash_idxs_unique_u64_;
  std::unordered_map<std::string, HashIndexNonuniqueU64*>
      hash_idxs_nonunique_u64_;

  std::unordered_map<std::string, BTreeIndexUniqueU64*> btree_idxs_unique_u64_;
  std::unordered_map<std::string, BTreeIndexNonuniqueU64*>
      btree_idxs_nonunique_u64_;

  // Modified by leader/worker threads very infrequently.
  volatile uint16_t leader_thread_id_;
  volatile uint16_t active_thread_count_;
  volatile bool thread_active_[StaticConfig::kMaxLCoreCount];
  bool clock_init_[StaticConfig::kMaxLCoreCount];

  // Modified by the leader thread.
  ConcurrentTimestamp min_wts_ __attribute__((aligned(64)));
  ConcurrentTimestamp min_rts_;
  volatile uint64_t ref_clock_;
  // volatile uint64_t gc_epoch_;

  volatile double backoff_;
  uint64_t last_backoff_print_;
  uint64_t last_backoff_update_;
  uint64_t last_committed_count_;
  double last_committed_tput_;
  double last_backoff_;

  // Modified and used only by the leader thread frequently.
  volatile uint16_t last_non_quiescence_thread_id_ __attribute__((aligned(64)));

  // Modified by worker threads.
  struct ThreadState {
    volatile bool quiescence;
  } __attribute__((aligned(64)));

  ThreadState thread_states_[StaticConfig::kMaxLCoreCount];

} __attribute__((aligned(64)));
}
}

#include "db_impl.h"
#include "db_print_stats.h"

#endif
