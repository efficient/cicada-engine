#include <cstdio>
#include <random>
#include <thread>
#include "mica/transaction/db.h"
#include "mica/util/lcore.h"
#include "mica/util/rand.h"
#include "mica/util/zipf.h"

struct DBConfig : public ::mica::transaction::BasicDBConfig {
  // Switch this for verification.
  typedef ::mica::transaction::NullLogger<DBConfig> Logger;
};

typedef DBConfig::Alloc Alloc;
typedef DBConfig::Logger Logger;
typedef DBConfig::Timing Timing;
typedef ::mica::transaction::PagePool<DBConfig> PagePool;
typedef ::mica::transaction::DB<DBConfig> DB;
typedef ::mica::transaction::Table<DBConfig> Table;
typedef DB::HashIndexUniqueU64 HashIndex;
typedef DB::BTreeIndexUniqueU64 BTreeIndex;
typedef ::mica::transaction::RowVersion<DBConfig> RowVersion;
typedef ::mica::transaction::RowAccessHandle<DBConfig> RowAccessHandle;
typedef ::mica::transaction::RowAccessHandlePeekOnly<DBConfig>
    RowAccessHandlePeekOnly;
typedef ::mica::transaction::Transaction<DBConfig> Transaction;
typedef ::mica::transaction::Result Result;
typedef ::mica::transaction::BTreeRangeType BTreeRangeType;

static ::mica::util::Stopwatch sw;

enum OpType : int {
  kInsert = 0,
  kRemove,
  kLookup,
  kLookupSnapshot,
  kScan,
  kScanSnapshot,
  kMax
};
static const char* op_type_names[] = {"Insert",  "Remove", "Lookup",
                                      "LookupS", "Scan",   "ScanS"};

// For the main table.  Not really used.
static const uint64_t kDataSize = 8;
static const uint64_t kScanLen = 100;

static const bool kShowPoolStats = false;

#if 0

static const bool kUseHashIndex = true;
static const bool kUseBTreeIndex = false;

#else

static const bool kUseHashIndex = false;
static const bool kUseBTreeIndex = true;

#endif

static const bool kCheckIndex = false;
// static const bool kCheckIndex = true;

// Worker task.

struct Task {
  DB* db;
  HashIndex* hash_idx;
  BTreeIndex* btree_idx;

  uint64_t thread_id;
  uint64_t num_threads;

  // Workload.
  uint64_t num_keys;
  uint64_t tx_count;
  bool disjoint_key_range;
  bool hash_keys;
  double zipf_theta;
  double op_frac[OpType::kMax];
  int post_condition;

  // Results.
  struct timeval tv_start;
  struct timeval tv_end;

  uint64_t succeeded[OpType::kMax];
  uint64_t failed[OpType::kMax];
  uint64_t aborted[OpType::kMax];
} __attribute__((aligned(64)));

static volatile uint16_t running_threads;

void worker_proc(Task* task) {
  ::mica::util::lcore.pin_thread(static_cast<uint16_t>(task->thread_id));

  auto ctx = task->db->context();

  auto hash_idx = task->hash_idx;
  auto btree_idx = task->btree_idx;

  double thresholds[OpType::kMax];
  thresholds[0] = task->op_frac[0];
  for (int op_type = 1; op_type < OpType::kMax; op_type++)
    thresholds[op_type] = thresholds[op_type - 1] + task->op_frac[op_type];

  __sync_add_and_fetch(&running_threads, 1);
  while (running_threads < task->num_threads) ::mica::util::pause();

  Timing t(ctx->timing_stack(), &::mica::transaction::Stats::worker);

  task->db->activate(static_cast<uint16_t>(task->thread_id));
  while (task->db->active_thread_count() < task->num_threads) {
    ::mica::util::pause();
    task->db->idle(static_cast<uint16_t>(task->thread_id));
  }

  // if (kVerbose) printf("lcore %" PRIu64 "\n", task->thread_id);

  uint64_t key_offset = 0;
  uint64_t num_keys = task->num_keys;

  if (task->disjoint_key_range) {
    num_keys = (num_keys + task->num_threads - 1) / task->num_threads;
    key_offset = num_keys * task->thread_id;
    if (key_offset >= task->num_keys) key_offset = task->num_keys;
    if (key_offset + num_keys >= task->num_keys)
      num_keys = task->num_keys - key_offset;
  }
  // printf("lcore %" PRIu64 " [%" PRIu64 ", %" PRIu64 ") zipf_theta=%.2lf\n",
  //        task->thread_id, key_offset, key_offset + num_keys, task->zipf_theta);

  uint64_t succeeded[OpType::kMax];
  uint64_t failed[OpType::kMax];
  uint64_t aborted[OpType::kMax];
  ::mica::util::memset(succeeded, 0, sizeof(succeeded));
  ::mica::util::memset(failed, 0, sizeof(failed));
  ::mica::util::memset(aborted, 0, sizeof(aborted));

  uint64_t op_result;
  uint64_t row_id;
  int left;
  bool suspicious;

  auto make_value = [](uint64_t key) { return ~(key + 100); };
  auto lookup_consumer = [&row_id](auto& k, auto& v) {
    (void)k;
    row_id = v;
    return false;
  };
  auto scan_consumer = [&row_id, &left, &suspicious, make_value](auto& k,
                                                                 auto& v) {
    (void)k;
    row_id = v;
    if (row_id != make_value(k)) suspicious = true;
    // TODO: Perform range check.
    if (--left > 0)
      return true;
    else
      return false;
  };
  const uint64_t max_key = static_cast<uint64_t>(-1);

  Transaction tx(ctx);

  gettimeofday(&task->tv_start, nullptr);

  if (num_keys != 0) {
    uint64_t seed = 4 * task->thread_id * ::mica::util::rdtsc();
    uint64_t seed_mask = (uint64_t(1) << 48) - 1;
    seed = seed & seed_mask;
    if (task->zipf_theta == -1) seed = seed % num_keys;
    ::mica::util::ZipfGen zg(num_keys, task->zipf_theta, seed);
    ::mica::util::Rand op_type_rand((seed + 1) & seed_mask);

    for (uint64_t i = 0; i < task->tx_count; i++) {
      double op_type_f = op_type_rand.next_f64();

      int op_type;
      for (op_type = 0; op_type < OpType::kMax - 1; op_type++)
        if (op_type_f < thresholds[op_type]) break;

      uint64_t key = zg.next();
      if (task->hash_keys) key = (key * 0x9ddfea08eb382d69ULL) % num_keys;
      key += key_offset;
      // printf("lcore %" PRIu64 " key=%" PRIu64 "\n", task->thread_id, key);

      int trial = 0;
      while (true) {
        trial++;

        if (kCheckIndex && i % 10000 == 0) {
          if (btree_idx) {
            if (!tx.begin(true)) assert(false);
            if (!btree_idx->check(&tx)) {
              // printf("last op: %s %" PRIu64 "\n", op_type_names[op_type], key);
              assert(false);
            }
            tx.abort();
          }
        }

        bool use_snapshot =
            op_type == kLookupSnapshot || op_type == kScanSnapshot;
        if (!tx.begin(use_snapshot)) assert(false);

        bool have_to_abort = false;

        row_id = 0;
        suspicious = false;

        switch (op_type) {
          case OpType::kInsert:
            if (hash_idx != nullptr)
              op_result = hash_idx->insert(&tx, key, make_value(key));
            else
              op_result = btree_idx->insert(&tx, key, make_value(key));
            break;
          case OpType::kRemove:
            if (hash_idx != nullptr)
              op_result = hash_idx->remove(&tx, key, make_value(key));
            else
              op_result = btree_idx->remove(&tx, key, make_value(key));
            break;
          case OpType::kLookup:
            if (hash_idx != nullptr)
              op_result = hash_idx->lookup(&tx, key, false, lookup_consumer);
            else
              op_result = btree_idx->lookup(&tx, key, false, lookup_consumer);
            if (op_result != 0 && row_id != make_value(key)) suspicious = true;
            break;
          case OpType::kLookupSnapshot:
            if (hash_idx != nullptr)
              op_result = hash_idx->lookup(&tx, key, true, lookup_consumer);
            else
              op_result = btree_idx->lookup(&tx, key, true, lookup_consumer);
            if (op_result != 0 && row_id != make_value(key)) suspicious = true;
            break;
          case OpType::kScan:
            if (hash_idx != nullptr)
              op_result = 0;  // Not implemented.
            else
              op_result = btree_idx->lookup<BTreeRangeType::kInclusive,
                                            BTreeRangeType::kOpen, false>(
                  &tx, key, max_key, false, scan_consumer);
            break;
          case OpType::kScanSnapshot:
            left = kScanLen;
            if (hash_idx != nullptr)
              op_result = 0;  // Not implemented.
            else
              op_result = btree_idx->lookup<BTreeRangeType::kInclusive,
                                            BTreeRangeType::kOpen, false>(
                  &tx, key, max_key, true, scan_consumer);
            break;
          default:
            assert(false);
        }

        if ((hash_idx != nullptr && op_result == HashIndex::kHaveToAbort) ||
            (btree_idx != nullptr && op_result == BTreeIndex::kHaveToAbort))
          have_to_abort = true;

        Result result;
        if (have_to_abort || !tx.commit(&result)) {
          tx.abort();
          aborted[op_type]++;

          if (trial == 10000) {
            printf("warning: many aborts with key=%" PRIu64 "\n", key);
            if (!tx.begin(true)) assert(false);
            btree_idx->check(&tx);
            printf("=== dump start ===\n");
            btree_idx->dump_tree(&tx);
            printf("=== dump end ===\n");
            tx.abort();
            assert(false);
          }
          continue;
        }

        // Suspicious reads should not be committed.
        if (suspicious) {
          printf("incorrect read got committed: key=%" PRIu64
                 " expected_value=%" PRIu64 " actual_value=%" PRIu64 "\n",
                 key, make_value(key), row_id);
          assert(false);
        }

        assert(result == Result::kCommitted);
        (void)result;

        if (op_result != 0)
          succeeded[op_type]++;
        else
          failed[op_type]++;
        break;
      }
    }
  }

  gettimeofday(&task->tv_end, nullptr);

  if (task->thread_id == 0 && task->post_condition != 0) {
    // Wait until other threads finish.
    while (task->db->active_thread_count() != 1) task->db->idle(0);

    usleep(10 * 1000);
    task->db->idle(0);

    // Do not use peek_only so that we get the highest wts.
    if (!tx.begin(false)) assert(false);
    if (task->post_condition == 1) {
      for (uint64_t key = 0; key < task->num_keys; key++) {
        if (hash_idx != nullptr)
          op_result = hash_idx->lookup(&tx, key, false, lookup_consumer);
        else
          op_result = btree_idx->lookup(&tx, key, false, lookup_consumer);

        if (op_result != 1) {
          printf("invalid post condition for key %" PRIu64 "\n", key);
          printf("=== dump start ===\n");
          btree_idx->dump_tree(&tx);
          printf("=== dump end ===\n");
          assert(false);
        }
      }
    } else if (task->post_condition == 2) {
      for (uint64_t key = 0; key < task->num_keys; key++) {
        if (hash_idx != nullptr)
          op_result = hash_idx->lookup(&tx, key, false, lookup_consumer);
        else
          op_result = btree_idx->lookup(&tx, key, false, lookup_consumer);

        if (op_result != 0) {
          printf("invalid post condition for key %" PRIu64 "\n", key);
          printf("=== dump start ===\n");
          btree_idx->dump_tree(&tx);
          printf("=== dump end ===\n");
          assert(false);
        }
      }
    }
    tx.abort();
  }

  task->db->deactivate(static_cast<uint16_t>(task->thread_id));

  ::mica::util::memcpy(task->succeeded, succeeded, sizeof(succeeded));
  ::mica::util::memcpy(task->failed, failed, sizeof(failed));
  ::mica::util::memcpy(task->aborted, aborted, sizeof(aborted));
}

int main(int argc, const char* argv[]) {
  if (argc != 5) {
    printf("%s NUM-KEYS ZIPF-THETA TX-COUNT THREAD-COUNT\n", argv[0]);
    return EXIT_FAILURE;
  }

  auto config = ::mica::util::Config::load_file("test_tx.json");

  uint64_t num_keys = static_cast<uint64_t>(atol(argv[1]));
  double zipf_theta = atof(argv[2]);
  uint64_t tx_count = static_cast<uint64_t>(atol(argv[3]));
  uint64_t num_threads = static_cast<uint64_t>(atol(argv[4]));

  Alloc alloc(config.get("alloc"));
  auto page_pool_size = 8 * uint64_t(1073741824);
  PagePool* page_pools[2];
  if (num_threads == 1) {
    page_pools[0] = new PagePool(&alloc, page_pool_size, 0);
    page_pools[1] = nullptr;
  } else {
    page_pools[0] = new PagePool(&alloc, page_pool_size / 2, 0);
    page_pools[1] = new PagePool(&alloc, page_pool_size / 2, 1);
  }

  ::mica::util::lcore.pin_thread(0);

  sw.init_start();
  sw.init_end();

  printf("num_keys = %" PRIu64 "\n", num_keys);
  printf("zipf_theta = %lf\n", zipf_theta);
  printf("tx_count = %" PRIu64 "\n", tx_count);
  printf("num_threads = %" PRIu64 "\n", num_threads);
#ifndef NDEBUG
  printf("!NDEBUG\n");
#endif
  printf("\n");

  Logger logger;
  DB db(page_pools, &logger, &sw, static_cast<uint16_t>(num_threads));

  const uint64_t kDataSizes[] = {kDataSize};
  bool ret = db.create_table("main", 1, kDataSizes);
  assert(ret);
  (void)ret;

  auto tbl = db.get_table("main");

  db.activate(0);

  HashIndex* hash_idx = nullptr;
  if (kUseHashIndex) {
    bool ret = db.create_hash_index_unique_u64("main_idx", tbl, num_keys);
    assert(ret);
    (void)ret;

    hash_idx = db.get_hash_index_unique_u64("main_idx");
    Transaction tx(db.context(0));
    hash_idx->init(&tx);
  }

  BTreeIndex* btree_idx = nullptr;
  if (kUseBTreeIndex) {
    bool ret = db.create_btree_index_unique_u64("main_idx", tbl);
    assert(ret);
    (void)ret;

    btree_idx = db.get_btree_index_unique_u64("main_idx");
    Transaction tx(db.context(0));
    btree_idx->init(&tx);
  }

  struct Workload {
    const char* name;
    uint64_t tx_count;
    bool disjoint_key_range;
    bool hash_keys;
    double zipf_theta;
    double op_frac[OpType::kMax];
    int post_condition;  // 0 = no check; 1 = full; 2 = empty

  } workloads[] = {
      {"[ 0] Sequential inserts (100% success)",
       (num_keys + num_threads - 1) / num_threads,
       true,
       false,
       -1,
       {1., 0., 0., 0., 0., 0.},
       1},
      {"[ 1] Sequential removals (100% success)",
       (num_keys + num_threads - 1) / num_threads,
       true,
       false,
       -1,
       {0., 1., 0., 0., 0., 0.},
       2},
      /*
      {"[ 0] Sequential inserts (100% success)",
       (num_keys + num_threads - 1) / num_threads,
       true,
       false,
       -1,
       {1., 0., 0., 0., 0., 0.},
       1},
      {"[ 1] Sequential removals (100% success)",
       (num_keys + num_threads - 1) / num_threads,
       true,
       false,
       -1,
       {0., 1., 0., 0., 0., 0.},
       2},
      {"[ 0] Sequential inserts (100% success)",
       (num_keys + num_threads - 1) / num_threads,
       true,
       false,
       -1,
       {1., 0., 0., 0., 0., 0.},
       1},
      {"[ 1] Sequential removals (100% success)",
       (num_keys + num_threads - 1) / num_threads,
       true,
       false,
       -1,
       {0., 1., 0., 0., 0., 0.},
       2},
       */
      {"[ 2] Sequential lookups (100% failure)",
       tx_count,
       true,
       false,
       -1,
       {0., 0., 1., 0., 0., 0.},
       0},
      {"[ 3] Sequential inserts (100% success)",
       (num_keys + num_threads - 1) / num_threads,
       true,
       false,
       -1,
       {1., 0., 0., 0., 0., 0.},
       0},
      {"[ 4] Sequential lookups (100% success)",
       tx_count,
       true,
       false,
       -1,
       {0., 0., 1., 0., 0., 0.},
       0},
      {"[ 5] Sequential snapshot-lookups (100% success)",
       tx_count,
       true,
       false,
       -1,
       {0., 0., 0., 1., 0., 0.},
       0},
      {"[ 6] Random lookups (100% success)",
       tx_count,
       false,
       true,
       zipf_theta,
       {0., 0., 1., 0., 0., 0.},
       0},
      {"[ 7] Random snapshot-lookups (100% success)",
       tx_count,
       false,
       true,
       zipf_theta,
       {0., 0., 0., 1., 0., 0.},
       0},
      {"[ 8] Random scans (100% success)",
       tx_count / kScanLen,
       false,
       true,
       zipf_theta,
       {0., 0., 0., 0., 1., 0.},
       0},
      {"[ 9] Random snapshot-scans (100% success)",
       tx_count / kScanLen,
       false,
       true,
       zipf_theta,
       {0., 0., 0., 0., 0., 1.},
       0},
      {"[10] Sequential removals (mixed success)",
       (num_keys + num_threads - 1) / num_threads,
       true,
       false,
       -1,
       {0., 1., 0., 0., 0., 0.},
       0},
      {"[11] Random inserts (mixed success)",
       tx_count,
       false,
       true,
       zipf_theta,
       {1., 0., 0., 0., 0., 0.},
       0},
      {"[12] Random inserts+removals+lookups (50% success)",
       tx_count,
       false,
       true,
       zipf_theta,
       {0.3333, 0.3333, 0.3334, 0., 0., 0.},
       0},
      {"[13] Random inserts+removals+snapshot-lookups (50% success)",
       tx_count,
       false,
       true,
       zipf_theta,
       {0.3333, 0.3333, 0., 0.3334, 0., 0.},
       0},
  };

  size_t run_perf = 10000;

  for (size_t i = 0; i < sizeof(workloads) / sizeof(workloads[0]); i++) {
    std::vector<Task> tasks(num_threads);

    for (uint64_t thread_id = 0; thread_id < num_threads; thread_id++) {
      tasks[thread_id].thread_id = static_cast<uint16_t>(thread_id);
      tasks[thread_id].num_threads = num_threads;
      tasks[thread_id].db = &db;
      tasks[thread_id].hash_idx = hash_idx;
      tasks[thread_id].btree_idx = btree_idx;

      tasks[thread_id].num_keys = num_keys;

      tasks[thread_id].tx_count = workloads[i].tx_count;
      tasks[thread_id].disjoint_key_range = workloads[i].disjoint_key_range;
      tasks[thread_id].hash_keys = workloads[i].hash_keys;
      tasks[thread_id].zipf_theta = workloads[i].zipf_theta;
      ::mica::util::memcpy(tasks[thread_id].op_frac, workloads[i].op_frac,
                           sizeof(workloads[i].op_frac));
      tasks[thread_id].post_condition = workloads[i].post_condition;
    }

    db.reset_stats();

    printf("-------------------------------------------------------\n");
    printf("executing workload: %s\n", workloads[i].name);
    printf("-------------------------------------------------------\n");
    printf("  tx_count=%" PRIu64 "\n", workloads[i].tx_count);
    printf("  disjoint_key_range=%d\n", workloads[i].disjoint_key_range);
    printf("  hash_keys=%d\n", workloads[i].hash_keys);
    printf("  zipf_theta=%.2lf\n", workloads[i].zipf_theta);
    for (int op_type = 0; op_type < OpType::kMax; op_type++) {
      if (workloads[i].op_frac[op_type] == 0.) continue;
      printf("  %7s=%3.2lf\n", op_type_names[op_type],
             workloads[i].op_frac[op_type]);
    }
    printf("\n");

    running_threads = 0;

    ::mica::util::memory_barrier();

    if (run_perf == i) {
      int r = system("perf record -a sleep 1h &");
      // int r = system("perf record -a -g sleep 1h &");
      (void)r;
    }

    std::vector<std::thread> threads;
    for (uint64_t thread_id = 1; thread_id < num_threads; thread_id++)
      threads.emplace_back(worker_proc, &tasks[thread_id]);

    worker_proc(&tasks[0]);

    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }

    if (run_perf == i) {
      int r = system("killall perf");
      (void)r;
    }

    {
      double diff;
      {
        double min_start = 0.;
        double max_end = 0.;
        for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
          double start = (double)tasks[thread_id].tv_start.tv_sec * 1. +
                         (double)tasks[thread_id].tv_start.tv_usec * 0.000001;
          double end = (double)tasks[thread_id].tv_end.tv_sec * 1. +
                       (double)tasks[thread_id].tv_end.tv_usec * 0.000001;
          if (thread_id == 0 || min_start > start) min_start = start;
          if (thread_id == 0 || max_end < end) max_end = end;
        }

        diff = max_end - min_start;
      }
      double total_time = diff * static_cast<double>(num_threads);

      uint64_t total_succeeded[OpType::kMax];
      uint64_t total_failed[OpType::kMax];
      uint64_t total_aborted[OpType::kMax];
      for (int op_type = 0; op_type < OpType::kMax; op_type++) {
        total_succeeded[op_type] = 0;
        total_failed[op_type] = 0;
        total_aborted[op_type] = 0;
        for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
          total_succeeded[op_type] += tasks[thread_id].succeeded[op_type];
          total_failed[op_type] += tasks[thread_id].failed[op_type];
          total_aborted[op_type] += tasks[thread_id].aborted[op_type];
        }
      }
      for (int op_type = 0; op_type < OpType::kMax; op_type++) {
        if (workloads[i].op_frac[op_type] == 0.) continue;

        printf("%7s throughput:           ", op_type_names[op_type]);

        printf("%7" PRIu64 " succeeded (%7.3lf M/sec), ",
               total_succeeded[op_type],
               static_cast<double>(total_succeeded[op_type]) / diff * 0.000001);

        printf("%7" PRIu64 " failed (%7.3lf M/sec), ", total_failed[op_type],
               static_cast<double>(total_failed[op_type]) / diff * 0.000001);

        printf("%7" PRIu64 " aborted (%5.2f%%)\n", total_aborted[op_type],
               100. * static_cast<double>(total_aborted[op_type]) /
                   static_cast<double>(total_succeeded[op_type] +
                                       total_failed[op_type] +
                                       total_aborted[op_type]));
      }
      printf("\n");

      db.print_stats(diff, total_time);
    }

    printf("\n");
  }

  tbl->print_table_status();

  if (hash_idx != nullptr) hash_idx->index_table()->print_table_status();
  if (btree_idx != nullptr) btree_idx->index_table()->print_table_status();

  if (kShowPoolStats) db.print_pool_status();

  return EXIT_SUCCESS;
}
