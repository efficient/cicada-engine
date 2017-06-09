#include <cstdio>
#include <thread>
#include <random>
#include "mica/transaction/db.h"
#include "mica/util/lcore.h"
#include "mica/util/zipf.h"
#include "mica/util/rand.h"
#include "mica/test/test_tx_conf.h"

typedef DBConfig::Alloc Alloc;
typedef DBConfig::Logger Logger;
typedef DBConfig::Timestamp Timestamp;
typedef DBConfig::ConcurrentTimestamp ConcurrentTimestamp;
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

static ::mica::util::Stopwatch sw;

// Worker task.

struct Task {
  DB* db;
  Table* tbl;
  HashIndex* hash_idx;
  BTreeIndex* btree_idx;

  uint64_t thread_id;
  uint64_t num_threads;

  // Workload.
  uint64_t num_rows;
  uint64_t tx_count;
  uint16_t* req_counts;
  uint8_t* read_only_tx;
  uint32_t* scan_lens;
  uint64_t* row_ids;
  uint8_t* column_ids;
  uint8_t* op_types;

  // State (for VerificationLogger).
  uint64_t tx_i;
  uint64_t req_i;
  uint64_t commit_i;

  // Results.
  struct timeval tv_start;
  struct timeval tv_end;

  uint64_t committed;
  uint64_t scanned;

  // Transaction log for verification.
  uint64_t* commit_tx_i;
  Timestamp* commit_ts;
  Timestamp* read_ts;
  Timestamp* write_ts;
} __attribute__((aligned(64)));

template <class StaticConfig>
class VerificationLogger
    : public ::mica::transaction::LoggerInterface<StaticConfig> {
 public:
  VerificationLogger() : tasks(nullptr) {}

  bool log(const ::mica::transaction::Transaction<StaticConfig>* tx) {
    if (tasks == nullptr) return true;

    auto task = &((*tasks)[tx->context()->thread_id()]);
    auto tx_i = task->tx_i;
    auto req_i = task->req_i;
    auto commit_i = task->commit_i;

    task->commit_tx_i[commit_i] = tx_i;
    task->commit_ts[tx_i] = tx->ts();

    // XXX: Assume we have the same row ordering in the read/write set.
    uint64_t req_j = 0;
    for (auto j = 0; j < tx->access_size(); j++) {
      if (tx->accesses()[j].state == ::mica::transaction::RowAccessState::kPeek)
        continue;

      assert(req_j < task->req_counts[tx_i]);

      task->read_ts[req_i + req_j] = tx->accesses()[j].read_rv->wts;
      if (tx->accesses()[j].write_rv != nullptr)
        task->write_ts[req_i + req_j] = tx->accesses()[j].write_rv->wts;
      else
        task->write_ts[req_i + req_j] = task->read_ts[req_i + req_j];
      req_j++;
    }
    assert(req_j == task->req_counts[tx_i]);
    return true;
  }

  std::vector<Task>* tasks;
};

template <class Logger>
void setup_logger(Logger* logger, std::vector<Task>* tasks) {
  (void)logger;
  (void)tasks;
}

template <>
void setup_logger(VerificationLogger<DBConfig>* logger,
                  std::vector<Task>* tasks) {
  logger->tasks = tasks;
}

static volatile uint16_t running_threads;
static volatile uint8_t stopping;

void worker_proc(Task* task) {
  ::mica::util::lcore.pin_thread(static_cast<uint16_t>(task->thread_id));

  auto ctx = task->db->context();
  auto tbl = task->tbl;
  auto hash_idx = task->hash_idx;
  auto btree_idx = task->btree_idx;

  __sync_add_and_fetch(&running_threads, 1);
  while (running_threads < task->num_threads) ::mica::util::pause();

  Timing t(ctx->timing_stack(), &::mica::transaction::Stats::worker);

  gettimeofday(&task->tv_start, nullptr);

  uint64_t next_tx_i = 0;
  uint64_t next_req_i = 0;
  uint64_t commit_i = 0;
  uint64_t scanned = 0;

  task->db->activate(static_cast<uint16_t>(task->thread_id));
  while (task->db->active_thread_count() < task->num_threads) {
    ::mica::util::pause();
    task->db->idle(static_cast<uint16_t>(task->thread_id));
  }

  if (kVerbose) printf("lcore %" PRIu64 "\n", task->thread_id);

  Transaction tx(ctx);

  while (next_tx_i < task->tx_count && !stopping) {
    uint64_t tx_i;
    uint64_t req_i;

    tx_i = next_tx_i++;
    req_i = next_req_i;
    next_req_i += task->req_counts[tx_i];

    task->tx_i = tx_i;
    task->req_i = req_i;
    task->commit_i = commit_i;

    while (true) {
      bool aborted = false;

      uint64_t v = 0;

      bool use_peek_only = kUseSnapshot && task->read_only_tx[tx_i];

      bool ret = tx.begin(use_peek_only);
      assert(ret);
      (void)ret;

      for (uint64_t req_j = 0; req_j < task->req_counts[tx_i]; req_j++) {
        uint64_t row_id = task->row_ids[req_i + req_j];
        uint8_t column_id = task->column_ids[req_i + req_j];
        bool is_read = task->op_types[req_i + req_j] == 0;
        bool is_rmw = task->op_types[req_i + req_j] == 1;

        if (hash_idx != nullptr) {
          auto lookup_result =
              hash_idx->lookup(&tx, row_id, kSkipValidationForIndexAccess,
                               [&row_id](auto& k, auto& v) {
                                 (void)k;
                                 row_id = v;
                                 return false;
                               });
          if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
            assert(false);
            tx.abort();
            aborted = true;
            break;
          }
        } else if (btree_idx != nullptr) {
          auto lookup_result =
              btree_idx->lookup(&tx, row_id, kSkipValidationForIndexAccess,
                                [&row_id](auto& k, auto& v) {
                                  (void)k;
                                  row_id = v;
                                  return false;
                                });
          if (lookup_result != 1 || lookup_result == BTreeIndex::kHaveToAbort) {
            assert(false);
            tx.abort();
            aborted = true;
            break;
          }
        }

        if (!use_peek_only) {
          RowAccessHandle rah(&tx);

          if (is_read) {
            if (!rah.peek_row(tbl, 0, row_id, false, true, false) ||
                !rah.read_row()) {
              tx.abort();
              aborted = true;
              break;
            }

            const char* data =
                rah.cdata() + static_cast<uint64_t>(column_id) * kColumnSize;
            for (uint64_t j = 0; j < kColumnSize; j += 64)
              v += static_cast<uint64_t>(data[j]);
            v += static_cast<uint64_t>(data[kColumnSize - 1]);
          } else {
            if (is_rmw) {
              if (!rah.peek_row(tbl, 0, row_id, false, true, true) ||
                  !rah.read_row() || !rah.write_row(kDataSize)) {
                tx.abort();
                aborted = true;
                break;
              }
            } else {
              if (!rah.peek_row(tbl, 0, row_id, false, false, true) ||
                  !rah.write_row(kDataSize)) {
                tx.abort();
                aborted = true;
                break;
              }
            }

            char* data =
                rah.data() + static_cast<uint64_t>(column_id) * kColumnSize;
            for (uint64_t j = 0; j < kColumnSize; j += 64) {
              v += static_cast<uint64_t>(data[j]);
              data[j] = static_cast<char>(v);
            }
            v += static_cast<uint64_t>(data[kColumnSize - 1]);
            data[kColumnSize - 1] = static_cast<char>(v);
          }
        } else {
          if (!kUseScan) {
            RowAccessHandlePeekOnly rah(&tx);

            if (!rah.peek_row(tbl, 0, row_id, false, false, false)) {
              tx.abort();
              aborted = true;
              break;
            }

            const char* data =
                rah.cdata() + static_cast<uint64_t>(column_id) * kColumnSize;
            for (uint64_t j = 0; j < kColumnSize; j += 64)
              v += static_cast<uint64_t>(data[j]);
            v += static_cast<uint64_t>(data[kColumnSize - 1]);
          } else if (!kUseFullTableScan) {
            RowAccessHandlePeekOnly rah(&tx);

            uint64_t next_row_id = row_id;
            uint64_t next_next_raw_row_id = task->row_ids[req_i + req_j] + 1;
            if (next_next_raw_row_id == task->num_rows)
              next_next_raw_row_id = 0;

            uint32_t scan_len = task->scan_lens[tx_i];
            for (uint32_t scan_i = 0; scan_i < scan_len; scan_i++) {
              uint64_t this_row_id = next_row_id;

              // TODO: Support btree_idx.
              assert(hash_idx != nullptr);

              // Lookup index for next row.
              auto lookup_result =
                  hash_idx->lookup(&tx, next_next_raw_row_id, true,
                                   [&next_row_id](auto& k, auto& v) {
                                     (void)k;
                                     next_row_id = v;
                                     return false;
                                   });
              if (lookup_result != 1 ||
                  lookup_result == HashIndex::kHaveToAbort) {
                tx.abort();
                aborted = true;
                break;
              }

              // Prefetch index for next next row.
              next_next_raw_row_id++;
              if (next_next_raw_row_id == task->num_rows)
                next_next_raw_row_id = 0;
              hash_idx->prefetch(&tx, next_next_raw_row_id);

              // Prefetch next row.
              rah.prefetch_row(tbl, 0, next_row_id,
                               static_cast<uint64_t>(column_id) * kColumnSize,
                               kColumnSize);

              // Access current row.
              if (!rah.peek_row(tbl, 0, this_row_id, false, false, false)) {
                tx.abort();
                aborted = true;
                break;
              }

              const char* data =
                  rah.cdata() + static_cast<uint64_t>(column_id) * kColumnSize;
              for (uint64_t j = 0; j < kColumnSize; j += 64)
                v += static_cast<uint64_t>(data[j]);
              v += static_cast<uint64_t>(data[kColumnSize - 1]);

              rah.reset();
            }

            if (aborted) break;
          } else /*if (kUseFullTableScan)*/ {
            if (!tbl->scan(&tx, 0,
                           static_cast<uint64_t>(column_id) * kColumnSize,
                           kColumnSize, [&v, column_id](auto& rah) {
                             const char* data =
                                 rah.cdata() +
                                 static_cast<uint64_t>(column_id) * kColumnSize;
                             for (uint64_t j = 0; j < kColumnSize; j += 64)
                               v += static_cast<uint64_t>(data[j]);
                             v += static_cast<uint64_t>(data[kColumnSize - 1]);
                           })) {
              tx.abort();
              aborted = true;
              break;
            }
          }
        }
      }

      if (aborted) continue;

      Result result;
      if (!tx.commit(&result)) continue;
      assert(result == Result::kCommitted);

      commit_i++;
      if (kUseScan && use_peek_only) {
        if (!kUseFullTableScan)
          scanned += task->scan_lens[tx_i];
        else
          scanned += task->num_rows;
      }

      break;
    }
  }

  task->db->deactivate(static_cast<uint16_t>(task->thread_id));

  if (!stopping) stopping = 1;

  task->committed = commit_i;
  task->scanned = scanned;

  gettimeofday(&task->tv_end, nullptr);
}

int main(int argc, const char* argv[]) {
  if (argc != 7) {
    printf(
        "%s NUM-ROWS REQS-PER-TX READ-RATIO ZIPF-THETA TX-COUNT "
        "THREAD-COUNT\n",
        argv[0]);
    return EXIT_FAILURE;
  }

  auto config = ::mica::util::Config::load_file("test_tx.json");

  uint64_t num_rows = static_cast<uint64_t>(atol(argv[1]));
  uint64_t reqs_per_tx = static_cast<uint64_t>(atol(argv[2]));
  double read_ratio = atof(argv[3]);
  double zipf_theta = atof(argv[4]);
  uint64_t tx_count = static_cast<uint64_t>(atol(argv[5]));
  uint64_t num_threads = static_cast<uint64_t>(atol(argv[6]));

  Alloc alloc(config.get("alloc"));
  auto page_pool_size = 24 * uint64_t(1073741824);
  PagePool* page_pools[2];
  // if (num_threads == 1) {
  //   page_pools[0] = new PagePool(&alloc, page_pool_size, 0);
  //   page_pools[1] = nullptr;
  // } else {
  page_pools[0] = new PagePool(&alloc, page_pool_size / 2, 0);
  page_pools[1] = new PagePool(&alloc, page_pool_size / 2, 1);
  // }

  ::mica::util::lcore.pin_thread(0);

  sw.init_start();
  sw.init_end();

  if (num_rows == 0) {
    num_rows = SYNTH_TABLE_SIZE;
    reqs_per_tx = REQ_PER_QUERY;
    read_ratio = READ_PERC;
    zipf_theta = ZIPF_THETA;
    tx_count = MAX_TXN_PER_PART;
    num_threads = THREAD_CNT;
#ifndef NDEBUG
    printf("!NDEBUG\n");
    return EXIT_FAILURE;
#endif
  }

  printf("num_rows = %" PRIu64 "\n", num_rows);
  printf("reqs_per_tx = %" PRIu64 "\n", reqs_per_tx);
  printf("read_ratio = %lf\n", read_ratio);
  printf("zipf_theta = %lf\n", zipf_theta);
  printf("tx_count = %" PRIu64 "\n", tx_count);
  printf("num_threads = %" PRIu64 "\n", num_threads);
#ifndef NDEBUG
  printf("!NDEBUG\n");
#endif
  printf("\n");

  Logger logger;
  DB db(page_pools, &logger, &sw, static_cast<uint16_t>(num_threads));

  const bool kVerify =
      typeid(typename DBConfig::Logger) == typeid(VerificationLogger<DBConfig>);

  const uint64_t data_sizes[] = {kDataSize};
  bool ret = db.create_table("main", 1, data_sizes);
  assert(ret);
  (void)ret;

  auto tbl = db.get_table("main");

  db.activate(0);

  HashIndex* hash_idx = nullptr;
  if (kUseHashIndex) {
    bool ret = db.create_hash_index_unique_u64("main_idx", tbl, num_rows);
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

  {
    printf("initializing table\n");

    std::vector<std::thread> threads;
    uint64_t init_num_threads = std::min(uint64_t(2), num_threads);
    for (uint64_t thread_id = 0; thread_id < init_num_threads; thread_id++) {
      threads.emplace_back([&, thread_id] {
        ::mica::util::lcore.pin_thread(thread_id);

        db.activate(static_cast<uint16_t>(thread_id));
        while (db.active_thread_count() < init_num_threads) {
          ::mica::util::pause();
          db.idle(static_cast<uint16_t>(thread_id));
        }

        // Randomize the data layout by shuffling row insert order.
        std::mt19937 g(thread_id);
        std::vector<uint64_t> row_ids;
        row_ids.reserve((num_rows + init_num_threads - 1) / init_num_threads);
        for (uint64_t i = thread_id; i < num_rows; i += init_num_threads)
          row_ids.push_back(i);
        std::shuffle(row_ids.begin(), row_ids.end(), g);

        Transaction tx(db.context(static_cast<uint16_t>(thread_id)));
        const uint64_t kBatchSize = 16;
        for (uint64_t i = 0; i < row_ids.size(); i += kBatchSize) {
          while (true) {
            bool ret = tx.begin();
            if (!ret) {
              printf("failed to start a transaction\n");
              continue;
            }

            bool aborted = false;
            auto i_end = std::min(i + kBatchSize, row_ids.size());
            for (uint64_t j = i; j < i_end; j++) {
              RowAccessHandle rah(&tx);
              if (!rah.new_row(tbl, 0, Transaction::kNewRowID, true,
                               kDataSize)) {
                // printf("failed to insert rows at new_row(), row = %" PRIu64
                //        "\n",
                //        j);
                aborted = true;
                tx.abort();
                break;
              }

              if (kUseHashIndex) {
                auto ret = hash_idx->insert(&tx, row_ids[j], rah.row_id());
                if (ret != 1 || ret == HashIndex::kHaveToAbort) {
                  // printf("failed to update index row = %" PRIu64 "\n", j);
                  aborted = true;
                  tx.abort();
                  break;
                }
              }
              if (kUseBTreeIndex) {
                auto ret = btree_idx->insert(&tx, row_ids[j], rah.row_id());
                if (ret != 1 || ret == BTreeIndex::kHaveToAbort) {
                  // printf("failed to update index row = %" PRIu64 "\n", j);
                  aborted = true;
                  tx.abort();
                  break;
                }
              }
            }

            if (aborted) continue;

            Result result;
            if (!tx.commit(&result)) {
              // printf("failed to insert rows at commit(), row = %" PRIu64
              //        "; result=%d\n",
              //        i_end - 1, static_cast<int>(result));
              continue;
            }
            break;
          }
        }

        db.deactivate(static_cast<uint16_t>(thread_id));
        return 0;
      });
    }

    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }

    // TODO: Use multiple threads to renew rows for more balanced memory access.

    db.activate(0);
    {
      uint64_t i = 0;
      tbl->renew_rows(db.context(0), 0, i, static_cast<uint64_t>(-1), false);
    }
    if (hash_idx != nullptr) {
      uint64_t i = 0;
      hash_idx->index_table()->renew_rows(db.context(0), 0, i,
                                          static_cast<uint64_t>(-1), false);
    }
    if (btree_idx != nullptr) {
      uint64_t i = 0;
      btree_idx->index_table()->renew_rows(db.context(0), 0, i,
                                           static_cast<uint64_t>(-1), false);
    }
    db.deactivate(0);

    db.reset_stats();
    db.reset_backoff();
  }

  std::vector<Task> tasks(num_threads);
  setup_logger(&logger, &tasks);
  {
    printf("generating workload\n");

    for (uint64_t thread_id = 0; thread_id < num_threads; thread_id++) {
      tasks[thread_id].thread_id = static_cast<uint16_t>(thread_id);
      tasks[thread_id].num_threads = num_threads;
      tasks[thread_id].db = &db;
      tasks[thread_id].tbl = tbl;
      tasks[thread_id].hash_idx = hash_idx;
      tasks[thread_id].btree_idx = btree_idx;
    }

    if (kUseContendedSet) zipf_theta = 0.;

    for (uint64_t thread_id = 0; thread_id < num_threads; thread_id++) {
      auto req_counts = reinterpret_cast<uint16_t*>(
          alloc.malloc_contiguous(sizeof(uint16_t) * tx_count, thread_id));
      auto read_only_tx = reinterpret_cast<uint8_t*>(
          alloc.malloc_contiguous(sizeof(uint8_t) * tx_count, thread_id));
      uint32_t* scan_lens = nullptr;
      if (kUseScan)
        scan_lens = reinterpret_cast<uint32_t*>(
            alloc.malloc_contiguous(sizeof(uint32_t) * tx_count, thread_id));
      auto row_ids = reinterpret_cast<uint64_t*>(alloc.malloc_contiguous(
          sizeof(uint64_t) * tx_count * reqs_per_tx, thread_id));
      auto column_ids = reinterpret_cast<uint8_t*>(alloc.malloc_contiguous(
          sizeof(uint8_t) * tx_count * reqs_per_tx, thread_id));
      auto op_types = reinterpret_cast<uint8_t*>(alloc.malloc_contiguous(
          sizeof(uint8_t) * tx_count * reqs_per_tx, thread_id));
      assert(req_counts);
      assert(row_ids);
      assert(column_ids);
      assert(op_types);
      tasks[thread_id].num_rows = num_rows;
      tasks[thread_id].tx_count = tx_count;
      tasks[thread_id].req_counts = req_counts;
      tasks[thread_id].read_only_tx = read_only_tx;
      tasks[thread_id].scan_lens = scan_lens;
      tasks[thread_id].row_ids = row_ids;
      tasks[thread_id].column_ids = column_ids;
      tasks[thread_id].op_types = op_types;

      if (kVerify) {
        auto commit_tx_i = reinterpret_cast<uint64_t*>(
            alloc.malloc_contiguous(sizeof(uint64_t) * tx_count, thread_id));
        auto commit_ts = reinterpret_cast<Timestamp*>(
            alloc.malloc_contiguous(sizeof(Timestamp) * tx_count, thread_id));
        auto read_ts = reinterpret_cast<Timestamp*>(alloc.malloc_contiguous(
            sizeof(Timestamp) * tx_count * reqs_per_tx, thread_id));
        auto write_ts = reinterpret_cast<Timestamp*>(alloc.malloc_contiguous(
            sizeof(Timestamp) * tx_count * reqs_per_tx, thread_id));
        assert(commit_tx_i);
        assert(commit_ts);
        assert(read_ts);
        ::mica::util::memset(commit_tx_i, 0, sizeof(uint64_t) * tx_count);
        ::mica::util::memset(commit_ts, 0, sizeof(Timestamp) * tx_count);
        ::mica::util::memset(read_ts, 0,
                             sizeof(Timestamp) * tx_count * reqs_per_tx);
        ::mica::util::memset(write_ts, 0,
                             sizeof(Timestamp) * tx_count * reqs_per_tx);
        tasks[thread_id].commit_tx_i = commit_tx_i;
        tasks[thread_id].commit_ts = commit_ts;
        tasks[thread_id].read_ts = read_ts;
        tasks[thread_id].write_ts = write_ts;
      }
    }

    std::vector<std::thread> threads;
    for (uint64_t thread_id = 0; thread_id < num_threads; thread_id++) {
      threads.emplace_back([&, thread_id] {
        ::mica::util::lcore.pin_thread(thread_id);

        uint64_t seed = 4 * thread_id * ::mica::util::rdtsc();
        uint64_t seed_mask = (uint64_t(1) << 48) - 1;
        ::mica::util::ZipfGen zg(num_rows, zipf_theta, seed & seed_mask);
        ::mica::util::Rand op_type_rand((seed + 1) & seed_mask);
        ::mica::util::Rand column_id_rand((seed + 2) & seed_mask);
        ::mica::util::Rand scan_len_rand((seed + 3) & seed_mask);
        uint32_t read_threshold =
            (uint32_t)(read_ratio * (double)((uint32_t)-1));
        uint32_t rmw_threshold =
            (uint32_t)(kReadModifyWriteRatio * (double)((uint32_t)-1));

        uint64_t req_offset = 0;

        for (uint64_t tx_i = 0; tx_i < tx_count; tx_i++) {
          bool read_only_tx = true;

          for (uint64_t req_i = 0; req_i < reqs_per_tx; req_i++) {
            size_t row_id;
            while (true) {
              if (kUseContendedSet) {
                if (req_i < kContendedReqPerTX)
                  row_id =
                      (zg.next() * 0x9ddfea08eb382d69ULL) % kContendedSetSize;
                else
                  row_id = (zg.next() * 0x9ddfea08eb382d69ULL) %
                               (num_rows - kContendedSetSize) +
                           kContendedSetSize;
              } else {
                row_id = (zg.next() * 0x9ddfea08eb382d69ULL) % num_rows;
              }
              // Avoid duplicate row IDs in a single transaction.
              uint64_t req_j;
              for (req_j = 0; req_j < req_i; req_j++)
                if (tasks[thread_id].row_ids[req_offset + req_j] == row_id)
                  break;
              if (req_j == req_i) break;
            }
            uint8_t column_id = static_cast<uint8_t>(column_id_rand.next_u32() %
                                                     (kDataSize / kColumnSize));
            tasks[thread_id].row_ids[req_offset + req_i] = row_id;
            tasks[thread_id].column_ids[req_offset + req_i] = column_id;

            if (op_type_rand.next_u32() <= read_threshold)
              tasks[thread_id].op_types[req_offset + req_i] = 0;
            else {
              tasks[thread_id].op_types[req_offset + req_i] =
                  op_type_rand.next_u32() <= rmw_threshold ? 1 : 2;
              read_only_tx = false;
            }
          }
          tasks[thread_id].req_counts[tx_i] =
              static_cast<uint16_t>(reqs_per_tx);
          tasks[thread_id].read_only_tx[tx_i] =
              static_cast<uint8_t>(read_only_tx);
          if (kUseScan) {
            tasks[thread_id].scan_lens[tx_i] =
                scan_len_rand.next_u32() % (kMaxScanLen - 1) + 1;
          }
          req_offset += reqs_per_tx;
        }
      });
    }

    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }
  }

  // tbl->print_table_status();
  //
  // if (kShowPoolStats) db.print_pool_status();

  // For verification.
  std::vector<Timestamp> table_ts;

  for (auto phase = 0; phase < 2; phase++) {
    // if (kVerify && phase == 0) {
    //   printf("skipping warming up\n");
    //   continue;
    // }

    if (kVerify && phase == 1) {
      for (uint64_t row_id = 0; row_id < num_rows; row_id++) {
        auto rv = tbl->latest_rv(0, row_id);
        table_ts.push_back(rv->wts);
      }
    }

    if (phase == 0)
      printf("warming up\n");
    else {
      db.reset_stats();
      printf("executing workload\n");
    }

    running_threads = 0;
    stopping = 0;

    ::mica::util::memory_barrier();

    std::vector<std::thread> threads;
    for (uint64_t thread_id = 1; thread_id < num_threads; thread_id++)
      threads.emplace_back(worker_proc, &tasks[thread_id]);

    if (phase != 0 && kRunPerf) {
      int r = system("perf record -a sleep 1 &");
      // int r = system("perf record -a -g sleep 1 &");
      (void)r;
    }

    worker_proc(&tasks[0]);

    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }
  }
  printf("\n");

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

    uint64_t total_committed = 0;
    uint64_t total_scanned = 0;
    for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
      total_committed += tasks[thread_id].committed;
      if (kUseScan) total_scanned += tasks[thread_id].scanned;
    }
    printf("throughput:                   %7.3lf M/sec\n",
           static_cast<double>(total_committed) / diff * 0.000001);
    if (kUseScan)
      printf("scan throughput:              %7.3lf M/sec\n",
             static_cast<double>(total_scanned) / diff * 0.000001);

    db.print_stats(diff, total_time);

    tbl->print_table_status();

    if (hash_idx != nullptr) hash_idx->index_table()->print_table_status();
    if (btree_idx != nullptr) btree_idx->index_table()->print_table_status();

    if (kShowPoolStats) db.print_pool_status();
  }

  if (kVerify) {
    printf("verifying\n");
    const bool print_verification = false;
    // const bool print_verification = true;

    if (kUseSnapshot)
      printf(
          "warning: verification currently does not support transactions using "
          "snapshots\n");

    uint64_t total_tx_i = 0;
    uint64_t current_commit_i[num_threads];
    for (uint64_t thread_id = 0; thread_id < num_threads; thread_id++)
      current_commit_i[thread_id] = 0;

    while (true) {
      // Simple but slow way to find the next executed transaction.
      uint64_t next_thread_id = static_cast<uint64_t>(-1);
      Timestamp next_ts = Timestamp::make(0, 0, 0);
      for (uint64_t thread_id = 0; thread_id < num_threads; thread_id++) {
        Task* task = &tasks[thread_id];

        if (current_commit_i[thread_id] >= task->committed) continue;

        uint64_t tx_i = task->commit_tx_i[current_commit_i[thread_id]];
        if (next_ts > task->commit_ts[tx_i] ||
            next_thread_id == static_cast<uint64_t>(-1)) {
          next_thread_id = thread_id;
          next_ts = task->commit_ts[tx_i];
        }
      }
      if (next_thread_id == static_cast<uint64_t>(-1)) break;

      Task* task = &tasks[next_thread_id];
      uint64_t tx_i = task->commit_tx_i[current_commit_i[next_thread_id]];
      // XXX: Assume the constant number of reqs per tx.
      uint64_t total_req_i = reqs_per_tx * tx_i;

      if (print_verification)
        printf("thread=%2" PRIu64 " tx=%" PRIu64 " commit_ts=0x%" PRIx64 "\n",
               next_thread_id, tx_i, next_ts.t2);

      for (uint64_t req_j = 0; req_j < task->req_counts[tx_i]; req_j++) {
        const Timestamp& read_ts = task->read_ts[total_req_i + req_j];
        const Timestamp& write_ts = task->write_ts[total_req_i + req_j];
        uint64_t row_id = task->row_ids[total_req_i + req_j];
        bool is_read = task->op_types[total_req_i + req_j] == 0;
        bool is_rmw = task->op_types[total_req_i + req_j] == 1;

        if (print_verification)
          printf("  req %2" PRIu64 " row=%9" PRIu64
                 " is_read=%d read_ts=0x%" PRIx64 "\n",
                 req_j, row_id, is_read ? 1 : 0, read_ts.t2);

        // The read set timestamp must be smaller than the commit timestamp
        // (consistency).
        if (read_ts >= next_ts) {
          printf("verification failed at %" PRIu64
                 " (read ts must predate commit ts)\n",
                 total_tx_i);
          assert(false);
          return 1;
        }

        if (is_read || is_rmw) {
          // There must be no extra version between the read and commit
          // timestamp unless it is a write-only operation (atomicity).
          const Timestamp& stored_ts = table_ts[row_id];
          if (stored_ts != read_ts) {
            printf("verification failed at %" PRIu64
                   " (read ts mismatch; expected 0x%" PRIx64 ", got 0x%" PRIx64
                   ")\n",
                   total_tx_i, read_ts.t2, stored_ts.t2);
            assert(false);
            return 1;
          }
        }

        // Register the new version.  It is guranteed to be the latest version
        // because we chose the transaction in their commit ts order.
        // if (!is_read) table_ts[row_id] = next_ts;
        // Since we now have a promotion, a read may be actually a write.
        // It keeps the old timestamp or updated to the transaction's timestamp.
        assert(write_ts == read_ts || write_ts == next_ts);
        table_ts[row_id] = write_ts;
      }

      current_commit_i[next_thread_id]++;
      total_tx_i++;
    }

    // Final check.
    for (uint64_t row_id = 0; row_id < num_rows; row_id++) {
      auto stored_ts = table_ts[row_id];
      auto rv = tbl->latest_rv(0, row_id);
      auto read_ts = rv->wts;

      if (stored_ts != read_ts) {
        printf("verification failed at %" PRIu64
               " (read ts mismatch; expected 0x%" PRIx64 ", got 0x%" PRIx64
               ")\n",
               total_tx_i, read_ts.t2, stored_ts.t2);
        assert(false);
        return 1;
      }
    }

    printf("passed verification\n");
    printf("\n");
  }

  {
    printf("cleaning up\n");
    for (uint64_t thread_id = 0; thread_id < num_threads; thread_id++) {
      alloc.free_contiguous(tasks[thread_id].req_counts);
      alloc.free_contiguous(tasks[thread_id].read_only_tx);
      if (kUseScan) alloc.free_contiguous(tasks[thread_id].scan_lens);
      alloc.free_contiguous(tasks[thread_id].row_ids);
      alloc.free_contiguous(tasks[thread_id].column_ids);
      alloc.free_contiguous(tasks[thread_id].op_types);
      if (kVerify) {
        alloc.free_contiguous(tasks[thread_id].commit_tx_i);
        alloc.free_contiguous(tasks[thread_id].commit_ts);
        alloc.free_contiguous(tasks[thread_id].read_ts);
        alloc.free_contiguous(tasks[thread_id].write_ts);
      }
    }
  }

  return EXIT_SUCCESS;
}
