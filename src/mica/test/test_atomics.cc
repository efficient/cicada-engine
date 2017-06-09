#include <cstdio>
#include <thread>
#include "mica/util/lcore.h"
#include "mica/util/zipf.h"
#include "mica/util/stopwatch.h"
#include "mica/util/barrier.h"
#include "mica/util/memcpy.h"

enum class OpsType {
  kRAW = 0,
  kRAWF,
  kFAA,
  kAAF,
  kCAS,
  kTAS,
  kRFAA,
  kRCAS,
  kHFAA,
  kCCAS,
  kMax,
};

static const char* ops_names[] = {
    "read, add, and write", "RAW and fence",  "fetch and add", "add and fetch",
    "compare and swap",     "test and set",   "remote FAA",    "remote CAS",
    "hybrid FAA",           "conditional CAS"};

struct Task {
  uint16_t lcore_id;
  uint16_t num_threads;
  uint16_t num_lock_threads;

  OpsType ops_type;

  uint64_t c;
  struct timeval tv_start;
  struct timeval tv_end;
} __attribute__((aligned(128)));

static ::mica::util::Stopwatch sw;
static volatile uint16_t running_threads;
static volatile uint8_t stop;

static const int kQueueDepth = 512;

struct faa_request {
  uint64_t* p;
  uint64_t incr;
};

struct cas_request {
  uint64_t* p;
  uint64_t old_v;
  uint64_t new_v;
} __attribute__((packed));
// } __attribute__((aligned(32)));

struct response {
  uint64_t result;
};

struct queue {
  volatile faa_request faa_req[2][kQueueDepth] __attribute__((aligned(64)));
  volatile cas_request cas_req[2][kQueueDepth] __attribute__((aligned(64)));
  volatile int num_req;
  volatile int req_q_idx;
  volatile int num_res;
  volatile response res[kQueueDepth] __attribute__((aligned(64)));
} __attribute__((aligned(64)));

static queue queues[64];

static uint64_t v = 0;

void handle_faa_request(uint16_t num_queues) {
  while (!stop) {
    for (uint16_t i = 0; i < num_queues; i++) {
      int num_req = queues[i].num_req;
      if (num_req == 0) continue;
      ::mica::util::memory_barrier();

      volatile faa_request* req = queues[i].faa_req[queues[i].req_q_idx];

      for (int j = 0; j < num_req; j++) {
        __builtin_prefetch(const_cast<faa_request*>(&req[j + 4]), 0);
        // __builtin_prefetch(const_cast<response*>(&queues[i].res[j + 4]), 1);
        uint64_t* p = req[j].p;
        uint64_t v = *p;
        queues[i].res[j].result = v;
        *p = v + req[j].incr;
      }

      // for (int j = 0; j < num_req; j++) clflush(&req[j]);

      ::mica::util::memory_barrier();
      queues[i].num_res = num_req;
    }
  }
}

void handle_atomic_faa_request(uint16_t num_queues, uint16_t lock_thread_id,
                               uint16_t num_lock_threads) {
  while (!stop) {
    for (int i = lock_thread_id; i < num_queues; i += num_lock_threads) {
      int num_req = queues[i].num_req;
      if (num_req == 0) continue;
      ::mica::util::memory_barrier();

      volatile faa_request* req = queues[i].faa_req[queues[i].req_q_idx];

      for (int j = 0; j < num_req; j++) {
        // __builtin_prefetch(const_cast<faa_request*>(&req[j + 4]), 0);
        // __builtin_prefetch(const_cast<response*>(&queues[i].res[j + 4]), 1);
        // TODO: Use atomics without LOCK prefix.
        queues[i].res[j].result = __sync_fetch_and_add(req[j].p, req[j].incr);
      }

      // for (int j = 0; j < num_req; j++) clflush(&req[j]);

      ::mica::util::memory_barrier();
      queues[i].num_res = num_req;
    }
  }
}

void handle_cas_request(uint16_t num_queues) {
  while (!stop) {
    for (uint16_t i = 0; i < num_queues; i++) {
      int num_req = queues[i].num_req;
      if (num_req == 0) continue;
      ::mica::util::memory_barrier();

      volatile cas_request* req = queues[i].cas_req[queues[i].req_q_idx];

      for (int j = 0; j < num_req; j++) {
        __builtin_prefetch(const_cast<cas_request*>(&req[j + 4]), 0);
        // __builtin_prefetch(const_cast<response*>(&queues[i].res[j + 4]), 1);
        uint64_t* p = req[j].p;
        uint64_t v = *p;

        if (v == req[j].old_v) {
          *p = req[j].new_v;
          queues[i].res[j].result = 1;
        } else {
          queues[i].res[j].result = 0;
        }
      }

      // for (int j = 0; j < num_req; j++) clflush(&req[j]);

      ::mica::util::memory_barrier();
      queues[i].num_res = num_req;
    }
  }
}

int worker_proc(void* arg) {
  auto task = reinterpret_cast<Task*>(arg);

  ::mica::util::lcore.pin_thread(task->lcore_id);

  // printf("worker running on lcore %" PRIu16 "\n", task->lcore_id);

  __sync_add_and_fetch(&running_threads, 1);
  while (running_threads < task->num_threads) ::mica::util::pause();

  uint64_t c = 0;

  gettimeofday(&task->tv_start, nullptr);
  uint64_t t = sw.now();

  if (task->ops_type == OpsType::kRFAA &&
      task->lcore_id >= task->num_threads - 1) {
    assert(task->num_threads >= 2);
    handle_faa_request(static_cast<uint16_t>(task->num_threads - 1));
    task->c = 0;
    task->tv_end = task->tv_start;
    return 0;
  } else if (task->ops_type == OpsType::kRCAS &&
             task->lcore_id >= task->num_threads - 1) {
    assert(task->num_threads >= 2);
    handle_cas_request(static_cast<uint16_t>(task->num_threads - 1));
    task->c = 0;
    task->tv_end = task->tv_start;
    return 0;
  } else if (task->ops_type == OpsType::kHFAA &&
             task->lcore_id >= task->num_threads - task->num_lock_threads) {
    assert(task->num_threads >= 2);
    handle_atomic_faa_request(
        static_cast<uint16_t>(task->num_threads - task->num_lock_threads),
        static_cast<uint16_t>(task->lcore_id -
                              (task->num_threads - task->num_lock_threads)),
        task->num_lock_threads);
    task->c = 0;
    task->tv_end = task->tv_start;
    return 0;
  }

  int req_q_idx = 0;
  int q_i = 0;

  const uint32_t n = 100000;
  while (!stop) {
    switch (task->ops_type) {
      case OpsType::kRAW: {
        for (uint32_t i = 0; i < n / 10; i++) {
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          ++*reinterpret_cast<volatile uint64_t*>(&v);
        }
      } break;
      case OpsType::kRAWF: {
        // auto fence = [] {};
        // auto fence = [] { ::mica::util::memory_barrier(); };
        // auto fence = [] { ::mica::util::lfence(); };
        auto fence = [] { ::mica::util::sfence(); };
        // auto fence = [] { ::mica::util::mfence(); };

        for (uint32_t i = 0; i < n / 10; i++) {
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          fence();
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          fence();
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          fence();
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          fence();
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          fence();
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          fence();
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          fence();
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          fence();
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          fence();
          ++*reinterpret_cast<volatile uint64_t*>(&v);
          fence();
        }
      } break;
      case OpsType::kFAA:
        // for (uint32_t i = 0; i < n; i++) __sync_fetch_and_add(&v,
        // uint64_t(1));
        for (uint32_t i = 0; i < n / 10; i++) {
          __sync_fetch_and_add(&v, uint64_t(1));
          __sync_fetch_and_add(&v, uint64_t(1));
          __sync_fetch_and_add(&v, uint64_t(1));
          __sync_fetch_and_add(&v, uint64_t(1));
          __sync_fetch_and_add(&v, uint64_t(1));
          __sync_fetch_and_add(&v, uint64_t(1));
          __sync_fetch_and_add(&v, uint64_t(1));
          __sync_fetch_and_add(&v, uint64_t(1));
          __sync_fetch_and_add(&v, uint64_t(1));
          __sync_fetch_and_add(&v, uint64_t(1));
        }
        break;
      case OpsType::kAAF:
        // for (uint32_t i = 0; i < n; i++) __sync_add_and_fetch(&v,
        // uint64_t(1));
        for (uint32_t i = 0; i < n / 10; i++) {
          __sync_add_and_fetch(&v, uint64_t(1));
          __sync_add_and_fetch(&v, uint64_t(1));
          __sync_add_and_fetch(&v, uint64_t(1));
          __sync_add_and_fetch(&v, uint64_t(1));
          __sync_add_and_fetch(&v, uint64_t(1));
          __sync_add_and_fetch(&v, uint64_t(1));
          __sync_add_and_fetch(&v, uint64_t(1));
          __sync_add_and_fetch(&v, uint64_t(1));
          __sync_add_and_fetch(&v, uint64_t(1));
          __sync_add_and_fetch(&v, uint64_t(1));
        }
        break;
      case OpsType::kCAS:
        // Faster without unrolling.
        for (uint32_t i = 0; i < n; i++)
          __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        // for (uint32_t i = 0; i < n / 10; i++) {
        //   __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        //   __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        //   __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        //   __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        //   __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        //   __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        //   __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        //   __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        //   __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        //   __sync_val_compare_and_swap(&v, uint64_t(0), uint64_t(0));
        // }
        break;
      case OpsType::kTAS:
        // Faster without unrolling.
        for (uint32_t i = 0; i < n; i++)
          __sync_lock_test_and_set(&v, uint64_t(0));
        // for (uint32_t i = 0; i < n / 10; i++) {
        //   __sync_lock_test_and_set(&v, uint64_t(0));
        //   __sync_lock_test_and_set(&v, uint64_t(0));
        //   __sync_lock_test_and_set(&v, uint64_t(0));
        //   __sync_lock_test_and_set(&v, uint64_t(0));
        //   __sync_lock_test_and_set(&v, uint64_t(0));
        //   __sync_lock_test_and_set(&v, uint64_t(0));
        //   __sync_lock_test_and_set(&v, uint64_t(0));
        //   __sync_lock_test_and_set(&v, uint64_t(0));
        //   __sync_lock_test_and_set(&v, uint64_t(0));
        //   __sync_lock_test_and_set(&v, uint64_t(0));
        // }
        break;
      case OpsType::kRFAA:
      case OpsType::kHFAA: {
        for (uint32_t i = 0; i < n; i++) {
          queues[task->lcore_id].faa_req[req_q_idx][q_i].p = &v;
          queues[task->lcore_id].faa_req[req_q_idx][q_i].incr = 1;
          q_i++;

          if (q_i == kQueueDepth) {
            // Wait for previous responses.
            while (queues[task->lcore_id].num_req != 0 &&
                   queues[task->lcore_id].num_res == 0 && !stop)
              ::mica::util::pause();

            // Consume responses.
            queues[task->lcore_id].num_res = 0;

            // Supply new requests.
            queues[task->lcore_id].req_q_idx = req_q_idx;
            ::mica::util::memory_barrier();
            queues[task->lcore_id].num_req = q_i;

            req_q_idx = 1 - req_q_idx;
            q_i = 0;
          }
        }
      } break;
      case OpsType::kRCAS: {
        for (uint32_t i = 0; i < n; i++) {
          queues[task->lcore_id].cas_req[req_q_idx][q_i].p = &v;
          queues[task->lcore_id].cas_req[req_q_idx][q_i].old_v = 0;
          queues[task->lcore_id].cas_req[req_q_idx][q_i].new_v = 0;
          q_i++;

          if (q_i == kQueueDepth) {
            // Wait for previous responses.
            while (queues[task->lcore_id].num_req != 0 &&
                   queues[task->lcore_id].num_res == 0 && !stop)
              ::mica::util::pause();

            // Consume responses.
            queues[task->lcore_id].num_res = 0;

            // Supply new requests.
            queues[task->lcore_id].req_q_idx = req_q_idx;
            ::mica::util::memory_barrier();
            queues[task->lcore_id].num_req = q_i;

            req_q_idx = 1 - req_q_idx;
            q_i = 0;
          }
        }
      } break;
      case OpsType::kCCAS: {
        uint64_t new_v = sw.now();
        for (uint32_t i = 0; i < n; i++) {
          uint64_t v_local = *(volatile uint64_t*)&v;
          while (v_local < new_v) {
            uint64_t actual_v = __sync_val_compare_and_swap(&v, v_local, new_v);
            if (actual_v == v_local) break;
            ::mica::util::pause();
            v_local = actual_v;
          }
          if ((i & 0xff) == 0)
            new_v = sw.now(); // Emulates occasional clock sync.
          else
            new_v += 1000UL;
        }
      } break;
      default:
        assert(false);
    }
    c += n;
    // printf("a\n");

    if (task->lcore_id == 0 &&
        sw.diff_in_cycles(sw.now(), t) >= 3 * sw.c_1_sec())
      stop = 1;
  }

  gettimeofday(&task->tv_end, nullptr);

  task->c = c;

  return 0;
}

int main(int argc, const char* argv[]) {
  if (argc != 2) {
    printf("%s THREAD-COUNT\n", argv[0]);
    return EXIT_FAILURE;
  }

  ::mica::util::lcore.pin_thread(0);

  sw.init_start();
  sw.init_end();

  printf("1 seconds: %" PRIu64 " cycles\n", sw.c_1_sec());

  uint16_t num_threads = static_cast<uint16_t>(atoi(argv[1]));
  assert(num_threads <=
         static_cast<uint16_t>(::mica::util::lcore.lcore_count()));
  printf("num_threads: %hu\n", num_threads);
  printf("\n");

  for (int type = 0; type < static_cast<int>(OpsType::kMax); type++) {
    // if (type != static_cast<int>(OpsType::kRFAA) &&
    //     type != static_cast<int>(OpsType::kRCAS) &&
    //     type != static_cast<int>(OpsType::kHFAA))
    //   continue;
    // if (type != static_cast<int>(OpsType::kCAS) &&
    //     type != static_cast<int>(OpsType::kCCAS))
    //   continue;
    std::vector<Task> tasks(num_threads);
    for (uint16_t lcore_id = 0; lcore_id < num_threads; lcore_id++) {
      tasks[lcore_id].lcore_id = lcore_id;
      tasks[lcore_id].num_threads = num_threads;
      if (type != static_cast<int>(OpsType::kHFAA))
        tasks[lcore_id].num_lock_threads = 1;
      else
        tasks[lcore_id].num_lock_threads = 1;
      tasks[lcore_id].ops_type = static_cast<OpsType>(type);
    }

    running_threads = 0;
    *reinterpret_cast<volatile uint64_t*>(&v) = 0;
    stop = 0;
    for (size_t i = 0; i < sizeof(queues) / sizeof(queues[0]); i++) {
      queues[i].num_req = 0;
      queues[i].num_res = 0;
    }
    ::mica::util::memory_barrier();

    std::vector<std::thread> threads;
    for (size_t thread_id = 1; thread_id < num_threads; thread_id++)
      threads.emplace_back(worker_proc, &tasks[thread_id]);
    worker_proc(&tasks[0]);

    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }

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

    uint64_t c = 0;
    {
      for (size_t thread_id = 0; thread_id < num_threads; thread_id++)
        c += tasks[thread_id].c;
    }

    printf("ops_type: %s\n", ops_names[type]);
    printf("elapsed: %lf\n", diff);
    printf("v:     %lu\n", *reinterpret_cast<volatile uint64_t*>(&v));
    printf("count: %lu (%.3lf M/sec)\n", c,
           static_cast<double>(c) / diff / 1000000.);
    printf("\n");
  }

  return EXIT_SUCCESS;
}