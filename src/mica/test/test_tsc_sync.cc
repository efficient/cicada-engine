#include <cstdio>
#include <thread>
#include "mica/util/lcore.h"
#include "mica/util/stopwatch.h"
#include "mica/util/barrier.h"

static ::mica::util::Stopwatch sw;
static volatile uint16_t running_threads;
static volatile uint16_t finished_threads;
static volatile uint8_t stop;

static const int kQueueDepth = 512;

volatile uint64_t sync_target;
volatile uint64_t master_tsc;
volatile uint64_t slave_tsc;
volatile int64_t slave_adjust;

struct Task {
  uint16_t lcore_id;
  uint16_t master_lcore_id;
  uint16_t num_threads;
} __attribute__((aligned(128)));

int worker_proc(void* arg) {
  auto task = reinterpret_cast<Task*>(arg);

  ::mica::util::lcore.pin_thread(task->lcore_id);

  // printf("worker running on lcore %" PRIu16 "\n", task->lcore_id);

  __sync_add_and_fetch(&running_threads, 1);
  while (running_threads < task->num_threads) ::mica::util::pause();

  uint64_t start_t = sw.now();

  int64_t diff_sum = 0;
  int64_t diff_cnt = 0;
  int64_t latency = 9999999;

  if (task->lcore_id == task->master_lcore_id) {
    uint64_t next_sync_target;
    if (task->master_lcore_id == 0)
      next_sync_target = 1;
    else
      next_sync_target = 0;

    uint64_t duration = sw.c_1_sec() * 6;
    while (true) {
      uint64_t now = sw.now();
      if (now - start_t > duration) break;

      master_tsc = now;
      slave_tsc = 0;
      ::mica::util::memory_barrier();
      sync_target = next_sync_target;

      ::mica::util::memory_barrier();
      while (slave_tsc == 0) {
        now = sw.now();
        if (now - start_t > duration) break;
        ::mica::util::pause();
      }
      if (now - start_t > duration) break;
      ::mica::util::memory_barrier();

      uint64_t master_tsc2 = sw.now();
      // printf("%" PRId64 " %" PRId64 "\n", master_tsc, slave_tsc);
      slave_adjust = (static_cast<int64_t>(master_tsc) +
                      static_cast<int64_t>(master_tsc2)) /
                         2 -
                     static_cast<int64_t>(slave_tsc);
      // slave_adjust = 1;
      // printf("%" PRId64 "\n", slave_adjust);

      if (latency >
          static_cast<int64_t>(master_tsc2) - static_cast<int64_t>(master_tsc))
        latency = static_cast<int64_t>(master_tsc2) -
                  static_cast<int64_t>(master_tsc);

      ::mica::util::memory_barrier();
      while (slave_adjust != 0) ::mica::util::pause();
      ::mica::util::memory_barrier();

      next_sync_target++;
      while (true) {
        if (next_sync_target == task->num_threads)
          next_sync_target = 0;
        else if (next_sync_target == task->master_lcore_id)
          next_sync_target++;
        else
          break;
      }
    }

    while (finished_threads != task->lcore_id) ::mica::util::pause();

    printf("one-way latency: %+6.2lf (%+7.1lf ns)\n",
           static_cast<double>(latency) / 2.,
           static_cast<double>(latency) / 2. /
               static_cast<double>(sw.c_1_sec()) * 1000. * 1000 * 1000.);

    __sync_add_and_fetch(&finished_threads, 1);
  } else {
    int64_t tsc_offset = 0;
    uint64_t duration = sw.c_1_sec() * 5;

    while (true) {
      uint64_t now = sw.now();
      if (now - start_t > duration) break;

      ::mica::util::memory_barrier();
      while (sync_target != task->lcore_id) ::mica::util::pause();
      sync_target = task->master_lcore_id;
      ::mica::util::memory_barrier();

      now = sw.now();
      slave_tsc = now + static_cast<uint64_t>(tsc_offset);

      ::mica::util::memory_barrier();
      while (slave_adjust == 0) ::mica::util::pause();
      ::mica::util::memory_barrier();

      int64_t adjust = slave_adjust;
      slave_adjust = 0;
      // printf("%" PRId64 "\n", adjust);

      tsc_offset += adjust * 1 / 50;

      diff_sum += adjust;
      diff_cnt++;
    }

    while (finished_threads != task->lcore_id) ::mica::util::pause();

    printf("thread %2" PRIu16 ": offset = %+5" PRId64
           " (%+7.1lf ns), avg_jitter = %+6.2lf (%+7.1lf ns)\n",
           task->lcore_id, tsc_offset,
           static_cast<double>(tsc_offset) / static_cast<double>(sw.c_1_sec()) *
               1000. * 1000 * 1000.,
           static_cast<double>(diff_sum) / static_cast<double>(diff_cnt),
           static_cast<double>(diff_sum) / static_cast<double>(diff_cnt) /
               static_cast<double>(sw.c_1_sec()) * 1000. * 1000 * 1000.);

    __sync_add_and_fetch(&finished_threads, 1);
  }
  return 0;
}

int main(int argc, const char* argv[]) {
  if (argc != 3) {
    printf("%s MASTER-LCORE-ID THREAD-COUNT\n", argv[0]);
    return EXIT_FAILURE;
  }

  ::mica::util::lcore.pin_thread(0);

  sw.init_start();
  sw.init_end();

  uint16_t master_lcore_id = static_cast<uint16_t>(atoi(argv[1]));
  uint16_t num_threads = static_cast<uint16_t>(atoi(argv[2]));
  assert(num_threads <=
         static_cast<uint16_t>(::mica::util::lcore.lcore_count()));
  printf("master_lcore_id: %hu\n", master_lcore_id);
  printf("num_threads: %hu\n", num_threads);
  printf("\n");

  std::vector<Task> tasks(num_threads);
  for (uint16_t lcore_id = 0; lcore_id < num_threads; lcore_id++) {
    tasks[lcore_id].lcore_id = lcore_id;
    tasks[lcore_id].master_lcore_id = master_lcore_id;
    tasks[lcore_id].num_threads = num_threads;
  }

  running_threads = 0;
  finished_threads = 0;
  stop = 0;

  sync_target = master_lcore_id;

  ::mica::util::memory_barrier();

  std::vector<std::thread> threads;
  for (size_t thread_id = 1; thread_id < num_threads; thread_id++)
    threads.emplace_back(worker_proc, &tasks[thread_id]);
  worker_proc(&tasks[0]);

  while (threads.size() > 0) {
    threads.back().join();
    threads.pop_back();
  }

  return EXIT_SUCCESS;
}