#pragma once
#ifndef MICA_TRANSACTION_ROW_VERSION_POOL_H_
#define MICA_TRANSACTION_ROW_VERSION_POOL_H_

#include <cstdio>
#include <vector>
#include "mica/transaction/page_pool.h"
#include "mica/transaction/row.h"
#include "mica/transaction/stats.h"
#include "mica/util/lcore.h"
#include "mica/util/tsc.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
class Context;

template <class StaticConfig>
class Table;

template <class StaticConfig>
class RowVersionPool;

template <class StaticConfig>
struct RowVersionGroup {
  RowVersionGroup() {}
  RowVersionGroup(RowVersion<StaticConfig>* rv, uint64_t count)
      : rv(rv), count(count) {}
  RowVersion<StaticConfig>* rv;
  uint64_t count;
};

template <class StaticConfig>
class SharedRowVersionPool {
 public:
  static constexpr uint64_t kSizeIncrement = 64;
  static constexpr uint16_t kClassCount = 1024;

  static constexpr uint16_t rv_size_to_class(uint64_t rv_size) {
    assert(rv_size != 0);
    auto cls = (rv_size - 1) / 64;  // Map 64-byte to class 0.
    assert(cls < kClassCount);
    return static_cast<uint16_t>(cls);
  }
  static constexpr uint16_t data_size_to_class(uint64_t data_size) {
    return rv_size_to_class(sizeof(RowVersion<StaticConfig>) + data_size);
  }

  static constexpr uint64_t class_to_rv_size(uint16_t cls) {
    assert(cls < kClassCount);
    return static_cast<uint64_t>(cls) * 64 + 64;
  }

  static constexpr uint64_t class_to_data_size(uint16_t cls) {
    return class_to_rv_size(cls) - sizeof(RowVersion<StaticConfig>);
  }

  SharedRowVersionPool(PagePool<StaticConfig>* page_pool, uint8_t numa_id)
      : page_pool_(page_pool) {
    assert(page_pool->numa_id() == numa_id);
    (void)numa_id;

    lock_ = 0;
    for (uint16_t cls = 0; cls < kClassCount; cls++) {
      auto& cls_info = classes_[cls];
      cls_info.total_count = 0;
      cls_info.free_count = 0;
    }
  }

  ~SharedRowVersionPool() {
    for (auto& page : pages_) page_pool_->free(page);
  }

  void acquire(uint16_t cls, RowVersionGroup<StaticConfig>* groups,
               uint64_t& group_count) {
    assert(cls < kClassCount);
    auto& cls_info = classes_[cls];

    while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

    uint64_t group_i;
    for (group_i = 0; group_i < group_count; group_i++) {
      if (cls_info.groups.empty()) {
        allocate(cls);

        if (cls_info.groups.empty()) {
          fprintf(stderr, "SharedRowVersionPool: Failed to allocate memory\n");
          break;
        }
      }

      groups[group_i] = cls_info.groups.back();
      cls_info.free_count -= cls_info.groups.back().count;
      cls_info.groups.pop_back();
    }

    __sync_lock_release(&lock_);

    group_count = group_i;
    // printf("acquire: %" PRIu64 " groups\n", group_count);
  }

  void release(uint16_t cls, const RowVersionGroup<StaticConfig>* groups,
               uint64_t group_count) {
    assert(cls < kClassCount);
    auto& cls_info = classes_[cls];

    while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

    for (uint64_t group_i = 0; group_i < group_count; group_i++) {
      cls_info.groups.push_back(groups[group_i]);
      cls_info.free_count += groups[group_i].count;
    }

    __sync_lock_release(&lock_);
  }

  uint64_t total_count(uint16_t cls) const { return classes_[cls].total_count; }
  uint64_t free_count(uint16_t cls) const { return classes_[cls].free_count; }

 private:
  void allocate(uint16_t cls) {
    assert(lock_ == 1);

    auto& cls_info = classes_[cls];

    uint64_t rv_size = class_to_rv_size(cls);
    uint64_t count = PagePool<StaticConfig>::kPageSize / rv_size;
    uint64_t group_count =
        (count + StaticConfig::kRowVersionPoolGroupSize - 1) /
        StaticConfig::kRowVersionPoolGroupSize;

    auto page = page_pool_->allocate();
    if (!page) return;

    // printf("new page at %p\n", page);

    // ::mica::util::memset(page, 0, PagePool<StaticConfig>::kPageSize);

    pages_.push_back(page);

    // Shuffle versions using a random seed to avoid making hotspots in the
    // memory addresses.
    uint64_t off = ::mica::util::rdtsc() % count;

    auto rv = [off, count](char* p, uint64_t i, uint64_t rv_size) {
      i += off;
      if (i >= count) i -= count;
      return reinterpret_cast<RowVersion<StaticConfig>*>(p + i * rv_size);
    };

    auto numa_id = page_pool_->numa_id();

    for (uint64_t i = 0; i < count; i++) {
      rv(page, i, rv_size)->older_rv = rv(page, i + 1, rv_size);
      rv(page, i, rv_size)->status = RowVersionStatus::kInvalid;
      rv(page, i, rv_size)->numa_id = numa_id;
      rv(page, i, rv_size)->size_cls = cls;
    }

    for (uint64_t i = 0; i < group_count; i++) {
      uint64_t group_i = (i + off) % group_count;

      uint64_t start_i = group_i * StaticConfig::kRowVersionPoolGroupSize;
      uint64_t end_i = start_i + StaticConfig::kRowVersionPoolGroupSize;
      if (end_i > count) end_i = count;
      // printf("%" PRIu64 " %" PRIu64 "\n", start_i, end_i);
      assert(start_i < end_i);

      rv(page, end_i - 1, rv_size)->older_rv = nullptr;

      cls_info.groups.emplace_back(rv(page, start_i, rv_size), end_i - start_i);
    }

    cls_info.total_count += count;
    cls_info.free_count += count;
  }

  struct ClassInfo {
    uint64_t total_count;
    uint64_t free_count;
    std::vector<RowVersionGroup<StaticConfig>> groups;
  };

  PagePool<StaticConfig>* page_pool_;

  volatile uint32_t lock_;
  ClassInfo classes_[kClassCount];
  std::vector<char*> pages_;
} __attribute__((aligned(64)));

template <class StaticConfig>
class RowVersionPool {
 public:
  typedef typename StaticConfig::Timing Timing;
  static constexpr uint64_t kSizeIncrement =
      SharedRowVersionPool<StaticConfig>::kSizeIncrement;
  static constexpr uint16_t kClassCount =
      SharedRowVersionPool<StaticConfig>::kClassCount;

  RowVersionPool(Context<StaticConfig>* ctx,
                 SharedRowVersionPool<StaticConfig>** shared_pool)
      : ctx_(ctx), shared_pools_(shared_pool) {
    // shown_gc_warning_ = false;

    for (uint8_t numa_id = 0; numa_id < ctx_->db()->numa_count(); numa_id++) {
      for (uint16_t cls = 0; cls < kClassCount; cls++) {
        auto state = &states_[numa_id * kClassCount + cls];
        state->total_count = 0;
        // state->free_count = 0;
        state->current_free_count = 0;
        state->rv = nullptr;
        state->group_count = 0;
      }
    }
  }

  ~RowVersionPool() {
    for (uint8_t numa_id = 0; numa_id < ctx_->db()->numa_count(); numa_id++) {
      for (uint16_t cls = 0; cls < kClassCount; cls++) {
        auto state = &states_[numa_id * kClassCount + cls];

        if (state->current_free_count != 0) {
          assert(state->group_count <
                 StaticConfig::kRowVersionPoolGroupMaxCount);
          state->groups[state->group_count].rv = state->rv;
          state->groups[state->group_count].count = state->current_free_count;
          state->group_count++;
        }

        return_rows(numa_id, cls, true);
      }
    }
  }

  RowVersion<StaticConfig>* allocate(uint16_t cls) {
    Timing t(ctx_->timing_stack(), &Stats::alloc);

    if (StaticConfig::kVerbose) printf("allocate\n");

    uint8_t numa_id = ctx_->numa_id();
    State* state = nullptr;

    // printf("1\n");
    for (auto trial = 0; trial < ctx_->db()->numa_count(); trial++) {
      state = &states_[numa_id * kClassCount + cls];

      if (state->current_free_count != 0) break;

      // printf("2\n");
      if (state->group_count == 0) refill_rows(numa_id, cls);

      // printf("3\n");
      if (state->group_count != 0) {
        // printf("4\n");
        state->group_count--;
        // state->free_count -= state->groups[state->group_count].count;
        state->current_free_count = state->groups[state->group_count].count;
        state->rv = state->groups[state->group_count].rv;
        break;
      }

      if (++numa_id == ctx_->db()->numa_count()) numa_id = 0;
    }

    // printf("5\n");
    assert(state != nullptr);
    if (state->current_free_count == 0) {
      // assert(false);
      return nullptr;
    }

    // If this fails, something like GC has written to an already deallocated
    // RowVersion to modify its older_rv.
    assert(state->rv != nullptr);

    auto rv = state->rv;
    state->rv = rv->older_rv;
    state->current_free_count--;

    __builtin_prefetch(state->rv, 1, 3);

    assert(rv->status == RowVersionStatus::kInvalid);
    assert(rv->numa_id == numa_id);
    assert(rv->size_cls == cls);

    return rv;
  }

  void deallocate(RowVersion<StaticConfig>* rv) {
    Timing t(ctx_->timing_stack(), &Stats::dealloc);

    if (StaticConfig::kVerbose) printf("deallocate\n");

    assert(rv != nullptr);

    assert(!StaticConfig::kInlinedRowVersion ||
           (StaticConfig::kInlinedRowVersion && !rv->is_inlined()));

    auto cls = rv->size_cls;

    uint8_t numa_id = rv->numa_id;
    auto state = &states_[numa_id * kClassCount + cls];

    rv->status = RowVersionStatus::kInvalid;
    rv->older_rv = state->rv;
    state->rv = rv;
    state->current_free_count++;

    if (state->current_free_count == StaticConfig::kRowVersionPoolGroupSize) {
      assert(state->group_count < StaticConfig::kRowVersionPoolGroupMaxCount);
      state->groups[state->group_count].rv = state->rv;
      state->groups[state->group_count].count = state->current_free_count;
      state->group_count++;

      state->rv = nullptr;
      // state->free_count += state->current_free_count;
      state->current_free_count = 0;

      if (state->group_count ==
          StaticConfig::kRowVersionPoolGroupMaxCount /*||
          numa_id != ctx_->numa_id()*/) {
        // Return groups of rows to the shared pool if there are too many unused
        // groups for the same NUMA node or there is any unused group for a
        // different NUMA node.
        return_rows(numa_id, cls, false);
      }
    }
  }

  uint64_t total_count(uint16_t cls) const {
    uint64_t c = 0;
    for (uint8_t numa_id = 0; numa_id < ctx_->db()->numa_count(); numa_id++) {
      auto state = &states_[numa_id * kClassCount + cls];
      c += state->total_count;
    }
    return c;
  }

  uint64_t free_count(uint16_t cls) const {
    uint64_t c = 0;
    for (uint8_t numa_id = 0; numa_id < ctx_->db()->numa_count(); numa_id++) {
      auto state = &states_[numa_id * kClassCount + cls];
      c += state->current_free_count;
      // c += state->free_count;
      for (uint64_t group_i = 0; group_i < state->group_count; group_i++)
        c += state->groups[group_i].count;
    }
    return c;
  }

 private:
  Context<StaticConfig>* ctx_;
  SharedRowVersionPool<StaticConfig>** shared_pools_;

  // bool shown_gc_warning_;

  struct State {
    uint64_t total_count;
    // uint64_t free_count;
    uint64_t current_free_count;
    RowVersion<StaticConfig>* rv;

    RowVersionGroup<StaticConfig>
        groups[StaticConfig::kRowVersionPoolGroupMaxCount];
    uint64_t group_count;
  };

  State states_[StaticConfig::kMaxNUMACount * kClassCount];

  void refill_rows(uint8_t numa_id, uint16_t cls) {
    if (StaticConfig::kVerbose) printf("refill_rows\n");

    if (StaticConfig::kCollectProcessingStats)
      ctx_->stats().refill_rows_count++;

    auto state = &states_[numa_id * kClassCount + cls];

    assert(state->group_count == 0);

    uint64_t new_group_count = StaticConfig::kRowVersionPoolGroupMaxCount / 2;
    shared_pools_[numa_id]->acquire(cls, state->groups, new_group_count);

    if (new_group_count == 0) {
      // assert(false);

      // if (!shown_gc_warning_) {
      //   printf(
      //       "Warning: Forced GC has been invoked!  Performance will be "
      //       "lower.\n");
      //   shown_gc_warning_ = true;
      // }
      // ctx_->gc(true);

      new_group_count = StaticConfig::kRowVersionPoolGroupMaxCount / 2;
      shared_pools_[numa_id]->acquire(cls, state->groups, new_group_count);

      if (new_group_count == 0) {
        // assert(false);

        // fprintf(stderr, "out of memory\n");
        return;
      }
    }

    for (uint64_t group_i = 0; group_i < new_group_count; group_i++) {
      auto& group = state->groups[group_i];
      assert(group.count != 0);
      state->total_count += group.count;
      // state->free_count += group.count;
    }
    state->group_count = new_group_count;
  }

  void return_rows(uint8_t numa_id, uint16_t cls, bool all) {
    if (StaticConfig::kVerbose) printf("return_rows\n");

    if (StaticConfig::kCollectProcessingStats)
      ctx_->stats().return_rows_count++;

    auto state = &states_[numa_id * kClassCount + cls];

    // Return half of unused groups.  Leaving the half reduces threshing for
    // frequent allocation/deallocation cycles.
    uint64_t new_group_count = all ? 0 : (state->group_count / 2);
    for (uint64_t group_i = new_group_count; group_i < state->group_count;
         group_i++) {
// Suppress a false compiler warning.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
      auto& group = state->groups[group_i];
#pragma GCC diagnostic pop
      state->total_count -= group.count;
      // state->free_count -= group.count;
    }
    shared_pools_[numa_id]->release(cls, state->groups + new_group_count,
                                    state->group_count - new_group_count);
    state->group_count = new_group_count;
  }
} __attribute__((aligned(64)));
}
}

#endif
