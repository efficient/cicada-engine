#pragma once
#ifndef MICA_TRANSACTION_TIMESTAMP_H_
#define MICA_TRANSACTION_TIMESTAMP_H_

#include <cassert>
#include "mica/common.h"
#include "mica/util/barrier.h"

namespace mica {
namespace transaction {
struct CompactTimestamp {
  uint64_t t2;

  static CompactTimestamp make(uint32_t era, uint64_t tsc, uint32_t thread_id) {
    CompactTimestamp ts;
    assert(era == 0);
    (void)era;
    assert(thread_id < 256);
    ts.t2 = (tsc << 8) | static_cast<uint64_t>(thread_id);
    return ts;
  }

  bool operator==(const CompactTimestamp& b) const { return t2 == b.t2; }

  bool operator!=(const CompactTimestamp& b) const { return t2 != b.t2; }

  bool operator<(const CompactTimestamp& b) const {
    return static_cast<int64_t>(t2 - b.t2) < 0;
  }

  bool operator<=(const CompactTimestamp& b) const {
    return static_cast<int64_t>(t2 - b.t2) <= 0;
  }

  bool operator>(const CompactTimestamp& b) const {
    return static_cast<int64_t>(t2 - b.t2) > 0;
  }

  bool operator>=(const CompactTimestamp& b) const {
    return static_cast<int64_t>(t2 - b.t2) >= 0;
  }

  bool about_to_expire(const CompactTimestamp& unstable_ts) const {
    return (*this < unstable_ts) &&
           (*this >
            CompactTimestamp{unstable_ts.t2 + (uint64_t(1) << (64 - 4))});
  }

  uint64_t clock_diff(const CompactTimestamp& b) const {
    // We OR the lower 8 bits to avoid thread IDs from causing an underflow
    // during the subtraction.
    return ((t2 | 0xff) - (b.t2 | 0xff)) >> 8;
  }
};

struct CompactConcurrentTimestamp {
  volatile uint64_t t2;

  CompactTimestamp get() const {
    CompactTimestamp ts;
    ts.t2 = t2;
    return ts;
  }

  void init(const CompactTimestamp& b) {
    // Initialize a concurrent ts (a) with b.  a is not being read by others.
    t2 = b.t2;
  }

  void write(const CompactTimestamp& b) {
    // Initialize a concurrent ts (a) with b.  a may be being read by others.
    t2 = b.t2;
  };

  void update(const CompactTimestamp& b) {
    // Make a concurrent ts (a) at least as large as b.
    uint64_t cts_t2 = t2;
    while (static_cast<int64_t>(cts_t2 - b.t2) < 0) {
      auto actual_cts_t2 = __sync_val_compare_and_swap(&t2, cts_t2, b.t2);
      if (actual_cts_t2 == cts_t2) break;

      cts_t2 = actual_cts_t2;
      ::mica::util::pause();
    }
    // if (cts_t2 < b.t2)
    //   __sync_fetch_and_add(&t2, b.t2 - cts_t2);
  }
};

struct WideTimestamp {
  // Logical order: era (32 bits) | tsc (64 bits) | thread id (32 bits)
  //                         t1 (64 bits) | t2 (64 bits)
  uint64_t t1;
  uint64_t t2;

  static WideTimestamp make(uint32_t era, uint64_t tsc, uint32_t thread_id) {
    WideTimestamp ts;
    ts.t1 = (static_cast<uint64_t>(era) << 32) | (tsc >> 32);
    ts.t2 = (tsc << 32) | static_cast<uint64_t>(thread_id);
    return ts;
  }

  bool operator==(const WideTimestamp& b) const {
    return t2 == b.t2 && t1 == b.t1;
  }

  bool operator!=(const WideTimestamp& b) const {
    return t2 != b.t2 || t1 != b.t1;
  }

  bool operator<(const WideTimestamp& b) const {
    if (t1 != b.t1)
      return t1 < b.t1;
    else
      return t2 < b.t2;
  }

  bool operator<=(const WideTimestamp& b) const {
    if (t1 != b.t1)
      return t1 < b.t1;
    else
      return t2 <= b.t2;
  }

  bool operator>(const WideTimestamp& b) const {
    if (t1 != b.t1)
      return t1 > b.t1;
    else
      return t2 > b.t2;
  }

  bool operator>=(const WideTimestamp& b) const {
    if (t1 != b.t1)
      return t1 > b.t1;
    else
      return t2 >= b.t2;
  }

  bool about_to_expire(const WideTimestamp& unstable_ts) const {
    (void)unstable_ts;
    return false;
  }

  uint64_t clock_diff(const WideTimestamp& b) const {
    uint64_t tsc = (t1 << 32) | (t2 >> 32);
    uint64_t b_tsc = (b.t1 << 32) | (b.t2 >> 32);
    return tsc - b_tsc;
  }
};

struct WideConcurrentTimestamp {
  volatile uint64_t t1;
  volatile uint64_t t2;
  volatile uint64_t version;

  WideTimestamp get() const {
    // Get a stable (not mutating) version of concurrent ts.
    WideTimestamp ts;
    while (true) {
      uint64_t cts_version = version;
      if ((cts_version & 1) != 0) {
        ::mica::util::pause();
        continue;
      }

      ::mica::util::memory_barrier();

      ts.t1 = t1;
      ts.t2 = t2;

      ::mica::util::memory_barrier();
      if (cts_version == version) break;
    }
    return ts;
  }

  void init(const WideTimestamp& b) {
    // Initialize a concurrent ts (a) with b.  a is not being read by others.
    t1 = b.t1;
    t2 = b.t2;
    version = 0;
  }

  void write(const WideTimestamp& b) {
    // Initialize a concurrent ts (a) with b.  a may be being read by others.
    assert((version & 1) == 0);
    version++;
    ::mica::util::memory_barrier();

    t1 = b.t1;
    t2 = b.t2;

    ::mica::util::memory_barrier();
    version++;
  }

  void update(const WideTimestamp& b) {
    // Make a concurrent ts (a) at least as large as b.
    if (get() < b) {
      uint64_t cts_version = version;
      while (true) {
        cts_version &= ~uint64_t(1);
        uint64_t cts_new_version = cts_version + 1;

        uint64_t actual_cts_version =
            __sync_val_compare_and_swap(&version, cts_version, cts_new_version);
        if (actual_cts_version == cts_version) break;

        ::mica::util::pause();
        cts_version = actual_cts_version;
      }

      uint64_t cts_t1 = t1;
      if (cts_t1 < b.t1 || (cts_t1 == b.t1 && t2 < b.t2)) {
        t1 = b.t1;
        t2 = b.t2;
      }

      ::mica::util::memory_barrier();
      version++;
    }
  }
};

struct CentralizedTimestamp {
  uint64_t t2;

  static CentralizedTimestamp make(uint32_t era, uint64_t tsc,
                                   uint32_t thread_id) {
    (void)era;
    (void)tsc;
    (void)thread_id;
    CentralizedTimestamp ts;
    ts.t2 = __sync_add_and_fetch(&CentralizedTimestamp::next_t2, 1);
    return ts;
  }

  bool operator==(const CentralizedTimestamp& b) const { return t2 == b.t2; }

  bool operator!=(const CentralizedTimestamp& b) const { return t2 != b.t2; }

  bool operator<(const CentralizedTimestamp& b) const {
    return static_cast<int64_t>(t2 - b.t2) < 0;
  }

  bool operator<=(const CentralizedTimestamp& b) const {
    return static_cast<int64_t>(t2 - b.t2) <= 0;
  }

  bool operator>(const CentralizedTimestamp& b) const {
    return static_cast<int64_t>(t2 - b.t2) > 0;
  }

  bool operator>=(const CentralizedTimestamp& b) const {
    return static_cast<int64_t>(t2 - b.t2) >= 0;
  }

  bool about_to_expire(const CentralizedTimestamp& unstable_ts) const {
    return (*this < unstable_ts) &&
           (*this >
            CentralizedTimestamp{unstable_ts.t2 + (uint64_t(1) << (64 - 4))});
  }

  uint64_t clock_diff(const CentralizedTimestamp& b) const {
    // Not meaningful because CentralizedTimestamp is not derived from a clock.
    return t2 - b.t2;
  }

 private:
  static volatile uint64_t next_t2;
};

struct CentralizedConcurrentTimestamp {
  volatile uint64_t t2;

  CentralizedTimestamp get() const {
    CentralizedTimestamp ts;
    ts.t2 = t2;
    return ts;
  }

  void init(const CentralizedTimestamp& b) {
    // Initialize a concurrent ts (a) with b.  a is not being read by others.
    t2 = b.t2;
  }

  void write(const CentralizedTimestamp& b) {
    // Initialize a concurrent ts (a) with b.  a may be being read by others.
    t2 = b.t2;
  };

  void update(const CentralizedTimestamp& b) {
    // Make a concurrent ts (a) at least as large as b.
    uint64_t cts_t2 = t2;
    while (static_cast<int64_t>(cts_t2 - b.t2) < 0) {
      auto actual_cts_t2 = __sync_val_compare_and_swap(&t2, cts_t2, b.t2);
      if (actual_cts_t2 == cts_t2) break;

      cts_t2 = actual_cts_t2;
      ::mica::util::pause();
    }
  }
};
}
}

#endif
