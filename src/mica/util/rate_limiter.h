#pragma once
#ifndef MICA_UTIL_RATE_LIMITER_H_
#define MICA_UTIL_RATE_LIMITER_H_

#include "mica/common.h"
#include <algorithm>
#include <cmath>
#include "mica/util/rand.h"

namespace mica {
namespace util {
class RateLimiter {
 public:
  void remove_tokens(double v);
  bool try_remove_tokens(double v);
  void remove_tokens_nowait(double v);
  void add_tokens(double v);
};

class RegularRateLimiter : public RateLimiter {
 public:
  RegularRateLimiter(const Stopwatch& sw, double initial_tokens,
                     double max_tokens, double new_tokens_per_cycle)
      : sw_(sw),
        tokens_(initial_tokens),
        max_tokens_(max_tokens),
        new_tokens_per_cycle_(new_tokens_per_cycle) {
    // printf("tokens=%lf\n", tokens_);
    // printf("max_tokens=%lf\n", max_tokens_);
    // printf("new_tokens_per_cycle=%lf\n", new_tokens_per_cycle);

    last_time_ = sw_.now();
  }

  void remove_tokens(double v) {
    while (true) {
      update_tokens();

      if (tokens_ < v) {
        ::mica::util::pause();
        continue;
      }

      tokens_ -= v;
    }
  }

  bool try_remove_tokens(double v) {
    update_tokens();

    if (tokens_ < v) return false;

    tokens_ -= v;
    return true;
  }

  void remove_tokens_nowait(double v) { tokens_ -= v; }

  void add_tokens(double v) { tokens_ += v; }

 private:
  void update_tokens() {
    uint64_t current_time = sw_.now();

    double new_cycles = static_cast<double>(current_time - last_time_);

    last_time_ = current_time;

    tokens_ =
        std::min(max_tokens_, tokens_ + new_cycles * new_tokens_per_cycle_);
  }

 private:
  const Stopwatch& sw_;

  double tokens_;
  double max_tokens_;
  double new_tokens_per_cycle_;

  uint64_t last_time_;
};

class ExponentialRateLimiter : public RateLimiter {
 public:
  ExponentialRateLimiter(const Stopwatch& sw, double initial_tokens,
                         double max_tokens, double new_tokens_per_cycle,
                         uint64_t seed)
      : sw_(sw),
        tokens_(initial_tokens),
        max_tokens_(max_tokens),
        new_tokens_per_cycle_(new_tokens_per_cycle),
        cycles_per_new_token_(1. / new_tokens_per_cycle),
        rand_(seed) {
    // printf("tokens=%lf\n", tokens_);
    // printf("max_tokens=%lf\n", max_tokens_);
    // printf("new_tokens_per_cycle=%lf\n", new_tokens_per_cycle);

    last_time_ = sw_.now();
    update_cycles_for_next_token();
  }

  void remove_tokens(double v) {
    while (true) {
      update_tokens();

      if (tokens_ < v) {
        ::mica::util::pause();
        continue;
      }

      tokens_ -= v;
    }
  }

  bool try_remove_tokens(double v) {
    update_tokens();

    if (tokens_ < v) return false;

    tokens_ -= v;
    return true;
  }

  void remove_tokens_nowait(double v) { tokens_ -= v; }

  void add_tokens(double v) { tokens_ += v; }

 private:
  void update_tokens() {
    uint64_t current_time = sw_.now();

    uint64_t new_cycles = current_time - last_time_;
    while (new_cycles >= cycles_for_next_token_) {
      tokens_ += 1.;
      new_cycles -= cycles_for_next_token_;
      last_time_ += cycles_for_next_token_;

      update_cycles_for_next_token();

      if (tokens_ >= max_tokens_) {
        tokens_ = max_tokens_;
        last_time_ = current_time;
        break;
      }
    }
  }

  void update_cycles_for_next_token() {
    double z;
    do {
      z = rand_.next_f64();
    } while (z >= 1.);

    cycles_for_next_token_ =
        static_cast<uint64_t>(-cycles_per_new_token_ * log(1. - z));
  }

 private:
  const Stopwatch& sw_;

  double tokens_;
  double max_tokens_;
  double new_tokens_per_cycle_;

  double cycles_per_new_token_;
  Rand rand_;

  uint64_t last_time_;
  uint64_t cycles_for_next_token_;
};
}
}

#endif