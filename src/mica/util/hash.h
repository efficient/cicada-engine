#pragma once
#ifndef MICA_UTIL_HASH_H_
#define MICA_UTIL_HASH_H_

#include "mica/common.h"
#include "mica/util/cityhash/citycrc_mod.h"

int siphash(uint8_t* out, const uint8_t* in, uint64_t inlen, const uint8_t* k);
static const uint8_t siphash_key[16] = {
    0,
};

namespace mica {
namespace util {
template <typename T>
static uint64_t hash_cityhash(const T* key, size_t len) {
  return CityHash64(reinterpret_cast<const char*>(key), len);
}

template <typename T>
static uint64_t hash_siphash(const T* key, size_t len) {
  uint64_t v;
  siphash(reinterpret_cast<uint8_t*>(&v), reinterpret_cast<const uint8_t*>(key),
          len, siphash_key);
  return v;
}

template <typename T>
static uint64_t hash(const T* key, size_t len) {
  return hash_cityhash(key, len);
}
}
}

#endif
