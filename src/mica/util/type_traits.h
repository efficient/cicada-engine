#pragma once
#ifndef MICA_UTIL_TYPE_TRAITS_H_
#define MICA_UTIL_TYPE_TRAITS_H_

#include <type_traits>

namespace std {
// Fix compiliers that claim  is_trivially_copyable<std::pair<uint64_t, uint64_t>>::value == false.
template <typename T>
struct is_trivially_copyable<std::pair<T, uint64_t>> {
  static constexpr bool value = is_trivially_copyable<T>::value;
};
}

#endif