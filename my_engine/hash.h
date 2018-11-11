// Copyright [2018]
//
// Simple hash function used for internal data structures

#ifndef ENGINE_RACE_UTIL_HASH_H_
#define ENGINE_RACE_UTIL_HASH_H_

#include <stddef.h>
#include <stdint.h>

namespace polar_race {

uint32_t Hash(const char* data, size_t n, uint32_t seed);

}  // namespace polar_race

#endif  // ENGINE_RACE_UTIL_HASH_H_