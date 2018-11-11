// Copyright [2018]

#ifndef ENGINE_RACE_UTIL_ENV_POSIX_TEST_HELPER_H_
#define ENGINE_RACE_UTIL_ENV_POSIX_TEST_HELPER_H_

namespace polar_race {

///class EnvPosixTest;

// A helper for the POSIX Env to facilitate testing.
class EnvPosixTestHelper {
 private:
  ///friend class EnvPosixTest;

  // Set the maximum number of read-only files that will be opened.
  // Must be called before creating an Env.
  static void SetReadOnlyFDLimit(int limit);

  // Set the maximum number of read-only files that will be mapped via mmap.
  // Must be called before creating an Env.
  static void SetReadOnlyMMapLimit(int limit);
};

}  // namespace polar_race

#endif  // ENGINE_RACE_UTIL_ENV_POSIX_TEST_HELPER_H_