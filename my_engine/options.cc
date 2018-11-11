/// Copyright [2018]

#include "options.h"

#include "comparator.h"
#include "env.h"

namespace polar_race {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(nullptr),
      write_buffer_size(4<<20),
      max_open_files(1000),
      block_cache(nullptr),
      block_size(4096),
      block_restart_interval(16),
      max_file_size(2<<20),
      compression(kSnappyCompression),
      reuse_logs(false),
      filter_policy(nullptr) {
}

}  // namespace polar_race
