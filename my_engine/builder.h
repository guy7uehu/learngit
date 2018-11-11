// Copyright [2018]
//

#ifndef ENGINE_RACE_DB_BUILDER_H_
#define ENGINE_RACE_DB_BUILDER_H_

#include "status.h"

namespace polar_race {

struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta);

}  // namespace polar_race

#endif  // ENGINE_RACE_DB_BUILDER_H_