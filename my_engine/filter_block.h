// Copyright [2018]
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef ENGINE_RACE_TABLE_FILTER_BLOCK_H_
#define ENGINE_RACE_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "slice.h"
#include "hash.h"

namespace polar_race {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;

  /** 注意本轮 keys 产生的位图计算完毕后，会将 keys_, start_ , 还有 tmp_keys_ 清空 */
  std::string keys_;              // Flattened key contents --- 暂时存放本轮所有keys，追加往后写入
  std::vector<size_t> start_;     // Starting index in keys_ of each key ---记录本轮key与key之间的边界的位置，便于分割成多个key
  std::string result_;            // Filter data computed so far---计算出来的位图，多轮计算则往后追加写入.
  std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument---将本轮的所有key，存入该vector，其实并无存在的必要，用临时变量即可
  std::vector<uint32_t> filter_offsets_; //---计算出来的多个位图的边界位置，用于分隔多轮keys产生的位图

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

class FilterBlockReader {
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  const char* data_;    // Pointer to filter data (at block-start)
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}

#endif  // ENGINE_RACE_TABLE_FILTER_BLOCK_H_