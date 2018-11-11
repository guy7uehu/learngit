// Copyright [2018]
#ifndef ENGINE_RACE_COMPARATOR_H_
#define ENGINE_RACE_COMPARATOR_H_

#include <string>
//#include "leveldb/export.h"

namespace polar_race {

class Slice;

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.  A Comparator implementation
// must be thread-safe since leveldb may invoke its methods concurrently
// from multiple threads.
class Comparator {
 public:
  virtual ~Comparator();

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  virtual int Compare(const Slice& a, const Slice& b) const = 0;

  // The name of the comparator.  Used to check for comparator
  // mismatches (i.e., a DB created with one comparator is
  // accessed using a different comparator.
  //
  // The client of this package should switch to a new name whenever
  // the comparator implementation changes in a way that will cause
  // the relative ordering of any two keys to change.
  //
  // Names starting with "leveldb." are reserved and should not be used
  // by any clients of this package. --- @2018-10-14 BY tianye  比较器的名字
  virtual const char* Name() const = 0;

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks. 
  // ---@2018-10-14 BY tianye  下面这两个函数作用是减少像 index blocks 这样的数据结构占用的空间。

  // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  // ---@2018-10-14 BY tianye 
  //    1. 这个函数的作用就是：如果*start < limit，就在 [start,limit) 中找到一个 共同前缀短字符串，
  //      共同前缀后面多一个字符并加1， 然后赋给 *start 再返回。 ---生成 "分割 key"。
  //    2. 简单的 comparator 实现也可能不改变 *start，这也是正确的。即上一个 data block 的最后一个 key 是
  //      下一个 data block 第一个 key 的子串。则就用上一个 data block 的最后一个 key 作为 "分割key"。
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const = 0;

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  // ---@2018-10-14 BY tianye 这个函数的作用就是：找一个 >= *key 的短字符串
  //      简单的 comparator 实现可能不改变 *key，这也是正确的。
  virtual void FindShortSuccessor(std::string* key) const = 0;
};

// Return a builtin comparator that uses lexicographic byte-wise
// ordering.  The result remains the property of this module and
// must not be deleted.
const Comparator* BytewiseComparator();

}  // namespace polar_race

#endif  // ENGINE_RACE_COMPARATOR_H_