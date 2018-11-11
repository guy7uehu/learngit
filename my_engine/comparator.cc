// Copyright [2018]

#include <algorithm>
#include <cstdint>
#include <string>

#include "comparator.h"
#include "slice.h"
#include "logging.h"
#include "no_destructor.h"

namespace polar_race {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  /** BY tianye @2018-10-14  比较 2 个字符串  */
  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  /** BY tianye @2018-10-14  Byte wise 的 FindShortestSeparator  */
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix --- 首先计算公共前缀 diff_index
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other  --- 说明 *start 是 limit 的前缀。 (* start) 或 limit 中较短者长度和内容恰好==公共前缀
      // --- 上一个 data block 的 key 是下一个 data block 的子串，则不处理。
    } else {
      /** 
       * BY tianye @2018-10-14 不是其前缀的话，就让 diff_index 位置的字符加1，
       * 并设置 start 的长度为 diff_index+1 , 返回。   
       * 当前不是所有的情况都 +1 的。 只有满足：diff_byte<0xff 且 diff_byte+1<limint[diff_index]
	   */
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace polar_race
