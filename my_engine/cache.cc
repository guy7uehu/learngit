// Copyright [2018]
//

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "cache.h"
#include "port_stdcxx.h"
#include "thread_annotations.h"
#include "hash.h"
#include "mutexlock.h"

namespace polar_race {

Cache::~Cache() {
}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time. --- 依据访问时间排序
struct LRUHandle {
  void* value;        /** BY tianye @2018-10-15   用以存储 kv 对，也是实际缓存的数据。 */
  void (*deleter)(const Slice&, void* value); // @2018-10-15 BY tianye  当回收一条缓存记录时，
  											  //  以 callback 的形式通知缓存用户，一条数据被移出缓存。 
  											  //  这里 Slice 是 key 类型，暂且认为是 string
  LRUHandle* next_hash; // --- next_hash 会链入到哈希表对应的桶对应的链表中.
  LRUHandle* next;      // --- next 和 prev ，不是在热链表就是在冷链表。
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  					  // @2018-10-15 BY tianye  charge 用来保存每条缓存记录的容量，
  					  //    当所有缓存记录的容量和超过缓存总容量时，最近最少被使用的缓存记录将被回收。
  size_t key_length;  /** BY tianye @2018-10-15   用以存储 kv 对，维护引用计数。 */
  bool in_cache;      // Whether entry is in the cache.
  uint32_t refs;      // References, including cache reference, if present.
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // Beginning of key ---  BY tianye @2018-10-15   用以存储 kv 对.  为何是 [1] ???  指针地址也需要 4 Byte 呢

  Slice key() const {
    // next_ is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable. 
//---BY tianye @2018-10-16  HandleTable 的主要功能就是维护 LRUHandle 组成的哈希表。
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  //BY tianye @2018-10-15 用以查找给定记录是否存在
  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  // BY tianye @2018-10-15  用以插入/更新一条记录，如果是更新操作，还会返回旧记录
  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h; // 插入/更新
    if (old == nullptr) {
      ++elems_; // 是插入操作
      if (elems_ > length_) {
	  	// --- 整个 hash 表中总共的元素个数 > 当前 hash 桶的个数
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize(); // --- 为了提升查找效率，提早增加桶的个数来尽可能地保持一个桶后面只有一个元素
      }
    }
    return old;
  }

  //BY tianye @2018-10-15  用以删除一条记录
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;   // 纪录的是当前 hash 桶的个数
  uint32_t elems_;    // 维护在整个 hash 表中一共存放了多少个元素
  LRUHandle** list_;  // 二维指针，每一个指针指向一个桶的表头位置

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  // ---BY tianye @2018-10-15 FindPointer(...) 返回一个指向 next_hash 的指针 ptr，
  // --- 有了ptr，就可以直接修改 next_hash 的指向，达到添加/删除链表节点的目的
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    // BY tianye @2018-10-15 通过哈希值和 list_ 元素数 (length_ - 1) 进行"与"运算，
    // 可以随机得到一个小于等于 (length_ - 1) 的整数，这样可以高效定位哈希桶。
    // 在没有哈希碰撞的情况下，哈希表的查找效率非常高。
    // 由于list_是指针数组，每个元素都是一个指针。list_[hash & (length_ - 1)] 本身就是一个指针。
    // 为了能在函数外面直接通过 ptr 维护 list_ ，需要将 ptr 定义为一个二级指针指向 list_ 中某个指针的地址。
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash; // 该 next_hash 向所找到的缓存记录
    }
    return ptr;
  }

  // BY tianye @2018-10-15 给哈希表扩容用的.
  // 1. 实现内存自动增长，用以保证哈希桶中的单链表的平均长度 <= 1，进而保证添删查操作 O(1) 的时间复杂度。
  // 2. 该函数会将 hash 桶的个数增加一倍, 同时将现有的元素搬迁到合适的桶的后面,
  // 3. 良好的 hash 函数会保证每个桶对应的链表中尽可能的只有1个元素
  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) { 
      // BY tianye @2018-10-15 初始构造 HandleTable() 对象时会把 elems_=0 值, 就是直接将 list_ 初始化为一个长度为 4 的指针数组. 数组元素 *LRUHandle 指针.
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

/**
 * BY tianye @2018-10-16
 * 1. LRUCache 同时维护了一个双向链表（LRU List）和一个哈希表（HandleTable），
 *   LRU List 中按访问时间排序缓存记录，prev从最近到最久，next反之。
 * 2. ！！！利用 哈希表 实现O(1)的查询;  利用 链表 维持对缓存记录按访问时间排序, 实现快速的插入和删除。
 */
// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache --- 设置缓存容量，当容量溢出时，触发回收逻辑。
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle*list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  size_t capacity_; //--- LRUCache的容量

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  size_t usage_ GUARDED_BY(mutex_); // ---当前使用的容量

  // Dummy head of LRU list. --- 双向链表（LRU List）
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.   ---lru_  是冷链表，属于冷宫，
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true. --- in_use_ 属于热链表，热数据在此链表
  LRUHandle in_use_ GUARDED_BY(mutex_);

  // --- Hash 表
  HandleTable table_ GUARDED_BY(mutex_);
};

LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

/**
 * BY tianye @2018-10-16
 * 1.  为什么维护 引用计数 来管理内存 ？ 
 *   读取数据时，用户首先从缓存中查找欲读的数据是否存在，如果存在，用户将获得命中缓存的 Handle。
 *   在用户持有该 Handle 的期间，该缓存可能被删除（多种原因，如：超过缓存容量触发回收、
 *   具有相同 key 的新缓存插入、整个缓存被析构等），导致用户访问到非法内存，程序崩溃。
 *   因此，需要使用引用计数来维护 Handle 的生命周期。
 * 2. Ref()  函数表示要使用该 cache，因此如果对应元素位于冷链表，需要将它从冷链表溢出，链入到热链表。
 */
void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

/**
 * BY tianye @2018-10-16
 * 1. Unref() 表示客户不再访问该元素，需要将引用计数--，如果彻底没人用了，引用计数为0了，就可以删除这个元素
 *  了，如果引用计数为1，则可以将元素打入冷宫，放入到冷链表。
 */
void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.  //---彻底没人访问了，而且也在冷链表中，可以删除了。
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value); // 元素的 deleter 函数，此时回调。
    free(e);
  } else if (e->in_cache && e->refs == 1) { // 移入冷链表 lru_
    // No longer in use; move to lru_ list.
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

/**
 * 1. Insert 操作是整个 LRUCache 实现的核心。
 * 2. 对于 LevelDB 而言，插入的时候，会判断是否超过了容量，如果超过了事先规划的容量，就会从冷链表中剔除
 */
Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  // --- 申请内存，存储用户数据
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e); // ---将该缓存记录插入到双向链表中的最新端---链入热链表
    usage_ += charge; // ---计算已使用容量（增加）
    FinishErase(table_.Insert(e)); // ---如果是更新操作，回收旧记录（元素）
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
  // ---已用容量超过总量，回收最近最少被使用的缓存记录。
  while (usage_ > capacity_ && lru_.next != &lru_) {
  	// --- 如果容量超过了设计的容量，并且冷链表中有内容，则从冷链表中删除所有元素
    LRUHandle* old = lru_.next;
    assert(old->refs == 1); // 处于冷链表, 且未被其他引用, 即引用次数 <= 0
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

/**
 * BY tianye @2018-10-16
 * 1. Unref() 表示客户不再访问该元素，需要将引用计数--，如果彻底没人用了，引用计数为0了，就可以删除这个元素
 *  了，如果引用计数为1，则可以将元素打入冷宫，放入到冷链表。
 * 2. 老的元素会调用 FinishErase() 函数来决定是移入 冷链,  还是彻底删除.
 */
// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e); 
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  virtual void Prune() {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  virtual size_t TotalCharge() const {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace polar_race