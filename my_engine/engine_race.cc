// Copyright [2018] Alibaba Cloud All rights reserved

#include "engine_race.h"

#include <stdint.h>

#include "builder.h"
#include "dbformat.h"
#include "filename.h"
#include "log_reader.h"
#include "log_writer.h"
#include "memtable.h"
#include "table_cache.h"
#include "version_set.h"
#include "write_batch_internal.h"
#include "status.h"
#include "logging.h"
#include "mutexlock.h"

namespace polar_race {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct EngineRace::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
	*eptr = NULL;
	//--2018-11-04   注意下面几个变量的声明周期。
	Options opts;  
	opts.create_if_missing = true;  
	// Recover handles create_if_missing, error_if_exists
	bool save_manifest = false;	
	static uint64_t tmpO = 0;
	
	printf("In [ EngineRace::Open ] %ld \r\n", tmpO++);

	//--o.s1. 创建 engine_race 对象, 返回 ''数据库引擎" 对外暴露的操作对象.
	EngineRace *engine_race = new EngineRace(opts, name); //--@2018-11-03 暂时不考虑 options
	//--加锁
	engine_race->mutex_.Lock();  
	VersionEdit edit;

	//--o.s2. Recover(...) 恢复数据库。如果存在数据库，则 Load 数据库数据，并对日志进行恢复，否则，创建新的数据。  
	Status s = engine_race->Recover(&edit, &save_manifest);
	//---@2018-11-08 增加 read 时打开异常的处理.
	if (s.IsIOError()) {		
		printf("In [ EngineRace::Open ] == s.IsIOError \r\n");
		return kIOError;
	}

	if (s.ok() && engine_race->mem_ == nullptr) {
		printf("[ EngineRace::Open : impl->mem_ == NULL \r\n]");
		//--- 1. impl->mem_ == NULL 说明没有继续使用旧日志需创建新的日志.
		//--- 2. 从version_set 获取一个新的文件序号用于日志文件，所以如果是新建的数据库，
		//---    则第一个log 序号为 2 （因为序号 1 已经被 manifest 占用, 在 NewDB 代码可以看出 ） 
		// Create new log and a corresponding memtable.
		uint64_t new_log_number = engine_race->versions_->NewFileNumber();
		//--- 记录日志文件号，创建新的 log 文件及 Writer 对象.
		WritableFile* lfile;
		s = opts.env->NewWritableFile(LogFileName(name, new_log_number),
		                                 &lfile);
		if (s.ok()) {
		  edit.SetLogNumber(new_log_number);
		  engine_race->logfile_ = lfile;
		  engine_race->logfile_number_ = new_log_number;
		  engine_race->log_ = new log::Writer(lfile);
		  engine_race->mem_ = new MemTable(engine_race->internal_comparator_);
		  engine_race->mem_->Ref();
		}
	}
  
	if (s.ok() && save_manifest) { //--- save_manifest == ture 说明 Recover() 过程中有新的 sst（新版是 ldb） 文件生成，需要更新并记录在 manifest 文件.  	
		edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
		edit.SetLogNumber(engine_race->logfile_number_);
		//---o.s3. 产生了新 log ，该 log 已经回放完毕了，应用 edit 生成新版本。
		//---     如果 DBImpl 在恢复过程中产生了新的文件，那么就会产生 VersionEdit，
		//---     需要将 VersionEdit 记录在 manifest 文件中，同时将 VersionEdit Apply 在 VersionSet 中，
		//---     产生新的 version 。
		s = engine_race->versions_->LogAndApply(&edit, &engine_race->mutex_);
	}
	if (s.ok()) {
		engine_race->DeleteObsoleteFiles();		//--- o.s4. 删除废弃的文件（如果存在）。
		engine_race->MaybeScheduleCompaction();	//--- o.s5. 检查是否需要 Compaction，如果需要，则让后台启动 Compaction 线程。
	}
	engine_race->mutex_.Unlock();
	//--释放锁
	if(s.ok()) {
		assert(engine_race->mem_ != nullptr);
		//---*eptr 接管 engine_race 申请的内存空间数据, 并传递给外部调用的函数。
		*eptr = engine_race;
	} else {
		delete eptr;        
	}
	
	printf("Out [ EngineRace::Open ] \r\n");	
	return kSucc;
}

/***/
// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  //--- 如果用户未指定 info log 文件（用于打印状态等文本信息的日志文件），则由引擎自己创建一个。
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist---如果目录不存在则创建。
    //--- 如果已存在以前的 info log 文件，则将其改名为 LOG.old，然后创建新的 log 文件与日志...
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }

  //--- 如果用户没指定block_cache LRU缓冲，则创建8MB的LRU缓冲
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

//---设置 table LRU cache 的 Entry 数目不能超过 max_open_files-10
static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

struct EngineRace::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {
  }
};

/** 2018-11-04 ADD Construction Fun */
EngineRace::EngineRace(const Options& raw_options, const std::string& dbname)
	:env_(raw_options.env),
	internal_comparator_(raw_options.comparator), //---初始化Comparator
	internal_filter_policy_(raw_options.filter_policy),
	options_(SanitizeOptions(dbname, &internal_comparator_, //--- 检查参数是否合法
							 &internal_filter_policy_, raw_options)),							 
	owns_info_log_(options_.info_log != raw_options.info_log), //---是拥有自己 info log，还是使用用户提供的。
	owns_cache_(options_.block_cache != raw_options.block_cache), //---是否拥有自己的block LRU cache，或者使用用户提供的。
	dbname_(dbname), //--数据库名称
	//---设置 table LRU cache 的 Entry 数目不能超过 max_open_files-10; 
	//---table_cache_ 是各个 sst 文件(.ldb)元数据（index块以及布隆块）的缓存
	//---leveldb 一共使用了两种 LRU Cache，它们的功能不同，一个是table_cache，一个是block_cache。
	table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
	db_lock_(nullptr),	
	shutting_down_(nullptr),
	background_work_finished_signal_(&mutex_), //---用于与后台线程交互的条件信号。
	mem_(nullptr), //---跳表初识为 NULL	
	imm_(nullptr),
	logfile_(nullptr),
	logfile_number_(0), //---log 文件的序号
	log_(nullptr),	
	tmp_batch_(new WriteBatch), //---用于 write
	background_compaction_scheduled_(false), //---当前是否有后台的compaction线程正在进行合并
	manual_compaction_(nullptr),
	//--- 创建一个 Version 管理器
	versions_(new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_))	
{
	//@2018-11-08 Test tianye	
	static uint64_t tmpEngineC = 0;
	printf("In [ EngineRace::EngineRace ] %ld \r\n", tmpEngineC++);
	has_imm_.Release_Store(nullptr);
}

// 2. Close engine
EngineRace::~EngineRace() {
	//@2018-11-08 Test tianye	
	static uint64_t tmpEngineD = 0;
	printf("In [ EngineRace::~EngineRace ] %ld \r\n", tmpEngineD++);

	// Wait for background work to finish
	mutex_.Lock();	
	shutting_down_.Release_Store(this);  // Any non-null value is ok
	while (background_compaction_scheduled_) {
	  background_work_finished_signal_.Wait();
	}
	mutex_.Unlock();	
	
	if (db_lock_ != nullptr) {
	  env_->UnlockFile(db_lock_); //---解锁操作
	}
	
	delete versions_;

	if (mem_ != nullptr) mem_->Unref();
	if (imm_ != nullptr) imm_->Unref();
	delete tmp_batch_;
	delete log_;
	delete logfile_;
	delete table_cache_;
#if 0
	if (owns_info_log_) {
		delete options_.info_log;
	}
	if (owns_cache_) {
		delete options_.block_cache;
	}
#endif //#if 0	
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
	/** @2018-11-05   tianye 比赛规定的逐次单条写入 API, 	  这里的代码会把逐次写优化为 batch    一批    	。 */
	WriteBatch batch;
	WriteOptions opt;
	//static uint64_t tmpW = 0;

	//printf("In [ EngineRace::Write ] %ld \r\n", tmpW++);
		
	//---w1. Write 操作会将 (key,value) 转化成 WriteBatch 后，通过 WriteDB 接口来完成 batch  一批写入。  
	//---w2. 使用类型转换。 
	batch.Put(reinterpret_cast<const Slice&>(key), reinterpret_cast<const Slice&>(value));
	opt.sync = true;
    WriteDB(opt, &batch);
	
	//printf("Out [ EngineRace::Write ] \r\n");
	return kSucc;
}

/**
 * @2018-10-29 TianYe
 *  DBImpl::Write(...) 主要有 5 个步骤的操作：
 *  w.s1	队列化请求。
 *  w.s2	写入的前期检查和保证。
 *  w.s3	按格式组装数据为二进制。
 *  w.s4	写入 log 文件和 memtable。
 *  w.s5	唤醒队列的其他人去干活，自己返回。
 */
Status EngineRace::WriteDB(const WriteOptions& options, WriteBatch* my_batch) {
	Writer w(&mutex_);
	w.batch = my_batch;
	w.sync = options.sync; //---log 是否马上刷到磁盘,如果 false 会有数据丢失的风险。
	w.done = false; //---标记写入是否完成。

	//static uint64_t tmpWDB = 0;
	//printf("In [ EngineRace::WriteDB ] %ld \r\n", tmpWDB++);

	//---w.s1 队列化 writer, 如果有其他 writer 在执行，则 my_batch 进入队列并等待被唤醒执行。！！小心，这是阻塞操作。
	MutexLock l(&mutex_);
	writers_.push_back(&w);
	while (!w.done && &w != writers_.front()) {
	  w.cv.Wait();
	}
	
	//---Writer 的任务可能被其他 Writer 帮忙执行了。
	//---如果是则直接返回。
	if (w.done) {
	  return w.status;
	}

	// May temporarily unlock and wait.
	//---@2018-10-28 TianYe 在 Write 之前需要通过 MakeRoomForWrite(...) 来保证 MemTable 
	//---  有空间来接受 write 请求，这个过程中可能阻塞写请求，以及进行 compaction 。
	//-----w.s2 	写入前的各种检查, 是否该停写, 是否该切 memtable, 是否该 compact。
	Status status = MakeRoomForWrite(my_batch == nullptr);
	//---获取本次写入的版本号, 就是一个 uint64 。
	uint64_t last_sequence = versions_->LastSequence();
	Writer* last_writer = &w;

	//---这里 writer 还是队列中第一个, 由于下面的处理, 队列前面的 writers 也可能合并起来,
	//---所以 " last_writer "  指针会指向被合并的最后一个 writer，该结论可以根据 BuildBatchGroup() 返回值得出。
	if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
	  WriteBatch* updates = BuildBatchGroup(&last_writer);
	  WriteBatchInternal::SetSequence(updates, last_sequence + 1); //---把版本号写入 batch 。
	  last_sequence += WriteBatchInternal::Count(updates); //---updates 如果合并了 n 条操作, 版本号也会向前跳跃 n 。
	
	  // Add to log and apply to memtable.	We can release the lock
	  // during this phase since &w is currently responsible for logging
	  // and protects against concurrent loggers and concurrent writes
	  // into mem_.
	  {
		mutex_.Unlock();	
		/**@2018-10-31	 TianYe 作用：在写入 MemTable 之前, 将当前的操作写入到 log 文件中。 */
		status = log_->AddRecord(WriteBatchInternal::Contents(updates)); //---w.s4 写 log	!!!
		bool sync_error = false;
		if (status.ok() && options.sync) { //---都为 ture 时，则马上要 flush 到磁盘。
		  status = logfile_->Sync();
		  if (!status.ok()) {
			sync_error = true;
		  }
		}
		if (status.ok()) {
		  status = WriteBatchInternal::InsertInto(updates, mem_); //---!!!将数据写/插入 memtable 中。
		}
		mutex_.Lock();
		if (sync_error) {
		  // The state of the log file is indeterminate: the log record we
		  // just added may or may not show up when the DB is re-opened.
		  // So we force the DB into a mode where all future writes fail.
		  RecordBackgroundError(status);
		}
	  }
	  if (updates == tmp_batch_) tmp_batch_->Clear();
	
	  versions_->SetLastSequence(last_sequence);
	}

	while (true) {
	  //----在这里唤醒已经帮它干完活的 writer 线程, 让它早早回家, 别再等了。
	  Writer* ready = writers_.front();
	  writers_.pop_front();
	  if (ready != &w) {
		ready->status = status;
		ready->done = true;
		ready->cv.Signal(); //---释放信号量， 唤醒相对应的线程。
	  }
	  if (ready == last_writer) break;
	}
	
	// Notify new head of write queue
	if (!writers_.empty()) {
	  //---队列 std::deque<Writer*> writers_ 非空时取队列首元素，并释放信号量。
	  //---w.s5 即，唤醒还没有干活的等待的第一位 , 叫醒它自己去干活, 这是典型 FIFO。
	  writers_.front()->cv.Signal();
	}

	return status;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  ReadOptions opt;  
  //static uint64_t tmpR = 0;
  
  //printf("In [ EngineRace::Read ] %ld \r\n", tmpR++);
  
  //---使用类型转换。 
  Status s = ReadDB(opt, reinterpret_cast<const Slice&>(key), value);
  if(s.IsNotFound()){  	
	printf("In [ EngineRace::Read kNotFound] \r\n");
    return kNotFound;
  }else{  	
	//printf("In [ EngineRace::Read kSucc] \r\n");
    return kSucc;
  }	

  //printf("Out [ EngineRace::Read ] \r\n");
}

Status EngineRace::ReadDB(const ReadOptions& options, const Slice& key, std::string* value){
	Status s;
	MutexLock l(&mutex_);
	//---版本号, 可以读取指定版本的数据, 否则读取最新版本的数据。
	SequenceNumber snapshot;
	//---R.1. 注意 : 读取的时候数据也是会有插入操作的，假如 Get 请求先到来, 而 Put 后插入一条数据, 
	//---    这时候新数据并不会被读取到。	
	if (options.snapshot != nullptr) {
	  //---读取指定版本的数据，只读取该版本号之前的数据。
	  snapshot =
		  static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
	} else {
	  snapshot = versions_->LastSequence();
	}
	
	//---分别获取到 Memtable 和 Imuable memtable 的指针。
	MemTable* mem = mem_;
	MemTable* imm = imm_;
	Version* current = versions_->current();
	//---R.2. 增加 reference 计数, 防止在读取的时候并发线程释放掉 MemTable 的数据。
	mem->Ref();
	if (imm != nullptr) imm->Ref();
	current->Ref();

	bool have_stat_update = false;
	Version::GetStats stats;

	{
	    mutex_.Unlock();
	    // First look in the memtable, then in the immutable memtable (if any).
	    //---LookupKey 是由 key 和版本号的封装，用来查找，不然每次都要传两个参数，把高耦合的参数合并成一个数据结构。
	    LookupKey lkey(key, snapshot);
	    if (mem->Get(lkey, value, &s)) { //R.3.--- MemTable 必然是第一优先级被查找，在内存中查找.
	      // Done
	    } else if (imm != nullptr && imm->Get(lkey, value, &s)) { //R.4.---Immutable MemTable 是第二优先级，也在内存.
	      // Done
	    } else {
	      s = current->Get(options, lkey, value, &stats); //R.5.---若内存中都没有命中，则寻找 sstable 文件。
	      have_stat_update = true;
	    }
	    mutex_.Lock();
	}
	
	if (have_stat_update && current->UpdateStats(stats)) {
	  MaybeScheduleCompaction(); //---R.6. 检查是否要进行 compaction 操作。
	}

	//---R.7. 释放引用计数。
	mem->Unref();
	if (imm != nullptr) imm->Unref();
	current->Unref();
	
	return s;
}

/**
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  return kSucc;
}

/**
 * @2018-11-04 	o.s2. Recover(...) 恢复数据库。
 *  如果存在数据库，则 Load 数据库数据，并对日志进行恢复，否则，创建新的数据。
 */
Status EngineRace::Recover(VersionEdit* edit, bool* save_manifest)
{
	//printf("In [ EngineRace::Recover ] \r\n");
	mutex_.AssertHeld();

	// Ignore error from CreateDir since the creation of the DB is
	// committed only when the descriptor is created, and this directory
	// may already exist from a previous failed creation attempt.
	//---创建 DB目录, 不关注错误.	@2018-11-08 增加 read 时打开 IOError 异常的处理.
	Status s;
	bool b = env_->FileExists(dbname_);
	if(false == b){
		s = env_->CreateDir(dbname_);
		//--文件夹不存在, 并且新创建文件夾失敗, IOError
		if(!s.ok()){
			return s;
		}
	}
	
	assert(db_lock_ == nullptr);
	//---锁上数据库。在 DB 目录下打开或创建（如果不存在）LOCK 文件并锁定它，防止其他进程打开此表。	
	s = env_->LockFile(LockFileName(dbname_), &db_lock_);
	if (!s.ok()) {
	  return s;
	}
	
	//---判断 CURRENT 文件是否存在， CURRENT 文件不存在说明数据库不存在。
	if (!env_->FileExists(CurrentFileName(dbname_))) {
	  if (options_.create_if_missing) {
		s = NewDB();
		if (!s.ok()) {
		  return s;
		}
	  } else {
		return Status::InvalidArgument(
			dbname_, "does not exist (create_if_missing is false)");
	  }
	} else {
	  if (options_.error_if_exists) {
		return Status::InvalidArgument(
			dbname_, "exists (error_if_exists is true)");
	  }
	}
	
	s = versions_->Recover(save_manifest);
	if (!s.ok()) {
	  return s;
	}	
	SequenceNumber max_sequence(0);

	// Recover from all newer log files than the ones named in the
	// descriptor (new log files may have been added by the previous
	// incarnation without registering them in the descriptor).
	//
	// Note that PrevLogNumber() is no longer used, but we pay
	// attention to it in case we are recovering a database
	// produced by an older version of DB.
	const uint64_t min_log = versions_->LogNumber();
	const uint64_t prev_log = versions_->PrevLogNumber();
	//printf("min_log : %ld \r\n", min_log);
	//printf("prev_log : %ld \r\n", prev_log);
	
	std::vector<std::string> filenames;
	s = env_->GetChildren(dbname_, &filenames); //--拿到当前 DB 目录下的文件名
	if (!s.ok()) {
		return s;
	}
	std::set<uint64_t> expected;
	versions_->AddLiveFiles(&expected);
	uint64_t number;
	FileType type;
	std::vector<uint64_t> logs;
	for (size_t i = 0; i < filenames.size(); i++) {
		if (ParseFileName(filenames[i], &number, &type)) {
			expected.erase(number);
			if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
				logs.push_back(number);
		}
	}
	if (!expected.empty()) {
		char buf[50];
		snprintf(buf, sizeof(buf), "%d missing files; e.g.",
		     static_cast<int>(expected.size()));
		return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
	}

	// Recover in the order in which the logs were generated
	std::sort(logs.begin(), logs.end());
	for (size_t i = 0; i < logs.size(); i++) {
	    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
	                       &max_sequence);
		if (!s.ok()) {
			return s;
		}

	    // The previous incarnation may not have written any MANIFEST
	    // records after allocating this log number.  So we manually
	    // update the file number allocation counter in VersionSet.
	    versions_->MarkFileNumberUsed(logs[i]);
	}

	if (versions_->LastSequence() < max_sequence) {
		versions_->SetLastSequence(max_sequence);
	}

	//printf("Out [ EngineRace::Recover ]\r\n");
	return Status::OK();
}

Status EngineRace::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status EngineRace::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();
	//---将 Immutale MemTable 中的内容写入到 SSTable。
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}								

/**
 * @2018-10-21 BY tianye
 * 1. 将内存中的 MemTable dump 到磁盘上，形成 SSTable 文件。
 * 
 */
void EngineRace::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.Release_Store(nullptr);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void EngineRace::DeleteObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            static_cast<int>(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
		printf("[ EngineRace::DeleteObsoleteFiles() - %s ]\r\n", filenames[i].c_str());
      }
    }
  }
}

Status EngineRace::NewDB() {
  VersionEdit new_db; //--- 创建 version 管理器
  new_db.SetComparatorName(user_comparator()->Name()); //--- 设置Comparator
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2); //--- 下一个序号从2开始，1留给清单文件 manifest...
  new_db.SetLastSequence(0);

  //--- 创建一个清单文件，MANIFEST-1
  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  
  {
    // 写入清单文件头
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.---设置 CURRENT 文件，使其指向清单文件。
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void EngineRace::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void EngineRace::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr &&					//---表示没有 Immutable MemTable 需要 dump 成 SST.
             manual_compaction_ == nullptr &&	//---表示不是手动调用 DBImpl::CompactRange
             !versions_->NeedsCompaction()) {	//---判断是是否需要进一步再发起 Compaction
    // No work to be done
    /** BY tianye @2018-10-21  防止无限递归，会判断需不需要再次 Compaction，如果不需要就返回了 */
  } else {
    background_compaction_scheduled_ = true;
	//---sc.1
    env_->Schedule(&EngineRace::BGWork, this);
  }
}

void EngineRace::BGWork(void* db) {
  //---sc.2
  reinterpret_cast<EngineRace*>(db)->BackgroundCall();
}

/**
 * @2018-10-21 BY tianye
 * 1. Previous compaction may have produced too many files in a level,
 *   so reschedule another compaction if needed.
 * 2. 第一轮的 Compaction 可能会产生出很多 files，需要再次发起一轮 Compaction，
 *   需不需要再发起一轮        compaction  就靠 versions_->NeedsCompaction() 函数来判断。
 */
void EngineRace::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    //---sc.3.1---分支 1
    BackgroundCompaction(); //---!!!关键!!!
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  //---sc.3.2---分支 2  又回到---> sc.1
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void EngineRace::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;

  /**
   * @2018-10-22 BY tianye
   *  1. 当非 Manual compaction 的时候, 就由 PickCompaction() 函数来计算是否需要发起 Compaction
   *    如果需要，则在 PickCompaction() 函数内确定是哪些层中的哪些文件需要参与 Compaction
   */
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void EngineRace::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status EngineRace::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status EngineRace::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}

Status EngineRace::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status EngineRace::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != nullptr) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

/**@2018-10-31 BY tianye 函数的作用就是尽可能的将多个 WriteBatch 合并在一起然后写下去，能够提升吞吐量。 */
// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* EngineRace::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front(); //---当前执行的writer兄弟
  WriteBatch* result = first->batch;
  assert(result != nullptr);
  //---Test BY tianye @2018-11-07
  //printf(" EngineRace::BuildBatchGroup - 1232 - result - %d \r\n", result);  

  size_t size = WriteBatchInternal::ByteSize(first->batch); //---计算要自己要写入的byte大小

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much. ---//---计算 maxsize , 避免自己帮太多忙了导致写入数据过大。
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  /** 
   *  1. 遍历队列后面的writer们，一直到遇到帮不了忙的writer就返回了. 帮不帮忙的标准是 :
   *  1.1  sync 类型是否一样（我不需要马上flush到磁盘而你要，你的活还是自己干吧）。
   *  1.2  写入的数据量是不是过大了？（避免单次写入数据量太大）。
   *  1.3  只要没有符合这两个限制条件，就可以帮忙，合并多条write操作为一条操作！
   */
  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      //---//sync类型不同,你的活我不帮你了。。。
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        //---//这个帮忙数据量过大了,我也不帮忙了。。。
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
	    //---Test BY tianye @2018-11-07	    
		assert(result != nullptr);
		//printf(" EngineRace::BuildBatchGroup - 1277 - result - %d \r\n", result);
        WriteBatchInternal::Append(result, first->batch); //---来吧, 你的活我可以帮你干了。。。
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

Status EngineRace::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld(); //---保证进入该函数前已经加锁。
  assert(!writers_.empty());
  bool allow_delay = !force; //---allow_delay 代表 compaction 可以延后。
  Status s;

  /**
   * @2018-10-28 tianye
   *  1. 下面的代码是一个 while 循环，直到确保数据库可以写入了才会返回。
   *    while(){} 的流程大概是6步：	Ma.s1 ~ Ma.s6  
   */
  while (true) {
    if (!bg_error_.ok()) { //---如果后台任务已经出错, 直接返回错误。    
	  /** Ma.s1 后台任务有无出现错误？出现错误直接返回错误。（compaction是后台线程执行的）。 */
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) { 
      //---减速写 level-0 ，因为 level 0 的文件数目需要严格控制。
	  //---Ma.s2 level-0 的文件数限制超过 8, 睡眠等待 1ms 再继续执, 简单等待后台任务执行。
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {               
	  /** Ma.s3 如果 memtable 的大小小于4MB（默认值，可以修改），直接返回可以插入。 */
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      /** Ma.s4  到达 .s4 说明memtable已经满了，这时候需要切换为Imuable memtable。所以这时候需要等待旧的Imuable memtable compact到level 0，进入等待 */
      Log(options_.info_log, "Current memtable full; waiting...\n"); //---等待之前的 imuable memtable 完成 compact 到 level0。
      background_work_finished_signal_.Wait(); //---后台程序的条件变量, 后台程序就是做 compact 的。
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) { //---level0 的文件数超过 12, 强制等待.
      /** Ma.s5 到达 .s5 说明旧的 Imuable memtable 已经 compact 到level 0了，这时候假如level 0的文件数目到达了12个，也需要等待。 */
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old---//下面是切换到新的 memtable 和触发旧的进行 compaction.
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile); //---生成新的 log 文件.
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
	  //---删除旧的 log 对象, 并分配新的.
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_; //---切换 memtable 到 Imuable memtable 。
      has_imm_.Release_Store(imm_);	  
      /** Ma.s6	到达 .s6 说明旧的 Imuable memtable 已经 compact 到磁盘了，level 0 的文件数目也符合要求，这时候就可以生成新的 memtable 用于数据的写入了。 */
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction(); //---如果需要进行 compaction , 后台执行.
    }
  }
  return s;
}

void EngineRace::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
	// No change needed
  } else {
	Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
	*s = Status::OK();
  }
}

Snapshot::~Snapshot() {
}

}  // namespace polar_race
