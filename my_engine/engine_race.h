// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include "include/engine.h"
#include <string>
//--INCLUDE FILES
#include <deque>
#include <set>

#include "dbformat.h"
#include "log_writer.h"
#include "snapshot.h"
#include "iterator.h"
#include "env.h"
#include "port_stdcxx.h"
#include "thread_annotations.h"
#include "options.h"

namespace polar_race {
class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch;

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);
  
  /** 2018-11-04 ADD Construction Fun -- @2018-11-06 To avoid of Read Check!!! */
  EngineRace(const Options& raw_options, const std::string& dbname);
  
//  explicit EngineRace(const std::string& dir) {
//  }

  ~EngineRace();

  RetCode Write(const PolarString& key,
      const PolarString& value) override;
  Status WriteDB(const WriteOptions& options, WriteBatch* my_batch);

  RetCode Read(const PolarString& key,
      std::string* value) override;  
  Status ReadDB(const ReadOptions& options, const Slice& key, std::string* value);

  /*
   * NOTICE: Implement 'Range' in quarter-final,
   *         you can skip it in preliminary.
   */
  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;
	  
 private: 	
	struct CompactionState;	
	struct Writer;
	
	Status NewDB();
	
	// Recover the descriptor from persistent storage.	May do a significant
	// amount of work to recover recently logged updates.  Any changes to
	// be made to the descriptor are added to *edit.
	Status Recover(VersionEdit* edit, bool* save_manifest)
		EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	void MaybeIgnoreError(Status* s) const;
		
	// Delete any unneeded files and stale in-memory entries.
	void DeleteObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
	
	// Compact the in-memory write buffer to disk.	Switches to a new
	// log-file/memtable and writes a new descriptor iff successful.
	// Errors are recorded in bg_error_.
	void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
						  VersionEdit* edit, SequenceNumber* max_sequence)
		EXCLUSIVE_LOCKS_REQUIRED(mutex_);
	
	Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
		EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	Status MakeRoomForWrite(bool force /* compact even if there is room? */)
		EXCLUSIVE_LOCKS_REQUIRED(mutex_);	
	WriteBatch* BuildBatchGroup(Writer** last_writer)
		EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	void RecordBackgroundError(const Status& s);

	void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
	static void BGWork(void* db);	
	void BackgroundCall();	
	void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);	
	void CleanupCompaction(CompactionState* compact)
		EXCLUSIVE_LOCKS_REQUIRED(mutex_);	
	Status DoCompactionWork(CompactionState* compact)
		EXCLUSIVE_LOCKS_REQUIRED(mutex_);
	
	Status OpenCompactionOutputFile(CompactionState* compact);
	Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
	Status InstallCompactionResults(CompactionState* compact)
		EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	// Constant after construction
	Env* const env_;		
	const InternalKeyComparator internal_comparator_;	
	const InternalFilterPolicy internal_filter_policy_;
	const Options options_;  // options_.comparator == &internal_comparator_	
	const bool owns_info_log_;
	const bool owns_cache_;
	const std::string dbname_;

	// table_cache_ provides its own synchronization
	TableCache* const table_cache_;
	
	// Lock over the persistent DB state.  Non-null iff successfully acquired.
	FileLock* db_lock_;

    // State below is protected by mutex_
    port::Mutex mutex_;	
	port::AtomicPointer shutting_down_;
	port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
	MemTable* mem_;	
	MemTable* imm_ GUARDED_BY(mutex_);	// Memtable being compacted	
	port::AtomicPointer has_imm_;		// So bg thread can detect non-null imm_
	WritableFile* logfile_;
	uint64_t logfile_number_ GUARDED_BY(mutex_);
	log::Writer* log_;
	
	// Queue of writers.
	std::deque<Writer*> writers_ GUARDED_BY(mutex_);
	
	WriteBatch* tmp_batch_ GUARDED_BY(mutex_);
	SnapshotList snapshots_ GUARDED_BY(mutex_);

	// Set of table files to protect from deletion because they are
	// part of ongoing compactions.
	std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);
	
	// Has a background compaction been scheduled or is running?
	bool background_compaction_scheduled_ GUARDED_BY(mutex_);
	
	// Information for a manual compaction
	struct ManualCompaction {
	  int level;
	  bool done;
	  const InternalKey* begin;   // null means beginning of key range
	  const InternalKey* end;	  // null means end of key range
	  InternalKey tmp_storage;	  // Used to keep track of compaction progress
	};
	ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

	VersionSet* const versions_;
	
	// Have we encountered a background error in paranoid mode?
	Status bg_error_ GUARDED_BY(mutex_);

	// Per level compaction stats.  stats_[level] stores the stats for
	// compactions that produced data for the specified "level".
	struct CompactionStats {
		int64_t micros;
		int64_t bytes_read;
		int64_t bytes_written;

		CompactionStats() : micros(0), bytes_read(0), bytes_written(0) { }

		void Add(const CompactionStats& c) {
		  this->micros += c.micros;
		  this->bytes_read += c.bytes_read;
		  this->bytes_written += c.bytes_written;
		}
	};
	CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
	
	const Comparator* user_comparator() const {
		return internal_comparator_.user_comparator();
	}

};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
						const InternalKeyComparator* icmp,
						const InternalFilterPolicy* ipolicy,
						const Options& src);

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
