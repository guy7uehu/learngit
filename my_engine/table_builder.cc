// Copyright [2018]
//

#include "table_builder.h"

#include <assert.h>
#include "comparator.h"
#include "env.h"
#include "filter_policy.h"
#include "options.h"
#include "block_builder.h"
#include "filter_block.h"
#include "format.h"
#include "coding.h"
#include "crc32c.h"

namespace polar_race {

struct TableBuilder::Rep {
  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;          // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry; //@2018-10-17 BY tianye 标志位用来判定是不是 data block 的第一个 key，
  						    // 1. 当下一个 data block 第一个 key-value 到来后，成功往 index block 插入分割 key 之后，就会清零。
  						    // 2. 在上一个 data block Flush() 的时候，会将该标志位置位 true，
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr ? nullptr
                     : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

/**
 * @2018-10-16 BY tianye 
 * 1. 向 sstable 添加一个 key－value 的函数。
 */
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  /** index block 的部分 */
  if (r->pending_index_entry) {
  	//@2018-10-17 BY tianye  标志位 pending_index_entry 用来判定是不是 data block 的第一个 key，当下
    // 一个 data block 第一个 key-value 到来后，成功往 index block 插入分割 key 之后，就会清零。
    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
	/**
 	 * @2018-10-17 BY tianye
	 *  计算出来的分割 key 即 r->last_key 作为 key，而上一个 data block 的 "位置信息" 作为 value。
	 *  pending_handle 里面存放的是上一个 data block 的位置信息，BlockHandle 类型*
	 *  注意 index_block 的组织形式和上一篇讲的 Data block 是一模一样的，
	 *  区别在于存放的 key-value pair 不同。
	 */
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding)); // --- index block :  分割 key , value
    r->pending_index_entry = false;
  }

  /** filter block 的部分 */
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value); /* 向 data block 中添加一组 key-value pair */

  /** 估算当前 data block 的长度，如果超过了阈值，就要 Flush */
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
  	//@2018-10-17 BY tianye  标志位 pending_index_entry 用来判定是不是 data block 的第一个 key，当下
    //  一个 data block 第一个 key-value 到来后，成功往 index block 插入 "分割 key"  之后，就会清零。
    //  pending_index_entry 为 true 时表示 "'前一个 data_block" 刚刚存盘 sstable
    //  -- 设置 pending_index_entry 为 true， 当下一个 DataBlock 的第一个 key-value 到来的时候，
    //  -- 就需要计算 "分割key" ，
    r->pending_index_entry = true;
	/** 文件 Flush，写入硬件 */
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

	/** 可以将内容压缩，但是一般不开启，走上面那个分支 */
    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_; // sstabe 文件的物理存储

  /** 
   * 1. 此处 handle 即为前面传入的 r->pending_handle, 记录下上一个 Data Block/Index Block  的 offset 和 size 
   * 2. r->offset 传入的参数表示的是 偏移量-BlockContent 的开头位置.
   */
  handle->set_offset(r->offset); //---记录了 Data Block/Index Block 在 sstable 中的 offset 和 size，以备后续写 Index Block Handle
  handle->set_size(block_contents.size());
  /** 追加写入 Data Block/Index Block 的内容 */
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type; // 内容是否是压缩的。 type:0-kNoCompression；	type:1-kSnappyCompression
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize; //---更新 r->offset
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush(); /** 写入尚未 Flush 的 Block 块 */
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block --- 即图中的 meta block,  写入磁盘
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }
    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block --- 实现了将 index block 写入 sstable file，并最终落盘。
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
	/** 
	 *  写入 Index Block 的内容。
	 *  最重要的是，算出 index block 在 file 中的 offset 和 size，存放到 index_block_handle 中，
	 *  这个信息要记录在 footer 中。
	 */
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer --- footer 为固定长度 48 Bytes，在文件的最尾部.
  if (ok()) {
    Footer footer;
	// BY tianye @2018-10-16 将 metaindex block 在文件中的位置信息记录在 footer
    footer.set_metaindex_handle(metaindex_block_handle);
    // BY tianye @2018-10-16  将 index block 在 sstabke 文件中的位置信息记录在 footer
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace polar_race