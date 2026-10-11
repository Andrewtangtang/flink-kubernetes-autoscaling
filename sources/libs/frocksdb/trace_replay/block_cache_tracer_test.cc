//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/block_cache_tracer.h"
#include <cstdio>
#include <limits>
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "trace_replay/shards_mrc.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

namespace {
const uint64_t kBlockSize = 1024;
const std::string kBlockKeyPrefix = "test-block-";
const uint32_t kCFId = 0;
const uint32_t kLevel = 1;
const uint64_t kSSTFDNumber = 100;
const std::string kRefKeyPrefix = "test-get-";
const uint64_t kNumKeysInBlock = 1024;
const uint64_t kReferencedDataSize = 10;
}  // namespace

class BlockCacheTracerTest : public testing::Test {
 public:
  BlockCacheTracerTest() {
    test_path_ = test::PerThreadDBPath("block_cache_tracer_test");
    env_ = ROCKSDB_NAMESPACE::Env::Default();
    clock_ = env_->GetSystemClock().get();
    EXPECT_OK(env_->CreateDir(test_path_));
    trace_file_path_ = test_path_ + "/block_cache_trace";
  }

  ~BlockCacheTracerTest() override {
    EXPECT_OK(env_->DeleteFile(trace_file_path_));
    EXPECT_OK(env_->DeleteDir(test_path_));
  }

  TableReaderCaller GetCaller(uint32_t key_id) {
    uint32_t n = key_id % 5;
    switch (n) {
      case 0:
        return TableReaderCaller::kPrefetch;
      case 1:
        return TableReaderCaller::kCompaction;
      case 2:
        return TableReaderCaller::kUserGet;
      case 3:
        return TableReaderCaller::kUserMultiGet;
      case 4:
        return TableReaderCaller::kUserIterator;
    }
    assert(false);
    return TableReaderCaller::kMaxBlockCacheLookupCaller;
  }

  void WriteBlockAccess(BlockCacheTraceWriter* writer, uint32_t from_key_id,
                        TraceType block_type, uint32_t nblocks) {
    assert(writer);
    for (uint32_t i = 0; i < nblocks; i++) {
      uint32_t key_id = from_key_id + i;
      BlockCacheTraceRecord record;
      record.block_type = block_type;
      record.block_size = kBlockSize + key_id;
      record.block_key = (kBlockKeyPrefix + std::to_string(key_id));
      record.access_timestamp = clock_->NowMicros();
      record.cf_id = kCFId;
      record.cf_name = kDefaultColumnFamilyName;
      record.caller = GetCaller(key_id);
      record.level = kLevel;
      record.sst_fd_number = kSSTFDNumber + key_id;
      record.is_cache_hit = Boolean::kFalse;
      record.no_insert = Boolean::kFalse;
      // Provide get_id for all callers. The writer should only write get_id
      // when the caller is either GET or MGET.
      record.get_id = key_id + 1;
      record.get_from_user_specified_snapshot = Boolean::kTrue;
      // Provide these fields for all block types.
      // The writer should only write these fields for data blocks and the
      // caller is either GET or MGET.
      record.referenced_key = (kRefKeyPrefix + std::to_string(key_id));
      record.referenced_key_exist_in_block = Boolean::kTrue;
      record.num_keys_in_block = kNumKeysInBlock;
      record.referenced_data_size = kReferencedDataSize + key_id;
      ASSERT_OK(writer->WriteBlockAccess(
          record, record.block_key, record.cf_name, record.referenced_key));
    }
  }

  BlockCacheTraceRecord GenerateAccessRecord() {
    uint32_t key_id = 0;
    BlockCacheTraceRecord record;
    record.block_type = TraceType::kBlockTraceDataBlock;
    record.block_size = kBlockSize;
    record.block_key = kBlockKeyPrefix + std::to_string(key_id);
    record.access_timestamp = clock_->NowMicros();
    record.cf_id = kCFId;
    record.cf_name = kDefaultColumnFamilyName;
    record.caller = GetCaller(key_id);
    record.level = kLevel;
    record.sst_fd_number = kSSTFDNumber + key_id;
    record.is_cache_hit = Boolean::kFalse;
    record.no_insert = Boolean::kFalse;
    record.referenced_key = kRefKeyPrefix + std::to_string(key_id);
    record.referenced_key_exist_in_block = Boolean::kTrue;
    record.num_keys_in_block = kNumKeysInBlock;
    return record;
  }

  void VerifyAccess(BlockCacheTraceReader* reader, uint32_t from_key_id,
                    TraceType block_type, uint32_t nblocks) {
    assert(reader);
    for (uint32_t i = 0; i < nblocks; i++) {
      uint32_t key_id = from_key_id + i;
      BlockCacheTraceRecord record;
      ASSERT_OK(reader->ReadAccess(&record));
      ASSERT_EQ(block_type, record.block_type);
      ASSERT_EQ(kBlockSize + key_id, record.block_size);
      ASSERT_EQ(kBlockKeyPrefix + std::to_string(key_id), record.block_key);
      ASSERT_EQ(kCFId, record.cf_id);
      ASSERT_EQ(kDefaultColumnFamilyName, record.cf_name);
      ASSERT_EQ(GetCaller(key_id), record.caller);
      ASSERT_EQ(kLevel, record.level);
      ASSERT_EQ(kSSTFDNumber + key_id, record.sst_fd_number);
      ASSERT_EQ(Boolean::kFalse, record.is_cache_hit);
      ASSERT_EQ(Boolean::kFalse, record.no_insert);
      if (record.caller == TableReaderCaller::kUserGet ||
          record.caller == TableReaderCaller::kUserMultiGet) {
        ASSERT_EQ(key_id + 1, record.get_id);
        ASSERT_EQ(Boolean::kTrue, record.get_from_user_specified_snapshot);
        ASSERT_EQ(kRefKeyPrefix + std::to_string(key_id),
                  record.referenced_key);
      } else {
        ASSERT_EQ(BlockCacheTraceHelper::kReservedGetId, record.get_id);
        ASSERT_EQ(Boolean::kFalse, record.get_from_user_specified_snapshot);
        ASSERT_EQ("", record.referenced_key);
      }
      if (block_type == TraceType::kBlockTraceDataBlock &&
          (record.caller == TableReaderCaller::kUserGet ||
           record.caller == TableReaderCaller::kUserMultiGet)) {
        ASSERT_EQ(Boolean::kTrue, record.referenced_key_exist_in_block);
        ASSERT_EQ(kNumKeysInBlock, record.num_keys_in_block);
        ASSERT_EQ(kReferencedDataSize + key_id, record.referenced_data_size);
        continue;
      }
      ASSERT_EQ(Boolean::kFalse, record.referenced_key_exist_in_block);
      ASSERT_EQ(0, record.num_keys_in_block);
      ASSERT_EQ(0, record.referenced_data_size);
    }
  }

  Env* env_;
  SystemClock* clock_;
  EnvOptions env_options_;
  std::string trace_file_path_;
  std::string test_path_;
};

#ifdef ROCKSDB_SHARDS_MRC_AVAILABLE
TEST_F(BlockCacheTracerTest, ShardsR1MatchesTwoBlockLRU) {
  ShardsMRC mrc(1.0, 3, 1);
  for (const char* key : {"a", "b", "a", "b"}) {
    mrc.ProcessAccess(key);
  }
  ASSERT_TRUE(mrc.DumpMRC(trace_file_path_));
  FILE* file = std::fopen(trace_file_path_.c_str(), "rb");
  ASSERT_NE(file, nullptr);
  uint64_t header[2];
  ASSERT_EQ(std::fread(header, sizeof(uint64_t), 2, file), 2U);
  ASSERT_EQ(header[0], 3U);
  ASSERT_EQ(header[1], 1U);
  for (const double expected : {1.0, 1.0, 0.5}) {
    uint64_t index;
    double miss_rate;
    ASSERT_EQ(std::fread(&index, sizeof(index), 1, file), 1U);
    ASSERT_EQ(std::fread(&miss_rate, sizeof(miss_rate), 1, file), 1U);
    ASSERT_DOUBLE_EQ(miss_rate, expected);
  }
  ASSERT_EQ(std::fclose(file), 0);
  ASSERT_OK(env_->DeleteFile(trace_file_path_ + ".txt"));
}

TEST_F(BlockCacheTracerTest, ShardsHashSeedChangesSample) {
  constexpr uint64_t seed = 0x9e3779b97f4a7c15ULL;
  const uint64_t threshold = static_cast<uint64_t>(
      0.01 * static_cast<double>(std::numeric_limits<uint64_t>::max()));
  ShardsMRC default_mrc(0.01, 10, 1);
  ShardsMRC zero_seed_mrc(0.01, 10, 1, 0);
  ShardsMRC seeded_mrc(0.01, 10, 1, seed);
  uint64_t default_count = 0, seeded_count = 0;
  bool different = false;
  for (int i = 0; i < 1000; ++i) {
    std::string key = "block-" + std::to_string(i);
    bool selected_default = GetSliceNPHash64(key) <= threshold;
    bool selected_seeded = GetSliceNPHash64(key, seed) <= threshold;
    default_count += selected_default;
    seeded_count += selected_seeded;
    different |= selected_default != selected_seeded;
    default_mrc.ProcessAccess(key);
    zero_seed_mrc.ProcessAccess(key);
    seeded_mrc.ProcessAccess(key);
  }
  ASSERT_TRUE(different);
  ASSERT_EQ(default_mrc.num_entries_processed(), default_count);
  ASSERT_EQ(zero_seed_mrc.num_entries_processed(), default_count);
  ASSERT_EQ(seeded_mrc.num_entries_processed(), seeded_count);
  ASSERT_TRUE(default_mrc.DumpMRC(trace_file_path_));
  ASSERT_OK(env_->DeleteFile(trace_file_path_ + ".txt"));
}
#endif  // ROCKSDB_SHARDS_MRC_AVAILABLE

TEST_F(BlockCacheTracerTest, AtomicWriteBeforeStartTrace) {
  BlockCacheTraceRecord record = GenerateAccessRecord();
  {
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    BlockCacheTracer writer;
    // The record should be written to the trace_file since StartTrace is not
    // called.
    ASSERT_OK(writer.WriteBlockAccess(record, record.block_key, record.cf_name,
                                      record.referenced_key));
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Verify trace file contains nothing.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_NOK(reader.ReadHeader(&header));
  }
}

TEST_F(BlockCacheTracerTest, AtomicWrite) {
  BlockCacheTraceRecord record = GenerateAccessRecord();
  {
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    BlockCacheTracer writer;
    ASSERT_OK(writer.StartTrace(clock_, trace_opt, std::move(trace_writer)));
    ASSERT_OK(writer.WriteBlockAccess(record, record.block_key, record.cf_name,
                                      record.referenced_key));
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Verify trace file contains one record.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));
    VerifyAccess(&reader, 0, TraceType::kBlockTraceDataBlock, 1);
    ASSERT_NOK(reader.ReadAccess(&record));
  }
}

TEST_F(BlockCacheTracerTest, ConsecutiveStartTrace) {
  TraceOptions trace_opt;
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(
      NewFileTraceWriter(env_, env_options_, trace_file_path_, &trace_writer));
  BlockCacheTracer writer;
  ASSERT_OK(writer.StartTrace(clock_, trace_opt, std::move(trace_writer)));
  ASSERT_NOK(writer.StartTrace(clock_, trace_opt, std::move(trace_writer)));
  ASSERT_OK(env_->FileExists(trace_file_path_));
}

TEST_F(BlockCacheTracerTest, AtomicNoWriteAfterEndTrace) {
  BlockCacheTraceRecord record = GenerateAccessRecord();
  {
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    BlockCacheTracer writer;
    ASSERT_OK(writer.StartTrace(clock_, trace_opt, std::move(trace_writer)));
    ASSERT_OK(writer.WriteBlockAccess(record, record.block_key, record.cf_name,
                                      record.referenced_key));
    writer.EndTrace();
    // Write the record again. This time the record should not be written since
    // EndTrace is called.
    ASSERT_OK(writer.WriteBlockAccess(record, record.block_key, record.cf_name,
                                      record.referenced_key));
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Verify trace file contains one record.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));
    VerifyAccess(&reader, 0, TraceType::kBlockTraceDataBlock, 1);
    ASSERT_NOK(reader.ReadAccess(&record));
  }
}

TEST_F(BlockCacheTracerTest, NextGetId) {
  BlockCacheTracer writer;
  {
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    // next get id should always return 0 before we call StartTrace.
    ASSERT_EQ(0, writer.NextGetId());
    ASSERT_EQ(0, writer.NextGetId());
    ASSERT_OK(writer.StartTrace(clock_, trace_opt, std::move(trace_writer)));
    ASSERT_EQ(1, writer.NextGetId());
    ASSERT_EQ(2, writer.NextGetId());
    writer.EndTrace();
    // next get id should return 0.
    ASSERT_EQ(0, writer.NextGetId());
  }

  // Start trace again and next get id should return 1.
  {
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    ASSERT_OK(writer.StartTrace(clock_, trace_opt, std::move(trace_writer)));
    ASSERT_EQ(1, writer.NextGetId());
  }
}

TEST_F(BlockCacheTracerTest, MixedBlocks) {
  {
    // Generate a trace file containing a mix of blocks.
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    BlockCacheTraceWriter writer(clock_, trace_opt, std::move(trace_writer));
    ASSERT_OK(writer.WriteHeader());
    // Write blocks of different types.
    WriteBlockAccess(&writer, 0, TraceType::kBlockTraceUncompressionDictBlock,
                     10);
    WriteBlockAccess(&writer, 10, TraceType::kBlockTraceDataBlock, 10);
    WriteBlockAccess(&writer, 20, TraceType::kBlockTraceFilterBlock, 10);
    WriteBlockAccess(&writer, 30, TraceType::kBlockTraceIndexBlock, 10);
    WriteBlockAccess(&writer, 40, TraceType::kBlockTraceRangeDeletionBlock, 10);
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }

  {
    // Verify trace file is generated correctly.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));
    // Read blocks.
    VerifyAccess(&reader, 0, TraceType::kBlockTraceUncompressionDictBlock, 10);
    VerifyAccess(&reader, 10, TraceType::kBlockTraceDataBlock, 10);
    VerifyAccess(&reader, 20, TraceType::kBlockTraceFilterBlock, 10);
    VerifyAccess(&reader, 30, TraceType::kBlockTraceIndexBlock, 10);
    VerifyAccess(&reader, 40, TraceType::kBlockTraceRangeDeletionBlock, 10);
    // Read one more record should report an error.
    BlockCacheTraceRecord record;
    ASSERT_NOK(reader.ReadAccess(&record));
  }
}

TEST_F(BlockCacheTracerTest, HumanReadableTrace) {
  BlockCacheTraceRecord record = GenerateAccessRecord();
  record.get_id = 1;
  record.referenced_key = "";
  record.caller = TableReaderCaller::kUserGet;
  record.get_from_user_specified_snapshot = Boolean::kTrue;
  record.referenced_data_size = kReferencedDataSize;
  PutFixed32(&record.referenced_key, 111);
  PutLengthPrefixedSlice(&record.referenced_key, "get_key");
  PutFixed64(&record.referenced_key, 2 << 8);
  PutLengthPrefixedSlice(&record.block_key, "block_key");
  PutVarint64(&record.block_key, 333);
  {
    // Generate a human readable trace file.
    BlockCacheHumanReadableTraceWriter writer;
    ASSERT_OK(writer.NewWritableFile(trace_file_path_, env_));
    ASSERT_OK(writer.WriteHumanReadableTraceRecord(record, 1, 1));
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    BlockCacheHumanReadableTraceReader reader(trace_file_path_);
    BlockCacheTraceHeader header;
    BlockCacheTraceRecord read_record;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_OK(reader.ReadAccess(&read_record));
    ASSERT_EQ(TraceType::kBlockTraceDataBlock, read_record.block_type);
    ASSERT_EQ(kBlockSize, read_record.block_size);
    ASSERT_EQ(kCFId, read_record.cf_id);
    ASSERT_EQ(kDefaultColumnFamilyName, read_record.cf_name);
    ASSERT_EQ(TableReaderCaller::kUserGet, read_record.caller);
    ASSERT_EQ(kLevel, read_record.level);
    ASSERT_EQ(kSSTFDNumber, read_record.sst_fd_number);
    ASSERT_EQ(Boolean::kFalse, read_record.is_cache_hit);
    ASSERT_EQ(Boolean::kFalse, read_record.no_insert);
    ASSERT_EQ(1, read_record.get_id);
    ASSERT_EQ(Boolean::kTrue, read_record.get_from_user_specified_snapshot);
    ASSERT_EQ(Boolean::kTrue, read_record.referenced_key_exist_in_block);
    ASSERT_EQ(kNumKeysInBlock, read_record.num_keys_in_block);
    ASSERT_EQ(kReferencedDataSize, read_record.referenced_data_size);
    ASSERT_EQ(record.block_key.size(), read_record.block_key.size());
    ASSERT_EQ(record.referenced_key.size(), record.referenced_key.size());
    ASSERT_EQ(112, BlockCacheTraceHelper::GetTableId(read_record));
    ASSERT_EQ(3, BlockCacheTraceHelper::GetSequenceNumber(read_record));
    ASSERT_EQ(333, BlockCacheTraceHelper::GetBlockOffsetInFile(read_record));
    // Read again should fail.
    ASSERT_NOK(reader.ReadAccess(&read_record));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
