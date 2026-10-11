#include "cache/shards_mrc_estimator.h"

#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include "rocksdb/trace_reader_writer.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/hash.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {

using AccessTrace =
    std::vector<std::pair<std::string, uint64_t>>;  // key, charge

// Brute-force byte-LRU stack: the distance of a re-access is the sum of the
// charges of the distinct blocks accessed since the previous access, divided
// by sampling_rate, plus the block's own charge. Returns the histogram of
// ShardsMrcEstimator::BinOf(distance) for blocks selected by the SHARDS hash
// test, with the infinite bin last (also for distances beyond the last bin).
std::vector<uint64_t> ReferenceHistogram(const AccessTrace& trace,
                                         double sampling_rate,
                                         uint64_t hash_seed, uint64_t bin_bytes,
                                         uint64_t num_bins) {
  const uint64_t threshold =
      sampling_rate >= 1.0
          ? std::numeric_limits<uint64_t>::max()
          : static_cast<uint64_t>(
                sampling_rate *
                static_cast<double>(std::numeric_limits<uint64_t>::max()));
  std::vector<uint64_t> histogram(num_bins + 1, 0);
  AccessTrace stack;  // most recent last
  for (const auto& access : trace) {
    if (GetSliceNPHash64(access.first, hash_seed) > threshold) {
      continue;
    }
    uint64_t newer = 0;
    bool found = false;
    for (size_t i = stack.size(); i-- > 0;) {
      if (stack[i].first == access.first) {
        stack.erase(stack.begin() + i);
        found = true;
        break;
      }
      newer += stack[i].second;
    }
    if (found) {
      uint64_t distance =
          static_cast<uint64_t>(static_cast<double>(newer) / sampling_rate) +
          access.second;
      ++histogram[std::min(ShardsMrcEstimator::BinOf(distance, bin_bytes),
                           num_bins)];
    } else {
      ++histogram[num_bins];
    }
    stack.push_back(access);
  }
  return histogram;
}

AccessTrace RandomTrace(uint32_t seed, int num_blocks, int num_accesses) {
  Random rnd(seed);
  std::vector<uint64_t> charge(num_blocks);
  for (auto& c : charge) {
    c = 1 + rnd.Uniform(4096);
  }
  AccessTrace trace;
  for (int i = 0; i < num_accesses; ++i) {
    // Skewed so that both short and long distances occur.
    int b = static_cast<int>(rnd.Skewed(10)) % num_blocks;
    trace.emplace_back("block-" + std::to_string(b), charge[b]);
  }
  return trace;
}

std::vector<uint64_t> BinCounts(const std::vector<uint64_t>& snapshot) {
  return std::vector<uint64_t>(
      snapshot.begin() + ShardsMrcEstimator::kHeaderSize, snapshot.end());
}

}  // namespace

TEST(ShardsMrcEstimatorTest, Header) {
  ShardsMrcEstimator estimator(0.25, 7, 100, 1000);
  ASSERT_EQ(estimator.num_bins(), 10U);
  std::vector<uint64_t> s = estimator.Snapshot();
  ASSERT_EQ(s.size(), ShardsMrcEstimator::kHeaderSize + 11);
  double rate;
  std::memcpy(&rate, &s[ShardsMrcEstimator::kSamplingRateBits], sizeof(rate));
  ASSERT_EQ(rate, 0.25);
  ASSERT_EQ(s[ShardsMrcEstimator::kHashSeed], 7U);
  ASSERT_EQ(s[ShardsMrcEstimator::kBinBytes], 100U);
  ASSERT_EQ(s[ShardsMrcEstimator::kMaxDistanceBytes], 1000U);
}

TEST(ShardsMrcEstimatorTest, R1MatchesByteLru) {
  for (uint32_t seed : {1, 2, 3}) {
    // Enough accesses to compact the slot array several times.
    AccessTrace trace = RandomTrace(seed, 300, 20000);
    // Large enough that nothing is evicted; one-byte bins give exact
    // distances.
    const uint64_t cap = 300 * 4096;
    ShardsMrcEstimator estimator(1.0, 0, 1, cap);
    for (const auto& access : trace) {
      estimator.Access(access.first, access.second);
    }
    std::vector<uint64_t> s = estimator.Snapshot();
    ASSERT_EQ(BinCounts(s), ReferenceHistogram(trace, 1.0, 0, 1, cap));
    ASSERT_EQ(s[ShardsMrcEstimator::kTotalRefs], trace.size());
    ASSERT_EQ(s[ShardsMrcEstimator::kSampledRefs], trace.size());
    ASSERT_EQ(s[ShardsMrcEstimator::kEvictedByCap], 0U);
  }
}

TEST(ShardsMrcEstimatorTest, SampledDistanceIsScaledByOneOverR) {
  // Exact: equals byte-LRU over the sampled blocks only, divided by R.
  AccessTrace trace = RandomTrace(4, 2000, 50000);
  const uint64_t seed = 0x9e3779b97f4a7c15ULL;
  // Above any scaled distance: 2000 blocks of at most 4096 bytes, R = 0.1.
  const uint64_t cap = 2000 * 4096 * 10;
  ShardsMrcEstimator estimator(0.1, seed, 4096, cap);
  for (const auto& access : trace) {
    estimator.Access(access.first, access.second);
  }
  std::vector<uint64_t> s = estimator.Snapshot();
  std::vector<uint64_t> reference =
      ReferenceHistogram(trace, 0.1, seed, 4096, estimator.num_bins());
  ASSERT_EQ(BinCounts(s), reference);
  uint64_t sampled = 0;
  for (uint64_t c : reference) {
    sampled += c;
  }
  ASSERT_EQ(s[ShardsMrcEstimator::kSampledRefs], sampled);
  ASSERT_EQ(s[ShardsMrcEstimator::kTotalRefs], trace.size());

  // Statistical: a cyclic scan over n equal-size blocks has true distance
  // (n - 1) * 4096; the estimate at R = 0.1 is close to it, not R times it.
  const int n = 20000;
  ShardsMrcEstimator cyclic(0.1, 0, 4096, 2 * n * 4096);
  for (int round = 0; round < 3; ++round) {
    for (int b = 0; b < n; ++b) {
      cyclic.Access("block-" + std::to_string(b), 4096);
    }
  }
  std::vector<uint64_t> h = BinCounts(cyclic.Snapshot());
  double weighted = 0, count = 0;
  for (size_t i = 0; i + 1 < h.size(); ++i) {
    weighted += static_cast<double>(h[i]) * static_cast<double>(i);
    count += static_cast<double>(h[i]);
  }
  ASSERT_GT(count, 0.0);
  const double mean_bins = weighted / count;
  ASSERT_GT(mean_bins, 0.8 * (n - 1));
  ASSERT_LT(mean_bins, 1.2 * (n - 1));
}

TEST(ShardsMrcEstimatorTest, CapEvictsLeastRecentlyUsed) {
  // Room for three 10-byte blocks.
  ShardsMrcEstimator estimator(1.0, 0, 1, 30);
  for (const char* key : {"a", "b", "c", "d", "a", "d", "c"}) {
    estimator.Access(key, 10);
  }
  // a, b, c, d first accesses; d evicts a; a (evicted, infinite) evicts b;
  // d has distance 20 (a + itself); c has distance 30 (a, d + itself), the
  // full cap, so it lands in the last finite bin.
  std::vector<uint64_t> s = estimator.Snapshot();
  std::vector<uint64_t> h = BinCounts(s);
  ASSERT_EQ(h.size(), 31U);
  ASSERT_EQ(h[30], 5U);
  ASSERT_EQ(h[19], 1U);
  ASSERT_EQ(h[29], 1U);
  ASSERT_EQ(s[ShardsMrcEstimator::kEvictedByCap], 2U);
  ASSERT_EQ(s[ShardsMrcEstimator::kTrackedEntries], 3U);
  ASSERT_EQ(s[ShardsMrcEstimator::kTrackedBytes], 30U);

  // A block larger than the cap is evicted right away.
  estimator.Access("huge", 31);
  s = estimator.Snapshot();
  ASSERT_EQ(s[ShardsMrcEstimator::kTrackedEntries], 0U);
  ASSERT_EQ(s[ShardsMrcEstimator::kEvictedByCap], 6U);

  // The scaled charge is bounded: at R = 0.5 only 15 sampled bytes fit.
  ShardsMrcEstimator half(0.5, 0, 1, 30);
  for (int i = 0; i < 1000; ++i) {
    half.Access("block-" + std::to_string(i), 10);
    ASSERT_LE(half.Snapshot()[ShardsMrcEstimator::kTrackedBytes], 15U);
  }
}

TEST(ShardsMrcEstimatorTest, CountersAreMonotonicAndNotResetByReads) {
  AccessTrace trace = RandomTrace(5, 500, 5000);
  ShardsMrcEstimator estimator(0.5, 0, 1024, 64 * 4096);
  std::vector<uint64_t> prev = estimator.Snapshot();
  for (size_t i = 0; i < trace.size(); ++i) {
    estimator.Access(trace[i].first, trace[i].second);
    if (i % 100 != 0) {
      continue;
    }
    std::vector<uint64_t> cur = estimator.Snapshot();
    ASSERT_EQ(estimator.Snapshot(), cur);
    for (size_t f :
         {ShardsMrcEstimator::kTotalRefs, ShardsMrcEstimator::kSampledRefs,
          ShardsMrcEstimator::kEvictedByCap,
          ShardsMrcEstimator::kEstimatorNanos}) {
      ASSERT_GE(cur[f], prev[f]);
    }
    for (size_t b = ShardsMrcEstimator::kHeaderSize; b < cur.size(); ++b) {
      ASSERT_GE(cur[b], prev[b]);
    }
    prev = std::move(cur);
  }
  ASSERT_GT(prev[ShardsMrcEstimator::kEvictedByCap], 0U);
}

TEST(ShardsMrcEstimatorTest, UserDataBlockFilter) {
  for (int t = 0; t < static_cast<int>(TraceType::kTraceMax); ++t) {
    for (int c = 0;
         c <= static_cast<int>(TableReaderCaller::kMaxBlockCacheLookupCaller);
         ++c) {
      TraceType type = static_cast<TraceType>(t);
      TableReaderCaller caller = static_cast<TableReaderCaller>(c);
      bool expected = type == TraceType::kBlockTraceDataBlock &&
                      (caller == TableReaderCaller::kUserGet ||
                       caller == TableReaderCaller::kUserMultiGet ||
                       caller == TableReaderCaller::kUserIterator);
      ASSERT_EQ(BlockCacheTraceHelper::IsUserDataBlockAccess(type, caller),
                expected);
    }
  }
}

// The table reader feeds the estimator exactly the accesses a block cache
// trace records with the same filter: replaying the trace through a second
// estimator reproduces the online snapshot.
TEST(ShardsMrcEstimatorTest, OnlineHookMatchesTraceReplay) {
  const std::string db_path = test::PerThreadDBPath("shards_mrc_hook");
  const std::string trace_path = db_path + "_trace";
  Env* env = Env::Default();
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  table_options.block_cache = NewLRUCache(1 << 20);
  const double rate = 1.0;
  table_options.block_cache->SetShardsMrcEstimator(
      std::make_shared<ShardsMrcEstimator>(rate, 0, 512, 1 << 20));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_OK(DestroyDB(db_path, options));
  DB* db = nullptr;
  ASSERT_OK(DB::Open(options, db_path, &db));
  ShardsMrcEstimator* online =
      table_options.block_cache->shards_mrc_estimator();

  const int num_keys = 200;
  for (int pass = 0; pass < 2; ++pass) {
    for (int i = 0; i < num_keys; ++i) {
      ASSERT_OK(db->Put(WriteOptions(), "key" + std::to_string(1000 + i),
                        std::string(300, static_cast<char>('a' + pass))));
    }
    ASSERT_OK(db->Flush(FlushOptions()));
  }

  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(NewFileTraceWriter(env, EnvOptions(), trace_path, &trace_writer));
  ASSERT_OK(db->StartBlockCacheTrace(TraceOptions(), std::move(trace_writer)));

  // Each Get reads one data block from each of the two files' newest match:
  // the newer file holds every key, so one data block per Get. Index and
  // filter blocks are cached but not counted.
  std::string value;
  for (int i = 0; i < num_keys; ++i) {
    ASSERT_OK(db->Get(ReadOptions(), "key" + std::to_string(1000 + i), &value));
  }
  ASSERT_EQ(online->Snapshot()[ShardsMrcEstimator::kTotalRefs],
            static_cast<uint64_t>(num_keys));
  // Bloom-filtered misses read no data block.
  for (int i = 0; i < num_keys; ++i) {
    db->Get(ReadOptions(), "absent" + std::to_string(i), &value)
        .PermitUncheckedError();
  }
  std::vector<std::string> mget_keys;
  for (int i = 0; i < num_keys; i += 7) {
    mget_keys.push_back("key" + std::to_string(1000 + i));
  }
  std::vector<Slice> mget_slices(mget_keys.begin(), mget_keys.end());
  std::vector<std::string> mget_values;
  db->MultiGet(ReadOptions(), mget_slices, &mget_values);
  uint64_t before_iter = online->Snapshot()[ShardsMrcEstimator::kTotalRefs];
  {
    std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
    }
    ASSERT_OK(it->status());
  }
  ASSERT_GT(online->Snapshot()[ShardsMrcEstimator::kTotalRefs], before_iter);
  // Compaction reads are not user accesses.
  uint64_t before_compaction =
      online->Snapshot()[ShardsMrcEstimator::kTotalRefs];
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(online->Snapshot()[ShardsMrcEstimator::kTotalRefs],
            before_compaction);
  for (int i = 0; i < num_keys; i += 3) {
    ASSERT_OK(db->Get(ReadOptions(), "key" + std::to_string(1000 + i), &value));
  }
  ASSERT_OK(db->EndBlockCacheTrace());
  delete db;

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(NewFileTraceReader(env, EnvOptions(), trace_path, &trace_reader));
  BlockCacheTraceReader reader(std::move(trace_reader));
  BlockCacheTraceHeader header;
  ASSERT_OK(reader.ReadHeader(&header));
  ShardsMrcEstimator replay(rate, 0, 512, 1 << 20);
  while (true) {
    BlockCacheTraceRecord record;
    if (!reader.ReadAccess(&record).ok()) {
      break;
    }
    if (BlockCacheTraceHelper::IsUserDataBlockAccess(record.block_type,
                                                     record.caller)) {
      replay.Access(record.block_key, record.block_size);
    }
  }
  std::vector<uint64_t> expected = replay.Snapshot();
  std::vector<uint64_t> actual = online->Snapshot();
  expected[ShardsMrcEstimator::kEstimatorNanos] = 0;
  actual[ShardsMrcEstimator::kEstimatorNanos] = 0;
  ASSERT_EQ(actual, expected);

  ASSERT_OK(env->DeleteFile(trace_path));
  ASSERT_OK(DestroyDB(db_path, options));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
