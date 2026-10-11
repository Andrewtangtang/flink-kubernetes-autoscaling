// Replay one RocksDB binary block-cache trace through the online ShardsMRC,
// or (--estimator) through the cache-owned ShardsMrcEstimator.
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "cache/shards_mrc_estimator.h"
#include "rocksdb/env.h"
#include "rocksdb/trace_reader_writer.h"
#include "trace_replay/block_cache_tracer.h"
#include "trace_replay/shards_mrc.h"

namespace ROCKSDB_NAMESPACE {
namespace {

bool ParseUint(const char* text, uint64_t* value) {
  if (text[0] == '-' || text[0] == '\0') return false;
  char* end = nullptr;
  errno = 0;
  unsigned long long parsed = std::strtoull(text, &end, 10);
  if (errno != 0 || *end != '\0') return false;
  *value = static_cast<uint64_t>(parsed);
  return true;
}

bool ParseRate(const char* text, double* value) {
  char* end = nullptr;
  errno = 0;
  *value = std::strtod(text, &end);
  return errno == 0 && *end == '\0' && std::isfinite(*value) &&
         *value > 0.0 && *value <= 1.0;
}

// Calls fn(record) for every data-block access from user Get, MultiGet, or
// Iterator, the same filter the table reader applies online. fn returns
// false to abort. Returns 0 on success.
template <typename Fn>
int ForEachUserDataBlockAccess(const char* path, uint64_t* total, Fn fn) {
  std::unique_ptr<TraceReader> file_reader;
  Status status =
      NewFileTraceReader(Env::Default(), EnvOptions(), path, &file_reader);
  if (!status.ok()) {
    std::fprintf(stderr, "Cannot open trace: %s\n", status.ToString().c_str());
    return 1;
  }
  BlockCacheTraceReader reader(std::move(file_reader));
  BlockCacheTraceHeader header;
  status = reader.ReadHeader(&header);
  if (!status.ok()) {
    std::fprintf(stderr, "Cannot read trace header: %s\n",
                 status.ToString().c_str());
    return 1;
  }
  *total = 0;
  while (true) {
    BlockCacheTraceRecord record;
    status = reader.ReadAccess(&record);
    // RocksDB's file reader returns a bare Incomplete status at EOF; a
    // decoder's Incomplete status has a message and indicates a broken record.
    if (status.IsIncomplete() && status.getState() == nullptr) {
      return 0;
    }
    if (!status.ok()) {
      std::fprintf(stderr, "Trace read failed after %llu accesses: %s\n",
                   static_cast<unsigned long long>(*total),
                   status.ToString().c_str());
      return 1;
    }
    ++*total;
    if (BlockCacheTraceHelper::IsUserDataBlockAccess(record.block_type,
                                                     record.caller) &&
        !fn(record)) {
      return 1;
    }
  }
}

// Writes ShardsMrcEstimator::Snapshot(), one value per line, in the same
// layout as the Java LRUCache SHARDS histogram (sampling rate as IEEE-754
// bits).
int ReplayEstimator(int argc, char** argv) {
  double sampling = 0.0;
  uint64_t bin_bytes = 0, max_distance_bytes = 0, hash_seed = 0;
  if (argc < 7 || argc > 8 || !ParseRate(argv[4], &sampling) ||
      !ParseUint(argv[5], &bin_bytes) || bin_bytes == 0 ||
      !ParseUint(argv[6], &max_distance_bytes) || max_distance_bytes == 0 ||
      (argc == 8 && !ParseUint(argv[7], &hash_seed))) {
    std::fprintf(stderr,
                 "Usage: %s --estimator TRACE OUTPUT R BIN_BYTES "
                 "MAX_DISTANCE_BYTES [HASH_SEED]\n",
                 argv[0]);
    return 2;
  }
  ShardsMrcEstimator estimator(sampling, hash_seed, bin_bytes,
                               max_distance_bytes);
  uint64_t total = 0;
  int rc = ForEachUserDataBlockAccess(
      argv[2], &total, [&](const BlockCacheTraceRecord& record) {
        estimator.Access(record.block_key, record.block_size);
        return true;
      });
  if (rc != 0) {
    return rc;
  }
  FILE* out = std::fopen(argv[3], "w");
  if (out == nullptr) {
    std::fprintf(stderr, "Cannot write %s\n", argv[3]);
    return 1;
  }
  std::vector<uint64_t> snapshot = estimator.Snapshot();
  for (uint64_t v : snapshot) {
    std::fprintf(out, "%llu\n", static_cast<unsigned long long>(v));
  }
  if (std::fclose(out) != 0) {
    std::fprintf(stderr, "Cannot write %s\n", argv[3]);
    return 1;
  }
  std::printf("total=%llu eligible=%llu sampled=%llu evicted_by_cap=%llu\n",
              static_cast<unsigned long long>(total),
              static_cast<unsigned long long>(
                  snapshot[ShardsMrcEstimator::kTotalRefs]),
              static_cast<unsigned long long>(
                  snapshot[ShardsMrcEstimator::kSampledRefs]),
              static_cast<unsigned long long>(
                  snapshot[ShardsMrcEstimator::kEvictedByCap]));
  return 0;
}

#ifdef ROCKSDB_SHARDS_MRC_AVAILABLE
int Replay(int argc, char** argv) {
  if (argc < 6 || argc > 8) {
    std::fprintf(stderr,
                 "Usage: %s TRACE OUTPUT R NUM_BINS BIN_SIZE [SNAPSHOT_EVERY [HASH_SEED]]\n",
                 argv[0]);
    return 2;
  }
  double sampling = 0.0;
  uint64_t num_bins = 0, bin_size = 0, snapshot_every = 0, hash_seed = 0;
  if (!ParseRate(argv[3], &sampling) ||
      !ParseUint(argv[4], &num_bins) || num_bins == 0 ||
      !ParseUint(argv[5], &bin_size) || bin_size == 0 ||
      (argc >= 7 &&
       (!ParseUint(argv[6], &snapshot_every) || snapshot_every == 0)) ||
      (argc == 8 && !ParseUint(argv[7], &hash_seed))) {
    std::fprintf(stderr, "Invalid R, NUM_BINS, BIN_SIZE, SNAPSHOT_EVERY, or HASH_SEED\n");
    return 2;
  }

  ShardsMRC mrc(sampling, num_bins, bin_size, hash_seed);
  uint64_t total = 0, eligible = 0, block_bytes = 0;
  int rc = ForEachUserDataBlockAccess(
      argv[1], &total, [&](const BlockCacheTraceRecord& record) {
        ++eligible;
        block_bytes += record.block_size;
        mrc.ProcessAccess(record.block_key);
        if (snapshot_every != 0 && eligible % snapshot_every == 0 &&
            !mrc.DumpMRC(std::string(argv[2]) + "." +
                         std::to_string(eligible / snapshot_every))) {
          std::fprintf(stderr, "Cannot write SHARDS snapshot at %llu accesses\n",
                       static_cast<unsigned long long>(eligible));
          return false;
        }
        return true;
      });
  if (rc != 0) {
    return rc;
  }
  if (eligible == 0 || !mrc.DumpMRC(argv[2])) {
    std::fprintf(stderr, "No eligible accesses or cannot write MRC: %s\n",
                 argv[2]);
    return 1;
  }
  std::printf("total=%llu eligible=%llu sampled=%llu mean_block_bytes=%.2f\n",
              static_cast<unsigned long long>(total),
              static_cast<unsigned long long>(eligible),
              static_cast<unsigned long long>(mrc.num_entries_processed()),
              static_cast<double>(block_bytes) / eligible);
  return 0;
}
#endif  // ROCKSDB_SHARDS_MRC_AVAILABLE

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  if (argc > 1 && std::strcmp(argv[1], "--estimator") == 0) {
    return ROCKSDB_NAMESPACE::ReplayEstimator(argc, argv);
  }
#ifdef ROCKSDB_SHARDS_MRC_AVAILABLE
  return ROCKSDB_NAMESPACE::Replay(argc, argv);
#else
  (void)argc;
  std::fprintf(stderr, "%s: ShardsMRC requires libstdc++; use --estimator\n",
               argv[0]);
  return 1;
#endif
}
