// Replay one RocksDB binary block-cache trace through the online ShardsMRC.
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/trace_reader_writer.h"
#include "trace_replay/block_cache_tracer.h"
#include "trace_replay/shards_mrc.h"

#ifdef ROCKSDB_SHARDS_MRC_AVAILABLE
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

bool IsEligible(const BlockCacheTraceRecord& record) {
  return record.block_type == TraceType::kBlockTraceDataBlock &&
         (record.caller == TableReaderCaller::kUserGet ||
          record.caller == TableReaderCaller::kUserMultiGet ||
          record.caller == TableReaderCaller::kUserIterator);
}

int Replay(int argc, char** argv) {
  if (argc < 6 || argc > 8) {
    std::fprintf(stderr,
                 "Usage: %s TRACE OUTPUT R NUM_BINS BIN_SIZE [SNAPSHOT_EVERY [HASH_SEED]]\n",
                 argv[0]);
    return 2;
  }
  char* end = nullptr;
  errno = 0;
  double sampling = std::strtod(argv[3], &end);
  uint64_t num_bins = 0, bin_size = 0, snapshot_every = 0, hash_seed = 0;
  if (errno != 0 || *end != '\0' || !std::isfinite(sampling) ||
      sampling <= 0.0 || sampling > 1.0 ||
      !ParseUint(argv[4], &num_bins) || num_bins == 0 ||
      !ParseUint(argv[5], &bin_size) || bin_size == 0 ||
      (argc >= 7 &&
       (!ParseUint(argv[6], &snapshot_every) || snapshot_every == 0)) ||
      (argc == 8 && !ParseUint(argv[7], &hash_seed))) {
    std::fprintf(stderr, "Invalid R, NUM_BINS, BIN_SIZE, SNAPSHOT_EVERY, or HASH_SEED\n");
    return 2;
  }

  std::unique_ptr<TraceReader> file_reader;
  Status status = NewFileTraceReader(Env::Default(), EnvOptions(), argv[1],
                                     &file_reader);
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

  ShardsMRC mrc(sampling, num_bins, bin_size, hash_seed);
  uint64_t total = 0, eligible = 0, block_bytes = 0;
  while (true) {
    BlockCacheTraceRecord record;
    status = reader.ReadAccess(&record);
    // RocksDB's file reader returns a bare Incomplete status at EOF; a
    // decoder's Incomplete status has a message and indicates a broken record.
    if (status.IsIncomplete() && status.getState() == nullptr) {
      break;
    }
    if (!status.ok()) {
      std::fprintf(stderr, "Trace read failed after %llu accesses: %s\n",
                   static_cast<unsigned long long>(total),
                   status.ToString().c_str());
      return 1;
    }
    ++total;
    if (!IsEligible(record)) {
      continue;
    }
    ++eligible;
    block_bytes += record.block_size;
    mrc.ProcessAccess(record.block_key);
    if (snapshot_every != 0 && eligible % snapshot_every == 0 &&
        !mrc.DumpMRC(std::string(argv[2]) + "." +
                     std::to_string(eligible / snapshot_every))) {
      std::fprintf(stderr, "Cannot write SHARDS snapshot at %llu accesses\n",
                   static_cast<unsigned long long>(eligible));
      return 1;
    }
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

}  // namespace
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_SHARDS_MRC_AVAILABLE

int main(int argc, char** argv) {
#ifdef ROCKSDB_SHARDS_MRC_AVAILABLE
  return ROCKSDB_NAMESPACE::Replay(argc, argv);
#else
  (void)argc;
  std::fprintf(stderr, "%s: ShardsMRC requires libstdc++\n", argv[0]);
  return 1;
#endif
}
