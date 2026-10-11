#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "port/port.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// Online fixed-rate SHARDS miss-ratio-curve estimator with byte distances.
//
// Access() is fed one block access at a time. A block is sampled if
// GetSliceNPHash64(block_key, hash_seed) <= sampling_rate * 2^64, the same
// spatial sampling as the offline ShardsMRC. For a re-access of a sampled
// block, the distance is the sum of the charges of the distinct sampled blocks
// accessed since its previous access, divided by sampling_rate. The histogram
// has fixed byte-width bins over [0, max_distance_bytes] plus one infinite bin
// that holds first accesses and re-accesses of blocks evicted by the bound.
//
// The sample set is bounded: while the tracked charge divided by
// sampling_rate exceeds max_distance_bytes, the least recently used sampled
// block is dropped (counted in evicted_by_cap).
//
// All counters are monotonic and never reset. Thread-safe; the hash is
// computed before the mutex, which is taken only for sampled accesses.
class ShardsMrcEstimator {
 public:
  // Layout of Snapshot(). Histogram counts are unscaled sampled references.
  enum SnapshotField : size_t {
    kSamplingRateBits = 0,  // IEEE-754 bits of sampling_rate
    kHashSeed,
    kBinBytes,
    kMaxDistanceBytes,
    kTotalRefs,       // all accesses passed to Access()
    kSampledRefs,     // accesses that passed the hash test
    kEvictedByCap,    // sampled blocks dropped by the distance bound
    kTrackedEntries,  // sampled blocks currently tracked
    kTrackedBytes,    // sum of their charges (unscaled)
    kEstimatorNanos,  // time spent updating for sampled accesses
    kHeaderSize,      // num_bins finite bins follow, then the infinite bin
  };

  // Requires 0 < sampling_rate <= 1, bin_bytes > 0, max_distance_bytes > 0.
  ShardsMrcEstimator(double sampling_rate, uint64_t hash_seed,
                     uint64_t bin_bytes, uint64_t max_distance_bytes);

  ShardsMrcEstimator(const ShardsMrcEstimator&) = delete;
  ShardsMrcEstimator& operator=(const ShardsMrcEstimator&) = delete;

  void Access(const Slice& block_key, uint64_t charge);

  // Header (see SnapshotField) followed by num_bins() + 1 histogram counts.
  std::vector<uint64_t> Snapshot() const;

  uint64_t num_bins() const { return num_bins_; }

 private:
  using KeyMap = std::unordered_map<std::string, uint64_t>;  // key -> slot

  // Fenwick tree over access slots in recency order; a slot holds the charge
  // of the block whose most recent access it is.
  void FenwickAdd(uint64_t slot, uint64_t delta);
  uint64_t FenwickPrefix(uint64_t slot) const;  // sum of slots [0, slot]
  void RemoveSlot(uint64_t slot);
  // Renumbers live slots to [0, n) when the slot array is full.
  void Compact();

  const double sampling_rate_;
  const uint64_t threshold_;
  const uint64_t hash_seed_;
  const uint64_t bin_bytes_;
  const uint64_t max_distance_bytes_;
  const uint64_t num_bins_;

  std::atomic<uint64_t> total_refs_{0};

  mutable port::Mutex mutex_;
  uint64_t sampled_refs_ = 0;
  uint64_t evicted_by_cap_ = 0;
  uint64_t tracked_bytes_ = 0;
  uint64_t estimator_nanos_ = 0;
  std::vector<uint64_t> histogram_;

  KeyMap blocks_;
  std::vector<uint64_t> fenwick_;
  std::vector<uint64_t> slot_charge_;
  std::vector<KeyMap::value_type*> slot_block_;  // nullptr if slot is free
  uint64_t oldest_slot_ = 0;
  uint64_t next_slot_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
