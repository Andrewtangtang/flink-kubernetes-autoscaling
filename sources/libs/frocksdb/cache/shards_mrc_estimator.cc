#include "cache/shards_mrc_estimator.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <limits>

#include "util/hash.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace {
constexpr uint64_t kMinSlots = 1024;
}  // namespace

ShardsMrcEstimator::ShardsMrcEstimator(double sampling_rate, uint64_t hash_seed,
                                       uint64_t bin_bytes,
                                       uint64_t max_distance_bytes)
    : sampling_rate_(sampling_rate),
      // Same threshold as ShardsMRC, so both select the same blocks.
      threshold_(
          sampling_rate >= 1.0
              ? std::numeric_limits<uint64_t>::max()
              : static_cast<uint64_t>(
                    sampling_rate *
                    static_cast<double>(std::numeric_limits<uint64_t>::max()))),
      hash_seed_(hash_seed),
      bin_bytes_(bin_bytes),
      max_distance_bytes_(max_distance_bytes),
      num_bins_((max_distance_bytes + bin_bytes - 1) / bin_bytes),
      histogram_(num_bins_ + 1, 0) {
  assert(sampling_rate > 0.0 && sampling_rate <= 1.0);
  assert(bin_bytes > 0 && max_distance_bytes > 0);
}

void ShardsMrcEstimator::Access(const Slice& block_key, uint64_t charge) {
  total_refs_.fetch_add(1, std::memory_order_relaxed);
  if (GetSliceNPHash64(block_key, hash_seed_) > threshold_) {
    return;
  }
  MutexLock l(&mutex_);
  const auto start = std::chrono::steady_clock::now();
  ++sampled_refs_;

  std::string key = block_key.ToString();
  auto it = blocks_.find(key);
  if (it == blocks_.end()) {
    it = blocks_.emplace(std::move(key), 0).first;
    ++histogram_[num_bins_];
  } else {
    // Bytes of the distinct sampled blocks accessed since the last access to
    // this block, i.e. the charges of all slots after its slot.
    const uint64_t newer = tracked_bytes_ - FenwickPrefix(it->second);
    const uint64_t distance =
        static_cast<uint64_t>(static_cast<double>(newer) / sampling_rate_);
    ++histogram_[std::min(distance / bin_bytes_, num_bins_ - 1)];
    RemoveSlot(it->second);
  }

  if (next_slot_ == slot_block_.size()) {
    Compact();
  }
  const uint64_t slot = next_slot_++;
  it->second = slot;
  slot_block_[slot] = &*it;
  slot_charge_[slot] = charge;
  FenwickAdd(slot, charge);
  tracked_bytes_ += charge;

  while (static_cast<double>(tracked_bytes_) / sampling_rate_ >
         static_cast<double>(max_distance_bytes_)) {
    while (slot_block_[oldest_slot_] == nullptr) {
      ++oldest_slot_;
    }
    KeyMap::value_type* victim = slot_block_[oldest_slot_];
    RemoveSlot(oldest_slot_);
    blocks_.erase(blocks_.find(victim->first));
    ++evicted_by_cap_;
  }

  estimator_nanos_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start)
          .count());
}

std::vector<uint64_t> ShardsMrcEstimator::Snapshot() const {
  std::vector<uint64_t> out(kHeaderSize);
  std::memcpy(&out[kSamplingRateBits], &sampling_rate_, sizeof(uint64_t));
  out[kHashSeed] = hash_seed_;
  out[kBinBytes] = bin_bytes_;
  out[kMaxDistanceBytes] = max_distance_bytes_;
  out[kTotalRefs] = total_refs_.load(std::memory_order_relaxed);
  MutexLock l(&mutex_);
  out[kSampledRefs] = sampled_refs_;
  out[kEvictedByCap] = evicted_by_cap_;
  out[kTrackedEntries] = blocks_.size();
  out[kTrackedBytes] = tracked_bytes_;
  out[kEstimatorNanos] = estimator_nanos_;
  out.insert(out.end(), histogram_.begin(), histogram_.end());
  return out;
}

void ShardsMrcEstimator::FenwickAdd(uint64_t slot, uint64_t delta) {
  // Unsigned wrap-around makes adding (0 - charge) a subtraction.
  for (uint64_t i = slot + 1; i < fenwick_.size(); i += i & (0 - i)) {
    fenwick_[i] += delta;
  }
}

uint64_t ShardsMrcEstimator::FenwickPrefix(uint64_t slot) const {
  uint64_t sum = 0;
  for (uint64_t i = slot + 1; i > 0; i -= i & (0 - i)) {
    sum += fenwick_[i];
  }
  return sum;
}

void ShardsMrcEstimator::RemoveSlot(uint64_t slot) {
  FenwickAdd(slot, 0 - slot_charge_[slot]);
  tracked_bytes_ -= slot_charge_[slot];
  slot_charge_[slot] = 0;
  slot_block_[slot] = nullptr;
}

void ShardsMrcEstimator::Compact() {
  std::vector<KeyMap::value_type*> blocks;
  std::vector<uint64_t> charges;
  for (uint64_t s = oldest_slot_; s < next_slot_; ++s) {
    if (slot_block_[s] != nullptr) {
      slot_block_[s]->second = blocks.size();
      blocks.push_back(slot_block_[s]);
      charges.push_back(slot_charge_[s]);
    }
  }
  const uint64_t capacity =
      std::max<uint64_t>(kMinSlots, 2 * blocks.size() + 2);
  blocks.resize(capacity, nullptr);
  charges.resize(capacity, 0);
  slot_block_.swap(blocks);
  slot_charge_.swap(charges);
  // Linear-time Fenwick build.
  fenwick_.assign(capacity + 1, 0);
  for (uint64_t i = 1; i <= capacity; ++i) {
    fenwick_[i] += slot_charge_[i - 1];
    const uint64_t parent = i + (i & (0 - i));
    if (parent <= capacity) {
      fenwick_[parent] += fenwick_[i];
    }
  }
  next_slot_ = 0;
  while (next_slot_ < capacity && slot_block_[next_slot_] != nullptr) {
    ++next_slot_;
  }
  oldest_slot_ = 0;
}

}  // namespace ROCKSDB_NAMESPACE
