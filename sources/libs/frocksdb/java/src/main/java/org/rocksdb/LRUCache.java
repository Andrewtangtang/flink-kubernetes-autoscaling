// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Least Recently Used Cache
 */
public class LRUCache extends Cache {

  /**
   * Create a new cache with a fixed size capacity
   *
   * @param capacity The fixed size capacity of the cache
   */
  public LRUCache(final long capacity) {
    this(capacity, -1, false, 0.0);
  }

  /**
   * Create a new cache with a fixed size capacity. The cache is sharded
   * to 2^numShardBits shards, by hash of the key. The total capacity
   * is divided and evenly assigned to each shard.
   * numShardBits = -1 means it is automatically determined: every shard
   * will be at least 512KB and number of shard bits will not exceed 6.
   *
   * @param capacity The fixed size capacity of the cache
   * @param numShardBits The cache is sharded to 2^numShardBits shards,
   *     by hash of the key
   */
  public LRUCache(final long capacity, final int numShardBits) {
    super(newLRUCache(capacity, numShardBits, false,0.0));
  }

  /**
   * Create a new cache with a fixed size capacity. The cache is sharded
   * to 2^numShardBits shards, by hash of the key. The total capacity
   * is divided and evenly assigned to each shard. If strictCapacityLimit
   * is set, insert to the cache will fail when cache is full.
   * numShardBits = -1 means it is automatically determined: every shard
   * will be at least 512KB and number of shard bits will not exceed 6.
   *
   * @param capacity The fixed size capacity of the cache
   * @param numShardBits The cache is sharded to 2^numShardBits shards,
   *     by hash of the key
   * @param strictCapacityLimit insert to the cache will fail when cache is full
   */
  public LRUCache(final long capacity, final int numShardBits,
                  final boolean strictCapacityLimit) {
    super(newLRUCache(capacity, numShardBits, strictCapacityLimit,0.0));
  }

  /**
   * Create a new cache with a fixed size capacity. The cache is sharded
   * to 2^numShardBits shards, by hash of the key. The total capacity
   * is divided and evenly assigned to each shard. If strictCapacityLimit
   * is set, insert to the cache will fail when cache is full. User can also
   * set percentage of the cache reserves for high priority entries via
   * highPriPoolRatio.
   * numShardBits = -1 means it is automatically determined: every shard
   * will be at least 512KB and number of shard bits will not exceed 6.
   *
   * @param capacity The fixed size capacity of the cache
   * @param numShardBits The cache is sharded to 2^numShardBits shards,
   *     by hash of the key
   * @param strictCapacityLimit insert to the cache will fail when cache is full
   * @param highPriPoolRatio percentage of the cache reserves for high priority
   *     entries
   */
  public LRUCache(final long capacity, final int numShardBits,
      final boolean strictCapacityLimit, final double highPriPoolRatio) {
    super(newLRUCache(capacity, numShardBits, strictCapacityLimit,
        highPriPoolRatio));
  }

  /**
   * Create a new cache with explicit quickMRC settings.
   *
   * @param capacity The fixed size capacity of the cache
   * @param numShardBits The cache is sharded to 2^numShardBits shards
   * @param strictCapacityLimit insert to the cache will fail when cache is full
   * @param highPriPoolRatio percentage of the cache reserved for high priority entries
   * @param quickMrcEnabled whether quickMRC stack-distance sampling is enabled
   * @param quickMrcMaxBucketSize max quickMRC bucket size
   * @param quickMrcGhostCacheMultiplier ghost-cache size multiplier
   * @param quickMrcSamplingRate sampling probability in [0, 1]
   * @param quickMrcHistogramBinSize exported histogram bin size
   */
  public LRUCache(final long capacity, final int numShardBits,
      final boolean strictCapacityLimit, final double highPriPoolRatio,
      final boolean quickMrcEnabled, final int quickMrcMaxBucketSize,
      final int quickMrcGhostCacheMultiplier, final double quickMrcSamplingRate,
      final int quickMrcHistogramBinSize) {
    super(newLRUCacheWithQuickMRC(capacity, numShardBits, strictCapacityLimit,
        highPriPoolRatio, quickMrcEnabled, quickMrcMaxBucketSize,
        quickMrcGhostCacheMultiplier, quickMrcSamplingRate,
        quickMrcHistogramBinSize));
  }

  /**
   * Returns stack-distance histogram counts binned by configured bin size.
   *
   * @return histogram bucket counts
   */
  public long[] getStackDistanceHistogram() {
    assert (isOwningHandle());
    return getStackDistanceHistogram(this.nativeHandle_);
  }

  /**
   * Resets collected quickMRC histogram statistics.
   */
  public void resetQuickMRCStats() {
    assert (isOwningHandle());
    resetQuickMRCStats(this.nativeHandle_);
  }

  /**
   * Number of header fields that precede the bins in
   * {@link #getShardsMrcHistogram()}.
   */
  public static final int SHARDS_MRC_HEADER_LENGTH = 10;

  /**
   * Enables the SHARDS miss-ratio-curve estimator of this cache. It models
   * the data-block accesses of user Get, MultiGet and iterators on tables
   * that use this cache. Call before the cache is used by any database; the
   * estimator lives as long as the cache and its counters are never reset.
   *
   * @param samplingRate spatial sampling rate R in (0, 1]
   * @param hashSeed seed of the sampling hash
   * @param binBytes width of a histogram bin in bytes
   * @param maxDistanceBytes largest tracked reuse distance in bytes; sampled
   *     blocks beyond it are dropped
   */
  public void enableShardsMrc(final double samplingRate, final long hashSeed,
      final long binBytes, final long maxDistanceBytes) {
    assert (isOwningHandle());
    if (!(samplingRate > 0.0 && samplingRate <= 1.0) || binBytes <= 0
        || maxDistanceBytes <= 0) {
      throw new IllegalArgumentException(
          "Require 0 < samplingRate <= 1, binBytes > 0, maxDistanceBytes > 0");
    }
    enableShardsMrc(
        this.nativeHandle_, samplingRate, hashSeed, binBytes, maxDistanceBytes);
  }

  /**
   * Returns the cumulative SHARDS histogram, or an empty array if the
   * estimator is not enabled. Layout:
   * <pre>
   * [0] sampling rate R, as Double.doubleToRawLongBits
   * [1] hash seed
   * [2] bin width in bytes
   * [3] max distance in bytes
   * [4] total_refs: user data-block accesses seen
   * [5] sampled_refs: accesses that passed the hash test
   * [6] evicted_by_cap: sampled blocks dropped by the max distance
   * [7] tracked_entries: sampled blocks currently tracked
   * [8] tracked_bytes: sum of their charges (unscaled)
   * [9] estimator_ns: time spent updating for sampled accesses
   * [10 .. 10+n-1] sampled references with byte distance in
   *     (i * binBytes, (i + 1) * binBytes], n = ceil(maxDistance / binBytes),
   *     i.e. hits in a byte-LRU cache of (i + 1) * binBytes; a distance is
   *     the scaled (1/R) charge of the blocks accessed since the previous
   *     access plus the block's own charge
   * [10+n] sampled references at infinite distance (first access, the block
   *     was dropped by the max distance, or beyond the last bin)
   * </pre>
   * All values except [7] and [8] are monotonic.
   *
   * @return header followed by histogram counts
   */
  public long[] getShardsMrcHistogram() {
    assert (isOwningHandle());
    return getShardsMrcHistogram(this.nativeHandle_);
  }

  private native static long newLRUCache(final long capacity,
      final int numShardBits, final boolean strictCapacityLimit,
      final double highPriPoolRatio);
  private native static long newLRUCacheWithQuickMRC(final long capacity,
      final int numShardBits, final boolean strictCapacityLimit,
      final double highPriPoolRatio, final boolean quickMrcEnabled,
      final int quickMrcMaxBucketSize, final int quickMrcGhostCacheMultiplier,
      final double quickMrcSamplingRate, final int quickMrcHistogramBinSize);
  private native static long[] getStackDistanceHistogram(final long handle);
  private native static void resetQuickMRCStats(final long handle);
  private native static void enableShardsMrc(final long handle,
      final double samplingRate, final long hashSeed, final long binBytes,
      final long maxDistanceBytes);
  private native static long[] getShardsMrcHistogram(final long handle);
  @Override protected final native void disposeInternal(final long handle);
}
