// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LRUCacheTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void newLRUCache() {
    final long capacity = 80000000;
    final int numShardBits = 16;
    final boolean strictCapacityLimit = true;
    final double highPriPoolRatio = 0.05;
    try(final Cache lruCache = new LRUCache(capacity,
        numShardBits, strictCapacityLimit, highPriPoolRatio)) {
      //no op
      assertThat(lruCache.getUsage()).isGreaterThanOrEqualTo(0);
      assertThat(lruCache.getPinnedUsage()).isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  public void newLRUCacheWithQuickMRC() {
    final long capacity = 80000000;
    final int numShardBits = 4;
    final boolean strictCapacityLimit = false;
    final double highPriPoolRatio = 0.0;
    final boolean quickMrcEnabled = true;
    final int quickMrcMaxBucketSize = 60;
    final int quickMrcGhostCacheMultiplier = 1;
    final double quickMrcSamplingRate = 0.01;
    final int quickMrcHistogramBinSize = 1024;
    try (final LRUCache lruCache = new LRUCache(capacity, numShardBits,
        strictCapacityLimit, highPriPoolRatio, quickMrcEnabled,
        quickMrcMaxBucketSize, quickMrcGhostCacheMultiplier,
        quickMrcSamplingRate, quickMrcHistogramBinSize)) {
      final long[] histogram = lruCache.getStackDistanceHistogram();
      assertThat(histogram).isNotNull();
      lruCache.resetQuickMRCStats();
      assertThat(lruCache.getStackDistanceHistogram()).isNotNull();
    }
  }

  @Test
  public void shardsMrcHistogram() throws RocksDBException {
    final int h = LRUCache.SHARDS_MRC_HEADER_LENGTH;
    try (final LRUCache lruCache = new LRUCache(8 << 20)) {
      assertThat(lruCache.getShardsMrcHistogram()).isEmpty();
      lruCache.enableShardsMrc(1.0, 42, 1024, 1 << 20);
      long[] histogram = lruCache.getShardsMrcHistogram();
      assertThat(histogram).hasSize(h + 1024 + 1);
      assertThat(Double.longBitsToDouble(histogram[0])).isEqualTo(1.0);
      assertThat(histogram[1]).isEqualTo(42);
      assertThat(histogram[2]).isEqualTo(1024);
      assertThat(histogram[3]).isEqualTo(1 << 20);
      assertThat(histogram[4]).isEqualTo(0);

      try (final Options options = new Options().setCreateIfMissing(true).setTableFormatConfig(
               new BlockBasedTableConfig().setBlockCache(lruCache));
           final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        for (int i = 0; i < 100; i++) {
          db.put(("key" + i).getBytes(StandardCharsets.UTF_8), new byte[100]);
        }
        db.flush(new FlushOptions());
        for (int i = 0; i < 100; i++) {
          assertThat(db.get(("key" + i).getBytes(StandardCharsets.UTF_8))).isNotNull();
        }
      }
      histogram = lruCache.getShardsMrcHistogram();
      assertThat(histogram[4]).isEqualTo(100); // total_refs: one data block per get
      assertThat(histogram[5]).isEqualTo(100); // sampled_refs at R = 1
      long binned = 0;
      for (int i = h; i < histogram.length; i++) {
        binned += histogram[i];
      }
      assertThat(binned).isEqualTo(100);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void shardsMrcRejectsInvalidRate() {
    try (final LRUCache lruCache = new LRUCache(8 << 20)) {
      lruCache.enableShardsMrc(0.0, 0, 1024, 1 << 20);
    }
  }
}
