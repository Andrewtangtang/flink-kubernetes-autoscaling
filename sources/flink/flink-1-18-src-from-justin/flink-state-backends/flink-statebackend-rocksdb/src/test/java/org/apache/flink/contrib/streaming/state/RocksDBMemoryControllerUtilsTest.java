/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBMemoryControllerUtils.ShardsMrcSettings;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.WriteBufferManager;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests to guard {@link RocksDBMemoryControllerUtils}. */
public class RocksDBMemoryControllerUtilsTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void ensureRocksDbNativeLibraryLoaded() throws IOException {
        NativeLibraryLoader.getInstance()
                .loadLibrary(temporaryFolder.newFolder().getAbsolutePath());
    }

    @Test
    public void testCreateSharedResourcesWithExpectedCapacity() {
        long totalMemorySize = 2048L;
        double writeBufferRatio = 0.5;
        double highPriPoolRatio = 0.1;
        TestingRocksDBMemoryFactory factory = new TestingRocksDBMemoryFactory();
        RocksDBSharedResources rocksDBSharedResources =
                RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                        totalMemorySize,
                        writeBufferRatio,
                        highPriPoolRatio,
                        -1,
                        false,
                        true,
                        60,
                        1,
                        0.01,
                        1,
                        null,
                        factory);
        long expectedCacheCapacity =
                RocksDBMemoryControllerUtils.calculateActualCacheCapacity(
                        totalMemorySize, writeBufferRatio);
        long expectedWbmCapacity =
                RocksDBMemoryControllerUtils.calculateWriteBufferManagerCapacity(
                        totalMemorySize, writeBufferRatio);

        assertThat(factory.actualCacheCapacity, is(expectedCacheCapacity));
        assertThat(factory.actualWbmCapacity, is(expectedWbmCapacity));
        assertThat(rocksDBSharedResources.getWriteBufferManagerCapacity(), is(expectedWbmCapacity));
    }

    @Test
    public void testShardsEstimatorIsDefaultAndDisablesQuickMrc() {
        RocksDBMemoryConfiguration config =
                RocksDBMemoryConfiguration.fromConfiguration(new Configuration());

        assertEquals(RocksDBOptions.A4SMrcEstimator.SHARDS, config.getMrcEstimator());
        assertFalse(config.isQuickMrcEnabled());
        ShardsMrcSettings settings = config.getShardsMrcSettings();
        assertEquals(0.01, settings.getSamplingRate(), 0.0);
        assertEquals(0L, settings.getHashSeed());
        assertEquals(1L << 20, settings.getBinBytes());
        assertEquals(4L << 30, settings.getMaxDistanceBytes());

        Configuration small = new Configuration();
        small.setString(RocksDBOptions.A4S_SHARDS_MAX_DISTANCE.key(), "4mb");
        small.setString(RocksDBOptions.A4S_SHARDS_HASH_SEED.key(), "7");
        TestingRocksDBMemoryFactory factory = new TestingRocksDBMemoryFactory();
        RocksDBSharedResources resources =
                allocate(RocksDBMemoryConfiguration.fromConfiguration(small), factory);

        assertFalse(factory.actualQuickMrcEnabled);
        assertEquals(1, factory.shardsEnableCalls);
        // Native check: the cache now carries the SHARDS header and 4 + 1 bins.
        long[] histogram = ((LRUCache) resources.getCache()).getShardsMrcHistogram();
        assertEquals(LRUCache.SHARDS_MRC_HEADER_LENGTH + 5, histogram.length);
        assertEquals(0.01, Double.longBitsToDouble(histogram[0]), 0.0);
        assertEquals(7L, histogram[1]);
        assertEquals(1L << 20, histogram[2]);
        assertEquals(4L << 20, histogram[3]);
        resources.close();
    }

    @Test
    public void testQuickMrcEstimatorKeepsQuickMrcAndSkipsShards() {
        Configuration quickMrc = new Configuration();
        quickMrc.setString(RocksDBOptions.A4S_MRC_ESTIMATOR.key(), "quickmrc");
        RocksDBMemoryConfiguration config = RocksDBMemoryConfiguration.fromConfiguration(quickMrc);

        assertTrue(config.isQuickMrcEnabled());
        assertNull(config.getShardsMrcSettings());

        TestingRocksDBMemoryFactory factory = new TestingRocksDBMemoryFactory();
        RocksDBSharedResources resources = allocate(config, factory);

        assertTrue(factory.actualQuickMrcEnabled);
        assertEquals(0, factory.shardsEnableCalls);
        assertEquals(0, ((LRUCache) resources.getCache()).getShardsMrcHistogram().length);
        resources.close();
    }

    /** Mirrors {@link RocksDBSharedResourcesFactory}. */
    private static RocksDBSharedResources allocate(
            RocksDBMemoryConfiguration config, TestingRocksDBMemoryFactory factory) {
        return RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                64L << 20,
                config.getWriteBufferRatio(),
                config.getHighPriorityPoolRatio(),
                config.getLruCacheNumShardBits(),
                config.isUsingPartitionedIndexFilters(),
                config.isQuickMrcEnabled(),
                config.getQuickMrcMaxBucketSize(),
                config.getQuickMrcGhostCacheMultiplier(),
                config.getQuickMrcSamplingRate(),
                config.getQuickMrcHistogramBinSize(),
                config.getShardsMrcSettings(),
                factory);
    }

    @Test
    public void testCalculateRocksDBDefaultArenaBlockSize() {
        final long align = 4 * 1024;
        final long writeBufferSize = 64 * 1024 * 1024;
        final long expectArenaBlockSize = writeBufferSize / 8;

        // Normal case test
        assertThat(
                "Arena block size calculation error for normal case",
                RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(writeBufferSize),
                is(expectArenaBlockSize));

        // Alignment tests
        assertThat(
                "Arena block size calculation error for alignment case",
                RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(
                        writeBufferSize - 1),
                is(expectArenaBlockSize));
        assertThat(
                "Arena block size calculation error for alignment case2",
                RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(
                        writeBufferSize + 8),
                is(expectArenaBlockSize + align));
    }

    @Test
    public void testCalculateRocksDBMutableLimit() {
        long bufferSize = 64 * 1024 * 1024;
        long limit = bufferSize * 7 / 8;
        assertThat(
                RocksDBMemoryControllerUtils.calculateRocksDBMutableLimit(bufferSize), is(limit));
    }

    @Test
    public void testValidateArenaBlockSize() {
        long arenaBlockSize = 8 * 1024 * 1024;
        assertFalse(
                RocksDBMemoryControllerUtils.validateArenaBlockSize(
                        arenaBlockSize, (long) (arenaBlockSize * 0.5)));
        assertTrue(
                RocksDBMemoryControllerUtils.validateArenaBlockSize(
                        arenaBlockSize, (long) (arenaBlockSize * 1.5)));
    }

    private static final class TestingRocksDBMemoryFactory
            implements RocksDBMemoryControllerUtils.RocksDBMemoryFactory {
        private Long actualCacheCapacity = null;
        private Long actualWbmCapacity = null;
        private boolean actualQuickMrcEnabled;
        private int shardsEnableCalls;

        @Override
        public Cache createCache(
                long cacheCapacity,
                double highPriorityPoolRatio,
                int lruCacheNumShardBits,
                boolean quickMrcEnabled,
                int quickMrcMaxBucketSize,
                int quickMrcGhostCacheMultiplier,
                double quickMrcSamplingRate,
                int quickMrcHistogramBinSize) {
            actualCacheCapacity = cacheCapacity;
            actualQuickMrcEnabled = quickMrcEnabled;
            return RocksDBMemoryControllerUtils.RocksDBMemoryFactory.DEFAULT.createCache(
                    cacheCapacity,
                    highPriorityPoolRatio,
                    lruCacheNumShardBits,
                    quickMrcEnabled,
                    quickMrcMaxBucketSize,
                    quickMrcGhostCacheMultiplier,
                    quickMrcSamplingRate,
                    quickMrcHistogramBinSize);
        }

        @Override
        public void enableShardsMrc(Cache cache, ShardsMrcSettings settings) {
            shardsEnableCalls++;
            RocksDBMemoryControllerUtils.RocksDBMemoryFactory.super.enableShardsMrc(
                    cache, settings);
        }

        @Override
        public WriteBufferManager createWriteBufferManager(
                long writeBufferManagerCapacity, Cache cache) {
            actualWbmCapacity = writeBufferManagerCapacity;
            return RocksDBMemoryControllerUtils.RocksDBMemoryFactory.DEFAULT
                    .createWriteBufferManager(writeBufferManagerCapacity, cache);
        }
    }
}
