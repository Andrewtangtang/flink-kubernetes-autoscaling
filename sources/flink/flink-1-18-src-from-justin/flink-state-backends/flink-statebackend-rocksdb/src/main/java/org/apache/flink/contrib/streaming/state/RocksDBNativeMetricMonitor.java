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

import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.a4s.core.StackDistanceHistogram.ShardsHeader;
import org.apache.flink.a4s.logging.A4SMetricsFlowStep;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;

import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.math.BigInteger;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.ToLongFunction;

/**
 * A monitor which pulls {{@link RocksDB}} native metrics and forwards them to Flink's metric group.
 * All metrics are unsigned longs and are reported at the column family level.
 */
@Internal
public class RocksDBNativeMetricMonitor implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBNativeMetricMonitor.class);

    private final RocksDBNativeMetricOptions options;

    private final MetricGroup metricGroup;

    private final Object lock;

    static final String COLUMN_FAMILY_KEY = "column_family";

    @GuardedBy("lock")
    private RocksDB rocksDB;

    @Nullable
    @GuardedBy("lock")
    private Statistics statistics;

    @Nullable private final LRUCache lruCache;

    /**
     * Shared block caches whose MRC histogram is already reported by some monitor. The cache is per
     * slot while there is one monitor per keyed backend, so only the first monitor of a cache
     * reports it; otherwise the histogram would be counted once per backend.
     */
    @GuardedBy("HISTOGRAM_REPORTING_CACHES")
    private static final Set<Cache> HISTOGRAM_REPORTING_CACHES =
            Collections.newSetFromMap(new IdentityHashMap<>());

    /** The cache this monitor reports the histogram of, or null. */
    @Nullable private LRUCache reportedCache;

    /** Whether the cache's SHARDS estimator is enabled (fixed when the cache is created). */
    private boolean shardsEnabled;

    public RocksDBNativeMetricMonitor(
            @Nonnull RocksDBNativeMetricOptions options,
            @Nonnull MetricGroup metricGroup,
            @Nonnull RocksDB rocksDB,
            @Nullable Statistics statistics,
            @Nullable Cache blockCache) {
        this.options = options;
        this.metricGroup = metricGroup;
        this.rocksDB = rocksDB;
        this.statistics = statistics;
        this.lruCache = (blockCache instanceof LRUCache) ? (LRUCache) blockCache : null;
        this.lock = new Object();
        registerStatistics();
        registerStackDistanceHistogram();
    }

    /** Register gauges to pull native metrics for the database. */
    private void registerStatistics() {
        if (statistics != null) {
            for (TickerType tickerType : options.getMonitorTickerTypes()) {
                metricGroup.gauge(
                        String.format("rocksdb.%s", tickerType.name().toLowerCase()),
                        new RocksDBNativeStatisticsMetricView(tickerType));
            }
        }
    }

    /** Registers the stack distance histogram metric if enabled. */
    private void registerStackDistanceHistogram() {
        if (!options.isStackDistanceHistogramEnabled()) {
            return;
        }
        if (lruCache != null) {
            synchronized (HISTOGRAM_REPORTING_CACHES) {
                if (!HISTOGRAM_REPORTING_CACHES.add(lruCache)) {
                    LOG.warn(
                            "The shared block cache is already reported by another keyed backend "
                                    + "in this slot; not registering its stack distance "
                                    + "histogram again. The MRC mixes all stateful operators "
                                    + "that share the slot.");
                    return;
                }
            }
            reportedCache = lruCache;
            shardsEnabled = lruCache.getShardsMrcHistogram().length > 0;
        }
        LOG.info(
                "Registering stack distance histogram metric for RocksDB (estimator: {}).",
                shardsEnabled ? "SHARDS" : "QuickMRC");
        RocksDBStackDistanceHistogramView view = new RocksDBStackDistanceHistogramView(lruCache);
        metricGroup.gauge("rocksdb.stack-distance-histogram", view);
        if (shardsEnabled) {
            registerShardsHeaderGauges(view);
        }
    }

    /** Exposes the SHARDS header, e.g. to watch sampled-refs / (R * total-refs). */
    private void registerShardsHeaderGauges(RocksDBStackDistanceHistogramView view) {
        metricGroup.gauge(
                "rocksdb.a4s.shards.sampling-rate",
                (Gauge<Double>)
                        () -> {
                            ShardsHeader header = view.getLatestShardsHeader();
                            return header == null ? 0.0 : header.getSamplingRate();
                        });
        Map<String, ToLongFunction<ShardsHeader>> fields = new LinkedHashMap<>();
        fields.put("hash-seed", ShardsHeader::getHashSeed);
        fields.put("bin-bytes", ShardsHeader::getBinBytes);
        fields.put("max-distance-bytes", ShardsHeader::getMaxDistanceBytes);
        fields.put("total-refs", ShardsHeader::getTotalRefs);
        fields.put("sampled-refs", ShardsHeader::getSampledRefs);
        fields.put("evicted-by-cap", ShardsHeader::getEvictedByCap);
        fields.put("tracked-entries", ShardsHeader::getTrackedEntries);
        fields.put("tracked-bytes", ShardsHeader::getTrackedBytes);
        fields.put("estimator-ns", ShardsHeader::getEstimatorNanos);
        fields.forEach(
                (name, field) ->
                        metricGroup.gauge(
                                "rocksdb.a4s.shards." + name,
                                (Gauge<Long>)
                                        () -> {
                                            ShardsHeader header = view.getLatestShardsHeader();
                                            return header == null ? 0L : field.applyAsLong(header);
                                        }));
    }

    /**
     * Register gauges to pull native metrics for the column family.
     *
     * @param columnFamilyName group name for the new gauges
     * @param handle native handle to the column family
     */
    void registerColumnFamily(String columnFamilyName, ColumnFamilyHandle handle) {

        boolean columnFamilyAsVariable = options.isColumnFamilyAsVariable();
        MetricGroup group =
                columnFamilyAsVariable
                        ? metricGroup.addGroup(COLUMN_FAMILY_KEY, columnFamilyName)
                        : metricGroup.addGroup(columnFamilyName);

        for (String property : options.getProperties()) {
            RocksDBNativePropertyMetricView gauge =
                    new RocksDBNativePropertyMetricView(handle, property);
            group.gauge(property, gauge);
        }
    }

    /** Updates the value of metricView if the reference is still valid. */
    private void setProperty(RocksDBNativePropertyMetricView metricView) {
        if (metricView.isClosed()) {
            return;
        }
        try {
            synchronized (lock) {
                if (rocksDB != null) {
                    long value = rocksDB.getLongProperty(metricView.handle, metricView.property);
                    metricView.setValue(value);
                }
            }
        } catch (RocksDBException e) {
            metricView.close();
            LOG.warn("Failed to read native metric {} from RocksDB.", metricView.property, e);
        }
    }

    private void setStatistics(RocksDBNativeStatisticsMetricView metricView) {
        if (metricView.isClosed()) {
            return;
        }
        if (statistics != null) {
            synchronized (lock) {
                metricView.setValue(statistics.getTickerCount(metricView.tickerType));
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            rocksDB = null;
            statistics = null;
        }
        if (reportedCache != null) {
            synchronized (HISTOGRAM_REPORTING_CACHES) {
                HISTOGRAM_REPORTING_CACHES.remove(reportedCache);
            }
            reportedCache = null;
        }
    }

    abstract static class RocksDBNativeView implements View {
        private boolean closed;

        RocksDBNativeView() {
            this.closed = false;
        }

        void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    /**
     * A gauge which periodically pulls a RocksDB property-based native metric for the specified
     * column family / metric pair.
     *
     * <p><strong>Note</strong>: As the returned property is of type {@code uint64_t} on C++ side
     * the returning value can be negative. Because java does not support unsigned long types, this
     * gauge wraps the result in a {@link BigInteger}.
     */
    class RocksDBNativePropertyMetricView extends RocksDBNativeView implements Gauge<BigInteger> {
        private final String property;

        private final ColumnFamilyHandle handle;

        private BigInteger bigInteger;

        private RocksDBNativePropertyMetricView(
                ColumnFamilyHandle handle, @Nonnull String property) {
            this.handle = handle;
            this.property = property;
            this.bigInteger = BigInteger.ZERO;
        }

        public void setValue(long value) {
            if (value >= 0L) {
                bigInteger = BigInteger.valueOf(value);
            } else {
                int upper = (int) (value >>> 32);
                int lower = (int) value;

                bigInteger =
                        BigInteger.valueOf(Integer.toUnsignedLong(upper))
                                .shiftLeft(32)
                                .add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
            }
        }

        @Override
        public BigInteger getValue() {
            return bigInteger;
        }

        @Override
        public void update() {
            setProperty(this);
        }
    }

    /**
     * A gauge which periodically pulls a RocksDB statistics-based native metric for the database.
     */
    class RocksDBNativeStatisticsMetricView extends RocksDBNativeView implements Gauge<Long> {
        private final TickerType tickerType;
        private long value;

        private RocksDBNativeStatisticsMetricView(TickerType tickerType) {
            this.tickerType = tickerType;
        }

        @Override
        public Long getValue() {
            return value;
        }

        void setValue(long value) {
            this.value = value;
        }

        @Override
        public void update() {
            setStatistics(this);
        }
    }

    /** A stack distance histogram gauge updated by the shared {@link View} update cycle. */
    class RocksDBStackDistanceHistogramView extends RocksDBNativeView implements Gauge<String> {

        @Nullable private final LRUCache viewLruCache;
        private final long bucketSizeScaling;
        private final long cacheItemSizeBytes;
        private StackDistanceHistogram latestHistogram;

        RocksDBStackDistanceHistogramView(@Nullable LRUCache lruCache) {
            this.viewLruCache = lruCache;
            this.bucketSizeScaling = options.getStackDistanceHistogramBucketSizeScaling();
            this.cacheItemSizeBytes = options.getStackDistanceHistogramCacheItemSizeBytes();
            this.latestHistogram =
                    new StackDistanceHistogram(
                            new long[0], 0L, 1, bucketSizeScaling, cacheItemSizeBytes);
        }

        @Override
        public void update() {
            synchronized (lock) {
                // A closed monitor must not touch the cache, which the slot may have released.
                if (isClosed() || rocksDB == null) {
                    return;
                }
                if (viewLruCache == null) {
                    StackDistanceHistogram emptyHistogram =
                            new StackDistanceHistogram(
                                    new long[0], 0L, 1, bucketSizeScaling, cacheItemSizeBytes);
                    LOG.debug(
                            "A4S [{}]: histogramFingerprint={} numBuckets={} numPartitions={} reason=lru_cache_null",
                            A4SMetricsFlowStep.ROCKSDB_HISTOGRAM_UPDATE_FAILED.name(),
                            emptyHistogram.getHistogramFingerprint(),
                            0,
                            1);
                    latestHistogram = emptyHistogram;
                    return;
                }
                try {
                    if (shardsEnabled) {
                        // Cumulative and monotonic: never reset on read.
                        latestHistogram =
                                StackDistanceHistogram.fromShardsMrcArray(
                                        viewLruCache.getShardsMrcHistogram());
                        LOG.debug(
                                "A4S [{}]: histogramFingerprint={} numBuckets={} sampledRefs={}",
                                A4SMetricsFlowStep.ROCKSDB_HISTOGRAM_UPDATED.name(),
                                latestHistogram.getHistogramFingerprint(),
                                latestHistogram.getNumBuckets(),
                                latestHistogram.getShards().getSampledRefs());
                        return;
                    }
                    long[] histogramCounts = viewLruCache.getStackDistanceHistogram();
                    viewLruCache.resetQuickMRCStats();
                    if (histogramCounts == null) {
                        StackDistanceHistogram emptyHistogram =
                                new StackDistanceHistogram(
                                        new long[0], 0L, 1, bucketSizeScaling, cacheItemSizeBytes);
                        LOG.warn(
                                "A4S [{}]: histogramFingerprint={} numBuckets={} numPartitions={} reason=histogram_null",
                                A4SMetricsFlowStep.ROCKSDB_HISTOGRAM_UPDATE_FAILED.name(),
                                emptyHistogram.getHistogramFingerprint(),
                                0,
                                1);
                        latestHistogram = emptyHistogram;
                        return;
                    }

                    long completeMisses =
                            histogramCounts.length == 0
                                    ? 0L
                                    : histogramCounts[histogramCounts.length - 1];
                    long[] finiteBucketCounts =
                            histogramCounts.length == 0
                                    ? new long[0]
                                    : java.util.Arrays.copyOf(
                                            histogramCounts, histogramCounts.length - 1);
                    latestHistogram =
                            new StackDistanceHistogram(
                                    finiteBucketCounts,
                                    completeMisses,
                                    1,
                                    bucketSizeScaling,
                                    cacheItemSizeBytes);
                    LOG.debug(
                            "A4S [{}]: histogramFingerprint={} numBuckets={} numPartitions={} totalFrequency={}",
                            A4SMetricsFlowStep.ROCKSDB_HISTOGRAM_UPDATED.name(),
                            latestHistogram.getHistogramFingerprint(),
                            latestHistogram.getNumBuckets(),
                            latestHistogram.getNumPartitions(),
                            latestHistogram.getTotalFrequency());
                } catch (RuntimeException e) {
                    StackDistanceHistogram empty =
                            new StackDistanceHistogram(
                                    new long[0], 0L, 1, bucketSizeScaling, cacheItemSizeBytes);
                    LOG.warn(
                            "A4S [{}]: histogramFingerprint={} numBuckets={} numPartitions={} reason=exception message={}",
                            A4SMetricsFlowStep.ROCKSDB_HISTOGRAM_UPDATE_FAILED.name(),
                            empty.getHistogramFingerprint(),
                            0,
                            1,
                            e.getMessage(),
                            e);
                    latestHistogram = empty;
                }
            }
        }

        @Nullable
        ShardsHeader getLatestShardsHeader() {
            synchronized (lock) {
                return latestHistogram == null ? null : latestHistogram.getShards();
            }
        }

        @Override
        public String getValue() {
            StackDistanceHistogram histogram;
            synchronized (lock) {
                histogram = latestHistogram;
            }
            if (histogram == null) {
                histogram =
                        new StackDistanceHistogram(
                                new long[0], 0L, 1, bucketSizeScaling, cacheItemSizeBytes);
            }
            return histogram.toMetricString();
        }
    }
}
