package org.apache.flink.a4s.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StackDistanceHistogramTest {
    @Test
    void testMerge_preservesSumsAndPartitions() {
        StackDistanceHistogram h1 =
                new StackDistanceHistogram(new long[] {3L, 1L}, 1L, 1, 2L, 4096L);
        StackDistanceHistogram h2 =
                new StackDistanceHistogram(new long[] {2L, 2L}, 1L, 2, 2L, 4096L);

        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(3, merged.getNumPartitions());
        assertEquals(10L, merged.getTotalFrequency());
        assertEquals(5L, merged.getFrequency(0));
        assertEquals(3L, merged.getFrequency(1));
        assertEquals(0L, merged.getFrequency(2));
        assertEquals(2L, merged.getCompleteMisses());
        assertEquals(2L, merged.getBucketSizeScaling());
        assertEquals(4096L, merged.getCacheItemSizeBytes());
    }

    @Test
    void testMerge_twoHistograms_differentNumBuckets() {
        StackDistanceHistogram h1 =
                new StackDistanceHistogram(new long[] {3L, 1L}, 1L, 1, 3L, 4096L);
        StackDistanceHistogram h2 = new StackDistanceHistogram(new long[] {2L}, 2L, 1, 3L, 4096L);

        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(2, merged.getNumPartitions());
        assertEquals(9L, merged.getTotalFrequency());
        assertEquals(5L, merged.getFrequency(0));
        assertEquals(1L, merged.getFrequency(1));
        assertEquals(0L, merged.getFrequency(2));
        assertEquals(3L, merged.getCompleteMisses());
        assertEquals(3L, merged.getBucketSizeScaling());
        assertEquals(4096L, merged.getCacheItemSizeBytes());
    }

    @Test
    void testMerge_twoHistograms_oneEmpty() {
        StackDistanceHistogram h1 =
                new StackDistanceHistogram(new long[] {3L, 1L}, 1L, 1, 4L, 4096L);
        StackDistanceHistogram h2 = new StackDistanceHistogram(new long[0], 0L, 1, 4L, 4096L);
        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(2, merged.getNumPartitions());
        assertEquals(5L, merged.getTotalFrequency());
        assertEquals(3L, merged.getFrequency(0));
        assertEquals(1L, merged.getFrequency(1));
        assertEquals(0L, merged.getFrequency(2));
        assertEquals(1L, merged.getCompleteMisses());
        assertEquals(4L, merged.getBucketSizeScaling());
        assertEquals(4096L, merged.getCacheItemSizeBytes());
    }

    @Test
    void testConstructor_extractsCompleteMissesFromLastBucket() {
        StackDistanceHistogram histogram =
                new StackDistanceHistogram(new long[] {3L, 5L}, 7L, 1, 1L, 4096L);

        assertEquals(2, histogram.getNumBuckets());
        assertEquals(3L, histogram.getFrequency(0));
        assertEquals(5L, histogram.getFrequency(1));
        assertEquals(7L, histogram.getCompleteMisses());
        assertTrue(Arrays.equals(new long[] {3L, 5L}, histogram.getBucketCounts()));
    }

    @Test
    void testConstructor_emptyBucketsHasZeroCompleteMisses() {
        StackDistanceHistogram histogram =
                new StackDistanceHistogram(new long[0], 0L, 1, 1L, 4096L);

        assertEquals(0, histogram.getNumBuckets());
        assertEquals(0L, histogram.getCompleteMisses());
        assertTrue(Arrays.equals(new long[0], histogram.getBucketCounts()));
    }

    @Test
    void testMRC_emptyHistogram() {
        StackDistanceHistogram empty = new StackDistanceHistogram(new long[0], 0L, 1, 1L, 4096L);
        assertTrue(MissRateCurve.fromStackDistanceHistogram(empty).getPoints().isEmpty());
    }

    @Test
    void testFromSerializedValue_parsesRocksDBPayload() {
        String payload =
                "{\"bucketCounts\":[5,7],\"completeMisses\":11,\"numPartitions\":1,\"bucketSizeScaling\":2,\"cacheItemSizeBytes\":4096,\"scopeInfo\":null,\"name\":null}";

        StackDistanceHistogram histogram = StackDistanceHistogram.fromMetricString(payload);

        assertEquals(2, histogram.getNumBuckets());
        assertEquals(1, histogram.getNumPartitions());
        assertEquals(5L, histogram.getFrequency(0));
        assertEquals(7L, histogram.getFrequency(1));
        assertEquals(0L, histogram.getFrequency(2));
        assertEquals(11L, histogram.getCompleteMisses());
        assertEquals(23L, histogram.getTotalFrequency());
        assertEquals(2L, histogram.getBucketSizeScaling());
        assertEquals(4096L, histogram.getCacheItemSizeBytes());
    }

    @Test
    void testFromSerializedValue_keepsSparseMapForZeroCountBuckets() {
        String payload =
                "{\"bucketCounts\":[0,10],\"completeMisses\":0,\"numPartitions\":1,\"bucketSizeScaling\":4,\"cacheItemSizeBytes\":4096,\"scopeInfo\":null,\"name\":null}";

        StackDistanceHistogram histogram = StackDistanceHistogram.fromMetricString(payload);
        long[] buckets = histogram.getBucketCounts();

        assertEquals(2, histogram.getNumBuckets());
        assertEquals(2, buckets.length);
        assertEquals(10L, buckets[1]);
        assertEquals(0L, buckets[0]);
        assertEquals(0L, histogram.getCompleteMisses());
        assertEquals(4L, histogram.getBucketSizeScaling());
        assertEquals(4096L, histogram.getCacheItemSizeBytes());
    }

    @Test
    void testFromSerializedValue_throwsWhenCountsLengthIsInvalid() {
        String payload =
                "{\"bucketCounts\":[5,-1],\"completeMisses\":11,\"numPartitions\":1,\"bucketSizeScaling\":1,\"cacheItemSizeBytes\":4096,\"scopeInfo\":null,\"name\":null}";

        assertThrows(
                IllegalArgumentException.class,
                () -> StackDistanceHistogram.fromMetricString(payload));
    }

    @Test
    void testFromSerializedValue_throwsWhenBucketSizeScalingMissing() {
        String payload = "{\"bucketCounts\":[5,7],\"completeMisses\":11,\"numPartitions\":1}";

        assertThrows(
                IllegalArgumentException.class,
                () -> StackDistanceHistogram.fromMetricString(payload));
    }

    @Test
    void testFromSerializedValue_throwsWhenCacheItemSizeBytesMissing() {
        String payload =
                "{\"bucketCounts\":[5,7],\"completeMisses\":11,\"numPartitions\":1,\"bucketSizeScaling\":1}";

        assertThrows(
                IllegalArgumentException.class,
                () -> StackDistanceHistogram.fromMetricString(payload));
    }

    @Test
    void testMerge_throwsWhenBucketSizeScalingDiffers() {
        StackDistanceHistogram h1 =
                new StackDistanceHistogram(new long[] {3L, 1L}, 1L, 1, 1L, 4096L);
        StackDistanceHistogram h2 =
                new StackDistanceHistogram(new long[] {2L, 2L}, 1L, 2, 2L, 4096L);

        assertThrows(
                IllegalArgumentException.class,
                () -> StackDistanceHistogram.merge(List.of(h1, h2)));
    }

    @Test
    void testMerge_throwsWhenCacheItemSizeBytesDiffers() {
        StackDistanceHistogram h1 =
                new StackDistanceHistogram(new long[] {3L, 1L}, 1L, 1, 1L, 4096L);
        StackDistanceHistogram h2 =
                new StackDistanceHistogram(new long[] {2L, 2L}, 1L, 2, 1L, 8192L);

        assertThrows(
                IllegalArgumentException.class,
                () -> StackDistanceHistogram.merge(List.of(h1, h2)));
    }

    @Test
    void testMetricRoundTrip_serializesEntireHistogramObject() {
        StackDistanceHistogram histogram =
                new StackDistanceHistogram(new long[] {1L, 2L}, 3L, 4, 8L, 4096L);

        StackDistanceHistogram parsed =
                StackDistanceHistogram.fromMetricString(histogram.toMetricString());

        assertTrue(Arrays.equals(new long[] {1L, 2L}, parsed.getBucketCounts()));
        assertEquals(3L, parsed.getCompleteMisses());
        assertEquals(4, parsed.getNumPartitions());
        assertEquals(8L, parsed.getBucketSizeScaling());
        assertEquals(4096L, parsed.getCacheItemSizeBytes());
    }
}
