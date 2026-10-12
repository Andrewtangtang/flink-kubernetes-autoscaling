package org.apache.flink.a4s.core;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class MissRateCurve {
    private final List<Point> points;

    @JsonCreator
    public MissRateCurve(List<Point> points) {
        this.points = points == null ? Collections.emptyList() : List.copyOf(points);
    }

    @JsonValue
    public List<Point> getPoints() {
        return points;
    }

    /** Represents a point on the Miss Rate Curve: cache size -> miss rate. */
    public static class Point {
        public static final String FIELD_NAME_CACHE_SIZE_BYTES = "cacheSizeBytes";
        public static final String FIELD_NAME_MISS_RATE = "missRate";

        @JsonProperty(FIELD_NAME_CACHE_SIZE_BYTES)
        private final long cacheSizeBytes;

        @JsonProperty(FIELD_NAME_MISS_RATE)
        private final double missRate;

        @JsonCreator
        public Point(
                @JsonProperty(FIELD_NAME_CACHE_SIZE_BYTES) long cacheSizeBytes,
                @JsonProperty(FIELD_NAME_MISS_RATE) double missRate) {
            this.cacheSizeBytes = cacheSizeBytes;
            this.missRate = missRate;
        }

        public long getCacheSizeBytes() {
            return cacheSizeBytes;
        }

        public double getMissRate() {
            return missRate;
        }

        @Override
        public String toString() {
            return String.format(
                    "Point{cacheSizeBytes=%d, missRate=%.6f}", cacheSizeBytes, missRate);
        }
    }

    public static MissRateCurve fromStackDistanceHistogram(StackDistanceHistogram histogram) {
        if (histogram == null) {
            throw new IllegalArgumentException("Merged histogram cannot be null");
        }

        long totalFrequency = histogram.getTotalFrequency();
        if (totalFrequency == 0) {
            return new MissRateCurve(Collections.emptyList());
        }

        List<Point> mrc = new ArrayList<>();
        long cumulativeFreqAtSize = 0L;
        long bucketSizeScaling = histogram.getBucketSizeScaling();
        long cacheItemSizeBytes = histogram.getCacheItemSizeBytes();
        int numBuckets = histogram.getNumBuckets();

        for (int i = 0; i < numBuckets; i++) {
            cumulativeFreqAtSize += histogram.getFrequency(i);
            long currentCacheSize = (i + 1L) * bucketSizeScaling;
            double missRate = 1.0 - ((double) cumulativeFreqAtSize / totalFrequency);
            missRate = Math.max(0.0, Math.min(1.0, missRate));
            long cacheSizeItems = currentCacheSize * cacheItemSizeBytes;
            long scaledCacheSizeBytes = cacheSizeItems * histogram.getNumPartitions();
            mrc.add(new Point(scaledCacheSizeBytes, missRate));
        }

        return new MissRateCurve(mrc);
    }

    public double leastMemoryBytesForMissRate(double maxMissRate) {
        if (points.isEmpty()) {
            throw new IllegalStateException("MissRateCurve has no points");
        }

        Optional<Point> bestPoint =
                points.stream()
                        .filter(point -> point.getMissRate() <= maxMissRate)
                        .min(Comparator.comparingDouble(Point::getCacheSizeBytes));

        if (bestPoint.isPresent()) {
            return bestPoint.get().getCacheSizeBytes();
        }

        // At this point, no point on the MRC can achieve the miss rate we want
        // We can just return the last point, but our MRC may be entering a
        // plateau region so returning the last point may be wasteful.
        // In this case, we attempt to detect the start of the plateau
        // and return the cache size at that point.
        if (points.size() == 1) {
            return points.get(0).getCacheSizeBytes();
        }

        // We say we're entering a plateau region if the rate of decrease
        // in missrate between consecutive points is less than some threshold
        for (int i = 1; i < points.size(); i++) {
            double currentDifference =
                    points.get(i - 1).getMissRate() - points.get(i).getMissRate();
            if (currentDifference < 0.01) {
                return points.get(i - 1).getCacheSizeBytes();
            }
        }

        return points.get(points.size() - 1).getCacheSizeBytes();
    }

    @Override
    public String toString() {
        if (points.isEmpty()) {
            return "MissRateCurve{points=[]}";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("MissRateCurve{points=[");
        for (int i = 0; i < points.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Point p = points.get(i);
            double cacheMiB = p.getCacheSizeBytes() / (1024.0 * 1024.0);
            sb.append(String.format("cacheSizeMiB=%.6f missRate=%.6f", cacheMiB, p.getMissRate()));
        }
        sb.append("]}");
        return sb.toString();
    }
}
