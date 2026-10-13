/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler.a4s;

import lombok.Getter;

import org.apache.flink.a4s.core.MissRateCurve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class MemoryParallelismCurve {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryParallelismCurve.class);

    /** Target throughput (records/sec) this curve is generated for. */
    @Getter
    private final double targetThroughput;

    /** List of (parallelism, memory) points on the curve, sorted by parallelism ascending. */
    @Getter
    private final List<Point> points;

    @Getter
    private final int minParallelism;

    @Getter
    private final int maxParallelism;

    /**
     * Represents a single point on the memory-parallelism curve.
     * 
     * x-axis: parallelism
     * y-axis: memory in MB
     */
    public static class Point implements Comparable<Point> {
        @Getter
        private final int parallelism;
        
        /** Memory in MB. */
        @Getter
        private final double memoryMB;

        public Point(int parallelism, double memoryMB) {
            this.parallelism = parallelism;
            this.memoryMB = memoryMB;
        }

        @Override
        public int compareTo(Point other) {
            return Integer.compare(this.parallelism, other.parallelism);
        }

        @Override
        public String toString() {
            return String.format("(p=%d, mem=%.2fMB)", parallelism, memoryMB);
        }
    }

    public MemoryParallelismCurve(double targetThroughput, List<Point> points) {
        this.targetThroughput = targetThroughput;
        this.points = new ArrayList<>(points);
        Collections.sort(this.points);
        
        if (this.points.isEmpty()) {
            this.minParallelism = 1;
            this.maxParallelism = 1;
        } else {
            this.minParallelism = this.points.get(0).getParallelism();
            this.maxParallelism = this.points.get(this.points.size() - 1).getParallelism();
        }
    }

    /**
     * @return The memory in MB for the associated parallelism level, or empty if no point exists for the given parallelism
     */
    public Optional<Double> getMemoryMbForParallelism(int parallelism) {
        return points.stream()
            .filter(point -> point.getParallelism() == parallelism)
            .map(Point::getMemoryMB)
            .findFirst();
    }

    @Override
    public String toString() {
        return String.format("MPC[target=%.2f, minParallelism=%d, maxParallelism=%d, points=%s]", targetThroughput, minParallelism, maxParallelism, points);
    }

    /**
     * Builder for creating MemoryParallelismCurve instances.
     */
    public static class Builder {
        private double targetThroughput;
        private final List<Point> points = new ArrayList<>();

        public Builder targetThroughput(double throughput) {
            this.targetThroughput = throughput;
            return this;
        }

        public Builder addPoint(int parallelism, double memoryMB) {
            points.add(new Point(parallelism, memoryMB));
            return this;
        }

        public MemoryParallelismCurve build() {
            return new MemoryParallelismCurve(targetThroughput, points);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a MemoryParallelismCurve from a MissRateCurve.
     *
     * <p>The algorithm calculates, for each parallelism level from 1 to 24, the minimum
     * memory required to achieve the target throughput. The required miss rate for a given
     * parallelism is:
     * <pre>
     * miss_rate = ((parallelism / targetThroughput) - hitLatencyMs) / (missLatencyMs - hitLatencyMs)
     * </pre>
     *
     * <p>The memory is then derived from the MissRateCurve by finding the cache size
     * that achieves this miss rate.
     *
     * @param targetThroughputPerSec the target throughput in records/sec
     * @param missLatencySec the latency (in sec) for a cache miss operation
     * @param hitLatencySec the latency (in sec) for a cache hit operation
     * @param minParallelism the minimum parallelism level
     * @param maxParallelism the maximum parallelism level
     * @param mrc the MissRateCurve to derive memory requirements from
     * @return a MemoryParallelismCurve
     */
    public static MemoryParallelismCurve fromMissRateCurve(
            double targetThroughputPerSec,
            double missLatencySec,
            double hitLatencySec,
            int minParallelism,
            int maxParallelism,
            MissRateCurve mrc) {
        Builder builder = builder().targetThroughput(targetThroughputPerSec);
        double latencyDiff = missLatencySec - hitLatencySec;

        if (latencyDiff <= 0) {
            throw new IllegalArgumentException("missLatencySec (" + missLatencySec + ") should be greater than hitLatencySec (" + hitLatencySec + ")");
        }

        for (int parallelism = minParallelism; parallelism <= maxParallelism; parallelism++) {
            // This is the maximum acceptable miss rate. We need a cache size
            // that achieves this miss rate or lower
            double maximumMissRate = ((parallelism / targetThroughputPerSec) - hitLatencySec) / latencyDiff;

            // It's impossible to achieve the desired throughput with the given parallelism and hit latency
            if (maximumMissRate <= 0) {
                throw new IllegalStateException(String.format("Maximum miss-rate <= 0. parallelism=%d targetThroughput=%f hitLatencySec=%f missLatencySec=%f", parallelism, targetThroughputPerSec, hitLatencySec, missLatencySec));
            }

            // Perhaps any level miss rate is acceptable, so we clamp to 1.0
            if (maximumMissRate > 1.0) {
                LOG.warn("Parallelism {} has maximum miss rate {}, clamping to 1.0", parallelism, maximumMissRate);
                maximumMissRate = 1.0;
            }

            double memoryMB = mrc.leastMemoryBytesForMissRate(maximumMissRate) / (1024.0 * 1024.0);
            builder.addPoint(parallelism, memoryMB);
        }
        return builder.build();
    }
}
