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

import org.junit.jupiter.api.Test;

import java.util.List;

import org.apache.flink.a4s.core.MissRateCurve;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link MemoryParallelismCurve#fromMissRateCurve}. */
public class MemoryParallelismCurveTest {
    @Test
    void testFromMissRateCurve_invalidLatencies_throwsException() {
        MissRateCurve mrc = new MissRateCurve(List.of(
                new MissRateCurve.Point(100L, 0.5)
        ));

        // missLatencyMs <= hitLatencyMs should throw
        assertThrows(IllegalArgumentException.class, () ->
                MemoryParallelismCurve.fromMissRateCurve(1000.0, 10.0, 10.0, 1, 24, mrc));

        assertThrows(IllegalArgumentException.class, () ->
                MemoryParallelismCurve.fromMissRateCurve(1000.0, 5.0, 10.0, 1, 24, mrc));
    }

    @Test
    void testFromMissRateCurve_basicCurveGeneration() {
        // Create MRC with decreasing miss rates as memory increases
        MissRateCurve mrc = new MissRateCurve(List.of(
                new MissRateCurve.Point(50L, 0.9),
                new MissRateCurve.Point(100L, 0.7),
                new MissRateCurve.Point(200L, 0.5),
                new MissRateCurve.Point(400L, 0.3),
                new MissRateCurve.Point(800L, 0.1)
            ));

        // targetThroughput = 100 rec/s, missLatency = 20ms, hitLatency = 5ms
        // Formula: maxMissRate = ((parallelism / throughput) - hitLatency) / (missLatency - hitLatency)
        // For parallelism 1: maxMissRate = ((1/100) - 0.005) / (0.020 - 0.005) = (0.01 - 0.005) / 0.015 = 0.333
        // For parallelism 2: maxMissRate = ((2/100) - 0.005) / 0.015 = 0.015 / 0.015 = 1.0
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                100.0,  // targetThroughputPerSec
                0.02,   // missLatencySec
                0.005,    // hitLatencySec
                1, 24, mrc);

        assertThat(mpc.getTargetThroughput()).isEqualTo(100.0);
        assertThat(mpc.getPoints()).isNotEmpty();
    }

    @Test
    void testFromMissRateCurve_respectsParallelismBounds() {
        MissRateCurve mrc = new MissRateCurve(List.of(
                new MissRateCurve.Point(100L , 0.01)  // Low miss rate, all parallelisms should find this
            ));

        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                10000.0, 5e-3, 5e-6, 5, 10, mrc);

        List<MemoryParallelismCurve.Point> points = mpc.getPoints();

        // All points should be within [5, 10]
        for (MemoryParallelismCurve.Point point : points) {
            assertThat(point.getParallelism()).isBetween(5, 10);
        }
    }

    @Test
    void testFromMissRateCurve_negativeMissRateThrowsException() {
        MissRateCurve mrc = new MissRateCurve(List.of(
                new MissRateCurve.Point(100L, 0.5)
            ));

        assertThrows(IllegalStateException.class, () -> MemoryParallelismCurve.fromMissRateCurve(
                1000.0,  // targetThroughput
                5e-2,    // missLatencySec
                5e-3,    // hitLatencySec
                1, 24, mrc));
    }

    @Test
    void testFromMissRateCurve_clampsMissRateToOne() {
        // When maxMissRate > 1.0, it should be clamped to 1.0
        MissRateCurve mrc = new MissRateCurve(List.of(
                new MissRateCurve.Point(50L, 1.0),
                new MissRateCurve.Point(100L, 0.5)
            ));

        // High parallelism relative to throughput will give maxMissRate > 1.0
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                10.0,    // Low throughput
                0.02,    // missLatencySec
                0.005,     // hitLatencySec
                1, 24, mrc);

        // Should still produce valid points (miss rate clamped to 1.0)
        assertThat(mpc.getPoints()).isNotEmpty();
    }

    @Test
    void testFromMissRateCurve_plateauRegion() {
        // MRC with only high miss rates
        MissRateCurve mrc = new MissRateCurve(List.of(
                new MissRateCurve.Point(100 * 1024 * 1024, 0.9),
                new MissRateCurve.Point(200 * 1024 * 1024, 0.8)
            ));

        // With very high throughput, required miss rate will be very low
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                10000.0,  // targetThroughputPerSec
                5e-3,     // missLatencySec
                5e-6,      // hitLatencySec
                1, 24, mrc);

        assertThat(mpc.getPoints().size()).isEqualTo(24);
    }

    @Test
    void testFromMissRateCurve_memoryDecreaseWithParallelism() {
        // Higher parallelism allows higher miss rate, which requires less memory
                MissRateCurve mrc = new MissRateCurve(List.of(
                new MissRateCurve.Point(100 * 1024 * 1024, 0.9),
                new MissRateCurve.Point(200 * 1024 * 1024, 0.7),
                new MissRateCurve.Point(400 * 1024 * 1024, 0.5),
                new MissRateCurve.Point(800 * 1024 * 1024, 0.3),
                new MissRateCurve.Point(1600 * 1024 * 1024, 0.1)
            ));

        // With reasonable params, higher parallelism should map to lower memory
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                10000.0,
                5e-3,
                5e-6,
                1, 10, mrc);

        List<MemoryParallelismCurve.Point> points = mpc.getPoints();

        // Verify the curve has the expected inverse relationship
        // (higher parallelism generally means lower or equal memory)
        if (points.size() >= 2) {
            for (int i = 1; i < points.size(); i++) {
                MemoryParallelismCurve.Point prev = points.get(i - 1);
                MemoryParallelismCurve.Point curr = points.get(i);
                // Higher parallelism should have same or lower memory requirement
                assertThat(curr.getMemoryMB()).isLessThanOrEqualTo(prev.getMemoryMB());
            }
        }
    }

    @Test
    void testFromMissRateCurve_singleParallelism() {
        MissRateCurve mrc = new MissRateCurve(List.of(
                new MissRateCurve.Point(100L, 0.5)
            ));

        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                100.0, 0.02, 0.005, 5, 5, mrc);

        List<MemoryParallelismCurve.Point> points = mpc.getPoints();

        // Should have at most 1 point
        assertThat(points.size()).isLessThanOrEqualTo(1);
        if (!points.isEmpty()) {
            assertThat(points.get(0).getParallelism()).isEqualTo(5);
        }
    }

    @Test
    void testFromMissRateCurve_formulaVerification() {
        // Manually verify the formula for a specific case
        // maxMissRate = ((parallelism / throughput) - hitLatencySec) / (missLatencySec - hitLatencySec)

        // Set up MRC with known miss rates
        MissRateCurve mrc = new MissRateCurve(List.of(
                new MissRateCurve.Point(100 * 1024 * 1024, 0.5),   // 100MB gives 0.5 miss rate
                new MissRateCurve.Point(200 * 1024 * 1024, 0.25)  // 200MB gives 0.25 miss rate
            ));

        // throughput = 100, missLatency = 0.02s (20ms), hitLatency = 0.01s (10ms)
        // For parallelism 2:
        //   maxMissRate = ((2/100) - 0.01) / (0.02 - 0.01) = (0.02 - 0.01) / 0.01 = 1.0
        // For parallelism 1:
        //   maxMissRate = ((1/100) - 0.01) / 0.01 = (0.01 - 0.01) / 0.01 = 0.0
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                100.0,   // throughput (records/sec)
                0.02,    // missLatencySec
                0.005,    // hitLatencySec
                1, 3, mrc);

        // With maxMissRate = 0 for parallelism 1, no MRC point satisfies (both have > 0 miss rate)
        // With maxMissRate = 1.0 for parallelism 2, MRC returns 100.0 (smallest that satisfies <= 1.0)
        List<MemoryParallelismCurve.Point> points = mpc.getPoints();

        // Parallelism 2 should have memory 100.0 (first point that satisfies maxMissRate = 1.0)
        boolean hasParallelism2 = points.stream()
                .anyMatch(p -> p.getParallelism() == 2 && p.getMemoryMB() == 100.0);
        assertThat(hasParallelism2).isTrue();
    }
}
