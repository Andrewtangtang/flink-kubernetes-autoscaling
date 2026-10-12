package org.apache.flink.a4s.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MissRateCurveTest {
    @Test
    void testScaledMRC_horizontalScalingLogic() {
        int partitions = 2;
        StackDistanceHistogram histogram =
                new StackDistanceHistogram(new long[] {5L, 3L, 2L}, 0L, partitions, 1L, 4096L);

        MissRateCurve scaled = MissRateCurve.fromStackDistanceHistogram(histogram);

        List<MissRateCurve.Point> expectedScaled =
                List.of(
                        new MissRateCurve.Point(8192L, 0.5),
                        new MissRateCurve.Point(16384L, 0.2),
                        new MissRateCurve.Point(24576L, 0.0));

        for (int i = 0; i < expectedScaled.size(); i++) {
            assertEquals(
                    expectedScaled.get(i).getCacheSizeBytes(),
                    scaled.getPoints().get(i).getCacheSizeBytes());
            assertEquals(
                    expectedScaled.get(i).getMissRate(),
                    scaled.getPoints().get(i).getMissRate(),
                    0.0001);
        }
    }

    @Test
    void testLeastMemoryBytesForMissRate_simpleCase() {
        MissRateCurve mrc =
                new MissRateCurve(
                        List.of(
                                new MissRateCurve.Point(100L, 0.5),
                                new MissRateCurve.Point(200L, 0.2),
                                new MissRateCurve.Point(300L, 0.1)));

        assertEquals(100L, mrc.leastMemoryBytesForMissRate(0.5));
        assertEquals(200L, mrc.leastMemoryBytesForMissRate(0.2));
        assertEquals(300L, mrc.leastMemoryBytesForMissRate(0.1));
    }

    @Test
    void testLeastMemoryBytesForMissRate_plateauCase() {
        MissRateCurve mrc =
                new MissRateCurve(
                        List.of(
                                new MissRateCurve.Point(100L, 0.6),
                                new MissRateCurve.Point(200L, 0.5),
                                new MissRateCurve.Point(300L, 0.4),
                                new MissRateCurve.Point(400L, 0.38),
                                new MissRateCurve.Point(500L, 0.37)));

        assertEquals(500L, mrc.leastMemoryBytesForMissRate(0.3));
    }

    @Test
    void testLeastMemoryBytesForMissRate_noPointFoundOnePoint() {
        MissRateCurve mrc = new MissRateCurve(List.of(new MissRateCurve.Point(100L, 0.5)));

        assertEquals(100L, mrc.leastMemoryBytesForMissRate(0.3));
    }

    @Test
    void testLeastMemoryBytesForMissRate_noPointFoundTwoPoints() {
        MissRateCurve mrc =
                new MissRateCurve(
                        List.of(
                                new MissRateCurve.Point(100L, 0.5),
                                new MissRateCurve.Point(200L, 0.49)));

        assertEquals(200L, mrc.leastMemoryBytesForMissRate(0.3));
    }

    @Test
    void testShardsMrc_fromHandBuiltBins() {
        // Bins (1 MiB): 4, 3, 2 sampled hits; 1 infinite. 10 sampled refs. Parallelism 2.
        StackDistanceHistogram merged =
                StackDistanceHistogram.mergeShards(
                        List.of(
                                StackDistanceHistogram.fromShardsMrcArray(
                                        StackDistanceHistogramTest.shardsArray(
                                                1000L, 4L, 3L, 2L, 1L))),
                        2);

        List<MissRateCurve.Point> points =
                MissRateCurve.fromStackDistanceHistogram(merged).getPoints();

        long mib = 1L << 20;
        assertEquals(3, points.size());
        assertEquals(2 * mib, points.get(0).getCacheSizeBytes());
        assertEquals(0.6, points.get(0).getMissRate(), 1e-12);
        assertEquals(4 * mib, points.get(1).getCacheSizeBytes());
        assertEquals(0.3, points.get(1).getMissRate(), 1e-12);
        assertEquals(6 * mib, points.get(2).getCacheSizeBytes());
        assertEquals(0.1, points.get(2).getMissRate(), 1e-12);
    }
}
