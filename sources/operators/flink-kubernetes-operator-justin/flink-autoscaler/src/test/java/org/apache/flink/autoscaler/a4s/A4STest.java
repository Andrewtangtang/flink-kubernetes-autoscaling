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

import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link A4S}. */
public class A4STest {

    // ==================== Tests for place method ====================

    @Test
    void testMakeDecision_startsFromRecommendedParallelism() {
        JobVertexID operator = new JobVertexID();
        JobTopology topology = new JobTopology(new VertexInfo(operator, Map.of(), 1, 100));

        MemoryParallelismCurve mpc = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(1, 800.0),
                new MemoryParallelismCurve.Point(2, 500.0),
                new MemoryParallelismCurve.Point(3, 350.0)));

        Map<ScalingMetric, EvaluatedScalingMetric> operatorMetrics = new HashMap<>();
        EvaluatedMetrics evaluatedMetrics =
                new EvaluatedMetrics(
                        Map.of(operator, operatorMetrics),
                        Map.of(),
                        Map.of(operator, mpc),
                        Map.of());

        Map<JobVertexID, ScalingSummary> scalingSummaries = Map.of(operator, new ScalingSummary(1, 3, Map.of()));
        Configuration conf = new Configuration();

        A4S a4s = new A4S(topology, evaluatedMetrics, scalingSummaries);
        Map<JobVertexID, A4S.Decision> result = a4s.makeDecision(conf);

        assertThat(result).containsKey(operator);
        assertThat(result.get(operator).getParallelism()).isEqualTo(3);
        assertThat(result.get(operator).getMemoryMB()).isEqualTo(350.0);
    }

    @ParameterizedTest
    @CsvSource({
            "1, 800.0",
            "2, 500.0",
            "4, 300.0",
            "8, 200.0"
    })
    void testPlace_mpcWithMultipleParallelismLevels(int parallelism, double expectedMemory) {
        JobVertexID operator1 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()), Map.of());

        // MPC with multiple parallelism levels
        MemoryParallelismCurve mpc = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(1, 800.0),
                new MemoryParallelismCurve.Point(2, 500.0),
                new MemoryParallelismCurve.Point(4, 300.0),
                new MemoryParallelismCurve.Point(8, 200.0)));

        Map<JobVertexID, Integer> parallelismForVertex = Map.of(operator1, parallelism);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(operator1, mpc);

        Optional<Map<JobVertexID, A4S.Decision>> result = a4s.place(parallelismForVertex, mpcs, 1000.0);

        assertThat(result).isPresent();
        assertThat(result.get().get(operator1).getParallelism()).isEqualTo(parallelism);
        assertThat(result.get().get(operator1).getMemoryMB()).isEqualTo(expectedMemory);
    }

    @Test
    void testPlace_multipleOperatorsWithValidMPCs() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()), Map.of());

        MemoryParallelismCurve mpc1 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(2, 500.0),
                new MemoryParallelismCurve.Point(4, 300.0)));
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(3, 600.0),
                new MemoryParallelismCurve.Point(6, 400.0)));

        Map<JobVertexID, Integer> parallelismForVertex = Map.of(
                operator1, 2,
                operator2, 3);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(
                operator1, mpc1,
                operator2, mpc2);

        Optional<Map<JobVertexID, A4S.Decision>> result = a4s.place(parallelismForVertex, mpcs, 600.0);

        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(2);
        assertThat(result.get().get(operator1).getParallelism()).isEqualTo(2);
        assertThat(result.get().get(operator1).getMemoryMB()).isEqualTo(500.0);
        assertThat(result.get().get(operator2).getParallelism()).isEqualTo(3);
        assertThat(result.get().get(operator2).getMemoryMB()).isEqualTo(600.0);
    }

    @Test
    void testPlace_operatorWithMissingMPC_usesZeroMemory() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()), Map.of());

        // Only operator2 has an MPC, operator1 is not in the map
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(3, 600.0)));

        Map<JobVertexID, Integer> parallelismForVertex = Map.of(
                operator1, 2,
                operator2, 3);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(operator2, mpc2);

        Optional<Map<JobVertexID, A4S.Decision>> result = a4s.place(parallelismForVertex, mpcs, 1000.0);

        // Result should be present, but operator1 is skipped
        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(1);
        assertThat(result.get().get(operator2).getParallelism()).isEqualTo(3);
        assertThat(result.get().get(operator2).getMemoryMB()).isEqualTo(600.0);
    }

    @Test
    void testPlace_emptyTopology_returnsEmptyDecisions() {
        // Create an empty topology (no operators)
        JobTopology topology = new JobTopology();

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()), Map.of());

        Map<JobVertexID, Integer> parallelismForVertex = Map.of();
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of();

        Optional<Map<JobVertexID, A4S.Decision>> result = a4s.place(parallelismForVertex, mpcs, 600.0);

        assertThat(result).isPresent();
        assertThat(result.get()).isEmpty();
    }

    // ==================== Tests for increaseParallelism method ====================

    @Test
    void testIncreaseParallelism_singleOperator_returnsOperator() {
        JobVertexID operator1 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()), Map.of());

        // MPC: parallelism 1 -> 800 MB, parallelism 2 -> 500 MB (decrease of 300)
        MemoryParallelismCurve mpc = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(1, 800.0),
                new MemoryParallelismCurve.Point(2, 500.0)));

        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        parallelismForVertex.put(operator1, 1);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(operator1, mpc);

        Optional<JobVertexID> result = a4s.increaseParallelism(parallelismForVertex, mpcs);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(operator1);
    }

    @Test
    void testIncreaseParallelism_multipleOperators_returnsOperatorWithLargestMemoryDecrease() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()), Map.of());

        // operator1: parallelism 1 -> 800, parallelism 2 -> 700 (decrease of 100)
        MemoryParallelismCurve mpc1 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(1, 800.0),
                new MemoryParallelismCurve.Point(2, 700.0)));

        // operator2: parallelism 1 -> 600, parallelism 2 -> 300 (decrease of 300 - larger)
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(1, 600.0),
                new MemoryParallelismCurve.Point(2, 300.0)));

        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        parallelismForVertex.put(operator1, 1);
        parallelismForVertex.put(operator2, 1);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(
                operator1, mpc1,
                operator2, mpc2);

        Optional<JobVertexID> result = a4s.increaseParallelism(parallelismForVertex, mpcs);

        // operator2 has the largest memory decrease (-300 vs -100)
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(operator2);
    }

    @Test
    void testIncreaseParallelism_allOperatorsAtMaxParallelism_returnsEmpty() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()), Map.of());

        // Both MPCs have max parallelism of 2
        MemoryParallelismCurve mpc1 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(1, 800.0),
                new MemoryParallelismCurve.Point(2, 500.0)));
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(1, 600.0),
                new MemoryParallelismCurve.Point(2, 300.0)));

        // Both operators at max parallelism
        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        parallelismForVertex.put(operator1, 2);
        parallelismForVertex.put(operator2, 2);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(
                operator1, mpc1,
                operator2, mpc2);

        Optional<JobVertexID> result = a4s.increaseParallelism(parallelismForVertex, mpcs);

        assertThat(result).isEmpty();
    }

    @Test
    void testIncreaseParallelism_oneOperatorAtMaxParallelism_returnsOther() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()), Map.of());

        // operator1 has max parallelism of 2
        MemoryParallelismCurve mpc1 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(1, 800.0),
                new MemoryParallelismCurve.Point(2, 500.0)));

        // operator2 has max parallelism of 4
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.Point(1, 600.0),
                new MemoryParallelismCurve.Point(2, 400.0),
                new MemoryParallelismCurve.Point(3, 350.0),
                new MemoryParallelismCurve.Point(4, 300.0)));

        // operator1 at max, operator2 can still increase
        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        parallelismForVertex.put(operator1, 2);
        parallelismForVertex.put(operator2, 2);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(
                operator1, mpc1,
                operator2, mpc2);

        Optional<JobVertexID> result = a4s.increaseParallelism(parallelismForVertex, mpcs);

        // Only operator2 can increase parallelism
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(operator2);
    }

    @Test
    void testIncreaseParallelism_emptyTopology_returnsEmpty() {
        JobTopology topology = new JobTopology();

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()), Map.of());

        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of();

        Optional<JobVertexID> result = a4s.increaseParallelism(parallelismForVertex, mpcs);

        assertThat(result).isEmpty();
    }
}
