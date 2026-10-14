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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A4S Scaling Policy implementation.
 */
public class A4S {

    private static final Logger LOG = LoggerFactory.getLogger(A4S.class);

    private final EvaluatedMetrics evaluatedMetrics;

    private final List<JobVertexID> operators;

    public A4S(JobTopology jobTopology, EvaluatedMetrics evaluatedMetrics) {
        this.evaluatedMetrics = evaluatedMetrics;
        this.operators = jobTopology.getVerticesInTopologicalOrder();
    }

    // Flink's state.backend.rocksdb.memory.write-buffer-ratio, read from the job configuration
    private static final ConfigOption<Double> WRITE_BUFFER_RATIO =
            ConfigOptions.key("state.backend.rocksdb.memory.write-buffer-ratio")
                    .doubleType()
                    .defaultValue(0.5);

    /** Per-vertex result; memoryMB is the managed memory of one task, excluding a4s.memory-base.mb. */
    public static class Decision {
        @Getter
        private final int parallelism;
        
        @Getter
        private final double memoryMB;
        

        public Decision(int parallelism, double memoryMB) {
            this.parallelism = parallelism;
            this.memoryMB = memoryMB;
        }

        @Override
        public String toString() {
            return String.format("A4SDecision[p=%d, mem=%.2fMB]",
                    parallelism, memoryMB);
        }
    }

    /**
     * Make a scaling decision using the A4S algorithm.
     */
    public Map<JobVertexID, Decision> makeDecision(Configuration conf) {
        Map<JobVertexID, MemoryParallelismCurve> mpcs = this.evaluatedMetrics.getMemoryParallelismCurves();
        double writeBufferRatio = conf.get(WRITE_BUFFER_RATIO);
        double maxTaskMemoryMB =
                conf.get(AutoScalerOptions.A4S_MAX_TASK_MANAGED_MEMORY_MB)
                        - conf.get(AutoScalerOptions.A4S_MEMORY_BASE_MB);

        // Alg 1 starts every stateful operator at Pmin, raised to the first P whose task fits
        // one slot (thesis 3.2.3). Operators that fit at no P get no decision.
        Map<JobVertexID, MemoryParallelismCurve> feasible = new HashMap<>();
        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        mpcs.forEach(
                (operator, mpc) -> {
                    for (int p = mpc.getMinParallelism(); p <= mpc.getMaxParallelism(); p++) {
                        if (taskMemoryMB(mpc, p, writeBufferRatio) <= maxTaskMemoryMB) {
                            feasible.put(operator, mpc);
                            parallelismForVertex.put(operator, p);
                            return;
                        }
                    }
                    LOG.warn("A4S: Operator {} exceeds {}MB per task at every parallelism of {}", operator, maxTaskMemoryMB, mpc);
                });

        int maxAttempts = conf.get(AutoScalerOptions.A4S_MAX_ATTEMPTS);

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            LOG.info("A4S: Attempt {} - Making decisions", attempt);
            Optional<Map<JobVertexID, Decision>> decisions =
                    place(parallelismForVertex, feasible, maxTaskMemoryMB, writeBufferRatio);
            if (decisions.isPresent()) {
                LOG.info("A4S: Decisions: {}", decisions.get());
                return decisions.get();
            }

            LOG.info("A4S: No decision can be made, increasing parallelism");
            Optional<JobVertexID> operator = increaseParallelism(parallelismForVertex, feasible);
            if (operator.isPresent()) {
                int newParallelism = parallelismForVertex.get(operator.get()) + 1;
                parallelismForVertex.put(operator.get(), newParallelism);
                LOG.info("A4S: Increasing parallelism for operator {} to {}", operator.get(), newParallelism);
            } else {
                LOG.warn("A4S: No operator can increase parallelism");
                break;
            }
        }

        LOG.warn("A4S: No decision can be made for any operator");
        return Map.of();
    }

    @VisibleForTesting
    Optional<Map<JobVertexID, Decision>> place(
        Map<JobVertexID, Integer> parallelismForVertex,
        Map<JobVertexID, MemoryParallelismCurve> memoryParallelismCurves,
        double maxTaskMemoryMB,
        double writeBufferRatio) {
        Map<JobVertexID, Decision> decisions = new HashMap<>();

        for (JobVertexID operator : operators) {
            MemoryParallelismCurve mpc = memoryParallelismCurves.get(operator);
            if (mpc == null) {
                LOG.warn("A4S: No memory parallelism curve found for operator {}", operator);
                continue;
            }

            int parallelism = parallelismForVertex.get(operator);
            double memoryMB = taskMemoryMB(mpc, parallelism, writeBufferRatio);

            if (memoryMB > maxTaskMemoryMB) {
                return Optional.empty();
            }
            
            LOG.info("A4S: Operator {} placed with parallelism {} and memory {}MB", operator, parallelism, memoryMB);
            Decision decision = new Decision(parallelism, memoryMB);
            decisions.put(operator, decision);
        }

        return Optional.of(decisions);
    }

    /**
     * Managed memory one task needs at parallelism p: the operator's cache need split evenly
     * across p tasks (Alg 2), scaled up because only (1 - write-buffer-ratio) of a slot's managed
     * memory is left for data blocks once memtables fill the write buffer manager.
     */
    @VisibleForTesting
    static double taskMemoryMB(MemoryParallelismCurve mpc, int p, double writeBufferRatio) {
        double operatorMemoryMB =
                mpc.getMemoryMbForParallelism(p)
                        .orElseThrow(() -> new IllegalStateException("No memory for parallelism " + p + " in " + mpc));
        return operatorMemoryMB / p / (1 - writeBufferRatio);
    }

    /**
     * Find an operator whose memory needs decrease most when its parallelism is increased by 1.
     */
    @VisibleForTesting
    Optional<JobVertexID> increaseParallelism(
        Map<JobVertexID, Integer> parallelismForVertex,
        Map<JobVertexID, MemoryParallelismCurve> mpcs) {

        double minMemoryDiff = Double.MAX_VALUE;
        JobVertexID minMemoryDiffOperator = null;

        for (JobVertexID operator : operators) {
            MemoryParallelismCurve mpc = mpcs.get(operator);
            if (mpc == null) {
                continue;
            }
            int parallelism = parallelismForVertex.get(operator);

            if (parallelism + 1 > mpc.getMaxParallelism()) {
                continue;
            }
            double memoryDiff = mpc.getMemoryMbForParallelism(parallelism + 1).get() - 
                mpc.getMemoryMbForParallelism(parallelism).get();
            if (memoryDiff < minMemoryDiff) {
                minMemoryDiff = memoryDiff;
                minMemoryDiffOperator = operator;
            }
        }

        return Optional.ofNullable(minMemoryDiffOperator);
    }

}
