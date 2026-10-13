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
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A4S Scaling Policy implementation.
 */
public class A4S {

    private static final Logger LOG = LoggerFactory.getLogger(A4S.class);

    private final EvaluatedMetrics evaluatedMetrics;

    private final List<JobVertexID> operators;

    private final Map<JobVertexID, ScalingSummary> scalingSummaries;

    public A4S(
        JobTopology jobTopology, 
        EvaluatedMetrics evaluatedMetrics, 
        Map<JobVertexID, ScalingSummary> scalingSummaries) {
        this.evaluatedMetrics = evaluatedMetrics;
        this.operators = jobTopology.getVerticesInTopologicalOrder();
        this.scalingSummaries = scalingSummaries;
    }

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
        Map<JobVertexID, Integer> parallelismForVertex = operators.
            stream().collect(Collectors.toMap(
                operator -> operator,
                operator -> 
                    Optional.ofNullable(scalingSummaries.get(operator)).
                        map(ScalingSummary::getNewParallelism).orElseGet(() -> {
                        return (int) evaluatedMetrics.
                            getVertexMetrics().
                            get(operator).
                            get(ScalingMetric.PARALLELISM).
                            getCurrent();
                    })
        ));
        
        
        double maxManagedMemoryMB = 600 - conf.get(AutoScalerOptions.A4S_MEMORY_BASE_MB);
        int maxAttempts = conf.get(AutoScalerOptions.A4S_MAX_ATTEMPTS);

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            LOG.info("A4S: Attempt {} - Making decisions", attempt);
            Optional<Map<JobVertexID, Decision>> decisions = place(parallelismForVertex, mpcs, maxManagedMemoryMB);
            if (decisions.isPresent()) {
                LOG.info("A4S: Decisions: {}", decisions.get());
                return decisions.get();
            }

            LOG.info("A4S: No decision can be made, increasing parallelism");
            Optional<JobVertexID> operator = increaseParallelism(parallelismForVertex, mpcs);
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
        double maxManagedMemoryMB) {
        Map<JobVertexID, Decision> decisions = new HashMap<>();

        for (JobVertexID operator : operators) {
            MemoryParallelismCurve mpc = memoryParallelismCurves.get(operator);
            if (mpc == null) {
                LOG.warn("A4S: No memory parallelism curve found for operator {}", operator);
                continue;
            }

            int parallelism = Math.max(parallelismForVertex.get(operator), mpc.getMinParallelism());
            double memoryMB = mpc.getMemoryMbForParallelism(parallelism).orElseThrow(() -> new IllegalStateException("No memory for parallelism " + parallelism + " found for operator " + operator));

            if (memoryMB > maxManagedMemoryMB) {
                return Optional.empty();
            }
            
            LOG.info("A4S: Operator {} placed with parallelism {} and memory {}MB", operator, parallelism, memoryMB);
            Decision decision = new Decision(parallelism, memoryMB);
            decisions.put(operator, decision);
        }

        return Optional.of(decisions);
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
            int parallelism = parallelismForVertex.get(operator);
            MemoryParallelismCurve mpc = mpcs.get(operator);

            if (mpc == null) {
                continue;
            }

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
