/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/** Persisted Justin scaling snapshot for a single autoscaler decision point. */
@Data
@NoArgsConstructor
public class ScalingConfigurationSnapshot {

    private int period;

    private Map<JobVertexID, VertexScalingSnapshot> scaling = new HashMap<>();

    public ScalingConfigurationSnapshot(
            int period, Map<JobVertexID, VertexScalingSnapshot> scaling) {
        this.period = period;
        this.scaling = scaling;
    }

    public static ScalingConfigurationSnapshot from(
            int period, ScalingConfigurations.ScalingConfiguration scalingConfiguration) {
        var snapshot = new ScalingConfigurationSnapshot();
        snapshot.setPeriod(period);
        scalingConfiguration
                .getScaling()
                .forEach(
                        (jobVertexID, scalingInformation) ->
                                snapshot.scaling.put(
                                        jobVertexID,
                                        VertexScalingSnapshot.from(scalingInformation)));
        return snapshot;
    }

    /** Persisted Justin scaling information for a single vertex. */
    @Data
    @NoArgsConstructor
    public static class VertexScalingSnapshot {

        private int parallelism;
        private int memoryLevel;
        private boolean verticalScaling;
        private boolean horizontalScaling;
        private boolean stopVerticalScaling;
        private double avgCacheHitRate;
        private double avgStateLatency;
        private double avgThroughput;

        public static VertexScalingSnapshot from(
                ScalingConfigurations.ScalingInformation scalingInformation) {
            var snapshot = new VertexScalingSnapshot();
            snapshot.setParallelism(scalingInformation.getParallelism());
            snapshot.setMemoryLevel(scalingInformation.getMemoryLevel());
            snapshot.setVerticalScaling(scalingInformation.isVerticalScaling());
            snapshot.setHorizontalScaling(scalingInformation.isHorizontalScaling());
            snapshot.setStopVerticalScaling(scalingInformation.isStopVerticalScaling());
            snapshot.setAvgCacheHitRate(scalingInformation.getAvgCacheHitRate());
            snapshot.setAvgStateLatency(scalingInformation.getAvgStateLatency());
            snapshot.setAvgThroughput(scalingInformation.getAvgThroughput());
            return snapshot;
        }
    }
}
