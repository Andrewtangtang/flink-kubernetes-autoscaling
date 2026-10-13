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

package org.apache.flink.autoscaler.metrics;

import org.apache.flink.autoscaler.a4s.MemoryParallelismCurve;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/** Collected scaling metrics. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EvaluatedMetrics {
    private Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics;
    private Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics;
    private Map<JobVertexID, MemoryParallelismCurve> memoryParallelismCurves;
    private Map<JobVertexID, MissRateCurve> missRateCurves;

    public EvaluatedMetrics(
        Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics, 
        Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics) {
        this(vertexMetrics, globalMetrics, null, null);
    }

    public EvaluatedMetrics(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics,
            Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics,
            Map<JobVertexID, MemoryParallelismCurve> memoryParallelismCurves) {
        this(vertexMetrics, globalMetrics, memoryParallelismCurves, null);
    }
}
