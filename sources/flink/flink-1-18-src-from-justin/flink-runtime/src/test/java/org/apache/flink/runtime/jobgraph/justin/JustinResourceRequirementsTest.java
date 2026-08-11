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

package org.apache.flink.runtime.jobgraph.justin;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests persistence of Justin resource requirements in a job graph. */
class JustinResourceRequirementsTest {

    @Test
    void testWriteToJobGraphAndReadFromJobGraph() throws Exception {
        JobVertexID vertexId = new JobVertexID();
        JustinResourceRequirements requirements =
                JustinResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(
                                vertexId,
                                1,
                                4,
                                ResourceProfile.newBuilder().setCpuCores(2.0).build())
                        .build();
        JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();

        JustinResourceRequirements.writeToJobGraph(jobGraph, requirements);

        assertThat(JustinResourceRequirements.readFromJobGraph(jobGraph))
                .contains(requirements);
    }

    @Test
    void testResourceProfileParticipatesInEquality() {
        JustinVertexResourceRequirements.Parallelism parallelism =
                new JustinVertexResourceRequirements.Parallelism(1, 4);
        JustinVertexResourceRequirements first =
                new JustinVertexResourceRequirements(
                        parallelism,
                        ResourceProfile.newBuilder().setCpuCores(1.0).build());
        JustinVertexResourceRequirements second =
                new JustinVertexResourceRequirements(
                        parallelism,
                        ResourceProfile.newBuilder().setCpuCores(2.0).build());

        assertThat(first).isNotEqualTo(second);
    }
}
