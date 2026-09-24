/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link JustinSlotAssigner}. */
class JustinSlotAssignerTest {

    private static final ResourceProfile SOURCE_PROFILE = resourceProfile(0);
    private static final ResourceProfile RANK_PROFILE = resourceProfile(2);
    private static final ResourceProfile JOIN_PROFILE = resourceProfile(3);

    @Test
    void testReturnsNoAssignmentsWhenOneResourceProfileIsMissing() {
        final TestJob job = createQ9ShapedJob();
        final List<SlotInfo> slots = new ArrayList<>();
        slots.addAll(createSlots(13, SOURCE_PROFILE));
        slots.addAll(createSlots(2, RANK_PROFILE));

        final Collection<SlotAssignment> assignments =
                new JustinSlotAssigner()
                        .assignSlots(
                                job.jobInformation,
                                slots,
                                job.vertexParallelism,
                                JobAllocationsInformation.empty());

        assertThat(assignments).isEmpty();
    }

    @Test
    void testAssignsEveryExecutionVertexWhenAllResourceProfilesAreAvailable() {
        final TestJob job = createQ9ShapedJob();
        final List<SlotInfo> slots = new ArrayList<>();
        slots.addAll(createSlots(13, SOURCE_PROFILE));
        slots.addAll(createSlots(2, RANK_PROFILE));
        slots.addAll(createSlots(6, JOIN_PROFILE));

        final Collection<SlotAssignment> assignments =
                new JustinSlotAssigner()
                        .assignSlots(
                                job.jobInformation,
                                slots,
                                job.vertexParallelism,
                                JobAllocationsInformation.empty());

        final Set<ExecutionVertexID> assignedVertices = new HashSet<>();
        for (SlotAssignment assignment : assignments) {
            assignedVertices.addAll(
                    assignment
                            .getTargetAs(ExecutionSlotSharingGroup.class)
                            .getContainedExecutionVertices());
        }

        assertThat(assignments).hasSize(21);
        assertThat(assignedVertices).hasSize(21);
    }

    private static TestJob createQ9ShapedJob() {
        final JobInformation.VertexInformation auctionSource = createVertex(12, SOURCE_PROFILE);
        final JobInformation.VertexInformation bidSource = createVertex(1, SOURCE_PROFILE);
        final JobInformation.VertexInformation rank = createVertex(2, RANK_PROFILE);
        final JobInformation.VertexInformation join = createVertex(6, JOIN_PROFILE);
        final List<JobInformation.VertexInformation> vertices =
                Arrays.asList(auctionSource, bidSource, rank, join);
        final Map<JobVertexID, Integer> parallelism = new HashMap<>();
        for (JobInformation.VertexInformation vertex : vertices) {
            parallelism.put(vertex.getJobVertexID(), vertex.getParallelism());
        }
        return new TestJob(new TestJobInformation(vertices), new VertexParallelism(parallelism));
    }

    private static JobInformation.VertexInformation createVertex(
            int parallelism, ResourceProfile resourceProfile) {
        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        slotSharingGroup.setResourceProfile(resourceProfile);
        return new TestVertexInformation(new JobVertexID(), parallelism, slotSharingGroup);
    }

    private static List<SlotInfo> createSlots(int count, ResourceProfile resourceProfile) {
        final List<SlotInfo> slots = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            slots.add(new TestSlotInfo(resourceProfile));
        }
        return slots;
    }

    private static ResourceProfile resourceProfile(long managedMemoryMebiBytes) {
        return ResourceProfile.newBuilder()
                .setCpuCores(1.0)
                .setManagedMemory(MemorySize.ofMebiBytes(managedMemoryMebiBytes))
                .build();
    }

    private static final class TestJob {
        private final JobInformation jobInformation;
        private final VertexParallelism vertexParallelism;

        private TestJob(JobInformation jobInformation, VertexParallelism vertexParallelism) {
            this.jobInformation = jobInformation;
            this.vertexParallelism = vertexParallelism;
        }
    }
}
