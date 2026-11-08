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

package org.apache.flink.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.resources.NoopResourceCheck;
import org.apache.flink.autoscaler.resources.ResourceCheck;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.tuning.MemoryTuning;
import org.apache.flink.autoscaler.utils.CalendarUtils;
import org.apache.flink.autoscaler.utils.ResourceCheckUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.*;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_EXECUTION_DISABLED_REASON;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.addToScalingHistoryAndStore;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.*;

/** Class responsible for executing scaling decisions. */
public class ScalingExecutor<KEY, Context extends JobAutoScalerContext<KEY>> {

    public static final String GC_PRESSURE_MESSAGE =
            "GC Pressure %s is above the allowed limit for scaling operations. Please adjust the available memory manually.";

    public static final String HEAP_USAGE_MESSAGE =
            "Heap Usage %s is above the allowed limit for scaling operations. Please adjust the available memory manually.";

    public static final String RESOURCE_QUOTA_REACHED_MESSAGE =
            "Resource usage is above the allowed limit for scaling operations. Please adjust the resource quota manually.";

    private static final Logger LOG = LoggerFactory.getLogger(ScalingExecutor.class);

    private final JobVertexScaler<KEY, Context> jobVertexScaler;
    private final AutoScalerEventHandler<KEY, Context> autoScalerEventHandler;
    private final AutoScalerStateStore<KEY, Context> autoScalerStateStore;
    private final ResourceCheck resourceCheck;

    private final ScalingConfigurations scalingConfigurations;

    private static final HashMap<JobID, Integer> periods = new HashMap<>();

    public ScalingExecutor(
            AutoScalerEventHandler<KEY, Context> autoScalerEventHandler,
            AutoScalerStateStore<KEY, Context> autoScalerStateStore) {
        this(autoScalerEventHandler, autoScalerStateStore, null);
    }

    public ScalingExecutor(
            AutoScalerEventHandler<KEY, Context> autoScalerEventHandler,
            AutoScalerStateStore<KEY, Context> autoScalerStateStore,
            @Nullable ResourceCheck resourceCheck) {
        this.jobVertexScaler = new JobVertexScaler<>(autoScalerEventHandler);
        this.autoScalerEventHandler = autoScalerEventHandler;
        this.autoScalerStateStore = autoScalerStateStore;
        this.resourceCheck = resourceCheck != null ? resourceCheck : new NoopResourceCheck();
        this.scalingConfigurations = new ScalingConfigurations();
    }

    public boolean scaleResource(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            ScalingTracking scalingTracking,
            Instant now,
            JobTopology jobTopology,
            DelayedScaleDown delayedScaleDown)
            throws Exception {
        var conf = context.getConfiguration();
        var restartTime = scalingTracking.getMaxRestartTimeOrDefault(conf);

        var scalingSummaries =
                computeScalingSummary(
                        context,
                        evaluatedMetrics,
                        scalingHistory,
                        restartTime,
                        jobTopology,
                        delayedScaleDown);

        if (scalingSummaries.isEmpty()) {
            LOG.info("All job vertices are currently running at their target parallelism.");
            return false;
        }

        updateRecommendedParallelism(evaluatedMetrics.getVertexMetrics(), scalingSummaries);

        if (checkIfBlockedAndTriggerScalingEvent(context, scalingSummaries, conf, now)) {
            return false;
        }

        var configOverrides =
                MemoryTuning.tuneTaskManagerMemory(
                        context,
                        evaluatedMetrics,
                        jobTopology,
                        scalingSummaries,
                        autoScalerEventHandler);

        var memoryTuningEnabled = conf.get(AutoScalerOptions.MEMORY_TUNING_ENABLED);
        if (scalingWouldExceedMaxResources(
                memoryTuningEnabled ? configOverrides.newConfigWithOverrides(conf) : conf,
                jobTopology,
                evaluatedMetrics,
                scalingSummaries,
                context)) {
            return false;
        }

        addToScalingHistoryAndStore(
                autoScalerStateStore, context, scalingHistory, now, scalingSummaries);

        scalingTracking.addScalingRecord(now, new ScalingRecord());
        autoScalerStateStore.storeScalingTracking(context, scalingTracking);

        autoScalerStateStore.storeParallelismOverrides(
                context,
                getVertexParallelismOverrides(
                        evaluatedMetrics.getVertexMetrics(), scalingSummaries));

        if (conf.get(JUSTIN_ENABLED)) {
            var currentPeriod = periods.getOrDefault(context.getJobID(), 0);
            var currentScalingConf =
                    scalingConfigurations.setCurrentConfiguration(
                            context.getJobID(),
                            evaluatedMetrics.getVertexMetrics(),
                            scalingSummaries,
                            currentPeriod);
            policy(context, currentScalingConf, conf);
            storeScalingConfigurationHistory(context, now, currentPeriod, currentScalingConf, conf);
            LOG.info(scalingConfigurations.toString());

            autoScalerStateStore.storeParallelismOverrides(
                    context,
                    getVertexParallelismOverrides(currentScalingConf));

            autoScalerStateStore.storeResourceProfileOverrides(
                    context,
                    getVertexResourceProfileOverrides(context.getJobID(), this.scalingConfigurations, conf));
        }

        autoScalerStateStore.storeConfigChanges(context, configOverrides);

        // Try to clear all delayed scale down requests after scaling.
        delayedScaleDown.clearAll();

        return true;
    }

    private void storeScalingConfigurationHistory(
            Context context,
            Instant now,
            int period,
            ScalingConfigurations.ScalingConfiguration scalingConfiguration,
            Configuration conf)
            throws Exception {
        SortedMap<Instant, ScalingConfigurationSnapshot> history =
                getTrimmedScalingConfigurationHistory(context, now, conf);
        history.put(now, ScalingConfigurationSnapshot.from(period, scalingConfiguration));
        autoScalerStateStore.storeScalingConfigurationHistory(context, history);
    }

    private SortedMap<Instant, ScalingConfigurationSnapshot> getTrimmedScalingConfigurationHistory(
            Context context, Instant now, Configuration conf) throws Exception {
        SortedMap<Instant, ScalingConfigurationSnapshot> history =
                new java.util.TreeMap<>(autoScalerStateStore.getScalingConfigurationHistory(context));
        Instant expectedStartTime = now.minus(conf.get(RICH_SCALING_CONFIGURATION_HISTORY_AGE));
        history = new java.util.TreeMap<>(history.tailMap(expectedStartTime));
        while (history.size() > conf.get(RICH_SCALING_CONFIGURATION_HISTORY_COUNT)) {
            history.remove(history.firstKey());
        }
        return history;
    }

    private void updateRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {
        scalingSummaries.forEach(
                (jobVertexID, scalingSummary) ->
                        evaluatedMetrics
                                .get(jobVertexID)
                                .put(
                                        ScalingMetric.RECOMMENDED_PARALLELISM,
                                        EvaluatedScalingMetric.of(
                                                scalingSummary.getNewParallelism())));
    }

    @VisibleForTesting
    static boolean allChangedVerticesWithinUtilizationTarget(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Set<JobVertexID> changedVertices) {
        // No vertices with changed parallelism.
        if (changedVertices.isEmpty()) {
            return true;
        }

        for (JobVertexID vertex : changedVertices) {
            var metrics = evaluatedMetrics.get(vertex);

            double trueProcessingRate = metrics.get(TRUE_PROCESSING_RATE).getAverage();
            double scaleUpRateThreshold = metrics.get(SCALE_UP_RATE_THRESHOLD).getCurrent();
            double scaleDownRateThreshold = metrics.get(SCALE_DOWN_RATE_THRESHOLD).getCurrent();

            if (trueProcessingRate < scaleUpRateThreshold
                    || trueProcessingRate > scaleDownRateThreshold) {
                LOG.debug(
                        "Vertex {} processing rate {} is outside ({}, {})",
                        vertex,
                        trueProcessingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
                return false;
            } else {
                LOG.debug(
                        "Vertex {} processing rate {} is within target ({}, {})",
                        vertex,
                        trueProcessingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
            }
        }
        LOG.info("All vertex processing rates are within target.");
        return true;
    }

    @VisibleForTesting
    Map<JobVertexID, ScalingSummary> computeScalingSummary(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            Duration restartTime,
            JobTopology jobTopology,
            DelayedScaleDown delayedScaleDown) {
        LOG.debug("Restart time used in scaling summary computation: {}", restartTime);

        if (isJobUnderMemoryPressure(context, evaluatedMetrics.getGlobalMetrics())) {
            LOG.info("Skipping vertex scaling due to memory pressure");
            return Map.of();
        }

        var out = new HashMap<JobVertexID, ScalingSummary>();

        var excludeVertexIdList =
                context.getConfiguration().get(AutoScalerOptions.VERTEX_EXCLUDE_IDS);
        evaluatedMetrics
                .getVertexMetrics()
                .forEach(
                        (v, metrics) -> {
                            if (excludeVertexIdList.contains(v.toHexString())) {
                                LOG.debug(
                                        "Vertex {} is part of `vertex.exclude.ids` config, Ignoring it for scaling",
                                        v);
                            } else {
                                var currentParallelism =
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent();

                                var parallelismChange =
                                        jobVertexScaler.computeScaleTargetParallelism(
                                                context,
                                                v,
                                                jobTopology.get(v).getInputs().values(),
                                                metrics,
                                                scalingHistory.getOrDefault(
                                                        v, Collections.emptySortedMap()),
                                                restartTime,
                                                delayedScaleDown);
                                if (parallelismChange.isNoChange()) {
                                    return;
                                }
                                out.put(
                                        v,
                                        new ScalingSummary(
                                                currentParallelism,
                                                parallelismChange.getNewParallelism(),
                                                metrics));
                            }
                        });

        // If the Utilization of all tasks is within range, we can skip scaling.
        if (allChangedVerticesWithinUtilizationTarget(
                evaluatedMetrics.getVertexMetrics(), out.keySet())) {
            return Map.of();
        }

        return out;
    }

    private boolean isJobUnderMemoryPressure(
            Context ctx, Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics) {

        var gcPressure = evaluatedMetrics.get(ScalingMetric.GC_PRESSURE).getCurrent();
        var conf = ctx.getConfiguration();
        if (gcPressure > conf.get(AutoScalerOptions.GC_PRESSURE_THRESHOLD)) {
            autoScalerEventHandler.handleEvent(
                    ctx,
                    AutoScalerEventHandler.Type.Normal,
                    "MemoryPressure",
                    String.format(GC_PRESSURE_MESSAGE, gcPressure),
                    "gcPressure",
                    conf.get(SCALING_EVENT_INTERVAL));
            return true;
        }

        var heapUsage = evaluatedMetrics.get(ScalingMetric.HEAP_MAX_USAGE_RATIO).getAverage();
        if (heapUsage > conf.get(AutoScalerOptions.HEAP_USAGE_THRESHOLD)) {
            autoScalerEventHandler.handleEvent(
                    ctx,
                    AutoScalerEventHandler.Type.Normal,
                    "MemoryPressure",
                    String.format(HEAP_USAGE_MESSAGE, heapUsage),
                    "heapUsage",
                    conf.get(SCALING_EVENT_INTERVAL));
            return true;
        }

        return false;
    }

    @VisibleForTesting
    protected boolean scalingWouldExceedMaxResources(
            Configuration tunedConfig,
            JobTopology jobTopology,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            Context ctx) {
        if (scalingWouldExceedClusterResources(
                tunedConfig, evaluatedMetrics, scalingSummaries, ctx)) {
            return true;
        }
        if (scalingWouldExceedResourceQuota(tunedConfig, jobTopology, scalingSummaries, ctx)) {
            autoScalerEventHandler.handleEvent(
                    ctx,
                    AutoScalerEventHandler.Type.Warning,
                    "ResourceQuotaReached",
                    RESOURCE_QUOTA_REACHED_MESSAGE,
                    null,
                    tunedConfig.get(SCALING_EVENT_INTERVAL));
            return true;
        }
        return false;
    }

    private boolean scalingWouldExceedClusterResources(
            Configuration tunedConfig,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            JobAutoScalerContext<?> ctx) {

        final double taskManagerCpu = ctx.getTaskManagerCpu().orElse(0.);
        final MemorySize taskManagerMemory = MemoryTuning.getTotalMemory(tunedConfig, ctx);

        if (taskManagerCpu <= 0 || taskManagerMemory.compareTo(MemorySize.ZERO) <= 0) {
            // We can't extract the requirements, we can't make any assumptions
            return false;
        }

        var globalMetrics = evaluatedMetrics.getGlobalMetrics();
        if (!globalMetrics.containsKey(ScalingMetric.NUM_TASK_SLOTS_USED)) {
            LOG.info("JM metrics not ready yet");
            return true;
        }

        int numTaskSlotsUsed =
                (int) globalMetrics.get(ScalingMetric.NUM_TASK_SLOTS_USED).getCurrent();
        final int numTaskSlotsAfterRescale =
                ResourceCheckUtils.estimateNumTaskSlotsAfterRescale(
                        evaluatedMetrics.getVertexMetrics(), scalingSummaries, numTaskSlotsUsed);

        int taskSlotsPerTm = tunedConfig.get(TaskManagerOptions.NUM_TASK_SLOTS);

        int currentNumTms = (int) Math.ceil(numTaskSlotsUsed / (double) taskSlotsPerTm);
        int newNumTms = (int) Math.ceil(numTaskSlotsAfterRescale / (double) taskSlotsPerTm);

        return !resourceCheck.trySchedule(
                currentNumTms, newNumTms, taskManagerCpu, taskManagerMemory);
    }

    protected static boolean scalingWouldExceedResourceQuota(
            Configuration tunedConfig,
            JobTopology jobTopology,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            JobAutoScalerContext<?> ctx) {

        if (jobTopology == null || jobTopology.getSlotSharingGroupMapping().isEmpty()) {
            return false;
        }

        var cpuQuota = tunedConfig.getOptional(AutoScalerOptions.CPU_QUOTA);
        var memoryQuota = tunedConfig.getOptional(AutoScalerOptions.MEMORY_QUOTA);
        var tmMemory = MemoryTuning.getTotalMemory(tunedConfig, ctx);
        var tmCpu = ctx.getTaskManagerCpu().orElse(0.);

        if (cpuQuota.isPresent() || memoryQuota.isPresent()) {
            var currentSlotSharingGroupMaxParallelisms = new HashMap<SlotSharingGroupId, Integer>();
            var newSlotSharingGroupMaxParallelisms = new HashMap<SlotSharingGroupId, Integer>();
            for (var e : jobTopology.getSlotSharingGroupMapping().entrySet()) {
                int currentMaxParallelism =
                        e.getValue().stream()
                                .filter(scalingSummaries::containsKey)
                                .mapToInt(v -> scalingSummaries.get(v).getCurrentParallelism())
                                .max()
                                .orElse(0);
                currentSlotSharingGroupMaxParallelisms.put(e.getKey(), currentMaxParallelism);
                int newMaxParallelism =
                        e.getValue().stream()
                                .filter(scalingSummaries::containsKey)
                                .mapToInt(v -> scalingSummaries.get(v).getNewParallelism())
                                .max()
                                .orElse(0);
                newSlotSharingGroupMaxParallelisms.put(e.getKey(), newMaxParallelism);
            }

            var numSlotsPerTm = tunedConfig.get(TaskManagerOptions.NUM_TASK_SLOTS);
            var currentTotalSlots =
                    currentSlotSharingGroupMaxParallelisms.values().stream()
                            .mapToInt(Integer::intValue)
                            .sum();
            var currentNumTms = currentTotalSlots / numSlotsPerTm;
            var newTotalSlots =
                    newSlotSharingGroupMaxParallelisms.values().stream()
                            .mapToInt(Integer::intValue)
                            .sum();
            var newNumTms = newTotalSlots / numSlotsPerTm;

            if (newNumTms <= currentNumTms) {
                LOG.debug(
                        "Skipping quota check due to new resource allocation is less or equals than the current");
                return false;
            }

            if (cpuQuota.isPresent()) {
                LOG.debug("CPU resource quota is {}, checking limits", cpuQuota.get());
                double totalCPU = tmCpu * newNumTms;
                if (totalCPU > cpuQuota.get()) {
                    LOG.info("CPU resource quota reached with value: {}", totalCPU);
                    return true;
                }
            }

            if (memoryQuota.isPresent()) {
                LOG.debug("Memory resource quota is {}, checking limits", memoryQuota.get());
                long totalMemory = tmMemory.getBytes() * newNumTms;
                if (totalMemory > memoryQuota.get().getBytes()) {
                    LOG.info(
                            "Memory resource quota reached with value: {}",
                            new MemorySize(totalMemory));
                    return true;
                }
            }
        }

        return false;
    }

    private static Map<String, String> getVertexParallelismOverrides(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries) {
        var overrides = new HashMap<String, String>();
        evaluatedMetrics.forEach(
                (id, metrics) -> {
                    if (summaries.containsKey(id)) {
                        overrides.put(
                                id.toString(),
                                String.valueOf(summaries.get(id).getNewParallelism()));
                    } else {
                        overrides.put(
                                id.toString(),
                                String.valueOf(
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent()));
                    }
                });
        return overrides;
    }

    private static Map<String, String> getVertexParallelismOverrides(
            ScalingConfigurations.ScalingConfiguration scalingConfiguration) {
        var overrides = new HashMap<String, String>();
        scalingConfiguration.getScaling().forEach((
                (jobVertexID, scalingInformation) -> {
                    overrides.put(
                            jobVertexID.toString(),
                            String.valueOf(scalingInformation.getParallelism()));
        }));

        return overrides;
    }

    @VisibleForTesting
    public static Map<String, String> getVertexResourceProfileOverrides(JobID jobID, ScalingConfigurations scalingConfigurations, Configuration conf) {
        var overrides = new HashMap<String, String>();
        scalingConfigurations.getCurrentConfiguration(jobID, periods.getOrDefault(jobID, 0)).getScaling().forEach((id, information) -> {
            overrides.put(
                    id.toString(),
                    String.valueOf(getResourceProfile(conf, information.getMemoryLevel()))
            );
        } );
        return overrides;
    }


    private boolean checkIfBlockedAndTriggerScalingEvent(
            Context context,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            Configuration conf,
            Instant now) {
        var scaleEnabled = conf.get(SCALING_ENABLED);
        var isExcluded = CalendarUtils.inExcludedPeriods(conf, now);
        String message;
        if (!scaleEnabled) {
            message =
                    SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED
                            + String.format(
                                    SCALING_EXECUTION_DISABLED_REASON,
                                    SCALING_ENABLED.key(),
                                    false);
        } else if (isExcluded) {
            message =
                    SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED
                            + String.format(
                                    SCALING_EXECUTION_DISABLED_REASON,
                                    EXCLUDED_PERIODS.key(),
                                    conf.get(EXCLUDED_PERIODS));
        } else {
            message = SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED;
        }
        autoScalerEventHandler.handleScalingEvent(
                context, scalingSummaries, message, conf.get(SCALING_EVENT_INTERVAL));

        return !scaleEnabled || isExcluded;
    }

    private static ResourceProfile getResourceProfile(Configuration conf, int memoryLevel) {
        var memory = conf.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
        if (memory.getGibiBytes() > 3) { // 4 GB
            return ResourceProfile.newBuilder()
                    .setCpuCores(1.0)
                    .setTaskHeapMemoryMB(363)
                    .setTaskOffHeapMemoryMB(0)
                    .setManagedMemoryMB(memoryLevel == -1 ? 0 : (int) (343 * Math.pow(2, memoryLevel)))
                    .setNetworkMemoryMB(84)
                    .build();
        } else {
            return  ResourceProfile.newBuilder()
                    .setCpuCores(1.0)
                    .setTaskHeapMemoryMB(134)
                    .setTaskOffHeapMemoryMB(0)
                    .setManagedMemoryMB(memoryLevel == -1 ? 0 : (int) (158 * Math.pow(2, memoryLevel)))
                    .setNetworkMemoryMB(39)
                    .build();
        }
    }

    private void policy(Context context, ScalingConfigurations.ScalingConfiguration scaling, Configuration conf) {
        LOG.info("Justin: policy() invoked. MAX_MEMORY_LEVEL={}, MIN_CACHE_HIT_RATE_THRESHOLD={}, STATE_ACCESS_LATENCY_THRESHOLD={}",
                conf.get(MAX_MEMORY_LEVEL), conf.get(MIN_CACHE_HIT_RATE_THRESHOLD), conf.get(STATE_ACCESS_LATENCY_THRESHOLD));
        scaling.getScaling().forEach((id, information) -> {
            var previousInformation =
                    scalingConfigurations.getPreviousScalingInformation(
                            context.getJobID(),
                            id,
                            periods.getOrDefault(context.getJobID(), 0));
            if (information.getAvgCacheHitRate() == 0.0) { // Stateless
                information.setMemoryLevel(-1);
            } else {
                if (previousInformation != null) {
                    if ( previousInformation.getParallelism() != information.getParallelism()) {
                        if (previousInformation.isVerticalScaling()) { // Previous decision was elastic scaling
                            LOG.info("Justin: Previous decision was elastic scaling");
                            if (((information.getAvgCacheHitRate() - previousInformation.getAvgCacheHitRate() > conf.get(IMPROVED_CACHE_HIT_RATE_THRESHOLD))
                                    || information.getAvgStateLatency() < previousInformation.getAvgStateLatency()) ) { // We see an improvement from vertical scaling

                                LOG.info("Justin: We see an improvement from vertical scaling -> {} {}, {} {}"
                                        , information.getAvgCacheHitRate()
                                        , previousInformation.getAvgCacheHitRate()
                                        , information.getAvgStateLatency()
                                        , previousInformation.getAvgStateLatency());
                                if (information.getAvgThroughput() > previousInformation.getAvgThroughput() * (1.0 + conf.get(MIN_IMPROVED_THROUGHPUT))
                                    && information.getAvgCacheHitRate() < conf.get(MAX_CACHE_HIT_RATE_THRESHOLD)) { // Improved Tput and cache hit ratio below max thold
                                    LOG.info("Justin: Improved Tput and cache hit ratio below max thold -> {} > {} "
                                            , information.getAvgThroughput()
                                            , previousInformation.getAvgThroughput() * (1.0 + conf.get(MIN_IMPROVED_THROUGHPUT)));
                                    if (previousInformation.getMemoryLevel()+1 < conf.get(MAX_MEMORY_LEVEL)) { // Can scale up
                                        LOG.info("Justin: Can scale up vertically, memLevel {} -> {}", previousInformation.getMemoryLevel(), previousInformation.getMemoryLevel()+1);
                                        information.setParallelism(previousInformation.getParallelism());
                                        information.setMemoryLevel(previousInformation.getMemoryLevel()+1);
                                        information.setVerticalScaling(true);
                                    } else { // Max level
                                        information.setMemoryLevel(previousInformation.getMemoryLevel());
                                        information.setHorizontalScaling(true);
                                    }

                                } else { // Improvement not enough to increase tput
                                    LOG.info("Justin: Improvement not enough to increase tput");
                                    information.setMemoryLevel(previousInformation.getMemoryLevel());
                                    information.setHorizontalScaling(true);
                                }
                            } else { // We see no an improvement from vertical scaling, rollback
                                LOG.info("Justin: We see no an improvement from vertical scaling, rollback");
                                information.setMemoryLevel(previousInformation.getMemoryLevel()-1);
                                information.setStopVerticalScaling(true);
                                information.setHorizontalScaling(true);
                            }
                        } else {
                            LOG.info("Justin: No previous vertical decision");
                            if (((information.getAvgCacheHitRate() < conf.get(MIN_CACHE_HIT_RATE_THRESHOLD))
                                    || information.getAvgStateLatency() > conf.get(STATE_ACCESS_LATENCY_THRESHOLD))
                              && previousInformation.getMemoryLevel()+1 < conf.get(MAX_MEMORY_LEVEL)) {
                                LOG.info("Justin: indicators show that we should scale up");
                                information.setParallelism(previousInformation.getParallelism());
                                information.setMemoryLevel(previousInformation.getMemoryLevel()+1);
                                information.setVerticalScaling(true);

                            } else {
                                information.setHorizontalScaling(true);
                                information.setMemoryLevel(previousInformation.getMemoryLevel());
                            }
                        }
                    }
                } else { // First decision
                    LOG.info("Making first scaling decision for job {}", context.getJobID());
                    if (information.getParallelism() != 1) { // Scale out decision
                        if (information.getAvgCacheHitRate() < conf.get(MIN_CACHE_HIT_RATE_THRESHOLD)
                                || information.getAvgStateLatency() > conf.get(STATE_ACCESS_LATENCY_THRESHOLD)) {
                            information.setParallelism(1);
                            information.setMemoryLevel(1);
                            information.setVerticalScaling(true);
                        } else {
                            information.setHorizontalScaling(true);
                        }
                    }
                }
            }
        });
    }

    public static void scalingTriggered(JobID jobID) {
        var newPeriod = periods.getOrDefault(jobID, 0) + 1;
        periods.put(jobID, newPeriod);
        LOG.info("Incrementing scaling period for job {}. New period: {}",
                jobID,
                newPeriod);
    }
}
