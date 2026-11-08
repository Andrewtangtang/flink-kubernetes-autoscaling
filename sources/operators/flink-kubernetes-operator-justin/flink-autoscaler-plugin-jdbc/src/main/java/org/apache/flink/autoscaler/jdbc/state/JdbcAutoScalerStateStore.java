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

package org.apache.flink.autoscaler.jdbc.state;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.DelayedScaleDown;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingConfigurationSnapshot;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.autoscaler.utils.AutoScalerSerDeModule;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JacksonException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.flink.autoscaler.jdbc.state.StateType.COLLECTED_METRICS;
import static org.apache.flink.autoscaler.jdbc.state.StateType.CONFIG_OVERRIDES;
import static org.apache.flink.autoscaler.jdbc.state.StateType.DELAYED_SCALE_DOWN;
import static org.apache.flink.autoscaler.jdbc.state.StateType.PARALLELISM_OVERRIDES;
import static org.apache.flink.autoscaler.jdbc.state.StateType.RESOURCE_PROFILE_OVERRIDES;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_CONFIGURATION_HISTORY;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_HISTORY;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_TRACKING;

/**
 * The state store which persists its state in JDBC related database.
 *
 * @param <KEY> The job key.
 * @param <Context> The job autoscaler context.
 */
@Experimental
public class JdbcAutoScalerStateStore<KEY, Context extends JobAutoScalerContext<KEY>>
        implements AutoScalerStateStore<KEY, Context> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcAutoScalerStateStore.class);

    private final JdbcStateStore jdbcStateStore;

    protected static final ObjectMapper YAML_MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .registerModule(new AutoScalerSerDeModule());

    public JdbcAutoScalerStateStore(JdbcStateStore jdbcStateStore) {
        this.jdbcStateStore = jdbcStateStore;
    }

    @Override
    public void storeScalingHistory(
            Context jobContext, Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory)
            throws Exception {
        jdbcStateStore.putSerializedState(
                getSerializeKey(jobContext),
                SCALING_HISTORY,
                serializeScalingHistory(scalingHistory));
    }

    @Nonnull
    @Override
    public Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getScalingHistory(
            Context jobContext) {
        Optional<String> serializedScalingHistory =
                jdbcStateStore.getSerializedState(getSerializeKey(jobContext), SCALING_HISTORY);
        if (serializedScalingHistory.isEmpty()) {
            return new HashMap<>();
        }
        try {
            return deserializeScalingHistory(serializedScalingHistory.get());
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize scaling history, possibly the format changed. Discarding...",
                    e);
            jdbcStateStore.removeSerializedState(getSerializeKey(jobContext), SCALING_HISTORY);
            return new HashMap<>();
        }
    }

    @Override
    public void storeScalingConfigurationHistory(
            Context jobContext,
            SortedMap<Instant, ScalingConfigurationSnapshot> scalingConfigurationHistory)
            throws Exception {
        jdbcStateStore.putSerializedState(
                getSerializeKey(jobContext),
                SCALING_CONFIGURATION_HISTORY,
                serializeScalingConfigurationHistory(scalingConfigurationHistory));
    }

    @Nonnull
    @Override
    public SortedMap<Instant, ScalingConfigurationSnapshot> getScalingConfigurationHistory(
            Context jobContext) {
        Optional<String> serializedScalingConfigurationHistory =
                jdbcStateStore.getSerializedState(
                        getSerializeKey(jobContext), SCALING_CONFIGURATION_HISTORY);
        if (serializedScalingConfigurationHistory.isEmpty()) {
            return new TreeMap<>();
        }
        try {
            return deserializeScalingConfigurationHistory(
                    serializedScalingConfigurationHistory.get());
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize scaling configuration history, possibly the format changed. Discarding...",
                    e);
            jdbcStateStore.removeSerializedState(
                    getSerializeKey(jobContext), SCALING_CONFIGURATION_HISTORY);
            return new TreeMap<>();
        }
    }

    @Override
    public void removeScalingConfigurationHistory(Context jobContext) {
        jdbcStateStore.removeSerializedState(
                getSerializeKey(jobContext), SCALING_CONFIGURATION_HISTORY);
    }

    @Override
    public void removeScalingHistory(Context jobContext) {
        jdbcStateStore.removeSerializedState(getSerializeKey(jobContext), SCALING_HISTORY);
    }

    @Override
    public void storeScalingTracking(Context jobContext, ScalingTracking scalingTrack)
            throws Exception {
        jdbcStateStore.putSerializedState(
                getSerializeKey(jobContext),
                SCALING_TRACKING,
                serializeScalingTracking(scalingTrack));
    }

    @Override
    public ScalingTracking getScalingTracking(Context jobContext) {
        Optional<String> serializedRescalingHistory =
                jdbcStateStore.getSerializedState(getSerializeKey(jobContext), SCALING_TRACKING);
        if (serializedRescalingHistory.isEmpty()) {
            return new ScalingTracking();
        }
        try {
            return deserializeScalingTracking(serializedRescalingHistory.get());
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize rescaling history, possibly the format changed. Discarding...",
                    e);
            jdbcStateStore.removeSerializedState(getSerializeKey(jobContext), SCALING_TRACKING);
            return new ScalingTracking();
        }
    }

    @Override
    public void storeCollectedMetrics(
            Context jobContext, SortedMap<Instant, CollectedMetrics> metrics) throws Exception {
        jdbcStateStore.putSerializedState(
                getSerializeKey(jobContext), COLLECTED_METRICS, serializeEvaluatedMetrics(metrics));
    }

    @Nonnull
    @Override
    public SortedMap<Instant, CollectedMetrics> getCollectedMetrics(Context jobContext) {
        Optional<String> serializedEvaluatedMetricsOpt =
                jdbcStateStore.getSerializedState(getSerializeKey(jobContext), COLLECTED_METRICS);
        if (serializedEvaluatedMetricsOpt.isEmpty()) {
            return new TreeMap<>();
        }
        try {
            return deserializeEvaluatedMetrics(serializedEvaluatedMetricsOpt.get());
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize metric history, possibly the format changed. Discarding...",
                    e);
            jdbcStateStore.removeSerializedState(getSerializeKey(jobContext), COLLECTED_METRICS);
            return new TreeMap<>();
        }
    }

    @Override
    public void removeCollectedMetrics(Context jobContext) {
        jdbcStateStore.removeSerializedState(getSerializeKey(jobContext), COLLECTED_METRICS);
    }

    @Override
    public void storeParallelismOverrides(
            Context jobContext, Map<String, String> parallelismOverrides) {
        jdbcStateStore.putSerializedState(
                getSerializeKey(jobContext),
                PARALLELISM_OVERRIDES,
                serializeParallelismOverrides(parallelismOverrides));
    }

    @Nonnull
    @Override
    public Map<String, String> getParallelismOverrides(Context jobContext) {
        return jdbcStateStore
                .getSerializedState(getSerializeKey(jobContext), PARALLELISM_OVERRIDES)
                .map(JdbcAutoScalerStateStore::deserializeParallelismOverrides)
                .orElse(new HashMap<>());
    }

    @Override
    public void removeParallelismOverrides(Context jobContext) {
        jdbcStateStore.removeSerializedState(getSerializeKey(jobContext), PARALLELISM_OVERRIDES);
    }

    @Override
    public void storeResourceProfileOverrides(
            Context jobContext, Map<String, String> resourceProfileOverrides) {
        jdbcStateStore.putSerializedState(
                getSerializeKey(jobContext),
                RESOURCE_PROFILE_OVERRIDES,
                serializeParallelismOverrides(resourceProfileOverrides));
    }

    @Nonnull
    @Override
    public Map<String, String> getResourceProfileOverrides(Context jobContext) {
        return jdbcStateStore
                .getSerializedState(getSerializeKey(jobContext), RESOURCE_PROFILE_OVERRIDES)
                .map(JdbcAutoScalerStateStore::deserializeParallelismOverrides)
                .orElse(new HashMap<>());
    }

    @Override
    public void removeResourceProfileOverrides(Context jobContext) {
        jdbcStateStore.removeSerializedState(
                getSerializeKey(jobContext), RESOURCE_PROFILE_OVERRIDES);
    }

    @Override
    public void storeConfigChanges(Context jobContext, ConfigChanges configChanges) {
        jdbcStateStore.putSerializedState(
                getSerializeKey(jobContext),
                CONFIG_OVERRIDES,
                serializeConfigOverrides(configChanges));
    }

    @Nonnull
    @Override
    public ConfigChanges getConfigChanges(Context jobContext) {
        return jdbcStateStore
                .getSerializedState(getSerializeKey(jobContext), CONFIG_OVERRIDES)
                .map(JdbcAutoScalerStateStore::deserializeConfigOverrides)
                .orElse(new ConfigChanges());
    }

    @Override
    public void removeConfigChanges(Context jobContext) {
        jdbcStateStore.removeSerializedState(getSerializeKey(jobContext), CONFIG_OVERRIDES);
    }

    @Override
    public void storeDelayedScaleDown(Context jobContext, DelayedScaleDown delayedScaleDown)
            throws Exception {
        jdbcStateStore.putSerializedState(
                getSerializeKey(jobContext),
                DELAYED_SCALE_DOWN,
                serializeDelayedScaleDown(delayedScaleDown));
    }

    @Nonnull
    @Override
    public DelayedScaleDown getDelayedScaleDown(Context jobContext) {
        Optional<String> delayedScaleDown =
                jdbcStateStore.getSerializedState(getSerializeKey(jobContext), DELAYED_SCALE_DOWN);
        if (delayedScaleDown.isEmpty()) {
            return new DelayedScaleDown();
        }

        try {
            return deserializeDelayedScaleDown(delayedScaleDown.get());
        } catch (JacksonException e) {
            LOG.warn(
                    "Could not deserialize delayed scale down, possibly the format changed. Discarding...",
                    e);
            jdbcStateStore.removeSerializedState(getSerializeKey(jobContext), DELAYED_SCALE_DOWN);
            return new DelayedScaleDown();
        }
    }

    @Override
    public void clearAll(Context jobContext) {
        jdbcStateStore.clearAll(getSerializeKey(jobContext));
    }

    @Override
    public void flush(Context jobContext) throws Exception {
        jdbcStateStore.flush(getSerializeKey(jobContext));
    }

    @Override
    public void removeInfoFromCache(KEY jobKey) {
        jdbcStateStore.removeInfoFromCache(getSerializeKey(jobKey));
    }

    private String getSerializeKey(Context jobContext) {
        return getSerializeKey(jobContext.getJobKey());
    }

    private String getSerializeKey(KEY jobKey) {
        return jobKey.toString();
    }

    // The serialization and deserialization are similar to KubernetesAutoScalerStateStore
    protected static String serializeScalingHistory(
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) throws Exception {
        return YAML_MAPPER.writeValueAsString(scalingHistory);
    }

    protected static String serializeScalingConfigurationHistory(
            SortedMap<Instant, ScalingConfigurationSnapshot> scalingConfigurationHistory)
            throws Exception {
        return YAML_MAPPER.writeValueAsString(scalingConfigurationHistory);
    }

    private static Map<JobVertexID, SortedMap<Instant, ScalingSummary>> deserializeScalingHistory(
            String scalingHistory) throws JacksonException {
        return YAML_MAPPER.readValue(scalingHistory, new TypeReference<>() {});
    }

    private static SortedMap<Instant, ScalingConfigurationSnapshot>
            deserializeScalingConfigurationHistory(String scalingConfigurationHistory)
                    throws JacksonException {
        return YAML_MAPPER.readValue(scalingConfigurationHistory, new TypeReference<>() {});
    }

    protected static String serializeScalingTracking(ScalingTracking scalingTracking)
            throws Exception {
        return YAML_MAPPER.writeValueAsString(scalingTracking);
    }

    private static ScalingTracking deserializeScalingTracking(String scalingTracking)
            throws JacksonException {
        return YAML_MAPPER.readValue(scalingTracking, new TypeReference<>() {});
    }

    @VisibleForTesting
    protected static String serializeEvaluatedMetrics(
            SortedMap<Instant, CollectedMetrics> evaluatedMetrics) throws Exception {
        return YAML_MAPPER.writeValueAsString(evaluatedMetrics);
    }

    private static SortedMap<Instant, CollectedMetrics> deserializeEvaluatedMetrics(
            String evaluatedMetrics) throws JacksonException {
        return YAML_MAPPER.readValue(evaluatedMetrics, new TypeReference<>() {});
    }

    private static String serializeParallelismOverrides(Map<String, String> overrides) {
        return ConfigurationUtils.convertValue(overrides, String.class);
    }

    private static Map<String, String> deserializeParallelismOverrides(String overrides) {
        return ConfigurationUtils.convertValue(overrides, Map.class);
    }

    @Nullable
    private static String serializeConfigOverrides(ConfigChanges configChanges) {
        try {
            return YAML_MAPPER.writeValueAsString(configChanges);
        } catch (Exception e) {
            LOG.error("Failed to serialize ConfigOverrides", e);
            return null;
        }
    }

    @Nullable
    private static ConfigChanges deserializeConfigOverrides(String configOverrides) {
        try {
            return YAML_MAPPER.readValue(configOverrides, new TypeReference<>() {});
        } catch (Exception e) {
            LOG.error("Failed to deserialize ConfigOverrides", e);
            return null;
        }
    }

    private static String serializeDelayedScaleDown(DelayedScaleDown delayedScaleDown)
            throws JacksonException {
        return YAML_MAPPER.writeValueAsString(delayedScaleDown);
    }

    private static DelayedScaleDown deserializeDelayedScaleDown(String delayedScaleDown)
            throws JacksonException {
        return YAML_MAPPER.readValue(delayedScaleDown, new TypeReference<>() {});
    }
}
