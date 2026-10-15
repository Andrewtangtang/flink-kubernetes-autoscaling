/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.a4s.logging.A4SMetricsFlowStep;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedVertexMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** Request handler that returns A4S-specific aggregated metrics for a job vertex. */
public class A4SAggregatingVertexMetricsHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                A4SAggregatedMetricsResponseBody,
                AggregatedSubtaskMetricsParameters> {

    private static final String STACK_DISTANCE_HISTOGRAM_METRIC_NAME = "stack-distance-histogram";

    private final Executor executor;
    private final MetricFetcher fetcher;

    public A4SAggregatingVertexMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                A4SAggregatedVertexMetricsHeaders.getInstance());
        this.executor = executor;
        this.fetcher = fetcher;
    }

    @Override
    protected CompletableFuture<A4SAggregatedMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        JobVertexID vertexID = request.getPathParameter(JobVertexIdPathParameter.class);

        return CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return collectSubtaskHistograms(jobId, vertexID);
                            } catch (Exception e) {
                                throw handlerFailure(jobId, vertexID, e);
                            }
                        },
                        this.executor)
                .thenCompose(
                        histograms -> {
                            if (histograms.isEmpty() || histograms.get(0).getShards() == null) {
                                return CompletableFuture.completedFuture(
                                        new A4SAggregatedMetricsResponseBody(
                                                buildScaledMrcPoints(histograms)));
                            }
                            // SHARDS: scale the memory axis by the vertex parallelism, not by
                            // the number of subtasks that have reported a histogram.
                            return gateway.requestJob(jobId, timeout)
                                    .thenApply(
                                            graph ->
                                                    buildShardsResponse(
                                                            jobId, vertexID, histograms, graph));
                        });
    }

    private A4SAggregatedMetricsResponseBody buildShardsResponse(
            JobID jobId,
            JobVertexID vertexID,
            List<StackDistanceHistogram> histograms,
            AccessExecutionGraph graph) {
        try {
            int parallelism = graph.getJobVertex(vertexID).getParallelism();
            StackDistanceHistogram merged = StackDistanceHistogram.mergeShards(histograms, parallelism);
            return new A4SAggregatedMetricsResponseBody(
                    MissRateCurve.fromStackDistanceHistogram(merged), merged);
        } catch (Exception e) {
            throw handlerFailure(jobId, vertexID, e);
        }
    }

    private CompletionException handlerFailure(JobID jobId, JobVertexID vertexID, Exception e) {
        log.warn(
                "A4S [{}]: jobId={} vertexId={} message={}",
                A4SMetricsFlowStep.HANDLER_FAILED.name(),
                jobId,
                vertexID,
                e.getMessage(),
                e);
        return new CompletionException(
                new RestHandlerException(
                        "Could not retrieve A4S metrics.",
                        HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }

    private List<StackDistanceHistogram> collectSubtaskHistograms(
            JobID jobId, JobVertexID vertexID) {
        this.fetcher.update();
        MetricStore store = this.fetcher.getMetricStore();

        MetricStore.TaskMetricStore taskMetricStore =
                store.getTaskMetricStore(jobId.toString(), vertexID.toString());
        if (taskMetricStore == null) {
            return Collections.emptyList();
        }

        List<StackDistanceHistogram> subtaskHistograms = new ArrayList<>();
        for (Map.Entry<Integer, MetricStore.SubtaskMetricStore> entry :
                taskMetricStore.getAllSubtaskMetricStores().entrySet()) {
            MetricStore.SubtaskMetricStore storeItem = entry.getValue();
            String histogramRaw = getStackDistanceHistogramRaw(storeItem.metrics);
            if (histogramRaw == null) {
                continue;
            }
            StackDistanceHistogram histogram =
                    StackDistanceHistogram.fromMetricString(histogramRaw);
            subtaskHistograms.add(histogram);
        }

        return subtaskHistograms;
    }

    @Nullable
    private String getStackDistanceHistogramRaw(Map<String, String> metrics) {
        for (Map.Entry<String, String> metricEntry : metrics.entrySet()) {
            String metricKey = metricEntry.getKey();
            String metricValue = metricEntry.getValue();
            if (metricKey == null || metricValue == null) {
                continue;
            }
            if (metricKey.contains(STACK_DISTANCE_HISTOGRAM_METRIC_NAME)) {
                return metricValue;
            }
        }
        return null;
    }

    private MissRateCurve buildScaledMrcPoints(List<StackDistanceHistogram> subtaskHistograms) {
        if (subtaskHistograms.isEmpty()) {
            return new MissRateCurve(Collections.emptyList());
        }
        StackDistanceHistogram mergedHistogram = StackDistanceHistogram.merge(subtaskHistograms);
        return MissRateCurve.fromStackDistanceHistogram(mergedHistogram);
    }
}
