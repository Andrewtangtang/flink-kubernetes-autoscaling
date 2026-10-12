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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedMetricsResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link A4SAggregatingVertexMetricsHandler}. */
public class A4SAggregatingVertexMetricsHandlerTest extends TestLogger {

    private static final Time TIMEOUT = Time.milliseconds(50L);

    @Mock private RestfulGateway restfulGateway;

    private JobID jobId;
    private JobVertexID jobVertexId;
    private MetricFetcher metricFetcher;
    private MetricStore metricStore;
    private A4SAggregatingVertexMetricsHandler handler;
    private AutoCloseable mocks;

    @Before
    public void setUp() {
        mocks = MockitoAnnotations.openMocks(this);

        jobId = JobID.generate();
        jobVertexId = new JobVertexID();
        metricFetcher =
                new MetricFetcherImpl<>(
                        () -> null,
                        ignoredRpcAddress -> null,
                        Executors.directExecutor(),
                        TestingUtils.TIMEOUT,
                        MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue());
        metricStore = metricFetcher.getMetricStore();

        GatewayRetriever<RestfulGateway> leaderRetriever =
                new GatewayRetriever<RestfulGateway>() {
                    @Override
                    public CompletableFuture<RestfulGateway> getFuture() {
                        return CompletableFuture.completedFuture(restfulGateway);
                    }
                };

        handler =
                new A4SAggregatingVertexMetricsHandler(
                        leaderRetriever,
                        TIMEOUT,
                        Collections.emptyMap(),
                        Executors.directExecutor(),
                        metricFetcher);
    }

    @After
    public void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    private HandlerRequest<EmptyRequestBody> createRequest(String jobId, String jobVertexId)
            throws Exception {
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId);
        pathParameters.put(JobVertexIdPathParameter.KEY, jobVertexId);

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                handler.getMessageHeaders().getUnresolvedMessageParameters(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }

    @Test
    public void testHandleRequestComputesScaledMrcFromSubtaskHistograms() throws Exception {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 0, 0, ""),
                        "stack-distance-histogram",
                        new StackDistanceHistogram(new long[] {4L, 2L, 1L}, 0L, 1, 1L, 4096L)
                                .toMetricString()));
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 1, 0, ""),
                        "foo.stack-distance-histogram",
                        new StackDistanceHistogram(new long[] {1L, 1L, 1L}, 0L, 1, 1L, 4096L)
                                .toMetricString()));

        A4SAggregatedMetricsResponseBody response =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();

        MissRateCurve curve = response.getScaledMrc();
        assertThat(curve.getPoints(), hasSize(3));
        assertThat(curve.getPoints().get(0).getCacheSizeBytes(), equalTo(8192L));
        assertThat(curve.getPoints().get(0).getMissRate(), closeTo(0.5, 1.0e-9));
        assertThat(curve.getPoints().get(1).getCacheSizeBytes(), equalTo(16384L));
        assertThat(curve.getPoints().get(1).getMissRate(), closeTo(0.2, 1.0e-9));
        assertThat(curve.getPoints().get(2).getCacheSizeBytes(), equalTo(24576L));
        assertThat(curve.getPoints().get(2).getMissRate(), closeTo(0.0, 1.0e-9));
    }

    @Test
    public void testHandleRequest_taskIdIsolation() throws Exception {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 0, 0, ""),
                        "stack-distance-histogram",
                        new StackDistanceHistogram(new long[] {4L, 2L, 2L}, 0L, 1, 1L, 4096L)
                                .toMetricString()));

        JobVertexID jobVertexId2 = new JobVertexID();
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId2.toString(), 0, 0, ""),
                        "stack-distance-histogram",
                        new StackDistanceHistogram(new long[] {1L, 1L, 1L}, 0L, 1, 1L, 4096L)
                                .toMetricString()));

        A4SAggregatedMetricsResponseBody response1 =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();

        MissRateCurve curve1 = response1.getScaledMrc();
        assertThat(curve1.getPoints(), hasSize(3));
        assertThat(curve1.getPoints().get(0).getCacheSizeBytes(), equalTo(4096L));
        assertThat(curve1.getPoints().get(0).getMissRate(), closeTo(0.5, 1.0e-9));
        assertThat(curve1.getPoints().get(1).getCacheSizeBytes(), equalTo(4096L * 2));
        assertThat(curve1.getPoints().get(1).getMissRate(), closeTo(0.25, 1.0e-9));
        assertThat(curve1.getPoints().get(2).getCacheSizeBytes(), equalTo(4096L * 3));
        assertThat(curve1.getPoints().get(2).getMissRate(), closeTo(0.0, 1.0e-9));

        A4SAggregatedMetricsResponseBody response2 =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId2.toString()),
                                restfulGateway)
                        .get();
        MissRateCurve curve2 = response2.getScaledMrc();
        assertThat(curve2.getPoints(), hasSize(3));
        assertThat(curve2.getPoints().get(0).getCacheSizeBytes(), equalTo(4096L));
        assertThat(curve2.getPoints().get(0).getMissRate(), closeTo(2.0 / 3.0, 1.0e-9));
        assertThat(curve2.getPoints().get(1).getCacheSizeBytes(), equalTo(4096L * 2));
        assertThat(curve2.getPoints().get(1).getMissRate(), closeTo(1.0 / 3.0, 1.0e-9));
        assertThat(curve2.getPoints().get(2).getCacheSizeBytes(), equalTo(4096L * 3));
        assertThat(curve2.getPoints().get(2).getMissRate(), closeTo(0, 1.0e-9));
    }

    @Test
    public void testHandleRequest_incorrectNameIgnored() throws Exception {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 0, 0, ""),
                        "foo",
                        new StackDistanceHistogram(new long[] {4L, 2L, 1L}, 0L, 1, 1L, 4096L)
                                .toMetricString()));
        A4SAggregatedMetricsResponseBody response =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();

        assertTrue(response.getScaledMrc().getPoints().isEmpty());
    }

    @Test
    public void testHandleRequestReturnsEmptyCurveWhenTaskMetricStoreMissing() throws Exception {
        A4SAggregatedMetricsResponseBody response =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();

        assertTrue(response.getScaledMrc().getPoints().isEmpty());
    }

    @Test
    public void testHandleRequestWrapsMalformedHistogramAsInternalServerError() throws Exception {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 0, 0, ""),
                        "stack-distance-histogram",
                        "{\"bucketCounts\": [1, \"bad\", 3]}"));

        try {
            handler.handleRequest(
                            createRequest(jobId.toString(), jobVertexId.toString()), restfulGateway)
                    .get();
            fail("Expected an exception.");
        } catch (ExecutionException e) {
            Throwable completionCause = e.getCause();
            assertTrue(completionCause instanceof RestHandlerException);
            RestHandlerException rhe = (RestHandlerException) completionCause;
            assertThat(
                    rhe.getHttpResponseStatus(), equalTo(HttpResponseStatus.INTERNAL_SERVER_ERROR));
            assertThat(rhe.getMessage(), equalTo("Could not retrieve A4S metrics."));
        }
    }
}
