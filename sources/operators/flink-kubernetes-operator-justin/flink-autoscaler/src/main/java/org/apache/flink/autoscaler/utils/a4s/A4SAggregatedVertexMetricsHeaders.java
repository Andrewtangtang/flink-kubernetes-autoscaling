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

package org.apache.flink.autoscaler.utils.a4s;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Autoscaler-local copy of A4S aggregating vertex metrics headers. */
public class A4SAggregatedVertexMetricsHeaders
        implements RuntimeMessageHeaders<
                EmptyRequestBody,
                A4SAggregatedMetricsResponseBody,
                AggregatedSubtaskMetricsParameters> {

    private static final A4SAggregatedVertexMetricsHeaders INSTANCE =
            new A4SAggregatedVertexMetricsHeaders();

    private A4SAggregatedVertexMetricsHeaders() {}

    @Override
    public Class<A4SAggregatedMetricsResponseBody> getResponseClass() {
        return A4SAggregatedMetricsResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public AggregatedSubtaskMetricsParameters getUnresolvedMessageParameters() {
        return new AggregatedSubtaskMetricsParameters();
    }

    @Override
    public String getTargetRestEndpointURL() {
        return "/jobs/:"
                + JobIDPathParameter.KEY
                + "/vertices/:"
                + JobVertexIdPathParameter.KEY
                + "/a4s-metrics";
    }

    public static A4SAggregatedVertexMetricsHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Provides access to A4S-specific aggregated vertex metrics including"
                + " parallelism and ResourceProfile information for Memory Parallelism Curves.";
    }
}
