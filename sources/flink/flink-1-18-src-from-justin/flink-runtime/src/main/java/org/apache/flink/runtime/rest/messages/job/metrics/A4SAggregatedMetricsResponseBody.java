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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collections;

/** A4S response body containing aggregated scalar metrics and scaled MRC points. */
public class A4SAggregatedMetricsResponseBody implements ResponseBody {

    public static final String FIELD_NAME_SCALED_MRC = "scaledMrc";
    public static final String FIELD_NAME_MERGED_HISTOGRAM = "mergedHistogram";

    @JsonProperty(FIELD_NAME_SCALED_MRC)
    private final MissRateCurve scaledMrc;

    /** SHARDS only: the cumulative vertex histogram, so a client can window and adjust it. */
    @JsonProperty(FIELD_NAME_MERGED_HISTOGRAM)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final StackDistanceHistogram mergedHistogram;

    public A4SAggregatedMetricsResponseBody(MissRateCurve scaledMrc) {
        this(scaledMrc, null);
    }

    @JsonCreator
    public A4SAggregatedMetricsResponseBody(
            @JsonProperty(FIELD_NAME_SCALED_MRC) MissRateCurve scaledMrc,
            @JsonProperty(FIELD_NAME_MERGED_HISTOGRAM) @Nullable
                    StackDistanceHistogram mergedHistogram) {
        this.scaledMrc = scaledMrc == null ? new MissRateCurve(Collections.emptyList()) : scaledMrc;
        this.mergedHistogram = mergedHistogram;
    }

    public MissRateCurve getScaledMrc() {
        return scaledMrc;
    }

    @Nullable
    public StackDistanceHistogram getMergedHistogram() {
        return mergedHistogram;
    }
}
