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
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/** Tests for {@link A4SAggregatedMetricsResponseBody}. */
public class A4SAggregatedMetricsResponseBodyTest
        extends RestResponseMarshallingTestBase<A4SAggregatedMetricsResponseBody> {

    @Override
    protected Class<A4SAggregatedMetricsResponseBody> getTestResponseClass() {
        return A4SAggregatedMetricsResponseBody.class;
    }

    @Override
    protected A4SAggregatedMetricsResponseBody getTestResponseInstance() {
        return new A4SAggregatedMetricsResponseBody(
                new MissRateCurve(
                        List.of(
                                new MissRateCurve.Point(0L, 1.0),
                                new MissRateCurve.Point(1048576L, 0.2))));
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            A4SAggregatedMetricsResponseBody expected, A4SAggregatedMetricsResponseBody actual) {
        assertThat(actual.getScaledMrc().getPoints(), hasSize(2));
        assertThat(actual.getScaledMrc().getPoints().get(1).getCacheSizeBytes(), equalTo(1048576L));
        assertThat(actual.getScaledMrc().getPoints().get(1).getMissRate(), equalTo(0.2));
    }

    @Test
    public void testResponseJsonContainsMrcField() throws Exception {
        String json =
                RestMapperUtils.getStrictObjectMapper()
                        .writeValueAsString(getTestResponseInstance());
        assertThat(json, containsString("\"scaledMrc\""));
        assertThat(json, containsString("\"cacheSizeBytes\""));
    }
}
