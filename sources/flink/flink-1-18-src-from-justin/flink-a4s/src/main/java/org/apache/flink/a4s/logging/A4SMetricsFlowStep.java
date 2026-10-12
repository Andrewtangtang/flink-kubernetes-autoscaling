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

package org.apache.flink.a4s.logging;

/**
 * Stages in the A4S stack-distance histogram and MRC pipeline for consolidated logging ({@code A4S
 * [stage]: ...}).
 */
public enum A4SMetricsFlowStep {
    ROCKSDB_HISTOGRAM_UPDATED,
    ROCKSDB_HISTOGRAM_UPDATE_FAILED,

    REST_REQUEST_RECEIVED,
    FETCHER_UPDATE_BEGIN,
    FETCHER_UPDATE_END,

    STORES_MISSING,
    STORES_FOUND,

    HISTOGRAM_SCAN_BEGIN,
    HISTOGRAM_SCAN_END,
    STORE_ITEM_BEGIN,
    HISTOGRAM_RAW_EXTRACTED,
    HISTOGRAM_PARSED,

    HISTOGRAM_MERGED,

    MRC_BUILD_BEGIN,
    MRC_BUILT,

    RESPONSE_READY,

    HANDLER_FAILED
}
