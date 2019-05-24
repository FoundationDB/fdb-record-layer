/*
 * TestLogMessageKeys.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.logging;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;

/**
 * Common {@link KeyValueLogMessage} keys logged by the Record Layer core.
 * In general, we try to consolidate all of the keys here, so that it's easy to check for collisions, ensure consistency, etc.
 */
@API(API.Status.UNSTABLE)
public enum TestLogMessageKeys {
    AGENT("agent"),
    AGENTS("agents"),
    BATCH_SIZE("batch_size"),
    BEGIN("begin"),
    END("end"),
    FAIL("fail"),
    INDEX("index"),
    ITERATION("iteration"),
    MANUAL_RESULT_COUNT("manual_result_count"),
    ONLY_MANUAL_COUNT("only_manual_count"),
    ONLY_QUERY_COUNT("only_query_count"),
    OVERLAP("overlap"),
    PLAN_HASH("plan_hash"),
    QUERY("query"),
    QUERY_RESULT_COUNT("query_result_count"),
    RECORDS("records"),
    RECORDS_PER_SECOND("records_per_second"),
    RECORD_TYPES("record_types"),
    RESULT_COUNT("result_count"),
    SCAN_MILLIS("scan_millis"),
    SEED("seed"),
    SPLIT_LONG_RECORDS("split_long_records"),
    TOTAL_QUERY_MILLIS("total_query_millis"),
    TOTAL_RESULT_COUNT("total_result_count"),
    TOTAL_SCAN_MILLIS("total_scan_millis");

    private final String logKey;

    TestLogMessageKeys(@Nonnull String key) {
        this.logKey = key;
    }

    @Override
    public String toString() {
        return logKey;
    }
}
