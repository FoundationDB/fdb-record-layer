/*
 * QueryPropertiesUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.relational.api.QueryProperties;

import javax.annotation.Nonnull;

public class QueryPropertiesUtils {
    public static ExecuteProperties getExecuteProperties(@Nonnull QueryProperties queryProperties) {
        ExecuteProperties.Builder builder = ExecuteProperties.newBuilder()
                .setIsolationLevel(queryProperties.isSnapshotIsolation() ? IsolationLevel.SNAPSHOT : IsolationLevel.SERIALIZABLE)
                .setSkip(queryProperties.getSkip())
                .setReturnedRowLimit(queryProperties.getRowLimit())
                .setTimeLimit(queryProperties.getTimeLimit())
                .setScannedRecordsLimit(queryProperties.getScannedRecordsLimit())
                .setScannedBytesLimit(queryProperties.getScannedBytesLimit())
                .setFailOnScanLimitReached(queryProperties.failOnScanLimitReached())
                .setDefaultCursorStreamingMode(queryProperties.loadAllRecordsImmediately() ? CursorStreamingMode.WANT_ALL : CursorStreamingMode.ITERATOR);
        return builder.build();
    }

    public static ScanProperties getScanProperties(@Nonnull QueryProperties queryProperties) {
        ExecuteProperties executeProperties = getExecuteProperties(queryProperties);
        return new ScanProperties(executeProperties, queryProperties.isReverse());
    }
}
