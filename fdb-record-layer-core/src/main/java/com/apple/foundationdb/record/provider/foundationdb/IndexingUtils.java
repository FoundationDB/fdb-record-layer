/*
 * IndexingUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.ScanProperties;

import javax.annotation.Nonnull;

public class IndexingUtils {

    private IndexingUtils() {
    }

    static int addOneToAllowForContinuation(final int limit) {
        // always respect limit in this path; +1 allows a continuation item
        return limit == Integer.MAX_VALUE ? limit : limit + 1;
    }

    @Nonnull
    static ScanProperties getScanProperties(final int limit, final IsolationLevel isolationLevel) {
        final ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(isolationLevel)
                .setReturnedRowLimit(addOneToAllowForContinuation(limit))
                .build();
        return new ScanProperties(executeProperties);
    }
}
