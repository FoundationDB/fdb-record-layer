/*
 * VersionFromTimestamp.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.clientlog;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.system.SystemKeyspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Map from wall-clock time to transaction time.
 */
@API(API.Status.EXPERIMENTAL)
@SpotBugsSuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class VersionFromTimestamp {

    /**
     * Get the last version from the timekeeper at or before the given timestamp.
     * @param tr an open transaction
     * @param timestamp the wall-clock time
     * @return a future that completes with the recorded version that comes immediately before the target time
     */
    @Nonnull
    public static CompletableFuture<Long> lastVersionBefore(@Nonnull ReadTransaction tr, @Nonnull Instant timestamp) {
        return versionFromTimestamp(tr, timestamp, true);
    }

    /**
     * Get the first version from the timekeeper at or after the given timestamp.
     * @param tr an open transaction
     * @param timestamp the wall-clock time
     * @return a future that completes with the recorded version that comes immediately after the target time
     */
    @Nonnull
    public static CompletableFuture<Long> nextVersionAfter(@Nonnull ReadTransaction tr, @Nonnull Instant timestamp) {
        return versionFromTimestamp(tr, timestamp, false);
    }

    private static CompletableFuture<Long> versionFromTimestamp(@Nonnull ReadTransaction tr, @Nonnull Instant timestamp, boolean start) {
        final byte[] dateKey = ByteArrayUtil.join(SystemKeyspace.TIMEKEEPER_KEY_PREFIX, Tuple.from(timestamp.getEpochSecond()).pack());
        final KeySelector startKey;
        final KeySelector endKey;
        if (start) {
            startKey = KeySelector.firstGreaterThan(SystemKeyspace.TIMEKEEPER_KEY_PREFIX);
            endKey = KeySelector.firstGreaterThan(dateKey);
        } else {
            startKey = KeySelector.firstGreaterOrEqual(dateKey);
            endKey = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(SystemKeyspace.TIMEKEEPER_KEY_PREFIX));
        }
        final AsyncIterator<KeyValue> range = tr.getRange(startKey, endKey, 1, start).iterator();
        return range.onHasNext().thenApply(hasNext -> {
            if (hasNext) {
                return Tuple.fromBytes(range.next().getValue()).getLong(0);
            } else if (start) {
                return 0L;
            } else {
                return Long.MAX_VALUE;
            }
        });
    }

    private VersionFromTimestamp() {
    }
}
