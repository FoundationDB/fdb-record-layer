/*
 * IndexMaintenanceUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintenanceFilter;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;

public class IndexMaintenanceUtils {

    @Nonnull
    public static <M extends Message> IndexMaintenanceFilter.IndexValues getFilterTypeForRecord(@Nonnull final IndexMaintainerState state,
                                                                                                @Nonnull final FDBIndexableRecord<M> savedRecord) {
        // Apply both filters:
        // 1. Index predicates (if exist) - currently supports filtering out (i.e. NONE). If not filtered out, fallthrough to the next filter
        // 2. IndexMaintenanceFilter - supports ALL, NONE, and SOME
        // In the longer term, we will probably think about deprecating the index maintenance filter.
        final FDBStoreTimer timer = state.store.getTimer();
        final IndexPredicate predicate = state.index.getPredicate();
        if (predicate != null) {
            final long startTime = timer != null ? System.nanoTime() : 0L;
            final boolean useMe = predicate.shouldIndexThisRecord(state.store, savedRecord);
            // Note: for now, IndexPredicate will not support filtering of certain index entries
            if (timer != null) {
                final FDBStoreTimer.Events event =
                        useMe ?
                        FDBStoreTimer.Events.USE_INDEX_RECORD_BY_PREDICATE :
                        FDBStoreTimer.Events.SKIP_INDEX_RECORD_BY_PREDICATE;
                timer.recordSinceNanoTime(event, startTime);
            }
            if (!useMe) {
                return IndexMaintenanceFilter.IndexValues.NONE;
            }
        }
        long startTime = System.nanoTime();
        IndexMaintenanceFilter.IndexValues ret = state.filter.maintainIndex(state.index, savedRecord.getRecord());
        if (ret == IndexMaintenanceFilter.IndexValues.NONE && timer != null) {
            // events are backward compatible
            timer.recordSinceNanoTime(FDBStoreTimer.Events.SKIP_INDEX_RECORD, startTime);
        }
        return ret;
    }
}
