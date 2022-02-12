/*
 * TimeWindowScanComparisons.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.leaderboard;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;

/**
 * Extend {@link IndexScanComparisons} to have {@link TimeWindowForFunction}.
 */
@API(API.Status.EXPERIMENTAL)
public class TimeWindowScanComparisons extends IndexScanComparisons {
    @Nonnull
    private final TimeWindowForFunction timeWindow;

    public TimeWindowScanComparisons(@Nonnull TimeWindowForFunction timeWindow, @Nonnull ScanComparisons comparisons) {
        super(IndexScanType.BY_TIME_WINDOW, comparisons);
        this.timeWindow = timeWindow;
    }

    @Nonnull
    @Override
    public TimeWindowScanRange bind(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull final EvaluationContext context) {
        return new TimeWindowScanRange(timeWindow.getLeaderboardType(context), timeWindow.getLeaderboardTimestamp(context), super.bind(store, index, context).getScanRange());
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return super.planHash(hashKind) + timeWindow.planHash(hashKind);
    }

    @Nonnull
    @Override
    public String getScanDetails() {
        return super.getScanDetails() + "@" + timeWindow;
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        super.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);
        detailsBuilder.add("time window type: {{timeWindowType}}");
        detailsBuilder.add("time window timestamp: {{timeWindowTimestamp}}");
        attributeMapBuilder.put("timeWindowType", Attribute.gml(timeWindow.leaderboardTypeString()));
        attributeMapBuilder.put("timeWindowTimestamp", Attribute.gml(timeWindow.leaderboardTimestampString()));
    }

    @Override
    public String toString() {
        return super.toString() + "@" + timeWindow;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        final TimeWindowScanComparisons that = (TimeWindowScanComparisons)o;

        return timeWindow.equals(that.timeWindow);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + timeWindow.hashCode();
        return result;
    }
}
