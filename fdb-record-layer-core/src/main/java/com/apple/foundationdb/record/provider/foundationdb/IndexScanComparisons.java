/*
 * IndexScanComparisons.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link ScanComparisons} for use in an index scan.
 */
@API(API.Status.MAINTAINED)
public class IndexScanComparisons implements IndexScanParameters {
    @Nonnull
    private final IndexScanType scanType;
    @Nonnull
    private final ScanComparisons scanComparisons;

    public IndexScanComparisons(@Nonnull IndexScanType scanType, @Nonnull ScanComparisons scanComparisons) {
        this.scanType = scanType;
        this.scanComparisons = scanComparisons;
    }

    @Nonnull
    public static IndexScanComparisons byValue() {
        return byValue(null);
    }

    @Nonnull
    public static IndexScanComparisons byValue(@Nullable ScanComparisons scanComparisons) {
        if (scanComparisons == null) {
            scanComparisons = ScanComparisons.EMPTY;
        }
        return new IndexScanComparisons(IndexScanType.BY_VALUE, scanComparisons);
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return scanType;
    }

    @Nonnull
    public ScanComparisons getComparisons() {
        return scanComparisons;
    }

    @Nonnull
    @Override
    public IndexScanRange bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        return new IndexScanRange(scanType, scanComparisons.toTupleRange(store, context));
    }

    @Override
    public int planHash(@Nonnull PlanHashKind hashKind) {
        return scanType.planHash(hashKind) + scanComparisons.planHash(hashKind);
    }

    @Override
    public boolean isUnique(@Nonnull Index index) {
        return scanComparisons.isEquality() && scanComparisons.size() == index.getColumnSize();
    }

    @Nonnull
    @Override
    public String getScanDetails() {
        @Nullable final TupleRange tupleRange = scanComparisons.toTupleRangeWithoutContext();
        return tupleRange == null ? scanComparisons.toString() : tupleRange.toString();
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        if (scanType != IndexScanType.BY_VALUE) {
            detailsBuilder.add("scan type: {{scanType}}");
            attributeMapBuilder.put("scanType", Attribute.gml(scanType.toString()));
        }

        @Nullable final TupleRange tupleRange = scanComparisons.toTupleRangeWithoutContext();
        if (tupleRange != null) {
            detailsBuilder.add("range: " + tupleRange.getLowEndpoint().toString(false) + "{{low}}, {{high}}" + tupleRange.getHighEndpoint().toString(true));
            attributeMapBuilder.put("low", Attribute.gml(tupleRange.getLow() == null ? "-∞" : tupleRange.getLow().toString()));
            attributeMapBuilder.put("high", Attribute.gml(tupleRange.getHigh() == null ? "∞" : tupleRange.getHigh().toString()));
        } else {
            detailsBuilder.add("comparisons: {{comparisons}}");
            attributeMapBuilder.put("comparisons", Attribute.gml(scanComparisons.toString()));
        }
    }

    @Override
    public String toString() {
        return scanType + ":" + scanComparisons;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final IndexScanComparisons that = (IndexScanComparisons)o;

        if (!scanType.equals(that.scanType)) {
            return false;
        }
        return scanComparisons.equals(that.scanComparisons);
    }

    @Override
    public int hashCode() {
        int result = scanType.hashCode();
        result = 31 * result + scanComparisons.hashCode();
        return result;
    }
}
