/*
 * LuceneScanQueryParameters.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;

/**
 * Scan parameters for making a {@link LuceneScanQuery}.
 */
@API(API.Status.UNSTABLE)
public class LuceneScanQueryParameters extends LuceneScanParameters {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Lucene-Scan-Query");

    @Nonnull
    final LuceneQueryClause query;

    protected LuceneScanQueryParameters(@Nonnull ScanComparisons groupComparisons, @Nonnull LuceneQueryClause query) {
        super(LuceneScanTypes.BY_LUCENE, groupComparisons);
        this.query = query;
    }

    @Nonnull
    public LuceneQueryClause getQuery() {
        return query;
    }

    @Override
    public int planHash(@Nonnull PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, scanType, groupComparisons, query);
    }

    @Nonnull
    @Override
    public LuceneScanQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        return new LuceneScanQuery(scanType, getGroupKey(store, context), query.bind(store, index, context));
    }

    @Nonnull
    @Override
    public String getScanDetails() {
        return getGroupScanDetails() + " " + query;
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        super.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);
        query.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);
    }

    @Override
    public String toString() {
        return super.toString() + " " + query;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }

        final LuceneScanQueryParameters that = (LuceneScanQueryParameters)o;

        return query.equals(that.query);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + query.hashCode();
        return result;
    }
}
