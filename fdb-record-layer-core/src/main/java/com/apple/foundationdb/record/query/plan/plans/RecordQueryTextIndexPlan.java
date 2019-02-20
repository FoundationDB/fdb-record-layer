/*
 * RecordQueryTextIndexPlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.TextScan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that can scan text indexes. These work slightly differently than reqular indexes
 * in that the comparison on a query might actually be split into multiple queries.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryTextIndexPlan implements RecordQueryPlanWithIndex {

    @Nonnull
    private final String indexName;
    @Nonnull
    private final TextScan textScan;
    private final boolean reverse;

    public RecordQueryTextIndexPlan(@Nonnull String indexName, @Nonnull TextScan textScan,
                                    boolean reverse) {
        this.indexName = indexName;
        this.textScan = textScan;
        this.reverse = reverse;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<IndexEntry> executeEntries(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                                       @Nullable byte[] continuation, @Nonnull ExecuteProperties executeProperties) {
        return textScan.scan(store, context, continuation, executeProperties.asScanProperties(reverse));
    }

    @Nonnull
    @Override
    public String getIndexName() {
        return indexName;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return IndexScanType.BY_TEXT_TOKEN;
    }

    @Nonnull
    public TextScan getTextScan() {
        return textScan;
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return this.indexName.equals(indexName);
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return Collections.singleton(indexName);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_INDEX);
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null) {
            return false;
        } else if (!getClass().isInstance(o)) {
            return false;
        }
        RecordQueryTextIndexPlan that = (RecordQueryTextIndexPlan) o;
        return this.reverse == that.reverse && this.indexName.equals(that.indexName) && this.textScan.equals(that.textScan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, textScan, reverse);
    }

    @Override
    public int planHash() {
        return indexName.hashCode() + textScan.planHash() + (reverse ? 1 : 0);
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Collections.emptyIterator();
    }

    @Nonnull
    @Override
    public String toString() {
        return "TextIndex(" + textScan.getIndex().getName() + " " + textScan.getGroupingComparisons() + ", " + textScan.getTextComparison() + ", " + textScan.getSuffixComparisons() + ")";
    }
}
