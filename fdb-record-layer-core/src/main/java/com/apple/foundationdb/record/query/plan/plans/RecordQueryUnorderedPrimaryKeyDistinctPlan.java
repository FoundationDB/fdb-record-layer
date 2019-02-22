/*
 * RecordQueryUnorderedPrimaryKeyDistinctPlan.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that removes duplicates by means of a hash table of primary keys already seen.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryUnorderedPrimaryKeyDistinctPlan implements RecordQueryPlanWithChild {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUnorderedPrimaryKeyDistinctPlan.class);

    @Nonnull
    private final ExpressionRef<RecordQueryPlan> inner;
    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_PK_DISTINCT);
    @Nonnull
    private static final Set<StoreTimer.Count> uniqueCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_PK_DISTINCT_PLAN_UNIQUES);
    @Nonnull
    private static final Set<StoreTimer.Count> duplicateCounts =
            ImmutableSet.of(FDBStoreTimer.Counts.QUERY_PK_DISTINCT_PLAN_DUPLICATES, FDBStoreTimer.Counts.QUERY_DISCARDED);

    public RecordQueryUnorderedPrimaryKeyDistinctPlan(@Nonnull RecordQueryPlan inner) {
        this.inner = SingleExpressionRef.of(inner);
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                         @Nonnull EvaluationContext context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        final Set<Tuple> seen = new HashSet<>();
        return getInner().execute(store, context, continuation, executeProperties.clearSkipAndLimit())
            .filterInstrumented(record -> seen.add(record.getPrimaryKey()), store.getTimer(),
                Collections.emptySet(), duringEvents, uniqueCounts, duplicateCounts)
            .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Override
    public boolean isReverse() {
        return getInner().isReverse();
    }

    @Nonnull
    private RecordQueryPlan getInner() {
        return inner.get();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInner();
    }

    @Override
    public boolean hasRecordScan() {
        return getInner().hasRecordScan();
    }

    @Override
    public boolean hasFullRecordScan() {
        return getInner().hasFullRecordScan();
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return getInner().hasIndexScan(indexName);
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return getInner().getUsedIndexes();
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(this.inner);
    }

    @Override
    public String toString() {
        return getInner() + " | UnorderedPrimaryKeyDistinct()";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordQueryUnorderedPrimaryKeyDistinctPlan that = (RecordQueryUnorderedPrimaryKeyDistinctPlan) o;
        return Objects.equals(getInner(), that.getInner());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getInner());
    }

    @Override
    public int planHash() {
        return getInner().planHash() + 1;
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_PK_DISTINCT);
        getInner().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getInner().getComplexity();
    }
}
