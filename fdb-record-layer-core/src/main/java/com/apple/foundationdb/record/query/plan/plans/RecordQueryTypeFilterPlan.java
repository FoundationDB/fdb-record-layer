/*
 * RecordQueryTypeFilterPlan.java
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
import com.apple.foundationdb.record.query.plan.temp.expressions.TypeFilterExpression;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that filters out records from a child plan that are not of the designated record type(s).
 */
@API(API.Status.MAINTAINED)
public class RecordQueryTypeFilterPlan implements RecordQueryPlanWithChild, TypeFilterExpression {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryTypeFilterPlan.class);

    @Nonnull
    private final ExpressionRef<RecordQueryPlan> inner;
    @Nonnull
    private final Collection<String> recordTypes;
    @Nonnull
    private static final Set<StoreTimer.Count> inCounts = ImmutableSet.of(FDBStoreTimer.Counts.QUERY_FILTER_GIVEN, FDBStoreTimer.Counts.QUERY_TYPE_FILTER_PLAN_GIVEN);
    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_TYPE_FILTER);
    @Nonnull
    private static final Set<StoreTimer.Count> successCounts = ImmutableSet.of(FDBStoreTimer.Counts.QUERY_FILTER_PASSED, FDBStoreTimer.Counts.QUERY_TYPE_FILTER_PLAN_PASSED);
    @Nonnull
    private static final Set<StoreTimer.Count> failureCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_DISCARDED);

    public RecordQueryTypeFilterPlan(@Nonnull RecordQueryPlan inner, @Nonnull Collection<String> recordTypes) {
        this(SingleExpressionRef.of(inner), recordTypes);
    }

    public RecordQueryTypeFilterPlan(@Nonnull ExpressionRef<RecordQueryPlan> inner, @Nonnull Collection<String> recordTypes) {
        this.inner = inner;
        this.recordTypes = recordTypes;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                         @Nonnull EvaluationContext context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        final RecordCursor<FDBQueriedRecord<M>> results = getInner().execute(store, context, continuation, executeProperties.clearSkipAndLimit());

        return results
                .filterInstrumented(record -> recordTypes.contains(record.getRecordType().getName()), store.getTimer(),
                        inCounts, duringEvents, successCounts, failureCounts)
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Override
    public boolean isReverse() {
        return getInner().isReverse();
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

    @Nonnull
    @Override
    public String toString() {
        return getInner() + " | " + recordTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordQueryTypeFilterPlan that = (RecordQueryTypeFilterPlan) o;
        return Objects.equals(getInner(), that.getInner()) &&
                Objects.equals(recordTypes, that.recordTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getInner(), recordTypes);
    }

    @Override
    public int planHash() {
        return getInner().planHash() + recordTypes.hashCode();
    }

    @Nonnull
    public RecordQueryPlan getInner() {
        return inner.get();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInner();
    }

    @Override
    @Nonnull
    public Collection<String> getRecordTypes() {
        return recordTypes;
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_TYPE_FILTER);
        getInner().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getInner().getComplexity();
    }
}
