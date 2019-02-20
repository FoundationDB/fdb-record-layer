/*
 * RecordQueryInJoinPlan.java
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
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that executes a child plan once for each of the elements of some {@code IN} list.
 */
@API(API.Status.MAINTAINED)
public abstract class RecordQueryInJoinPlan implements RecordQueryPlanWithChild {
    @SuppressWarnings("unchecked")
    protected static final Comparator<Object> VALUE_COMPARATOR = (o1, o2) -> ((Comparable)o1).compareTo((Comparable)o2);

    protected final ExpressionRef<RecordQueryPlan> plan;
    protected final List<ExpressionRef<? extends PlannerExpression>> children;
    protected final String bindingName;
    protected final boolean sortValuesNeeded;
    protected final boolean sortReverse;

    public RecordQueryInJoinPlan(RecordQueryPlan plan, String bindingName, boolean sortValuesNeeded, boolean sortReverse) {
        this.plan = SingleExpressionRef.of(plan);
        this.children = Collections.singletonList(this.plan);
        this.bindingName = bindingName;
        this.sortValuesNeeded = sortValuesNeeded;
        this.sortReverse = sortReverse;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                         @Nonnull EvaluationContext context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        return RecordCursor.flatMapPipelined(
                outerContinuation -> {
                    final List<Object> values = getValues(context);
                    if (values == null) {
                        return RecordCursor.empty(store.getExecutor());
                    } else {
                        return RecordCursor.fromList(store.getExecutor(), values, outerContinuation);
                    }
                },
                (outerValue, innerContinuation) -> getInner().execute(store, context.withBinding(bindingName, outerValue),
                        innerContinuation, executeProperties.clearSkipAndLimit()),
                outerObject -> Tuple.from(ScanComparisons.toTupleItem(outerObject)).pack(),
                continuation,
                store.getPipelineSize(PipelineOperation.IN_JOIN))
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Nonnull
    public RecordQueryPlan getInner() {
        return plan.get();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInner();
    }

    public boolean isSorted() {
        return sortValuesNeeded;
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
        return children.iterator();
    }

    @Override
    public boolean isReverse() {
        if (sortValuesNeeded) {
            return sortReverse;
        } else {
            throw new RecordCoreException("RecordQueryInJoinPlan does not have well defined reverse-ness");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof RecordQueryInJoinPlan)) {
            return false;
        }
        RecordQueryInJoinPlan that = (RecordQueryInJoinPlan) o;
        return sortValuesNeeded == that.sortValuesNeeded &&
                sortReverse == that.sortReverse &&
                Objects.equals(getChild(), that.getChild()) &&
                Objects.equals(bindingName, that.bindingName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChild(), bindingName, sortValuesNeeded, sortReverse);
    }

    @Override
    public int planHash() {
        return getInner().planHash() + bindingName.hashCode() + (sortValuesNeeded ? 1 : 0) + (sortReverse ? 1 : 0);
    }

    @Nullable
    protected List<Object> sortValues(@Nullable List<Object> values) {
        if (values == null || values.size() < 2 || !sortValuesNeeded) {
            return values;
        }
        List<Object> copy = new ArrayList<>(values);
        copy.sort(sortReverse ? VALUE_COMPARATOR.reversed() : VALUE_COMPARATOR);
        return copy;
    }

    @Nullable
    protected abstract List<Object> getValues(EvaluationContext context);

    @Override
    public int getComplexity() {
        return 1 + getInner().getComplexity();
    }
}
