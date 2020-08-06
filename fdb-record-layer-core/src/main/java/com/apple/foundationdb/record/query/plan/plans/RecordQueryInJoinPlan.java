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
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * A query plan that executes a child plan once for each of the elements of some {@code IN} list.
 */
@API(API.Status.INTERNAL)
public abstract class RecordQueryInJoinPlan implements RecordQueryPlanWithChild {
    @SuppressWarnings("unchecked")
    protected static final Comparator<Object> VALUE_COMPARATOR = (o1, o2) -> ((Comparable)o1).compareTo((Comparable)o2);

    @Nonnull
    protected final Quantifier.Physical inner;
    @Nonnull
    protected final String bindingName;
    protected final boolean sortValuesNeeded;
    protected final boolean sortReverse;

    protected RecordQueryInJoinPlan(@Nonnull final Quantifier.Physical inner,
                                    @Nonnull final String bindingName,
                                    final boolean sortValuesNeeded,
                                    final boolean sortReverse) {
        this.inner = inner;
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
                (outerValue, innerContinuation) -> getInnerPlan().execute(store, context.withBinding(bindingName, outerValue),
                        innerContinuation, executeProperties.clearSkipAndLimit()),
                outerObject -> Tuple.from(ScanComparisons.toTupleItem(outerObject)).pack(),
                continuation,
                store.getPipelineSize(PipelineOperation.IN_JOIN))
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Nonnull
    public RecordQueryPlan getInnerPlan() {
        return inner.getRangesOverPlan();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInnerPlan();
    }

    public boolean isSorted() {
        return sortValuesNeeded;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
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
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final RecordQueryInJoinPlan other = (RecordQueryInJoinPlan) otherExpression;
        return bindingName.equals(other.bindingName) &&
               sortValuesNeeded == other.sortValuesNeeded &&
               sortReverse == other.sortReverse;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(bindingName, sortValuesNeeded, sortReverse);
    }

    @Override
    public int planHash() {
        return getInnerPlan().planHash() + bindingName.hashCode() + (sortValuesNeeded ? 1 : 0) + (sortReverse ? 1 : 0);
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
        return 1 + getInnerPlan().getComplexity();
    }
}
