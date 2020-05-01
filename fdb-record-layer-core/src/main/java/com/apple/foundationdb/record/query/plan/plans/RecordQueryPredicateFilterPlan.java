/*
 * RecordQueryPredicateFilterPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.apple.foundationdb.record.query.plan.temp.view.SourceEntry;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A query plan that filters out records from a child plan that do not satisfy a {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordQueryPredicateFilterPlan extends RecordQueryFilterPlanBase {
    @Nonnull
    private final Source baseSource;
    @Nonnull
    private final ExpressionRef<QueryPredicate> filter;
    @Nonnull
    private final List<ExpressionRef<? extends PlannerExpression>> children;

    public RecordQueryPredicateFilterPlan(@Nonnull RecordQueryPlan inner,
                                          @Nonnull Source baseSource,
                                          @Nonnull QueryPredicate filter) {
        this(SingleExpressionRef.of(inner), baseSource, SingleExpressionRef.of(filter));
    }

    public RecordQueryPredicateFilterPlan(@Nonnull ExpressionRef<RecordQueryPlan> inner,
                                          @Nonnull Source baseSource,
                                          @Nonnull ExpressionRef<QueryPredicate> filter) {
        super(inner);
        this.baseSource = baseSource;
        this.filter = filter;
        this.children = ImmutableList.of(inner, filter);
    }

    @Override
    protected boolean hasAsyncFilter() {
        return false;
    }

    @Nullable
    @Override
    protected <M extends Message> Boolean evalFilter(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable FDBRecord<M> record) {
        if (record == null) {
            return null;
        }
        Boolean result = null;
        Iterator<SourceEntry> entries = baseSource.evalSourceEntriesFor(record.getRecord()).iterator();
        while (entries.hasNext()) {
            Boolean entryResult = filter.get().eval(store, context, entries.next());
            if (entryResult != null && entryResult) {
                return true;
            } else if (result == null) {
                result = entryResult;
            }
        }
        return result;
    }

    @Nullable
    @Override
    protected <M extends Message> CompletableFuture<Boolean> evalFilterAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable FDBRecord<M> record) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    public Source getBaseSource() {
        return baseSource;
    }

    @Nonnull
    public QueryPredicate getFilter() {
        return filter.get();
    }


    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return children.iterator();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull PlannerExpression otherExpression) {
        return otherExpression instanceof RecordQueryPredicateFilterPlan;
    }

    @Override
    public String toString() {
        return getInner() + " | " + getFilter();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordQueryPredicateFilterPlan that = (RecordQueryPredicateFilterPlan)o;
        return Objects.equals(getInner(), that.getInner()) &&
               Objects.equals(baseSource, that.baseSource) &&
               Objects.equals(getFilter(), that.getFilter());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getInner(), baseSource, getFilter());
    }

    @Override
    public int planHash() {
        return getInner().planHash() + getFilter().planHash();
    }
}
