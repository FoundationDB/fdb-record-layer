/*
 * RecordQueryFilterPlan.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A query plan that filters out records from a child plan that do not satisfy a filter component.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryFilterPlan extends RecordQueryFilterPlanBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryFilterPlan.class);

    @Nonnull
    private final QueryComponent filter;

    public RecordQueryFilterPlan(@Nonnull RecordQueryPlan inner, @Nonnull QueryComponent filter) {
        this(GroupExpressionRef.of(inner), filter);
    }

    public RecordQueryFilterPlan(@Nonnull ExpressionRef<RecordQueryPlan> inner,
                                 @Nonnull QueryComponent filter) {
        super(inner);
        this.filter = filter;
    }

    public RecordQueryFilterPlan(@Nonnull RecordQueryPlan inner, @Nonnull List<QueryComponent> filters) {
        this(inner, filters.size() == 1 ? filters.get(0) : Query.and(filters));
    }

    @Override
    protected boolean hasAsyncFilter() {
        return filter.isAsync();
    }

    @Nullable
    @Override
    protected <M extends Message> Boolean evalFilter(@Nonnull FDBRecordStoreBase<M> store,
                                                     @Nonnull EvaluationContext context,
                                                     @Nullable FDBRecord<M> record) {
        return filter.eval(store, context, record);
    }

    @Nullable
    @Override
    protected <M extends Message> CompletableFuture<Boolean> evalFilterAsync(@Nonnull FDBRecordStoreBase<M> store,
                                                                             @Nonnull EvaluationContext context,
                                                                             @Nullable FDBRecord<M> record) {
        return filter.evalAsync(store, context, record);
    }

    @Override
    public boolean isReverse() {
        return getInner().isReverse();
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends RelationalExpression>> getPlannerExpressionChildren() {
        return Collections.emptyIterator();
    }

    @Nonnull
    @Override
    public String toString() {
        return getInner() + " | " + getFilter();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression) {
        return otherExpression instanceof RecordQueryFilterPlan &&
               filter.equals(((RecordQueryFilterPlan)otherExpression).getFilter());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordQueryFilterPlan that = (RecordQueryFilterPlan) o;
        return Objects.equals(getInner(), that.getInner()) &&
                Objects.equals(getFilter(), that.getFilter());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getInner(), getFilter());
    }

    @Override
    public int planHash() {
        return getInner().planHash() + getFilter().planHash();
    }

    @Nonnull
    public QueryComponent getFilter() {
        return filter;
    }

}
