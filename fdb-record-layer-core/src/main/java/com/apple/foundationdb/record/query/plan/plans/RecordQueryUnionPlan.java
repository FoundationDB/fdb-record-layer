/*
 * RecordQueryUnionPlan.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.UnionCursor;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A query plan that executes by taking the union of records from two or more compatibly-sorted child plans.
 * To work, each child cursor must order its children the same way according to the comparison key.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryUnionPlan extends RecordQueryUnionPlanBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUnionPlan.class);

    private static final StoreTimer.Count PLAN_COUNT = FDBStoreTimer.Counts.PLAN_UNION;

    @Nonnull
    private final ExpressionRef<KeyExpression> comparisonKey;
    @Nonnull
    private final List<ExpressionRef<? extends PlannerExpression>> expressionChildren;
    private final boolean showComparisonKey;

    public RecordQueryUnionPlan(@Nonnull RecordQueryPlan left, @Nonnull RecordQueryPlan right,
                                @Nonnull KeyExpression comparisonKey, boolean reverse, boolean showComparisonKey) {
        this(ImmutableList.of(left, right), comparisonKey, reverse, showComparisonKey);
    }

    public RecordQueryUnionPlan(@Nonnull List<RecordQueryPlan> children,
                                @Nonnull KeyExpression comparisonKey, boolean reverse, boolean showComparisonKey) {
        super(children, reverse);
        final ImmutableList.Builder<ExpressionRef<? extends PlannerExpression>> expressionChildrenBuilder = ImmutableList.builder();
        expressionChildrenBuilder.addAll(super.getPlannerExpressionChildren());
        this.comparisonKey = SingleExpressionRef.of(comparisonKey);
        this.expressionChildren = expressionChildrenBuilder.add(this.comparisonKey).build();
        this.showComparisonKey = showComparisonKey;
    }


    @Nonnull
    @Override
    <M extends Message> RecordCursor<FDBQueriedRecord<M>> createUnionCursor(@Nonnull FDBRecordStoreBase<M> store,
                                                                            @Nonnull List<Function<byte[], RecordCursor<FDBQueriedRecord<M>>>> childCursorFunctions,
                                                                            @Nullable byte[] continuation) {
        return UnionCursor.create(store, getComparisonKey(), isReverse(), childCursorFunctions, continuation);
    }

    @Nonnull
    public KeyExpression getComparisonKey() {
        return comparisonKey.get();
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return expressionChildren.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordQueryUnionPlan that = (RecordQueryUnionPlan) o;
        return super.equals(o) && Objects.equals(getComparisonKey(), that.getComparisonKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(Sets.newHashSet(getQueryPlanChildren()), getComparisonKey(), isReverse()); // isomorphic under re-ordering of children
    }

    @Override
    public int planHash() {
        return PlanHashable.planHash(getQueryPlanChildren()) + getComparisonKey().planHash() + (isReverse() ? 1 : 0);
    }

    @Nonnull
    @Override
    String getDelimiter() {
        return " " + UNION + (showComparisonKey ? getComparisonKey().toString() : "") + " ";
    }

    @Nonnull
    @Override
    StoreTimer.Count getPlanCount() {
        return PLAN_COUNT;
    }
}
