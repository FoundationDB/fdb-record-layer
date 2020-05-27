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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.UnionCursor;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A query plan that executes by taking the union of records from two or more compatibly-sorted child plans.
 * To work, each child cursor must order its children the same way according to the comparison key.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryUnionPlan extends RecordQueryUnionPlanBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUnionPlan.class);

    private static final StoreTimer.Count PLAN_COUNT = FDBStoreTimer.Counts.PLAN_UNION;

    @Nonnull
    private final KeyExpression comparisonKey;
    private final boolean showComparisonKey;

    /**
     * Construct a new union of two compatibly-ordered plans. This constructor has been deprecated in favor
     * of the static initializer {@link #from(RecordQueryPlan, RecordQueryPlan, KeyExpression, boolean)}.
     *
     * @param left the first plan to union
     * @param right the second plan to union
     * @param comparisonKey a key expression by which the results of both plans are ordered
     * @param reverse whether both plans return results in reverse (i.e., descending) order by the comparison key
     * @param showComparisonKey whether the comparison key should be included in string representations of the plan
     * @deprecated in favor of {@link #from(RecordQueryPlan, RecordQueryPlan, KeyExpression, boolean)}
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    public RecordQueryUnionPlan(@Nonnull RecordQueryPlan left, @Nonnull RecordQueryPlan right,
                                @Nonnull KeyExpression comparisonKey, boolean reverse, boolean showComparisonKey) {
        this(ImmutableList.of(left, right), comparisonKey, reverse, showComparisonKey);
    }

    /**
     * Construct a union of two or more compatibly-ordered plans. This constructor has been deprecated in favor
     * of the static initializer {@link #from(List, KeyExpression, boolean)}.
     *
     * @param children the list of compatibly-ordered plans to take the union of
     * @param comparisonKey a key expression by which the results of all plans are ordered
     * @param reverse whether all plans return results in reverse (i.e., descending) order by the comparison key
     * @param showComparisonKey whether the comparison key should be included in string representations of the plan
     * @deprecated in favor of {@link #from(RecordQueryPlan, RecordQueryPlan, KeyExpression, boolean)}
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    public RecordQueryUnionPlan(@Nonnull List<RecordQueryPlan> children,
                                @Nonnull KeyExpression comparisonKey, boolean reverse, boolean showComparisonKey) {
        this(children.stream().map(GroupExpressionRef::of).collect(Collectors.toList()), comparisonKey,
                reverse, showComparisonKey, false);
    }

    @SuppressWarnings("PMD.UnusedFormalParameter")
    private RecordQueryUnionPlan(@Nonnull List<ExpressionRef<RecordQueryPlan>> children,
                                 @Nonnull KeyExpression comparisonKey,
                                 boolean reverse, boolean showComparisonKey,
                                 boolean ignoredTemporaryFlag) {
        super(children, reverse);
        this.comparisonKey = comparisonKey;
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
        return comparisonKey;
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression) {
        if (!(otherExpression instanceof RecordQueryUnionPlan)) {
            return false;
        }
        final RecordQueryUnionPlan other = (RecordQueryUnionPlan) otherExpression;
        return comparisonKey.equals(other.comparisonKey) &&
               isReverse() == other.isReverse();
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

    /**
     * Construct a new union of two compatibly-ordered plans. The resulting plan will return all results that are
     * returned by either the {@code left} or {@code right} child plans. Each plan should return results in the same
     * order according to the provided {@code comparisonKey}. The two children should also either both return results
     * in forward order, or they should both return results in reverse order. (That is, {@code left.isReverse()} should
     * equal {@code right.isReverse()}.)
     *
     * @param left the first plan to union
     * @param right the second plan to union
     * @param comparisonKey a key expression by which the results of both plans are ordered
     * @param showComparisonKey whether the comparison key should be included in string representations of the plan
     * @return a new plan that will return the union of all results from both child plans
     */
    @Nonnull
    public static RecordQueryUnionPlan from(@Nonnull RecordQueryPlan left, @Nonnull RecordQueryPlan right,
                                            @Nonnull KeyExpression comparisonKey, boolean showComparisonKey) {
        if (left.isReverse() != right.isReverse()) {
            throw new RecordCoreArgumentException("left plan and right plan for union do not have same value for reverse field");
        }
        final List<ExpressionRef<RecordQueryPlan>> childRefs = ImmutableList.of(GroupExpressionRef.of(left), GroupExpressionRef.of(right));
        return new RecordQueryUnionPlan(childRefs, comparisonKey, left.isReverse(), showComparisonKey, false);
    }

    /**
     * Construct a new union of two or more compatibly-ordered plans. The resulting plan will return all results that are
     * returned by any of the child plans. Each plan should return results in the same order according to the provided
     * {@code comparisonKey}. The children should also either all return results in forward order, or they should all
     * return results in reverse order. (That is, {@link RecordQueryPlan#isReverse()} should return the same value
     * for each child.)
     *
     * @param children the list of plans to take the union of
     * @param comparisonKey a key expression by which the results of both plans are ordered
     * @param showComparisonKey whether the comparison key should be included in string representations of the plan
     * @return a new plan that will return the union of all results from all child plans
     */
    @Nonnull
    public static RecordQueryUnionPlan from(@Nonnull List<RecordQueryPlan> children, @Nonnull KeyExpression comparisonKey,
                                            boolean showComparisonKey) {
        if (children.size() < 2) {
            throw new RecordCoreArgumentException("fewer than two children given to union plan");
        }
        boolean firstReverse = children.get(0).isReverse();
        if (!children.stream().allMatch(child -> child.isReverse() == firstReverse)) {
            throw new RecordCoreArgumentException("children of union plan do all have same value for reverse field");
        }
        final ImmutableList.Builder<ExpressionRef<RecordQueryPlan>> childRefsBuilder = ImmutableList.builder();
        for (RecordQueryPlan child : children) {
            childRefsBuilder.add(GroupExpressionRef.of(child));
        }
        return new RecordQueryUnionPlan(childRefsBuilder.build(), comparisonKey, firstReverse, showComparisonKey, false);
    }
}
