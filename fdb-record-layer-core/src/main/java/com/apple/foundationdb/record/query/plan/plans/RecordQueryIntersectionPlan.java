/*
 * RecordQueryIntersectionPlan.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.IntersectionCursor;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A query plan that executes by taking the union of records from two or more compatibly-sorted child plans.
 * To work, each child cursor must order its children the same way according to the comparison key.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryIntersectionPlan implements RecordQueryPlanWithChildren {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryIntersectionPlan.class);

    private static final String INTERSECT = "∩"; // U+2229
    /* The current implementations of equals() and hashCode() treat RecordQueryIntersectionPlan as if it were isomorphic under
     * a reordering of its children. In particular, all of the tests assume that a RecordQueryIntersectionPlan with its children
     * reordered is identical. This is accurate in the current implementation (except that the continuation might no longer
     * be valid); if this ever changes, equals() and hashCode() must be updated.
     */
    @Nonnull
    private final List<ExpressionRef<RecordQueryPlan>> children;
    @Nonnull
    private final KeyExpression comparisonKey;
    @Nonnull
    private final List<ExpressionRef<? extends PlannerExpression>> expressionChildren;
    private boolean reverse;

    /**
     * Construct a new intersection of two compatibly-ordered plans. This constructor has been deprecated in favor
     * of the static initializer {@link #from(RecordQueryPlan, RecordQueryPlan, KeyExpression)}.
     *
     * @param left the first plan to intersect
     * @param right the second plan to intersect
     * @param comparisonKey a key expression by which the results of both plans are ordered
     * @param reverse whether both plans return results in reverse (i.e., descending) order by the comparison key
     * @deprecated in favor of {@link #from(RecordQueryPlan, RecordQueryPlan, KeyExpression)}
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    public RecordQueryIntersectionPlan(@Nonnull RecordQueryPlan left, @Nonnull RecordQueryPlan right,
                                       @Nonnull KeyExpression comparisonKey, boolean reverse) {
        this(ImmutableList.of(SingleExpressionRef.of(left), SingleExpressionRef.of(right)), comparisonKey, reverse, false);
    }

    /**
     * Construct a new intersection of two or more compatibly-ordered plans. This constructor has been deprecated in favor
     * of the static initializer {@link #from(List, KeyExpression)}.
     *
     * @param children the list of plans to take the intersection of
     * @param comparisonKey a key expression by which the results of both plans are ordered
     * @param reverse whether all plans return results in reverse (i.e., descending) order by the comparison key
     * @deprecated in favor of {@link #from(List, KeyExpression)}
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    public RecordQueryIntersectionPlan(@Nonnull List<RecordQueryPlan> children,
                                       @Nonnull KeyExpression comparisonKey, boolean reverse) {
        this(children.stream().map(SingleExpressionRef::of).collect(Collectors.toList()), comparisonKey, reverse, false);
    }

    @SuppressWarnings("PMD.UnusedFormalParameter")
    private RecordQueryIntersectionPlan(@Nonnull List<ExpressionRef<RecordQueryPlan>> children,
                                        @Nonnull KeyExpression comparisonKey,
                                        boolean reverse, boolean ignoredTemporaryFlag) {
        this.children = children;
        this.comparisonKey = comparisonKey;
        this.reverse = reverse;

        final ImmutableList.Builder<ExpressionRef<? extends PlannerExpression>> expressionChildrenBuilder = ImmutableList.builder();
        expressionChildrenBuilder.addAll(children);
        expressionChildren = expressionChildrenBuilder.build();
    }

    @Nonnull
    @Override
    @SuppressWarnings("squid:S2095") // SonarQube doesn't realize that the intersection cursor is wrapped and returned
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                         @Nonnull EvaluationContext context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        final ExecuteProperties childExecuteProperties = executeProperties.clearSkipAndLimit();
        return IntersectionCursor.create(store, getComparisonKey(), reverse,
                children.stream()
                        .map(childPlan -> (Function<byte[], RecordCursor<FDBQueriedRecord<M>>>)
                                ((byte[] childContinuation) -> childPlan.get().execute(store, context, childContinuation, childExecuteProperties)))
                        .collect(Collectors.toList()),
                continuation).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Override
    public boolean hasRecordScan() {
        return getChildStream().anyMatch(RecordQueryPlan::hasRecordScan);
    }

    @Override
    public boolean hasFullRecordScan() {
        return getChildStream().anyMatch(RecordQueryPlan::hasFullRecordScan);
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return getChildStream().anyMatch(childPlan -> childPlan.hasIndexScan(indexName));
    }

    @Nonnull
    private Stream<RecordQueryPlan> getChildStream() {
        return children.stream().map(ExpressionRef::get);
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return children.stream().map(ExpressionRef::get).collect(Collectors.toList());
    }

    @Nonnull
    public KeyExpression getComparisonKey() {
        return comparisonKey;
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        HashSet<String> usedIndexes = new HashSet<>();
        for (ExpressionRef<RecordQueryPlan> childRef : children) {
            usedIndexes.addAll(childRef.get().getUsedIndexes());
        }
        return usedIndexes;
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return expressionChildren.iterator();
    }

    @Nonnull
    @Override
    public String toString() {
        return String.join(" " + INTERSECT + " ",
                getChildStream().map(RecordQueryPlan::toString).collect(Collectors.toList()));
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public boolean equalsWithoutChildren(@Nonnull PlannerExpression otherExpression) {
        if (!(otherExpression instanceof RecordQueryIntersectionPlan)) {
            return false;
        }
        final RecordQueryIntersectionPlan other = (RecordQueryIntersectionPlan) otherExpression;
        return reverse == other.reverse &&
               comparisonKey.equals(other.comparisonKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordQueryIntersectionPlan that = (RecordQueryIntersectionPlan) o;
        return reverse == that.reverse &&
                Objects.equals(Sets.newHashSet(getQueryPlanChildren()), Sets.newHashSet(that.getQueryPlanChildren())) &&
                Objects.equals(getComparisonKey(), that.getComparisonKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(Sets.newHashSet(getQueryPlanChildren()), getComparisonKey(), reverse);
    }

    @Override
    public int planHash() {
        return PlanHashable.planHash(getQueryPlanChildren()) + getComparisonKey().planHash() + (reverse ? 1 : 0);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_INTERSECTION);
        for (ExpressionRef<RecordQueryPlan> childRef : children) {
            childRef.get().logPlanStructure(timer);
        }
    }

    @Override
    public int getComplexity() {
        return 1 + getChildStream().mapToInt(RecordQueryPlan::getComplexity).sum();
    }

    @Override
    public int getRelationalChildCount() {
        return children.size();
    }

    /**
     * Construct a new union of two compatibly-ordered plans. The resulting plan will return all results that are
     * returned by both the {@code left} or {@code right} child plans. Each plan should return results in the same
     * order according to the provided {@code comparisonKey}. The two children should also either both return results
     * in forward order, or they should both return results in reverse order. (That is, {@code left.isReverse()} should
     * equal {@code right.isReverse()}.)
     *
     * @param left the first plan to intersect
     * @param right the second plan to intersect
     * @param comparisonKey a key expression by which the results of both plans are ordered
     * @return a new plan that will return the intersection of all results from both child plans
     */
    @Nonnull
    public static RecordQueryIntersectionPlan from(@Nonnull RecordQueryPlan left, @Nonnull RecordQueryPlan right,
                                                   @Nonnull KeyExpression comparisonKey) {
        if (left.isReverse() != right.isReverse()) {
            throw new RecordCoreArgumentException("left plan and right plan for union do not have same value for reverse field");
        }
        final List<ExpressionRef<RecordQueryPlan>> childRefs = ImmutableList.of(SingleExpressionRef.of(left), SingleExpressionRef.of(right));
        return new RecordQueryIntersectionPlan(childRefs, comparisonKey, left.isReverse(), false);
    }

    /**
     * Construct a new union of two or more compatibly-ordered plans. The resulting plan will return all results that are
     * returned by all of the child plans. Each plan should return results in the same order according to the provided
     * {@code comparisonKey}. The children should also either all return results in forward order, or they should all
     * return results in reverse order. (That is, {@link RecordQueryPlan#isReverse()} should return the same value
     * for each child.)
     *
     * @param children the list of plans to take the intersection of
     * @param comparisonKey a key expression by which the results of both plans are ordered
     * @return a new plan that will return the intersection of all results from both child plans
     */
    @Nonnull
    public static RecordQueryIntersectionPlan from(@Nonnull List<RecordQueryPlan> children, @Nonnull KeyExpression comparisonKey) {
        if (children.size() < 2) {
            throw new RecordCoreArgumentException("fewer than two children given to union plan");
        }
        boolean firstReverse = children.get(0).isReverse();
        if (!children.stream().allMatch(child -> child.isReverse() == firstReverse)) {
            throw new RecordCoreArgumentException("children of union plan do all have same value for reverse field");
        }
        final ImmutableList.Builder<ExpressionRef<RecordQueryPlan>> childRefsBuilder = ImmutableList.builder();
        for (RecordQueryPlan child : children) {
            childRefsBuilder.add(SingleExpressionRef.of(child));
        }
        return new RecordQueryIntersectionPlan(childRefsBuilder.build(), comparisonKey, firstReverse, false);
    }
}
