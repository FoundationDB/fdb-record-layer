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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.IntersectionCursor;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
@API(API.Status.INTERNAL)
public class RecordQueryIntersectionPlan implements RecordQueryPlanWithChildren, RecordQueryPlanWithRequiredFields {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Intersection-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryIntersectionPlan.class);

    private static final String INTERSECT = "∩"; // U+2229
    /* The current implementations of equals() and hashCode() treat RecordQueryIntersectionPlan as if it were isomorphic under
     * a reordering of its children. In particular, all of the tests assume that a RecordQueryIntersectionPlan with its children
     * reordered is identical. This is accurate in the current implementation (except that the continuation might no longer
     * be valid); if this ever changes, equals() and hashCode() must be updated.
     */
    @Nonnull
    private final List<Quantifier.Physical> quantifiers;
    @Nonnull
    private final KeyExpression comparisonKey;

    private final boolean reverse;

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
    @Deprecated
    public RecordQueryIntersectionPlan(@Nonnull RecordQueryPlan left, @Nonnull RecordQueryPlan right,
                                       @Nonnull KeyExpression comparisonKey, boolean reverse) {
        this(Quantifiers.fromPlans(ImmutableList.of(GroupExpressionRef.of(left), GroupExpressionRef.of(right))),
                comparisonKey,
                reverse,
                false);
    }

    /**
     * Construct a new intersection of two or more compatibly-ordered plans. This constructor has been deprecated in favor
     * of the static initializer {@link #from(List, KeyExpression)}.
     *
     * @param plans the list of plans to take the intersection of
     * @param comparisonKey a key expression by which the results of both plans are ordered
     * @param reverse whether all plans return results in reverse (i.e., descending) order by the comparison key
     * @deprecated in favor of {@link #from(List, KeyExpression)}
     */
    @Deprecated
    public RecordQueryIntersectionPlan(@Nonnull List<RecordQueryPlan> plans,
                                       @Nonnull KeyExpression comparisonKey, boolean reverse) {
        this(Quantifiers.fromPlans(plans.stream().map(GroupExpressionRef::of).collect(Collectors.toList())), comparisonKey, reverse, false);
    }

    @SuppressWarnings("PMD.UnusedFormalParameter")
    private RecordQueryIntersectionPlan(@Nonnull List<Quantifier.Physical> quantifiers,
                                        @Nonnull KeyExpression comparisonKey,
                                        boolean reverse,
                                        boolean ignoredTemporaryFlag) {
        this.quantifiers = ImmutableList.copyOf(quantifiers);
        this.comparisonKey = comparisonKey;
        this.reverse = reverse;
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
                quantifiers.stream()
                        .map(Quantifier.Physical::getRangesOverPlan)
                        .map(childPlan -> (Function<byte[], RecordCursor<FDBQueriedRecord<M>>>)
                                ((byte[] childContinuation) -> childPlan.execute(store, context, childContinuation, childExecuteProperties)))
                        .collect(Collectors.toList()),
                continuation).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    private Stream<RecordQueryPlan> getChildStream() {
        return quantifiers.stream().map(Quantifier.Physical::getRangesOverPlan);
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return quantifiers.stream().map(Quantifier.Physical::getRangesOverPlan).collect(Collectors.toList());
    }

    @Nonnull
    public KeyExpression getComparisonKey() {
        return comparisonKey;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return quantifiers;
    }

    @Nonnull
    @Override
    public String toString() {
        return getChildStream().map(RecordQueryPlan::toString).collect(Collectors.joining(" " + INTERSECT + " "));
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryIntersectionPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                    @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryIntersectionPlan(
                Quantifiers.narrow(Quantifier.Physical.class, rebasedQuantifiers),
                getComparisonKey(),
                isReverse(),
                false);
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
        final RecordQueryIntersectionPlan other = (RecordQueryIntersectionPlan) otherExpression;
        return reverse == other.reverse &&
               comparisonKey.equals(other.comparisonKey);
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.intersection(quantifiers.stream()
                .map(child -> child.getRangesOverPlan().getAvailableFields())
                .collect(Collectors.toList()));
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getComparisonKey(), reverse);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return PlanHashable.planHash(hashKind, getQueryPlanChildren()) + getComparisonKey().planHash(hashKind) + (reverse ? 1 : 0);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getQueryPlanChildren(), getComparisonKey(), reverse);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_INTERSECTION);
        for (final Quantifier.Physical quantifier : quantifiers) {
            quantifier.getRangesOverPlan().logPlanStructure(timer);
        }
    }

    @Override
    public int getComplexity() {
        return 1 + getChildStream().mapToInt(RecordQueryPlan::getComplexity).sum();
    }

    @Override
    public int getRelationalChildCount() {
        return quantifiers.size();
    }

    @Nonnull
    @Override
    public Set<KeyExpression> getRequiredFields() {
        return ImmutableSet.copyOf(getComparisonKey().normalizeKeyForPositions());
    }

    @Override
    public int maxCardinality(@Nonnull RecordMetaData metaData) {
        return getChildStream().map(p -> p.maxCardinality(metaData)).min(Integer::compare).orElse(UNKNOWN_MAX_CARDINALITY);
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
        final List<ExpressionRef<RecordQueryPlan>> childRefs = ImmutableList.of(GroupExpressionRef.of(left), GroupExpressionRef.of(right));
        return new RecordQueryIntersectionPlan(Quantifiers.fromPlans(childRefs), comparisonKey, left.isReverse(), false);
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
            childRefsBuilder.add(GroupExpressionRef.of(child));
        }
        return new RecordQueryIntersectionPlan(Quantifiers.fromPlans(childRefsBuilder.build()), comparisonKey, firstReverse, false);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.INTERSECTION_OPERATOR,
                        ImmutableList.of("COMPARE BY {{comparisonKey}}"),
                        ImmutableMap.of("comparisonKey", Attribute.gml(comparisonKey.toString()))),
                childGraphs);
    }
}
