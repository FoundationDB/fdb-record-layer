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
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryIntersectionPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.IntersectionCursor;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
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
public abstract class RecordQueryIntersectionPlan implements RecordQueryPlanWithChildren, RecordQuerySetPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Intersection-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryIntersectionPlan.class);

    /* The current implementations of equals() and hashCode() treat RecordQueryIntersectionPlan as if it were isomorphic under
     * a reordering of its children. In particular, all of the tests assume that a RecordQueryIntersectionPlan with its children
     * reordered is identical. This is accurate in the current implementation (except that the continuation might no longer
     * be valid); if this ever changes, equals() and hashCode() must be updated.
     */
    @Nonnull
    private final List<Quantifier.Physical> quantifiers;
    @Nonnull
    private final ComparisonKeyFunction comparisonKeyFunction;

    protected final boolean reverse;

    @Nonnull
    private final Value resultValue;

    protected RecordQueryIntersectionPlan(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PRecordQueryIntersectionPlan recordQueryIntersectionPlanProto) {
        Verify.verify(recordQueryIntersectionPlanProto.hasReverse());
        final ImmutableList.Builder<Quantifier.Physical> quantifiersBuilder = ImmutableList.builder();
        for (int i = 0; i < recordQueryIntersectionPlanProto.getQuantifiersCount(); i ++) {
            quantifiersBuilder.add(Quantifier.Physical.fromProto(serializationContext, recordQueryIntersectionPlanProto.getQuantifiers(i)));
        }
        this.quantifiers = quantifiersBuilder.build();
        this.comparisonKeyFunction = ComparisonKeyFunction.fromComparisonKeyFunctionProto(serializationContext, Objects.requireNonNull(recordQueryIntersectionPlanProto.getComparisonKeyFunction()));
        this.reverse = recordQueryIntersectionPlanProto.getReverse();
        this.resultValue = RecordQuerySetPlan.mergeValues(quantifiers);
    }

    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected RecordQueryIntersectionPlan(@Nonnull List<Quantifier.Physical> quantifiers,
                                          @Nonnull ComparisonKeyFunction comparisonKeyFunction,
                                          boolean reverse) {
        this.quantifiers = ImmutableList.copyOf(quantifiers);
        this.comparisonKeyFunction = comparisonKeyFunction;
        this.reverse = reverse;
        this.resultValue = RecordQuerySetPlan.mergeValues(quantifiers);
    }

    @Nonnull
    public ComparisonKeyFunction getComparisonKeyFunction() {
        return comparisonKeyFunction;
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final ExecuteProperties childExecuteProperties = executeProperties.clearSkipAndLimit();
        return IntersectionCursor.create(
                        comparisonKeyFunction.apply(store, context),
                        reverse,
                        quantifiers.stream()
                                .map(Quantifier.Physical::getRangesOverPlan)
                                .map(childPlan -> (Function<byte[], RecordCursor<QueryResult>>)
                                        ((byte[] childContinuation) -> childPlan
                                                .executePlan(store, context, childContinuation, childExecuteProperties)))
                                .collect(Collectors.toList()),
                        continuation,
                        store.getTimer())
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
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
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return quantifiers;
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
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
               comparisonKeyFunction.equals(other.comparisonKeyFunction);
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
        return Objects.hash(comparisonKeyFunction, reverse);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return PlanHashable.planHash(mode, getQueryPlanChildren()) + comparisonKeyFunction.planHash(mode) + (reverse ? 1 : 0);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getQueryPlanChildren(), comparisonKeyFunction, reverse);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
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

    @Override
    public int maxCardinality(@Nonnull RecordMetaData metaData) {
        return getChildStream().map(p -> p.maxCardinality(metaData)).min(Integer::compare).orElse(UNKNOWN_MAX_CARDINALITY);
    }

    @Override
    public boolean isStrictlySorted() {
        return getChildren().stream().allMatch(RecordQueryPlan::isStrictlySorted);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.INTERSECTION_OPERATOR,
                        ImmutableList.of("COMPARE BY {{comparisonKeyFunction}}"),
                        ImmutableMap.of("comparisonKeyFunction", Attribute.gml(comparisonKeyFunction.toString()))),
                childGraphs);
    }

    @Nonnull
    protected PRecordQueryIntersectionPlan toRecordQueryIntersectionPlan(@Nonnull final PlanSerializationContext serializationContext) {
        final PRecordQueryIntersectionPlan.Builder builder = PRecordQueryIntersectionPlan.newBuilder();
        for (final Quantifier.Physical quantifier : quantifiers) {
            builder.addQuantifiers(quantifier.toProto(serializationContext));
        }
        builder.setComparisonKeyFunction(comparisonKeyFunction.toComparisonKeyFunctionProto(serializationContext))
                .setReverse(reverse);
        return builder.build();
    }

    @Nonnull
    public static RecordQueryIntersectionOnKeyExpressionPlan fromQuantifiers(@Nonnull final List<Quantifier.Physical> quantifiers,
                                                                             @Nonnull final KeyExpression comparisonKey) {
        return new RecordQueryIntersectionOnKeyExpressionPlan(quantifiers,
                comparisonKey,
                Quantifiers.isReversed(quantifiers));
    }

    @Nonnull
    public static RecordQueryIntersectionOnValuesPlan fromQuantifiers(@Nonnull final List<Quantifier.Physical> quantifiers,
                                                                      @Nonnull final List<? extends Value> comparisonKeyValues,
                                                                      final boolean isReverse) {
        return RecordQueryIntersectionOnValuesPlan.intersection(quantifiers,
                comparisonKeyValues,
                isReverse);
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
    public static RecordQueryIntersectionOnKeyExpressionPlan from(@Nonnull final RecordQueryPlan left,
                                                                  @Nonnull final RecordQueryPlan right,
                                                                  @Nonnull final KeyExpression comparisonKey) {
        if (left.isReverse() != right.isReverse()) {
            throw new RecordCoreArgumentException("left plan and right plan for union do not have same value for reverse field");
        }
        final List<Reference> childRefs = ImmutableList.of(Reference.of(left), Reference.of(right));
        return new RecordQueryIntersectionOnKeyExpressionPlan(Quantifiers.fromPlans(childRefs), comparisonKey, left.isReverse());
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
    public static RecordQueryIntersectionOnKeyExpressionPlan from(@Nonnull final List<? extends RecordQueryPlan> children,
                                                                  @Nonnull final KeyExpression comparisonKey) {
        if (children.size() < 2) {
            throw new RecordCoreArgumentException("fewer than two children given to union plan");
        }
        boolean firstReverse = children.get(0).isReverse();
        if (!children.stream().allMatch(child -> child.isReverse() == firstReverse)) {
            throw new RecordCoreArgumentException("children of union plan do all have same value for reverse field");
        }
        final ImmutableList.Builder<Reference> childRefsBuilder = ImmutableList.builder();
        for (RecordQueryPlan child : children) {
            childRefsBuilder.add(Reference.of(child));
        }
        return new RecordQueryIntersectionOnKeyExpressionPlan(Quantifiers.fromPlans(childRefsBuilder.build()), comparisonKey, firstReverse);
    }
}
