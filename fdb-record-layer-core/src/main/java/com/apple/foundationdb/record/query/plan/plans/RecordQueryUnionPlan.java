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
import com.apple.foundationdb.record.query.plan.HeuristicPlanner;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryUnionPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.UnionCursor;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A query plan that executes by taking the union of records from two or more compatibly-sorted child plans.
 * To work, each child cursor must order its children the same way according to the comparison key.
 */
@API(API.Status.INTERNAL)
@SuppressWarnings({"squid:S1206", "squid:S2160", "PMD.OverrideBothEqualsAndHashcode"})
public abstract class RecordQueryUnionPlan extends RecordQueryUnionPlanBase {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Union-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUnionPlan.class);

    private static final StoreTimer.Count PLAN_COUNT = FDBStoreTimer.Counts.PLAN_UNION;

    private final ComparisonKeyFunction comparisonKeyFunction;
    protected final boolean showComparisonKey;

    protected RecordQueryUnionPlan(final PlanSerializationContext serializationContext,
                                   final PRecordQueryUnionPlan recordQueryUnionPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryUnionPlanProto.getSuper()));
        Verify.verify(recordQueryUnionPlanProto.hasShowComparisonKey());
        this.comparisonKeyFunction = ComparisonKeyFunction.fromComparisonKeyFunctionProto(serializationContext,
                Objects.requireNonNull(recordQueryUnionPlanProto.getComparisonKeyFunction()));
        this.showComparisonKey = recordQueryUnionPlanProto.getShowComparisonKey();
    }

    protected RecordQueryUnionPlan(final List<Quantifier.Physical> quantifiers,
                                   final ComparisonKeyFunction comparisonKeyFunction,
                                   final boolean reverse,
                                   final boolean showComparisonKey) {
        super(quantifiers, reverse);
        this.comparisonKeyFunction = comparisonKeyFunction;
        this.showComparisonKey = showComparisonKey;
    }

    @Override
    <M extends Message> RecordCursor<QueryResult> createUnionCursor(FDBRecordStoreBase<M> store,
                                                                    EvaluationContext context,
                                                                    List<Function<byte[], RecordCursor<QueryResult>>> childCursorFunctions,
                                                                    @Nullable byte[] continuation) {
        return UnionCursor.create(comparisonKeyFunction.apply(store, context),
                isReverse(),
                childCursorFunctions,
                continuation,
                store.getTimer());
    }

    public ComparisonKeyFunction getComparisonKeyFunction() {
        return comparisonKeyFunction;
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(RelationalExpression otherExpression,
                                         final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (!(super.equalsWithoutChildren(otherExpression, equivalencesMap))) {
            return false;
        }
        final RecordQueryUnionPlan other = (RecordQueryUnionPlan)otherExpression;
        return comparisonKeyFunction.equals(other.comparisonKeyFunction);
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(super.computeHashCodeWithoutChildren(), comparisonKeyFunction, isReverse());
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return super.basePlanHash(mode, BASE_HASH) + comparisonKeyFunction.planHash(mode);
            case FOR_CONTINUATION:
                return super.basePlanHash(mode, BASE_HASH, comparisonKeyFunction);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String getDelimiter() {
        return " " + UNION + (showComparisonKey ? comparisonKeyFunction.toString() : "") + " ";
    }

    @Override
    StoreTimer.Count getPlanCount() {
        return PLAN_COUNT;
    }

    protected PRecordQueryUnionPlan toRecordQueryUnionPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryUnionPlan.newBuilder()
                .setSuper(toRecordQueryUnionPlanBaseProto(serializationContext))
                .setComparisonKeyFunction(comparisonKeyFunction.toComparisonKeyFunctionProto(serializationContext))
                .setShowComparisonKey(showComparisonKey)
                .build();
    }

    public static RecordQueryUnionOnKeyExpressionPlan fromQuantifiers(List<Quantifier.Physical> quantifiers,
                                                                      final KeyExpression comparisonKey,
                                                                      boolean showComparisonKey) {
        return new RecordQueryUnionOnKeyExpressionPlan(quantifiers,
                comparisonKey,
                Quantifiers.isReversed(quantifiers),
                showComparisonKey);
    }

    public static RecordQueryUnionOnValuesPlan fromQuantifiers(List<Quantifier.Physical> quantifiers,
                                                               final List<ProvidedOrderingPart> comparisonKeyOrderingParts,
                                                               final boolean isReverse,
                                                               boolean showComparisonKey) {
        return RecordQueryUnionOnValuesPlan.union(quantifiers,
                comparisonKeyOrderingParts,
                isReverse,
                showComparisonKey);
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
    @HeuristicPlanner
    public static RecordQueryUnionOnKeyExpressionPlan from(RecordQueryPlan left, RecordQueryPlan right,
                                                           KeyExpression comparisonKey, boolean showComparisonKey) {
        Debugger.verifyHeuristicPlanner();
        if (left.isReverse() != right.isReverse()) {
            throw new RecordCoreArgumentException("left plan and right plan for union do not have same value for reverse field");
        }
        final List<Reference> childRefs = ImmutableList.of(Reference.plannedOf(left), Reference.plannedOf(right));
        return new RecordQueryUnionOnKeyExpressionPlan(Quantifiers.fromPlans(childRefs),
                comparisonKey,
                left.isReverse(),
                showComparisonKey);
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
    @HeuristicPlanner
    public static RecordQueryUnionOnKeyExpressionPlan from(List<? extends RecordQueryPlan> children,
                                                           KeyExpression comparisonKey,
                                                           boolean showComparisonKey) {
        Debugger.verifyHeuristicPlanner();
        if (children.size() < 2) {
            throw new RecordCoreArgumentException("fewer than two children given to union plan");
        }
        boolean firstReverse = children.get(0).isReverse();
        if (!children.stream().allMatch(child -> child.isReverse() == firstReverse)) {
            throw new RecordCoreArgumentException("children of union plan do all have same value for reverse field");
        }
        final ImmutableList.Builder<Reference> childRefsBuilder = ImmutableList.builder();
        for (RecordQueryPlan child : children) {
            childRefsBuilder.add(Reference.plannedOf(child));
        }
        return new RecordQueryUnionOnKeyExpressionPlan(Quantifiers.fromPlans(childRefsBuilder.build()),
                comparisonKey,
                firstReverse,
                showComparisonKey);
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.UNION_OPERATOR,
                        ImmutableList.of("COMPARE BY {{comparisonKeyFunction}}"),
                        ImmutableMap.of("comparisonKeyFunction", Attribute.gml(comparisonKeyFunction.toString()))),
                childGraphs);
    }
}
