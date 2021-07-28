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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.UnionCursor;
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

/**
 * A query plan that executes by taking the union of records from two or more compatibly-sorted child plans.
 * To work, each child cursor must order its children the same way according to the comparison key.
 */
@API(API.Status.INTERNAL)
@SuppressWarnings({"squid:S1206", "squid:S2160", "PMD.OverrideBothEqualsAndHashcode"})
public class RecordQueryUnionPlan extends RecordQueryUnionPlanBase {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Union-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUnionPlan.class);

    private static final StoreTimer.Count PLAN_COUNT = FDBStoreTimer.Counts.PLAN_UNION;

    @Nonnull
    private final KeyExpression comparisonKey;
    private final boolean showComparisonKey;

    @SuppressWarnings("PMD.UnusedFormalParameter")
    private RecordQueryUnionPlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                 @Nonnull final KeyExpression comparisonKey,
                                 final boolean reverse,
                                 final boolean showComparisonKey,
                                 boolean ignoredTemporaryFlag) {
        super(quantifiers, reverse);
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

    @Nonnull
    @Override
    public Set<KeyExpression> getRequiredFields() {
        return ImmutableSet.copyOf(comparisonKey.normalizeKeyForPositions());
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryUnionPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                             @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryUnionPlan(Quantifiers.narrow(Quantifier.Physical.class, rebasedQuantifiers),
                getComparisonKey(),
                isReverse(),
                showComparisonKey,
                false);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (!(super.equalsWithoutChildren(otherExpression, equivalencesMap))) {
            return false;
        }
        final RecordQueryUnionPlan other = (RecordQueryUnionPlan)otherExpression;
        return comparisonKey.equals(other.comparisonKey);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(super.hashCodeWithoutChildren(), getComparisonKey(), isReverse());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return super.basePlanHash(hashKind, BASE_HASH) + getComparisonKey().planHash(hashKind);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return super.basePlanHash(hashKind, BASE_HASH, getComparisonKey());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
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

    @Nonnull
    public static RecordQueryUnionPlan fromQuantifiers(@Nonnull List<Quantifier.Physical> quantifiers,
                                                       @Nonnull final KeyExpression comparisonKey,
                                                       boolean showComparisonKey) {
        return new RecordQueryUnionPlan(quantifiers,
                comparisonKey,
                isReversed(quantifiers),
                showComparisonKey,
                false);
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
        return new RecordQueryUnionPlan(Quantifiers.fromPlans(childRefs),
                comparisonKey,
                left.isReverse(),
                showComparisonKey,
                false);
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
    public static RecordQueryUnionPlan from(@Nonnull List<? extends RecordQueryPlan> children, @Nonnull KeyExpression comparisonKey,
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
        return new RecordQueryUnionPlan(Quantifiers.fromPlans(childRefsBuilder.build()),
                comparisonKey,
                firstReverse,
                showComparisonKey,
                false);
    }

    @Nonnull
    @Override
    public RecordQuerySetPlan withChildrenReferences(@Nonnull final List<? extends ExpressionRef<? extends RecordQueryPlan>> newChildren) {
        return new RecordQueryUnionPlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                comparisonKey,
                isReverse(),
                showComparisonKey,
                false);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.UNION_OPERATOR,
                        ImmutableList.of("COMPARE BY {{comparisonKey}}"),
                        ImmutableMap.of("comparisonKey", Attribute.gml(comparisonKey.toString()))),
                childGraphs);
    }
}
