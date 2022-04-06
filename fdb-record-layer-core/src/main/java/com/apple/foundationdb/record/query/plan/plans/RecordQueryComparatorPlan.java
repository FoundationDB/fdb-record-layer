/*
 * RecordQueryComparatorPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.ComparatorCursor;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link RecordQueryChooserPlanBase} that executes all child plans and compares their results using the provided
 * comparison key.
 * Results from child plans are assumed to all be with a compatible sort order.
 */
@API(API.Status.INTERNAL)
public class RecordQueryComparatorPlan extends RecordQueryChooserPlanBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryComparatorPlan.class);
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Comparator-Plan");

    @Nonnull
    private final KeyExpression comparisonKey;
    private final int referencePlanIndex;
    private final boolean abortOnComparisonFailure;

    private RecordQueryComparatorPlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                      @Nonnull final KeyExpression comparisonKey,
                                      final int referencePlanIndex,
                                      final boolean abortOnComparisonFailure) {
        super(quantifiers);
        this.comparisonKey = comparisonKey;
        this.referencePlanIndex = referencePlanIndex;
        this.abortOnComparisonFailure = abortOnComparisonFailure;
    }

    /**
     * Factory method to create a new instance of the Comparator plan.
     *
     * @param children the list of plans to compare results from
     * @param comparisonKey a key expression by which the results of all plans are compared by
     * @param referencePlanIndex the index of the "reference plan" (source of truth) among the given sub-plans
     *
     * @return a new plan that will compare all results from child plans
     */
    @Nonnull
    public static RecordQueryComparatorPlan from(@Nonnull List<? extends RecordQueryPlan> children,
                                                 @Nonnull KeyExpression comparisonKey,
                                                 final int referencePlanIndex) {
        return from(children, comparisonKey, referencePlanIndex, false);
    }

    /**
     * Factory method to create a new instance of the Comparator plan.
     *
     * @param children the list of plans to compare results from
     * @param comparisonKey a key expression by which the results of all plans are compared by
     * @param referencePlanIndex the index of the "reference plan" (source of truth) among the given sub-plans
     * @param abortOnComparisonFailure whether to abort the plan execution when encountering comparison failure. This parameter
     *        is used for testing since we don't want to fail normal plans on that kind of failure, but it allows a fast
     *        and easy-to-check condition for testing
     *
     * @return a new plan that will compare all results from child plans
     */
    @Nonnull
    @VisibleForTesting
    public static RecordQueryComparatorPlan from(@Nonnull List<? extends RecordQueryPlan> children,
                                                 @Nonnull KeyExpression comparisonKey,
                                                 final int referencePlanIndex,
                                                 final boolean abortOnComparisonFailure) {
        if (children.isEmpty()) {
            throw new RecordCoreArgumentException("Comparator plan should have at least one plan");
        }
        if ((referencePlanIndex < 0) || (referencePlanIndex >= children.size())) {
            throw new RecordCoreArgumentException("Reference Plan Index should be within the range of sub plans");
        }

        final ImmutableList.Builder<ExpressionRef<RecordQueryPlan>> childRefsBuilder = ImmutableList.builder();
        for (RecordQueryPlan child : children) {
            childRefsBuilder.add(GroupExpressionRef.of(child));
        }
        return new RecordQueryComparatorPlan(Quantifiers.fromPlans(childRefsBuilder.build()), comparisonKey, referencePlanIndex, abortOnComparisonFailure);
    }

    /**
     * Execute children and compare results.
     * @param store record store from which to fetch records
     * @param context evaluation context containing parameter bindings
     * @param continuation continuation from a previous execution of this same plan
     * @param executeProperties limits on execution
     * @param <M> the type of records in the store
     * @return {@link RecordCursor} that iterates through the results of the execution
     */
    @Nonnull
    @Override
    @SuppressWarnings("squid:S2095")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        // The child plans all keep their skip and limit - this way we can ensure that they all handle their skip and
        // limit correctly. The parent plan adds no skip and limit of its own - the reference plan is handling that.
        final ExecuteProperties parentExecuteProperties = executeProperties.clearSkipAndLimit();
        return ComparatorCursor.create(store, getComparisonKey(),
                        getChildren().stream()
                                .map(childPlan -> childCursorFunction(store, context, executeProperties, childPlan))
                                .collect(Collectors.toList()),
                        continuation,
                        referencePlanIndex,
                        abortOnComparisonFailure,
                        () -> toString(),
                        () -> planHash(PlanHashKind.STRUCTURAL_WITHOUT_LITERALS))
                .skipThenLimit(parentExecuteProperties.getSkip(), parentExecuteProperties.getReturnedRowLimit())
                .map(QueryResult::of);
    }

    /*
     * Return a function that creates a cursor for the given child plan using the provided continuation
     */
    @Nonnull
    private <M extends Message> Function<byte[], RecordCursor<FDBQueriedRecord<M>>> childCursorFunction(
            final @Nonnull FDBRecordStoreBase<M> store,
            final @Nonnull EvaluationContext context,
            final ExecuteProperties childExecuteProperties,
            final RecordQueryPlan childPlan) {
        return ((byte[] childContinuation) -> childPlan
                .executePlan(store, context, childContinuation, childExecuteProperties)
                .map(QueryResult::getQueriedRecord));
    }

    @Nonnull
    public KeyExpression getComparisonKey() {
        return comparisonKey;
    }

    @Nonnull
    @Override
    public String toString() {
        return "COMPARATOR OF " + getChildStream().map(RecordQueryPlan::toString).collect(Collectors.joining(" "));
    }

    @Nonnull
    @Override
    public RecordQueryComparatorPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                  @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryComparatorPlan(
                Quantifiers.narrow(Quantifier.Physical.class, rebasedQuantifiers),
                getComparisonKey(), referencePlanIndex, abortOnComparisonFailure);
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
        final RecordQueryComparatorPlan other = (RecordQueryComparatorPlan)otherExpression;
        return ((isReverse() == other.isReverse()) &&
                (referencePlanIndex == other.referencePlanIndex) &&
                comparisonKey.equals(other.comparisonKey));
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
        return Objects.hash(getComparisonKey(), referencePlanIndex, isReverse());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getChildren(), getComparisonKey(), referencePlanIndex, isReverse());
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_COMPARATOR);
        for (final Quantifier.Physical quantifier : quantifiers) {
            quantifier.getRangesOverPlan().logPlanStructure(timer);
        }
    }

    @Override
    public RecordQueryComparatorPlan strictlySorted() {
        return new RecordQueryComparatorPlan(Quantifiers.fromPlans(getChildStream()
                    .map(p -> GroupExpressionRef.of((RecordQueryPlan)p.strictlySorted())).collect(Collectors.toList())),
                comparisonKey, referencePlanIndex, abortOnComparisonFailure);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.COMPARATOR_OPERATOR,
                        List.of("COMPARE BY {{comparisonKey}}"),
                        Map.of("comparisonKey", Attribute.gml(comparisonKey.toString()))),
                childGraphs);
    }
}
