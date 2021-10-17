/*
 * RecordQueryNestedLoopJoinPlan.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.cursors.MapCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.predicates.Formatter;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A query plan that reconstructs records from the entries in a covering index.
 */
@API(API.Status.INTERNAL)
public class RecordQueryNestedLoopJoinPlan implements RecordQueryPlanWithChildren, RelationalExpressionWithChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Nested-Loop-Join-Plan");

    @Nonnull
    private final Quantifier.Physical outer;
    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final Supplier<List<? extends Value>> resultValuesSupplier;

    public RecordQueryNestedLoopJoinPlan(@Nonnull Quantifier.Physical outer,
                                         @Nonnull Quantifier.Physical inner) {
        this.outer = outer;
        this.inner = inner;
        this.resultValuesSupplier = Suppliers.memoize(this::computeResultValues);
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return RecordCursor.flatMapPipelined(
                outerContinuation ->
                        outer.getRangesOverPlan().executePlan(store, context, continuation, executeProperties),
                (outerResult, innerContinuation) -> {
                    final EvaluationContext nestedContext = context.withBinding(outer.getAlias(), outerResult);

                    return new MapCursor<>(inner.getRangesOverPlan().executePlan(store, nestedContext, continuation, executeProperties),
                            innerResult -> {
                                final ImmutableList.Builder<Object> resultBuilder = ImmutableList.builder();
                                resultBuilder.addAll(outerResult.getElements());
                                resultBuilder.addAll(innerResult.getElements());
                                return QueryResult.of(resultBuilder.build());
                            });
                },
                continuation,
                5);
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return ImmutableList.of(outer.getRangesOverPlan(), inner.getRangesOverPlan());
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Override
    public int getRelationalChildCount() {
        return 2;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryNestedLoopJoinPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap, @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryNestedLoopJoinPlan(rebasedQuantifiers.get(0).narrow(Quantifier.Physical.class),
                rebasedQuantifiers.get(0).narrow(Quantifier.Physical.class));
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Nonnull
    @Override
    public List<? extends Value> getResultValue() {
        return resultValuesSupplier.get();
    }

    @Nonnull
    private List<? extends Value> computeResultValues() {
        return ImmutableList.copyOf(Iterables.concat(outer.getFlowedValues(), inner.getFlowedValues()));
    }

    @Nonnull
    @Override
    public String toString() {
        return "flatmap(" + outer.getRangesOverPlan() + ", " + inner.getRangesOverPlan() + ")";
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        // TODO
        return RecordQueryPlanWithChildren.super.explain(formatter);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap aliasMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        return semanticEqualsForResults(otherExpression, aliasMap);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getResultValue());
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // nothing to increment
    }

    @Override
    public int getComplexity() {
        return outer.getRangesOverPlan().getComplexity() * inner.getRangesOverPlan().getComplexity();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, outer.getRangesOverPlan(), inner.getRangesOverPlan(), getResultValue());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(outer, inner);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("NLJN"),
                        ImmutableMap.of()),
                childGraphs);
    }
}
