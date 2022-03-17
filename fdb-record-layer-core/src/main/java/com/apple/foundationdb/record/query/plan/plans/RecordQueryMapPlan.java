/*
 * RecordQueryMapPlan.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A query plan that applies the values it contains over the incoming ones. In a sense, this is similar to the {@code Stream.map()}
 * method: Mapping one {@link Value} to another.
 */
@API(API.Status.INTERNAL)
public class RecordQueryMapPlan implements RecordQueryPlanWithChild, RelationalExpressionWithChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Map-Plan");

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final List<? extends Value> resultValues;

    public RecordQueryMapPlan(@Nonnull Quantifier.Physical inner,
                              @Nonnull List<? extends Value> resultValues) {
        this.inner = inner;
        this.resultValues = ImmutableList.copyOf(resultValues);
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return getChild().executePlan(store, context, continuation, executeProperties)
                .map(innerResult -> {
                    final EvaluationContext nestedContext = context.withBinding(inner.getAlias(), innerResult);
                    // Apply (map) each value to the incoming record
                    return resultValues.stream().map(value -> value.eval(store, nestedContext, null, null)).collect(Collectors.toList());
                })
                .map(QueryResult::of);
    }

    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryMapPlan(Quantifier.physical(GroupExpressionRef.of(child), inner.getAlias()), resultValues);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return resultValues.stream().flatMap(value -> value.getCorrelatedToWithoutChildren().stream()).collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public RecordQueryMapPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap, @Nonnull final List<Quantifier> rebasedQuantifiers) {
        Verify.verify(rebasedQuantifiers.size() == 1);
        final List<? extends Value> rebasedResultValues = resultValues.stream().map(value -> value.rebase(translationMap)).collect(Collectors.toList());
        return new RecordQueryMapPlan(Iterables.getOnlyElement(rebasedQuantifiers).narrow(Quantifier.Physical.class), rebasedResultValues);
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
    }

    @Override
    public boolean isStrictlySorted() {
        return false;
    }

    @Override
    public RecordQueryMapPlan strictlySorted() {
        return this;
    }

    @Nonnull
    @Override
    public List<? extends Value> getResultValues() {
        return resultValues;
    }

    @Nonnull
    @Override
    public String toString() {
        return "map(" + getChild() + "[" + resultValues + "])";
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
        return Objects.hash(getResultValues());
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // nothing to increment
    }

    @Override
    public int getComplexity() {
        return getChild().getComplexity();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getChild(), getResultValues());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("MAP {{expr}}"),
                        ImmutableMap.of("expr", Attribute.gml(getResultValues().toString()))),
                childGraphs);
    }
}
