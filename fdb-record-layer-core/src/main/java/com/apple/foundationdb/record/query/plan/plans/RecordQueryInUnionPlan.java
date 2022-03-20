/*
 * RecordQueryInUnionPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreException;
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
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A query plan that executes union over instantiations of a child plan for each of the elements of some {@code IN} list(s).
 */
@API(API.Status.INTERNAL)
public class RecordQueryInUnionPlan implements RecordQueryPlanWithChild, RecordQuerySetPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("In-Union-Plan");

    @Nonnull
    protected final Quantifier.Physical inner;
    @Nonnull
    private final List<? extends InSource> valuesSources;
    @Nonnull
    private final KeyExpression comparisonKey;
    private final boolean reverse;
    private final int maxNumberOfValuesAllowed;

    public RecordQueryInUnionPlan(@Nonnull final Quantifier.Physical inner,
                                  @Nonnull final List<? extends InSource> valuesSources,
                                  @Nonnull final KeyExpression comparisonKey, final boolean reverse,
                                  final int maxNumberOfValuesAllowed) {
        this.inner = inner;
        this.valuesSources = valuesSources;
        this.comparisonKey = comparisonKey;
        this.reverse = reverse;
        this.maxNumberOfValuesAllowed = maxNumberOfValuesAllowed;
    }

    public RecordQueryInUnionPlan(@Nonnull final RecordQueryPlan inner,
                                  @Nonnull final List<? extends InSource> valuesSources,
                                  @Nonnull final KeyExpression comparisonKey, final boolean reverse,
                                  final int maxNumberOfValuesAllowed) {
        this(Quantifier.physical(GroupExpressionRef.of(inner)), valuesSources, comparisonKey, reverse, maxNumberOfValuesAllowed);
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
    public RecordQuerySetPlan withChildrenReferences(@Nonnull final List<? extends ExpressionRef<? extends RecordQueryPlan>> newChildren) {
        return withChild(Iterables.getOnlyElement(newChildren).get());
    }

    @Override
    public boolean isDynamic() {
        return true;
    }

    @Nonnull
    public List<? extends InSource> getInSources() {
        return valuesSources;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        int size = getValuesSize(context);
        if (size > maxNumberOfValuesAllowed) {
            throw new RecordCoreException("too many IN values").addLogInfo("size", size);
        }
        if (size == 0) {
            return RecordCursor.empty();
        }
        final RecordQueryPlan childPlan = getInnerPlan();
        if (size == 1) {
            final EvaluationContext childContext = getValuesContexts(context).get(0);
            return childPlan.executePlan(store, childContext, continuation, executeProperties);
        }
        final ExecuteProperties childExecuteProperties;
        // Can pass the limit down to all sides, since that is the most we'll take total.
        if (executeProperties.getSkip() > 0) {
            childExecuteProperties = executeProperties.clearSkipAndAdjustLimit();
        } else {
            childExecuteProperties = executeProperties;
        }
        final List<Function<byte[], RecordCursor<FDBQueriedRecord<M>>>> childCursorFunctions = getValuesContexts(context).stream()
                .map(childContext -> (Function<byte[], RecordCursor<FDBQueriedRecord<M>>>)childContinuation -> childPlan.execute(store, childContext, childContinuation, childExecuteProperties))
                .collect(Collectors.toList());
        return UnionCursor.create(store, comparisonKey, reverse, childCursorFunctions, continuation)
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit())
                .map(QueryResult::of);
    }

    @Nonnull
    public RecordQueryPlan getInnerPlan() {
        return inner.getRangesOverPlan();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInnerPlan();
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_IN_UNION);
        getInnerPlan().logPlanStructure(timer);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        final PlannerGraph.Node root =
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.IN_UNION_OPERATOR,
                        ImmutableList.of("COMPARE BY {{comparisonKey}}"),
                        ImmutableMap.of("comparisonKey", Attribute.gml(comparisonKey.toString())));
        final PlannerGraph graphForInner = Iterables.getOnlyElement(childGraphs);
        final PlannerGraph.DataNodeWithInfo valuesNode =
                new PlannerGraph.DataNodeWithInfo(NodeInfo.VALUES_DATA,
                        getResultType(),
                        ImmutableList.of("VALUES({{values}}"),
                        ImmutableMap.of("values",
                                Attribute.gml(Objects.requireNonNull(valuesSources).stream()
                                        .map(String::valueOf)
                                        .map(Attribute::gml)
                                        .collect(ImmutableList.toImmutableList()))));
        final PlannerGraph.Edge fromValuesEdge = new PlannerGraph.Edge();
        return PlannerGraph.builder(root)
                .addGraph(graphForInner)
                .addNode(valuesNode)
                .addEdge(valuesNode, root, fromValuesEdge)
                .addEdge(graphForInner.getRoot(), root, new PlannerGraph.Edge(ImmutableSet.of(fromValuesEdge)))
                .build();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RelationalExpressionWithChildren rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap, @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryInUnionPlan(Iterables.getOnlyElement(rebasedQuantifiers).narrow(Quantifier.Physical.class),
                valuesSources, comparisonKey, reverse, maxNumberOfValuesAllowed);
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public RecordQueryInUnionPlan withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryInUnionPlan(child, valuesSources, comparisonKey, reverse, maxNumberOfValuesAllowed);
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
        final RecordQueryInUnionPlan other = (RecordQueryInUnionPlan)otherExpression;
        return valuesSources.equals(other.valuesSources);
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
        return Objects.hash(valuesSources);
    }

    @Override
    public int planHash(@Nonnull PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getInnerPlan(), valuesSources);
    }

    @Nonnull
    @Override
    public String toString() {
        return valuesSources.stream().map(Object::toString).collect(Collectors.joining(", ", "∪(", ") ")) +
               getChild();
    }

    @Override
    public int getComplexity() {
        int complexity = getInnerPlan().getComplexity();
        for (InSource values : valuesSources) {
            if (values instanceof InValuesSource) {
                complexity *= values.size(EvaluationContext.EMPTY);
            }
        }
        return complexity;
    }

    protected int getValuesSize(@Nonnull EvaluationContext context) {
        int size = 1;
        for (InSource values : valuesSources) {
            size *= values.size(context);
        }
        return size;
    }

    @Nonnull
    protected List<EvaluationContext> getValuesContexts(@Nonnull EvaluationContext context) {
        List<EvaluationContext> parents = Collections.singletonList(context);
        for (InSource values : valuesSources) {
            final List<EvaluationContext> children = new ArrayList<>();
            for (EvaluationContext parent : parents) {
                for (Object value : values.getValues(parent)) {
                    children.add(parent.withBinding(values.getBindingName(), value));
                }
            }
            parents = children;
        }
        return parents;
    }
}
