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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryInUnionPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.UnionCursor;
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
public abstract class RecordQueryInUnionPlan implements RecordQueryPlanWithChild, RecordQuerySetPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("In-Union-Plan");

    @Nonnull
    protected final Quantifier.Physical inner;
    @Nonnull
    private final List<? extends InSource> inSources;
    @Nonnull
    private final ComparisonKeyFunction comparisonKeyFunction;
    protected final boolean reverse;
    protected final int maxNumberOfValuesAllowed;
    @Nonnull
    protected final Bindings.Internal internal;

    protected RecordQueryInUnionPlan(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PRecordQueryInUnionPlan recordQueryInUnionPlanProto) {
        Verify.verify(recordQueryInUnionPlanProto.hasReverse());
        Verify.verify(recordQueryInUnionPlanProto.hasMaxNumberOfValuesAllowed());

        Bindings.Internal internal = Bindings.Internal.fromProto(serializationContext, Objects.requireNonNull(recordQueryInUnionPlanProto.getInternal()));
        Verify.verify(internal == Bindings.Internal.IN || internal == Bindings.Internal.CORRELATION);

        final ImmutableList.Builder<InSource> inSourcesBuilder = ImmutableList.builder();
        for (int i = 0; i < recordQueryInUnionPlanProto.getInSourcesCount(); i ++) {
            inSourcesBuilder.add(InSource.fromInSourceProto(serializationContext, recordQueryInUnionPlanProto.getInSources(i)));
        }

        this.inner = Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryInUnionPlanProto.getInner()));
        this.inSources = inSourcesBuilder.build();
        this.comparisonKeyFunction = ComparisonKeyFunction.fromComparisonKeyFunctionProto(serializationContext, Objects.requireNonNull(recordQueryInUnionPlanProto.getComparisonKeyFunction()));
        this.reverse = recordQueryInUnionPlanProto.getReverse();
        this.maxNumberOfValuesAllowed = recordQueryInUnionPlanProto.getMaxNumberOfValuesAllowed();
        this.internal = internal;
    }

    protected RecordQueryInUnionPlan(@Nonnull final Quantifier.Physical inner,
                                     @Nonnull final List<? extends InSource> inSources,
                                     @Nonnull final ComparisonKeyFunction comparisonKeyFunction,
                                     final boolean reverse,
                                     final int maxNumberOfValuesAllowed,
                                     @Nonnull final Bindings.Internal internal) {
        Verify.verify(internal == Bindings.Internal.IN || internal == Bindings.Internal.CORRELATION);
        this.inner = inner;
        this.inSources = inSources;
        this.comparisonKeyFunction = comparisonKeyFunction;
        this.reverse = reverse;
        this.maxNumberOfValuesAllowed = maxNumberOfValuesAllowed;
        this.internal = internal;
    }

    @Nonnull
    public ComparisonKeyFunction getComparisonKeyFunction() {
        return comparisonKeyFunction;
    }
    
    @Override
    public boolean isDynamic() {
        return true;
    }

    @Nonnull
    public List<? extends InSource> getInSources() {
        return inSources;
    }

    @Nonnull
    public CorrelationIdentifier getInAlias(@Nonnull final InSource inSource) {
        return CorrelationIdentifier.of(internal.identifier(inSource.getBindingName()));
    }

    @SuppressWarnings("resource")
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
        final List<Function<byte[], RecordCursor<QueryResult>>> childCursorFunctions = getValuesContexts(context).stream()
                .map(childContext -> (Function<byte[], RecordCursor<QueryResult>>)childContinuation -> childPlan.executePlan(store, childContext, childContinuation, childExecuteProperties))
                .collect(Collectors.toList());
        return UnionCursor.create(comparisonKeyFunction.apply(store, context), reverse, childCursorFunctions, continuation, store.getTimer())
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
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
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();

        final var inAliases = getInSources()
                .stream()
                .map(inSource -> CorrelationIdentifier.of(Bindings.Internal.CORRELATION.identifier(inSource.getBindingName())))
                .collect(ImmutableSet.toImmutableSet());
        inner.getCorrelatedTo()
                .stream()
                // filter out the correlations that are satisfied by this plan
                .filter(alias -> !inAliases.contains(alias))
                .forEach(builder::add);

        return builder.build();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    public boolean canCorrelate() {
        return true;
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        final PlannerGraph.Node root =
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.IN_UNION_OPERATOR,
                        ImmutableList.of("COMPARE BY {{comparisonKey}}"),
                        ImmutableMap.of("comparisonKey", Attribute.gml(comparisonKeyFunction.toString())));
        final PlannerGraph graphForInner = Iterables.getOnlyElement(childGraphs);
        final PlannerGraph.DataNodeWithInfo valuesNode =
                new PlannerGraph.DataNodeWithInfo(NodeInfo.VALUES_DATA,
                        getResultType(),
                        ImmutableList.of("VALUES({{values}}"),
                        ImmutableMap.of("values",
                                Attribute.gml(Objects.requireNonNull(inSources).stream()
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
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    public Quantifier.Physical getInner() {
        return inner;
    }

    @Nonnull
    @Override
    public abstract RecordQueryInUnionPlan withChild(@Nonnull Reference childRef);

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
        final RecordQueryInUnionPlan other = (RecordQueryInUnionPlan)otherExpression;
        return inSources.equals(other.inSources) && comparisonKeyFunction.equals(other.comparisonKeyFunction);
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
        return Objects.hash(inSources, comparisonKeyFunction);
    }

    @Override
    public int planHash(@Nonnull PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, getInnerPlan(), inSources, comparisonKeyFunction);
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Override
    public int getComplexity() {
        int complexity = getInnerPlan().getComplexity();
        for (InSource values : inSources) {
            if (values instanceof InValuesSource) {
                complexity *= values.size(EvaluationContext.EMPTY);
            }
        }
        return complexity;
    }

    protected int getValuesSize(@Nonnull EvaluationContext context) {
        int size = 1;
        for (InSource values : inSources) {
            size *= values.size(context);
        }
        return size;
    }

    @Nonnull
    protected List<EvaluationContext> getValuesContexts(@Nonnull EvaluationContext context) {
        List<EvaluationContext> parents = Collections.singletonList(context);
        for (InSource values : inSources) {
            final List<EvaluationContext> children = new ArrayList<>();
            for (EvaluationContext parent : parents) {
                for (Object value : values.getValues(parent)) {
                    final Object bindingValue = (internal == Bindings.Internal.IN) ? value : QueryResult.ofComputed(value);
                    children.add(parent.withBinding(values.getBindingName(), bindingValue));
                }
            }
            parents = children;
        }
        return parents;
    }

    @Nonnull
    protected PRecordQueryInUnionPlan toRecordQueryInUnionPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PRecordQueryInUnionPlan.Builder builder = PRecordQueryInUnionPlan.newBuilder()
                .setInner(inner.toProto(serializationContext));
        for (final InSource inSource : inSources) {
            builder.addInSources(inSource.toInSourceProto(serializationContext));
        }
        return builder.setComparisonKeyFunction(comparisonKeyFunction.toComparisonKeyFunctionProto(serializationContext))
                .setReverse(reverse)
                .setMaxNumberOfValuesAllowed(maxNumberOfValuesAllowed)
                .setInternal(internal.toProto(serializationContext))
                .build();
    }

    /**
     * Construct a new in-union plan based on an existing physical quantifier.
     *
     * @param inner the input/inner plan to this in-union
     * @param inSources a list of outer in-sources
     * @param comparisonKey a key expression by which the results of both plans are ordered
     * @param maxNumberOfValuesAllowed maximum number of parallel legs of this in-union
     * @param internal indicator if bindings are modelled using correlation or old-style in-bindings
     * @return a new plan that will return the union of all results from both child plans
     */
    @Nonnull
    public static RecordQueryInUnionOnKeyExpressionPlan from(@Nonnull final Quantifier.Physical inner,
                                                             @Nonnull final List<? extends InSource> inSources,
                                                             @Nonnull KeyExpression comparisonKey,
                                                             final int maxNumberOfValuesAllowed,
                                                             @Nonnull final Bindings.Internal internal) {
        return new RecordQueryInUnionOnKeyExpressionPlan(inner,
                inSources,
                comparisonKey,
                Quantifiers.isReversed(ImmutableList.of(inner)),
                maxNumberOfValuesAllowed,
                internal);
    }

    /**
     * Construct a new in-union plan based on an existing physical quantifier.
     *
     * @param inner the input/inner plan to this in-union
     * @param inSources a list of outer in-sources
     * @param comparisonKeyValues values by which the results of both plans are ordered
     * @param isReverse indicator if {@code comparisonKeyValues} should be considered reversed (inverted).
     * @param maxNumberOfValuesAllowed maximum number of parallel legs of this in-union
     * @param internal indicator if bindings are modelled using correlation or old-style in-bindings
     * @return a new plan that will return the union of all results from both child plans
     */
    @Nonnull
    public static RecordQueryInUnionOnValuesPlan from(@Nonnull final Quantifier.Physical inner,
                                                      @Nonnull final List<? extends InSource> inSources,
                                                      @Nonnull final List<? extends Value> comparisonKeyValues,
                                                      final boolean isReverse,
                                                      final int maxNumberOfValuesAllowed,
                                                      @Nonnull final Bindings.Internal internal) {
        return RecordQueryInUnionOnValuesPlan.inUnion(inner,
                inSources,
                comparisonKeyValues,
                isReverse,
                maxNumberOfValuesAllowed,
                internal);
    }

    /**
     * Construct a new in-union plan.
     *
     * @param inner the input/inner plan to this in-union
     * @param inSources a list of outer in-sources
     * @param comparisonKey a key expression by which the results of both plans are ordered
     * @param isReverse indicator if this operator produces its ordering in reverse order
     * @param maxNumberOfValuesAllowed maximum number of parallel legs of this in-union
     * @param internal indicator if bindings are modelled using correlation or old-style in-bindings
     * @return a new plan that will return the union of all results from both child plans
     */
    @Nonnull
    public static RecordQueryInUnionOnKeyExpressionPlan from(@Nonnull RecordQueryPlan inner,
                                                             @Nonnull final List<? extends InSource> inSources,
                                                             @Nonnull KeyExpression comparisonKey,
                                                             final boolean isReverse,
                                                             final int maxNumberOfValuesAllowed,
                                                             @Nonnull final Bindings.Internal internal) {
        return new RecordQueryInUnionOnKeyExpressionPlan(Quantifier.physical(Reference.of(inner)),
                inSources,
                comparisonKey,
                isReverse,
                maxNumberOfValuesAllowed,
                internal);
    }
}
