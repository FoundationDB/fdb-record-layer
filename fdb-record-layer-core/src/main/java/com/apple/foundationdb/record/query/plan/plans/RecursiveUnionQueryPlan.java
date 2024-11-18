/*
 * RecursiveUnionQueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.cursors.RecursiveUnionCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecursiveUnionQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * TODO.
 */
@API(API.Status.INTERNAL)
public class RecursiveUnionQueryPlan implements RecordQueryPlanWithChildren {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Recursive-Union-Query-Plan");

    @Nonnull
    private final List<Quantifier.Physical> quantifiers;

    @Nonnull
    private final Value initialTempTableValueReference;

    @Nonnull
    private final Value recursiveTempTableValueReference;

    @Nonnull
    private final Value resultValue;

    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlationsSupplier;

    @Nonnull
    private final Supplier<List<RecordQueryPlan>> computeChildren;

    @Nonnull
    private final Supplier<Integer> computeComplexity;

    private TempTable<?> readTempTable;

    private TempTable<?> writeTempTable;

    private boolean initialIsRead;

    public RecursiveUnionQueryPlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                   @Nonnull final Value initialTempTableValueReference,
                                   @Nonnull final Value recursiveTempTableValueReference) {
        this.quantifiers = quantifiers;
        this.initialTempTableValueReference = initialTempTableValueReference;
        this.recursiveTempTableValueReference = recursiveTempTableValueReference;
        this.resultValue = RecordQuerySetPlan.mergeValues(quantifiers);
        this.correlationsSupplier = Suppliers.memoize(this::computeCorrelatedTo);
        this.computeChildren = Suppliers.memoize(this::computeChildren);
        this.computeComplexity = Suppliers.memoize(this::computeComplexity);
        this.initialIsRead = false;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, initialTempTableValueReference,
                recursiveTempTableValueReference);
    }

    @Override
    public int getRelationalChildCount() {
        return quantifiers.size();
    }

    private <M extends Message> EvaluationContext resetTempTableBindings(@Nonnull final FDBRecordStoreBase<M> store,
                                                                         @Nonnull final EvaluationContext context) {
        if (readTempTable == null) {
            readTempTable = getRecursiveTempTable(store, context);
            writeTempTable = getInitialTempTable(store, context);
            return context;
        }
        if (initialIsRead) {
            final var newEvaluationContext = overrideTempTableBinding(context, initialTempTableValueReference, readTempTable);
            return overrideTempTableBinding(newEvaluationContext, recursiveTempTableValueReference, writeTempTable);
        } else {
            final var newEvaluationContext = overrideTempTableBinding(context, initialTempTableValueReference, writeTempTable);
            return overrideTempTableBinding(newEvaluationContext, recursiveTempTableValueReference, readTempTable);
        }
    }

    @Nonnull
    private <M extends Message> TempTable<?> getInitialTempTable(@Nonnull final FDBRecordStoreBase<M> store,
                                                                 @Nonnull final EvaluationContext context) {
        return Objects.requireNonNull((TempTable<?>)this.initialTempTableValueReference.eval(store, context));
    }

    @Nonnull
    private <M extends Message> TempTable<?> getRecursiveTempTable(@Nonnull final FDBRecordStoreBase<M> store,
                                                                   @Nonnull final EvaluationContext context) {
        return Objects.requireNonNull((TempTable<?>)this.recursiveTempTableValueReference.eval(store, context));
    }

    @Nonnull
    private <M extends Message> TempTable<?> getReadTempTable(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return initialIsRead ? getInitialTempTable(store, context) : getRecursiveTempTable(store, context);
    }

    @Nonnull
    private <M extends Message> TempTable<?> getWriteTempTable(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return initialIsRead ? getRecursiveTempTable(store, context) : getInitialTempTable(store, context);
    }

    @Nonnull
    private static EvaluationContext overrideTempTableBinding(@Nonnull final EvaluationContext context,
                                                              @Nonnull final Value key,
                                                              @Nonnull final TempTable<?> value) {
        if (key instanceof ConstantObjectValue) {
            final var constantKey = (ConstantObjectValue)key;
            final ImmutableMap.Builder<String, Object> constants = ImmutableMap.builder();
            constants.put(constantKey.getConstantId(), value);
            return context.withBinding(Bindings.Internal.CONSTANT, constantKey.getAlias(), constants.build());
        }
        Verify.verify(key instanceof QuantifiedObjectValue);
        final var quantifiedKey = (QuantifiedObjectValue)key;
        return context.withBinding(quantifiedKey.getAlias().getId(), value);
    }

    @Nonnull
    private RecordQueryPlan getInitialStatePlan() {
        return quantifiers.get(0).getRangesOverPlan();
    }

    @Nonnull
    private RecordQueryPlan getRecursiveStatePlan() {
        return quantifiers.get(1).getRangesOverPlan();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return correlationsSupplier.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelatedTo() {
        return ImmutableSet.<CorrelationIdentifier>builder()
                .addAll(initialTempTableValueReference.getCorrelatedTo())
                .addAll(recursiveTempTableValueReference.getCorrelatedTo())
                .build();
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return RecursiveUnionCursor.from(continuation,
                initialContinuation -> getInitialStatePlan().executePlan(store, resetTempTableBindings(store, context),
                        initialContinuation == null ? null : initialContinuation.toByteArray(), executeProperties),
                recursiveContinuation -> getRecursiveStatePlan().executePlan(store, resetTempTableBindings(store, context),
                        recursiveContinuation == null ? null : recursiveContinuation.toByteArray(), executeProperties),
                () -> initialIsRead,
                resumedInitialIsRead -> initialIsRead = resumedInitialIsRead,
                () -> {
                    System.out.println("before switch");
                    getReadTempTable(store, context).clear();
                    initialIsRead = !initialIsRead;
                    resetTempTableBindings(store, context);
                    return !getReadTempTable(store, context).isEmpty();
                },
                store.getExecutor());
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return computeChildren.get();
    }

    @Nonnull
    private List<RecordQueryPlan> computeChildren() {
        return quantifiers.stream().map(Quantifier.Physical::getRangesOverPlan).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    @Override
    public PRecursiveUnionQueryPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PRecursiveUnionQueryPlan.newBuilder();
        for (final Quantifier.Physical quantifier : quantifiers) {
            builder.addQuantifiers(quantifier.toProto(serializationContext));
        }
        builder.setInitialTempTableValueReference(initialTempTableValueReference.toValueProto(serializationContext))
                .setRecursiveTempTableValueReference(recursiveTempTableValueReference.toValueProto(serializationContext));
        return builder.build();
    }

    @Nonnull
    public static RecursiveUnionQueryPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                    @Nonnull final PRecursiveUnionQueryPlan recordQueryUnorderedDistinctPlanProto) {
        Verify.verify(recordQueryUnorderedDistinctPlanProto.getQuantifiersCount() > 0);
        ImmutableList.Builder<Quantifier.Physical> quantifiersBuilder = ImmutableList.builder();
        for (int i = 0; i < recordQueryUnorderedDistinctPlanProto.getQuantifiersCount(); i ++) {
            quantifiersBuilder.add(Quantifier.Physical.fromProto(serializationContext, recordQueryUnorderedDistinctPlanProto.getQuantifiers(i)));
        }
        return new RecursiveUnionQueryPlan(quantifiersBuilder.build(),
                Value.fromValueProto(serializationContext, recordQueryUnorderedDistinctPlanProto.getInitialTempTableValueReference()),
                Value.fromValueProto(serializationContext, recordQueryUnorderedDistinctPlanProto.getRecursiveTempTableValueReference()));
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setRecursiveUnionQueryPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.RECURSIVE_UNION_OPERATOR),
                childGraphs);
    }

    @Override
    public boolean isReverse() {
        return false; // todo: improve
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_RECURSIVE_UNION);
        for (Quantifier.Physical quantifier : quantifiers) {
            quantifier.getRangesOverPlan().logPlanStructure(timer);
        }
    }

    @Override
    public int getComplexity() {
        return computeComplexity.get();
    }

    private int computeComplexity() {
        // the complexity is calculated statically, it does not convoy the recursive nature of the
        // actual execution of this operator, which for the most part can not be statically determined anyway.
        return getChildren().stream().map(QueryPlan::getComplexity).reduce(1, Integer::sum);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return quantifiers;
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof RecursiveUnionQueryPlan)) {
            return false;
        }

        final var otherRecursiveUnionQueryPlan = (RecursiveUnionQueryPlan)otherExpression;

        return initialTempTableValueReference.semanticEquals(otherRecursiveUnionQueryPlan.initialTempTableValueReference, equivalences)
                && recursiveTempTableValueReference.semanticEquals(otherRecursiveUnionQueryPlan.recursiveTempTableValueReference, equivalences);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Nonnull
    @Override
    public RecursiveUnionQueryPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        final var translatedInitialTempTableValueReference = initialTempTableValueReference.translateCorrelations(translationMap);
        final var translatedRecursiveTempTableValueReference = recursiveTempTableValueReference.translateCorrelations(translationMap);
        if (translatedInitialTempTableValueReference != initialTempTableValueReference
                || translatedRecursiveTempTableValueReference != recursiveTempTableValueReference) {
            return new RecursiveUnionQueryPlan(translatedQuantifiers.stream()
                    .map(quantifier -> quantifier.narrow(Quantifier.Physical.class)).collect(ImmutableList.toImmutableList()),
                    initialTempTableValueReference.translateCorrelations(translationMap),
                    recursiveTempTableValueReference.translateCorrelations(translationMap));
        }
        return this;
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecursiveUnionQueryPlan, RecursiveUnionQueryPlan> {
        @Nonnull
        @Override
        public Class<PRecursiveUnionQueryPlan> getProtoMessageClass() {
            return PRecursiveUnionQueryPlan.class;
        }

        @Nonnull
        @Override
        public RecursiveUnionQueryPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PRecursiveUnionQueryPlan recordQueryUnorderedDistinctPlanProto) {
            return RecursiveUnionQueryPlan.fromProto(serializationContext, recordQueryUnorderedDistinctPlanProto);
        }
    }
}
