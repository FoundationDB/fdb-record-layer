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
import com.apple.foundationdb.record.cursors.RecursiveUnionCursor.RecursiveStateManager;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecursiveUnionQueryPlan;
import com.apple.foundationdb.record.planprotos.PTempTable;
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
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
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This is a physical representation of a recursive union, as with any other plan operator, its creates a {@link RecordCursor}
 * upon execution, however it is heavily involved in the execution orchestration of the created cursor due to the recursive
 * nature of execution and the necessity of mutating the execution state at its own level by means of overriding the
 * {@link EvaluationContext}'s reference to read and write {@link TempTable} used to execute current recursive step {@code n}
 * while preparing at the same time the state required to execute next recursive step {@code n+1}.
 * <br>
 * The recursive state is abstracted by the interface {@link RecursiveStateManager} which is implemented internally by
 * {@link RecursiveStateManagerImpl}.
 * <br>
 * for more information see {@link RecursiveUnionCursor}.
 */
@API(API.Status.INTERNAL)
public class RecursiveUnionQueryPlan implements RecordQueryPlanWithChildren {

    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Recursive-Union-Query-Plan");

    @Nonnull
    private final List<Quantifier.Physical> quantifiers;

    @Nonnull
    private final Value tempTableScanValueReference;

    @Nonnull
    private final Value tempTableInsertValueReference;

    @Nonnull
    private final Value resultValue;

    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlationsSupplier;

    @Nonnull
    private final Supplier<List<RecordQueryPlan>> computeChildren;

    @Nonnull
    private final Supplier<Integer> computeComplexity;

    public RecursiveUnionQueryPlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                   @Nonnull final Value tempTableScanValueReference,
                                   @Nonnull final Value tempTableInsertValueReference) {
        this.quantifiers = quantifiers;
        this.tempTableScanValueReference = tempTableScanValueReference;
        this.tempTableInsertValueReference = tempTableInsertValueReference;
        this.resultValue = RecordQuerySetPlan.mergeValues(quantifiers);
        this.correlationsSupplier = Suppliers.memoize(this::computeCorrelatedTo);
        this.computeChildren = Suppliers.memoize(this::computeChildren);
        this.computeComplexity = Suppliers.memoize(this::computeComplexity);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, tempTableScanValueReference,
                tempTableInsertValueReference);
    }

    @Override
    public int getRelationalChildCount() {
        return quantifiers.size();
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
                .addAll(tempTableScanValueReference.getCorrelatedTo())
                .addAll(tempTableInsertValueReference.getCorrelatedTo())
                .build();
    }

    @Nonnull
    @Override
    public Set<Type> getDynamicTypes() {
        Verify.verify(tempTableScanValueReference.getResultType().isRelation());
        return ImmutableSet.of(Objects.requireNonNull(((Type.Relation)tempTableScanValueReference.getResultType()).getInnerType()));
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final var type = getInnerTypeDescriptor(context, tempTableScanValueReference);
        final var recursiveStateManager = new RecursiveStateManagerImpl(
                (initialContinuation, evaluationContext) -> getInitialStatePlan().executePlan(store, evaluationContext,
                        initialContinuation == null ? null : initialContinuation.toByteArray(), executeProperties),
                (recursiveContinuation, evaluationContext) -> getRecursiveStatePlan().executePlan(store, evaluationContext,
                        recursiveContinuation == null ? null : recursiveContinuation.toByteArray(), executeProperties),
                context,
                tempTableScanValueReference,
                tempTableInsertValueReference,
                store,
                proto -> TempTable.from(proto, type), continuation);
        return new RecursiveUnionCursor<>(recursiveStateManager, store.getExecutor());
    }

    @Nullable
    private Descriptors.Descriptor getInnerTypeDescriptor(@Nonnull final EvaluationContext context, @Nonnull Value tempTableReferenceValue) {
        final Descriptors.Descriptor typeDescriptor;
        if (tempTableReferenceValue.getResultType().isRelation() && ((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType().isRecord()) {
            final var type = (Type.Record)((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType();
            typeDescriptor = context.getTypeRepository().getMessageDescriptor(type);
        } else {
            typeDescriptor = null;
        }
        return typeDescriptor;
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
        builder.setInitialTempTableValueReference(tempTableScanValueReference.toValueProto(serializationContext))
                .setRecursiveTempTableValueReference(tempTableInsertValueReference.toValueProto(serializationContext));
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
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof RecursiveUnionQueryPlan)) {
            return false;
        }

        final var otherRecursiveUnionQueryPlan = (RecursiveUnionQueryPlan)otherExpression;

        return tempTableScanValueReference.semanticEquals(otherRecursiveUnionQueryPlan.tempTableScanValueReference, equivalences)
                && tempTableInsertValueReference.semanticEquals(otherRecursiveUnionQueryPlan.tempTableInsertValueReference, equivalences);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // intentional
    public RecursiveUnionQueryPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        final var translatedInitialTempTableValueReference = tempTableScanValueReference.translateCorrelations(translationMap);
        final var translatedRecursiveTempTableValueReference = tempTableInsertValueReference.translateCorrelations(translationMap);
        if (translatedInitialTempTableValueReference != tempTableScanValueReference
                || translatedRecursiveTempTableValueReference != tempTableInsertValueReference) {
            return new RecursiveUnionQueryPlan(translatedQuantifiers.stream()
                    .map(quantifier -> quantifier.narrow(Quantifier.Physical.class)).collect(ImmutableList.toImmutableList()),
                    tempTableScanValueReference.translateCorrelations(translationMap),
                    tempTableInsertValueReference.translateCorrelations(translationMap));
        }
        return this;
    }

    /**
     * Deserializer of {@link RecursiveUnionQueryPlan}.
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

    static class RecursiveStateManagerImpl implements RecursiveStateManager<QueryResult> {

        private boolean isInitialState;

        private TempTable recursiveUnionTempTable;

        private boolean buffersAreFlipped;

        private final BiFunction<ByteString, EvaluationContext, RecordCursor<QueryResult>> recursiveCursorCreator;

        private final EvaluationContext baseContext;

        private final Value insertTempTableReference;

        private final Value scanTempTableReference;

        private RecordCursor<QueryResult> activeCursor;

        @Nullable
        private final FDBRecordStoreBase<?> store;

        // transient
        private EvaluationContext overridenEvaluationContext;

        /**
         * Creates a new instance of the {@link RecursiveStateManagerImpl}.
         * @param initialCursorCreator a creator of the {@code initial} state cursor.
         * @param recursiveCursorCreator a creator of the {@code recursive} state cursor.
         * @param baseContext the initial {@link EvaluationContext} used to execute the plan.
         * @param scanTempTableReference a {@link Value} reference to the {@link TempTable} used in recursive scan.
         * @param insertTempTableReference a {@link Value} reference to the {@link TempTable} used by insert operator(s).
         * @param store The record store.
         * @param tempTableDeserializer a deserializer of {@link TempTable} used by the recursive scan.
         * @param continuationBytes optional continuation of the {@link RecursiveUnionCursor}.
         */
        RecursiveStateManagerImpl(@Nonnull final BiFunction<ByteString, EvaluationContext, RecordCursor<QueryResult>> initialCursorCreator,
                                  @Nonnull final BiFunction<ByteString, EvaluationContext,  RecordCursor<QueryResult>> recursiveCursorCreator,
                                  @Nonnull final EvaluationContext baseContext,
                                  @Nonnull final Value scanTempTableReference,
                                  @Nonnull final Value insertTempTableReference,
                                  @Nullable final FDBRecordStoreBase<?> store,
                                  @Nonnull Function<PTempTable, TempTable> tempTableDeserializer,
                                  @Nullable byte[] continuationBytes) {
            this.recursiveCursorCreator = recursiveCursorCreator;
            this.baseContext = baseContext;
            this.insertTempTableReference = insertTempTableReference;
            this.scanTempTableReference = scanTempTableReference;
            overridenEvaluationContext = withEmptyTempTable(insertTempTableReference, baseContext);
            overridenEvaluationContext = withEmptyTempTable(scanTempTableReference, overridenEvaluationContext);
            buffersAreFlipped = false;
            this.store = store;
            if (continuationBytes == null) {
                isInitialState = true;
                recursiveUnionTempTable = baseContext.getTempTableFactory().createTempTable();
                activeCursor = initialCursorCreator.apply(null, overridenEvaluationContext);
            } else {
                final var continuation = RecursiveUnionCursor.Continuation.from(continuationBytes, tempTableDeserializer);
                isInitialState = continuation.isInitialState();
                recursiveUnionTempTable = continuation.getTempTable();
                overridenEvaluationContext = overrideTempTableBinding(overridenEvaluationContext, scanTempTableReference, recursiveUnionTempTable);
                if (isInitialState) {
                    activeCursor = initialCursorCreator.apply(continuation.getActiveStateContinuation().toByteString(), overridenEvaluationContext);
                } else {
                    if (continuation.buffersAreFlipped()) {
                        overridenEvaluationContext = flipBuffers(baseContext, false);
                    }
                    activeCursor = recursiveCursorCreator.apply(continuation.getActiveStateContinuation().toByteString(), overridenEvaluationContext);
                }
            }
        }

        @Override
        public void notifyCursorIsExhausted() {
            if (isInitialState) {
                isInitialState = false;
            }
            overridenEvaluationContext = flipBuffers(overridenEvaluationContext, true);
            activeCursor = recursiveCursorCreator.apply(null, overridenEvaluationContext); // restart
        }

        @Override
        public boolean canTransitionToNewStep() {
            if (isInitialState) {
                return true;
            } else {
                return !recursiveUnionTempTable.isEmpty();
            }
        }

        @Override
        public @Nonnull RecordCursor<QueryResult> getActiveStateCursor() {
            return activeCursor;
        }

        @Override
        public @Nonnull TempTable getRecursiveUnionTempTable() {
            return recursiveUnionTempTable;
        }

        @Override
        public boolean isInitialState() {
            return isInitialState;
        }

        @Override
        public boolean buffersAreFlipped() {
            return buffersAreFlipped;
        }

        @Nonnull
        @SuppressWarnings("PMD.CompareObjectsWithEquals") // intentional
        private EvaluationContext flipBuffers(@Nonnull final EvaluationContext evaluationContext, boolean cleanBuffers) {
            final var insertTempTable = getTempTable(evaluationContext, insertTempTableReference);
            final var scanTempTable = getTempTable(evaluationContext, scanTempTableReference);
            buffersAreFlipped = !buffersAreFlipped;
            if (recursiveUnionTempTable == insertTempTable) {
                recursiveUnionTempTable = scanTempTable;
                if (cleanBuffers) {
                    insertTempTable.clear();
                }
                return overrideTempTableBinding(
                        overrideTempTableBinding(baseContext, insertTempTableReference, insertTempTable),
                        scanTempTableReference, scanTempTable);
            } else {
                recursiveUnionTempTable = insertTempTable;
                if (cleanBuffers) {
                    scanTempTable.clear();
                }
                return overrideTempTableBinding(
                        overrideTempTableBinding(baseContext, insertTempTableReference, scanTempTable),
                        scanTempTableReference, insertTempTable);
            }
        }

        /**
         * Creates a nested {@link EvaluationContext} that has a reference, implied by the {@code key}, to a new
         * {@link TempTable} referred to by {@code value}.
         * @param context The {@link EvaluationContext} to override.
         * @param key The new reference key, which is a simple {@code reference} {@link Value}.
         * @param value The new reference value, which is a {@link TempTable}.
         * @return A nested {@link EvaluationContext} that has a reference, implied by the {@code key}, to a new
         * {@link TempTable} referred to by {@code value}.
         */
        @Nonnull
        private static EvaluationContext overrideTempTableBinding(@Nonnull final EvaluationContext context,
                                                                  @Nonnull final Value key,
                                                                  @Nonnull final TempTable value) {
            // TODO there should be a better, more streamlined way, of setting a constant value in an evaluation context.
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

        /**
         * Returns a new {@link EvaluationContext} with binding to a newly created {@link TempTable}.
         * @param tempTableReferenceValue The temp table reference.
         * @param context The context to add the new binding to.
         * @return a new {@link EvaluationContext} with binding to a newly created {@link TempTable}.
         */
        @Nonnull
        private static EvaluationContext withEmptyTempTable(@Nonnull final Value tempTableReferenceValue,
                                                            @Nonnull final EvaluationContext context) {
            if (tempTableReferenceValue instanceof ConstantObjectValue) {
                final var tempTableConstantReferenceValue = (ConstantObjectValue)tempTableReferenceValue;
                return context.withNewTempTableBinding(Bindings.Internal.CONSTANT, tempTableConstantReferenceValue.getAlias());
            }
            Verify.verify(tempTableReferenceValue instanceof QuantifiedObjectValue);
            final var tempTableQuantifiedReferenceValue = (QuantifiedObjectValue)tempTableReferenceValue;
            return context.withNewTempTableBinding(Bindings.Internal.CORRELATION, tempTableQuantifiedReferenceValue.getAlias());
        }

        @Nonnull
        private TempTable getTempTable(@Nonnull final EvaluationContext evaluationContext, @Nonnull final Value value) {
            return Objects.requireNonNull((TempTable)value.eval(store, evaluationContext));
        }
    }
}
