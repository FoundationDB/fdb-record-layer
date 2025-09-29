/*
 * RecursiveUnionQueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.planprotos.PRecordQueryRecursiveLevelUnionPlan;
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
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
 * A physical representation of a recursive union. As with other operators, it delegates the execution to a corresponding
 * {@link RecordCursor}, but unlike most operators, it is heavily involved in orchestrating the execution of the cursor
 * itself due to the recursive nature of the union.
 * <br>
 * The orchestration involves, for example, overriding the {@link EvaluationContext} references to flip the read- and write-
 * {@link TempTable}s when moving to a new recursive step.
 * <br>
 * The recursive state is abstracted by the interface {@link RecursiveStateManager} which is implemented internally by
 * {@link RecursiveStateManagerImpl}.
 * <br>
 * for more information see {@link RecursiveUnionCursor}.
 */
@API(API.Status.INTERNAL)
public class RecordQueryRecursiveLevelUnionPlan extends AbstractRelationalExpressionWithChildren implements RecordQueryPlanWithChildren {

    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Recursive-Union-Query-Plan");

    @Nonnull
    private final Quantifier.Physical initialStateQuantifier;

    @Nonnull
    private final Quantifier.Physical recursiveStateQuantifier;

    @Nonnull
    private final CorrelationIdentifier tempTableScanAlias;

    @Nonnull
    private final CorrelationIdentifier tempTableInsertAlias;

    @Nonnull
    private final Value resultValue;

    @Nonnull
    private final Supplier<List<RecordQueryPlan>> computeChildren;

    @Nonnull
    private final Supplier<Integer> computeComplexitySupplier;

    public RecordQueryRecursiveLevelUnionPlan(@Nonnull final Quantifier.Physical initialStateQuantifier,
                                              @Nonnull final Quantifier.Physical recursiveStateQuantifier,
                                              @Nonnull final CorrelationIdentifier tempTableScanAlias,
                                              @Nonnull final CorrelationIdentifier tempTableInsertAlias) {
        this.initialStateQuantifier = initialStateQuantifier;
        this.recursiveStateQuantifier = recursiveStateQuantifier;
        this.tempTableScanAlias = tempTableScanAlias;
        this.tempTableInsertAlias = tempTableInsertAlias;
        this.resultValue = RecordQuerySetPlan.mergeValues(ImmutableList.of(initialStateQuantifier, recursiveStateQuantifier));
        this.computeChildren = Suppliers.memoize(this::computeChildren);
        this.computeComplexitySupplier = Suppliers.memoize(this::computeComplexity);
    }

    @Override
    public int getRelationalChildCount() {
        return 2;
    }

    @Nonnull
    private RecordQueryPlan getInitialStatePlan() {
        return initialStateQuantifier.getRangesOverPlan();
    }

    @Nonnull
    private RecordQueryPlan getRecursiveStatePlan() {
        return recursiveStateQuantifier.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        Streams.concat(initialStateQuantifier.getCorrelatedTo().stream(),
                        recursiveStateQuantifier.getCorrelatedTo().stream())
                // filter out the correlations that are satisfied by this plan
                .filter(alias -> !alias.equals(tempTableInsertAlias) && !alias.equals(tempTableScanAlias))
                .forEach(builder::add);
        return builder.build();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final var type = getInnerTypeDescriptor(context);
        final var childExecuteProperties = executeProperties.clearSkipAndLimit();
        final var recursiveStateManager = new RecursiveStateManagerImpl(
                (initialContinuation, evaluationContext) -> getInitialStatePlan().executePlan(store, evaluationContext,
                        initialContinuation == null ? null : initialContinuation.toByteArray(), childExecuteProperties),
                (recursiveContinuation, evaluationContext) -> getRecursiveStatePlan().executePlan(store, evaluationContext,
                        recursiveContinuation == null ? null : recursiveContinuation.toByteArray(), childExecuteProperties),
                context,
                tempTableScanAlias,
                tempTableInsertAlias,
                proto -> TempTable.from(proto, type), store.getContext().getTempTableFactory(), continuation);
        return new RecursiveUnionCursor<>(recursiveStateManager, store.getExecutor())
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Nullable
    private Descriptors.Descriptor getInnerTypeDescriptor(@Nonnull final EvaluationContext context) {
        @Nullable final Descriptors.Descriptor typeDescriptor;
        final var innerType = getResultValue().getResultType();
        if (Objects.requireNonNull(innerType).isRecord()) {
            typeDescriptor = context.getTypeRepository().getMessageDescriptor(innerType);
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
        return ImmutableList.of(getInitialStatePlan(), getRecursiveStatePlan());
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    @Override
    public PRecordQueryRecursiveLevelUnionPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PRecordQueryRecursiveLevelUnionPlan.newBuilder()
                .setInitialStateQuantifier(initialStateQuantifier.toProto(serializationContext))
                .setRecursiveStateQuantifier(recursiveStateQuantifier.toProto(serializationContext))
                .setInitialTempTableAlias(tempTableScanAlias.getId())
                .setRecursiveTempTableAlias(tempTableInsertAlias.getId());
        return builder.build();
    }

    @Nonnull
    public static RecordQueryRecursiveLevelUnionPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                               @Nonnull final PRecordQueryRecursiveLevelUnionPlan recordQueryUnorderedDistinctPlanProto) {
        final var initialStateQuantifier = Quantifier.Physical.fromProto(serializationContext, recordQueryUnorderedDistinctPlanProto.getInitialStateQuantifier());
        final var recursiveStateQuantifier = Quantifier.Physical.fromProto(serializationContext, recordQueryUnorderedDistinctPlanProto.getRecursiveStateQuantifier());
        return new RecordQueryRecursiveLevelUnionPlan(initialStateQuantifier, recursiveStateQuantifier,
                CorrelationIdentifier.of(recordQueryUnorderedDistinctPlanProto.getInitialTempTableAlias()),
                CorrelationIdentifier.of(recordQueryUnorderedDistinctPlanProto.getRecursiveTempTableAlias()));
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setRecursiveLevelUnionPlan(toProto(serializationContext)).build();
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
        return false;
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_RECURSIVE_UNION);
        getInitialStatePlan().logPlanStructure(timer);
        getRecursiveStatePlan().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return computeComplexitySupplier.get();
    }

    private int computeComplexity() {
        // the complexity is calculated statically, it does not convoy the recursive nature of the
        // actual execution of this operator, which for the most part can not be statically determined anyway.
        return 1 + getChildren().stream().map(QueryPlan::getComplexity).reduce(1, Integer::sum);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(initialStateQuantifier, recursiveStateQuantifier); // memoize
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof RecordQueryRecursiveLevelUnionPlan)) {
            return false;
        }

        final var otherRecursiveUnionQueryPlan = (RecordQueryRecursiveLevelUnionPlan)otherExpression;

        return tempTableScanAlias.equals(otherRecursiveUnionQueryPlan.tempTableScanAlias)
                && tempTableInsertAlias.equals(otherRecursiveUnionQueryPlan.tempTableInsertAlias);
    }

    public int computeHashCodeWithoutChildren() {
        return Objects.hash(tempTableInsertAlias, tempTableInsertAlias);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, getChildren());
    }

    @Nonnull
    @Override
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        Verify.verify(!translationMap.containsSourceAlias(tempTableScanAlias));
        Verify.verify(!translationMap.containsSourceAlias(tempTableInsertAlias));
        final var translatedInitialQuantifier = translatedQuantifiers.get(0).narrow(Quantifier.Physical.class);
        final var translatedRecursiveQuantifier = translatedQuantifiers.get(1).narrow(Quantifier.Physical.class);
        return new RecordQueryRecursiveLevelUnionPlan(translatedInitialQuantifier, translatedRecursiveQuantifier,
                tempTableScanAlias, tempTableInsertAlias);
    }

    @Nonnull
    public CorrelationIdentifier getTempTableScanAlias() {
        return tempTableScanAlias;
    }

    @Nonnull
    public CorrelationIdentifier getTempTableInsertAlias() {
        return tempTableInsertAlias;
    }

    /**
     * Deserializer of {@link RecordQueryRecursiveLevelUnionPlan}.
     */
    @AutoService(PlanDeserializer.class)
    public static final class Deserializer implements PlanDeserializer<PRecordQueryRecursiveLevelUnionPlan, RecordQueryRecursiveLevelUnionPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryRecursiveLevelUnionPlan> getProtoMessageClass() {
            return PRecordQueryRecursiveLevelUnionPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryRecursiveLevelUnionPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                            @Nonnull final PRecordQueryRecursiveLevelUnionPlan recordQueryUnorderedDistinctPlanProto) {
            return RecordQueryRecursiveLevelUnionPlan.fromProto(serializationContext, recordQueryUnorderedDistinctPlanProto);
        }
    }

    /**
     * A reference implementation of {@link RecursiveStateManager} that orchestrates the recursive execution of
     * {@link RecordQueryRecursiveLevelUnionPlan}.
     */
    private static final class RecursiveStateManagerImpl implements RecursiveStateManager<QueryResult> {

        private boolean isInitialState;

        @Nonnull
        private TempTable recursiveUnionTempTable;

        @Nonnull
        private final BiFunction<ByteString, EvaluationContext, RecordCursor<QueryResult>> recursiveCursorCreator;

        @Nonnull
        private final EvaluationContext baseContext;

        @Nonnull
        private final CorrelationIdentifier insertTempTableAlias;

        @Nonnull
        private final CorrelationIdentifier scanTempTableAlias;

        @Nonnull
        private RecordCursor<QueryResult> activeCursor;

        // transient
        @Nonnull
        private EvaluationContext overridenEvaluationContext;

        /**
         * Creates a new instance of the {@link RecursiveStateManagerImpl}.
         * @param initialCursorCreator a creator of the {@code initial} state cursor.
         * @param recursiveCursorCreator a creator of the {@code recursive} state cursor.
         * @param baseContext the initial {@link EvaluationContext} used to execute the plan.
         * @param scanTempTableAlias a reference to the {@link TempTable} used in recursive scan.
         * @param insertTempTableAlias a reference to the {@link TempTable} used by insert operator(s).
         * @param tempTableDeserializer a deserializer of {@link TempTable} used by the recursive scan.
         * @param tempTableFactory the {@link TempTable} factory.
         * @param continuationBytes optional continuation of the {@link RecursiveUnionCursor}.
         */
        RecursiveStateManagerImpl(@Nonnull final BiFunction<ByteString, EvaluationContext, RecordCursor<QueryResult>> initialCursorCreator,
                                  @Nonnull final BiFunction<ByteString, EvaluationContext,  RecordCursor<QueryResult>> recursiveCursorCreator,
                                  @Nonnull final EvaluationContext baseContext,
                                  @Nonnull final CorrelationIdentifier scanTempTableAlias,
                                  @Nonnull final CorrelationIdentifier insertTempTableAlias,
                                  @Nonnull final Function<PTempTable, TempTable> tempTableDeserializer,
                                  @Nonnull final TempTable.Factory tempTableFactory,
                                  @Nullable byte[] continuationBytes) {
            this.recursiveCursorCreator = recursiveCursorCreator;
            this.baseContext = baseContext;
            this.insertTempTableAlias = insertTempTableAlias;
            this.scanTempTableAlias = scanTempTableAlias;
            overridenEvaluationContext = withEmptyTempTable(baseContext, insertTempTableAlias, tempTableFactory);
            if (continuationBytes == null) {
                isInitialState = true;
                recursiveUnionTempTable = tempTableFactory.createTempTable();
                overridenEvaluationContext = withTempTable(overridenEvaluationContext, scanTempTableAlias, recursiveUnionTempTable);
                activeCursor = initialCursorCreator.apply(null, overridenEvaluationContext);
            } else {
                final var continuation = RecursiveUnionCursor.Continuation.from(continuationBytes, tempTableDeserializer);
                isInitialState = continuation.isInitialState();
                recursiveUnionTempTable = continuation.getTempTable();
                overridenEvaluationContext = withTempTable(overridenEvaluationContext, scanTempTableAlias, recursiveUnionTempTable);
                if (isInitialState) {
                    activeCursor = initialCursorCreator.apply(continuation.getActiveStateContinuation().toByteString(), overridenEvaluationContext);
                } else {
                    activeCursor = recursiveCursorCreator.apply(continuation.getActiveStateContinuation().toByteString(), overridenEvaluationContext);
                }
            }
        }

        @Override
        public void notifyCursorIsExhausted() {
            if (isInitialState) {
                isInitialState = false;
            }
            overridenEvaluationContext = flipBuffers(overridenEvaluationContext);
            activeCursor = recursiveCursorCreator.apply(null, overridenEvaluationContext); // restart
        }

        @Override
        public boolean canTransitionToNewStep() {
            return !recursiveUnionTempTable.isEmpty();
        }

        @Override
        @Nonnull
        public RecordCursor<QueryResult> getActiveStateCursor() {
            return activeCursor;
        }

        @Override
        @Nonnull
        public TempTable getRecursiveUnionTempTable() {
            return recursiveUnionTempTable;
        }

        @Override
        public boolean isInitialState() {
            return isInitialState;
        }

        /**
         * Flips the runtime buffers by creating a nested {@link EvaluationContext} with reversed references to both
         * the insert {@link TempTable} that is initially used by the insert operators on top of the recursive union
         * legs, and the scan {@link TempTable} that is used by the underlying {@link TempTableScanPlan} and is
         * maintained by the recursive union plan itself. Flipping the buffers is done everytime an underlying union
         * cursor stops producing new results.
         * @param evaluationContext The evaluation context used to as basis for a nested {@link EvaluationContext} with
         * reversed references to {@link TempTable}s participating in the recursive execution.
         * @return A nested {@link EvaluationContext} with reversed referenced to {@link TempTable}s participating in
         * the recursive execution.
         */
        @Nonnull
        @SuppressWarnings("PMD.CompareObjectsWithEquals") // intentional
        private EvaluationContext flipBuffers(@Nonnull final EvaluationContext evaluationContext) {
            final var insertTempTable = getTempTable(evaluationContext, insertTempTableAlias);
            final var scanTempTable = getTempTable(evaluationContext, scanTempTableAlias);
            if (recursiveUnionTempTable == insertTempTable) {
                recursiveUnionTempTable = scanTempTable;
                insertTempTable.clear();
                return withTempTable(
                        withTempTable(baseContext, insertTempTableAlias, insertTempTable),
                        scanTempTableAlias, scanTempTable);
            } else {
                recursiveUnionTempTable = insertTempTable;
                scanTempTable.clear();
                return withTempTable(
                        withTempTable(baseContext, insertTempTableAlias, scanTempTable),
                        scanTempTableAlias, insertTempTable);
            }
        }

        @Nonnull
        private TempTable getTempTable(@Nonnull final EvaluationContext evaluationContext, @Nonnull final CorrelationIdentifier alias) {
            return (TempTable)evaluationContext.getBinding(Bindings.Internal.CORRELATION, alias);
        }

        /**
         * Creates a nested {@link EvaluationContext} that has a mapping between a {@code key} and a {@code TempTable}
         * value.
         * @param context The {@link EvaluationContext} to override.
         * @param key The key used to reference the {@link TempTable}.
         * @param value The {@link TempTable} value.
         * @return A nested {@link EvaluationContext} that has a mapping between a {@code key} and a {@code TempTable}
         * value.
         */
        @Nonnull
        private static EvaluationContext withTempTable(@Nonnull final EvaluationContext context,
                                                       @Nonnull final CorrelationIdentifier key,
                                                       @Nonnull final TempTable value) {
            return context.withBinding(Bindings.Internal.CORRELATION.bindingName(key.getId()), value);
        }

        /**
         * Creates a nested {@link EvaluationContext} that has a mapping between a {@code key} and an empty {@link TempTable}.
         * @param context The context to add the new binding to.
         * @param key The temp table reference.
         * @param tempTableFactory a {@link TempTable} factory.
         * @return a new {@link EvaluationContext} with binding to a newly created {@link TempTable}.
         */
        @Nonnull
        private static EvaluationContext withEmptyTempTable(@Nonnull final EvaluationContext context,
                                                            @Nonnull final CorrelationIdentifier key,
                                                            @Nonnull final TempTable.Factory tempTableFactory) {
            return context
                    .childBuilder()
                    .setBinding(Bindings.Internal.CORRELATION.bindingName(key.getId()), tempTableFactory.createTempTable())
                    .build(context.getTypeRepository());
        }
    }
}
