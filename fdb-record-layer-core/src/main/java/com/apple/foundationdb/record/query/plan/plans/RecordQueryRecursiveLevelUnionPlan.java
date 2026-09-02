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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

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

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Recursive-Union-Query-Plan");

    private final Quantifier.Physical initialStateQuantifier;

    private final Quantifier.Physical recursiveStateQuantifier;

    private final CorrelationIdentifier tempTableScanAlias;

    private final CorrelationIdentifier tempTableInsertAlias;

    private final Value resultValue;

    @SuppressWarnings("this-escape")
    private final Supplier<List<RecordQueryPlan>> computeChildren = Suppliers.memoize(this::computeChildren);

    @SuppressWarnings("this-escape")
    private final Supplier<Integer> computeComplexitySupplier = Suppliers.memoize(this::computeComplexity);

    public RecordQueryRecursiveLevelUnionPlan(final Quantifier.Physical initialStateQuantifier,
                                              final Quantifier.Physical recursiveStateQuantifier,
                                              final CorrelationIdentifier tempTableScanAlias,
                                              final CorrelationIdentifier tempTableInsertAlias) {
        this.initialStateQuantifier = initialStateQuantifier;
        this.recursiveStateQuantifier = recursiveStateQuantifier;
        this.tempTableScanAlias = tempTableScanAlias;
        this.tempTableInsertAlias = tempTableInsertAlias;
        this.resultValue = RecordQuerySetPlan.mergeValues(ImmutableList.of(initialStateQuantifier, recursiveStateQuantifier));
    }

    @Override
    public int getRelationalChildCount() {
        return 2;
    }

    private RecordQueryPlan getInitialStatePlan() {
        return initialStateQuantifier.getRangesOverPlan();
    }

    private RecordQueryPlan getRecursiveStatePlan() {
        return recursiveStateQuantifier.getRangesOverPlan();
    }

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

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @SuppressWarnings({"resource", "NullAway"}) // NullAway doesn't reliably track @Nullable on byte[] through the
                                                 // `x == null ? null : x.toByteArray()` ternary, even though the
                                                 // target executePlan parameter is declared @Nullable byte[].
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(final FDBRecordStoreBase<M> store,
                                                                     final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     final ExecuteProperties executeProperties) {
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
    private Descriptor getInnerTypeDescriptor(final EvaluationContext context) {
        @Nullable final Descriptor typeDescriptor;
        final var innerType = getResultValue().getResultType();
        if (Objects.requireNonNull(innerType).isRecord()) {
            typeDescriptor = context.getTypeRepository().getMessageDescriptor(innerType);
        } else {
            typeDescriptor = null;
        }
        return typeDescriptor;
    }

    @Override
    public List<RecordQueryPlan> getChildren() {
        return computeChildren.get();
    }

    private List<RecordQueryPlan> computeChildren() {
        return ImmutableList.of(getInitialStatePlan(), getRecursiveStatePlan());
    }

    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Override
    public PRecordQueryRecursiveLevelUnionPlan toProto(final PlanSerializationContext serializationContext) {
        final var builder = PRecordQueryRecursiveLevelUnionPlan.newBuilder()
                .setInitialStateQuantifier(initialStateQuantifier.toProto(serializationContext))
                .setRecursiveStateQuantifier(recursiveStateQuantifier.toProto(serializationContext))
                .setInitialTempTableAlias(tempTableScanAlias.getId())
                .setRecursiveTempTableAlias(tempTableInsertAlias.getId());
        return builder.build();
    }

    public static RecordQueryRecursiveLevelUnionPlan fromProto(final PlanSerializationContext serializationContext,
                                                               final PRecordQueryRecursiveLevelUnionPlan recordQueryUnorderedDistinctPlanProto) {
        final var initialStateQuantifier = Quantifier.Physical.fromProto(serializationContext, recordQueryUnorderedDistinctPlanProto.getInitialStateQuantifier());
        final var recursiveStateQuantifier = Quantifier.Physical.fromProto(serializationContext, recordQueryUnorderedDistinctPlanProto.getRecursiveStateQuantifier());
        return new RecordQueryRecursiveLevelUnionPlan(initialStateQuantifier, recursiveStateQuantifier,
                CorrelationIdentifier.of(recordQueryUnorderedDistinctPlanProto.getInitialTempTableAlias()),
                CorrelationIdentifier.of(recordQueryUnorderedDistinctPlanProto.getRecursiveTempTableAlias()));
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setRecursiveLevelUnionPlan(toProto(serializationContext)).build();
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
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

    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(initialStateQuantifier, recursiveStateQuantifier); // memoize
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(final RelationalExpression otherExpression,
                                         final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof RecordQueryRecursiveLevelUnionPlan)) {
            return false;
        }
        if (!semanticEqualsForResults(otherExpression, equivalences)) {
            return false;
        }

        final var otherRecursiveUnionQueryPlan = (RecordQueryRecursiveLevelUnionPlan)otherExpression;

        return tempTableScanAlias.equals(otherRecursiveUnionQueryPlan.tempTableScanAlias)
                && tempTableInsertAlias.equals(otherRecursiveUnionQueryPlan.tempTableInsertAlias);
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(tempTableScanAlias, tempTableInsertAlias);
    }

    @Override
    public int planHash(final PlanHashMode hashMode) {
        return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, getChildren());
    }

    @Override
    public RelationalExpression translateCorrelations(final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        Verify.verify(!translationMap.containsSourceAlias(tempTableScanAlias));
        Verify.verify(!translationMap.containsSourceAlias(tempTableInsertAlias));
        final var translatedInitialQuantifier = translatedQuantifiers.get(0).narrow(Quantifier.Physical.class);
        final var translatedRecursiveQuantifier = translatedQuantifiers.get(1).narrow(Quantifier.Physical.class);
        return new RecordQueryRecursiveLevelUnionPlan(translatedInitialQuantifier, translatedRecursiveQuantifier,
                tempTableScanAlias, tempTableInsertAlias);
    }

    public CorrelationIdentifier getTempTableScanAlias() {
        return tempTableScanAlias;
    }

    public CorrelationIdentifier getTempTableInsertAlias() {
        return tempTableInsertAlias;
    }

    /**
     * Deserializer of {@link RecordQueryRecursiveLevelUnionPlan}.
     */
    @AutoService(PlanDeserializer.class)
    public static final class Deserializer implements PlanDeserializer<PRecordQueryRecursiveLevelUnionPlan, RecordQueryRecursiveLevelUnionPlan> {
        @Override
        public Class<PRecordQueryRecursiveLevelUnionPlan> getProtoMessageClass() {
            return PRecordQueryRecursiveLevelUnionPlan.class;
        }

        @Override
        public RecordQueryRecursiveLevelUnionPlan fromProto(final PlanSerializationContext serializationContext,
                                                            final PRecordQueryRecursiveLevelUnionPlan recordQueryUnorderedDistinctPlanProto) {
            return RecordQueryRecursiveLevelUnionPlan.fromProto(serializationContext, recordQueryUnorderedDistinctPlanProto);
        }
    }

    /**
     * A reference implementation of {@link RecursiveStateManager} that orchestrates the recursive execution of
     * {@link RecordQueryRecursiveLevelUnionPlan}.
     */
    private static final class RecursiveStateManagerImpl implements RecursiveStateManager<QueryResult> {

        private boolean isInitialState;

        private TempTable recursiveUnionTempTable;

        private final BiFunction<@Nullable ByteString, EvaluationContext, RecordCursor<QueryResult>> recursiveCursorCreator;

        private final EvaluationContext baseContext;

        private final CorrelationIdentifier insertTempTableAlias;

        private final CorrelationIdentifier scanTempTableAlias;

        private RecordCursor<QueryResult> activeCursor;

        // transient
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
        @SuppressWarnings("NullAway") // NullAway doesn't reliably narrow @Nullable byte[] via the enclosing
                                       // `continuationBytes == null` check below; Continuation.from's parameter
                                       // is genuinely non-null here.
        RecursiveStateManagerImpl(final BiFunction<@Nullable ByteString, EvaluationContext, RecordCursor<QueryResult>> initialCursorCreator,
                                  final BiFunction<@Nullable ByteString, EvaluationContext,  RecordCursor<QueryResult>> recursiveCursorCreator,
                                  final EvaluationContext baseContext,
                                  final CorrelationIdentifier scanTempTableAlias,
                                  final CorrelationIdentifier insertTempTableAlias,
                                  final Function<PTempTable, TempTable> tempTableDeserializer,
                                  final TempTable.Factory tempTableFactory,
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
        public RecordCursor<QueryResult> getActiveStateCursor() {
            return activeCursor;
        }

        @Override
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
        @SuppressWarnings("PMD.CompareObjectsWithEquals") // intentional
        private EvaluationContext flipBuffers(final EvaluationContext evaluationContext) {
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

        private TempTable getTempTable(final EvaluationContext evaluationContext, final CorrelationIdentifier alias) {
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
        private static EvaluationContext withTempTable(final EvaluationContext context,
                                                       final CorrelationIdentifier key,
                                                       final TempTable value) {
            return context.withBinding(Bindings.Internal.CORRELATION.bindingName(key.getId()), value);
        }

        /**
         * Creates a nested {@link EvaluationContext} that has a mapping between a {@code key} and an empty {@link TempTable}.
         * @param context The context to add the new binding to.
         * @param key The temp table reference.
         * @param tempTableFactory a {@link TempTable} factory.
         * @return a new {@link EvaluationContext} with binding to a newly created {@link TempTable}.
         */
        private static EvaluationContext withEmptyTempTable(final EvaluationContext context,
                                                            final CorrelationIdentifier key,
                                                            final TempTable.Factory tempTableFactory) {
            return context
                    .childBuilder()
                    .setBinding(Bindings.Internal.CORRELATION.bindingName(key.getId()), tempTableFactory.createTempTable())
                    .build(context.getTypeRepository());
        }
    }
}
