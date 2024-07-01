/*
 * RecordQueryAbstractDataModificationPlan.java
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
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryAbstractDataModificationPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A query plan that performs transformations on the incoming record according to a set of transformation instructions
 * in conjunction with a target {@link Type} and a target {@link com.google.protobuf.Descriptors.Descriptor} which both
 * together define a set of instructions for type promotion and for protobuf coercion. All transformations and
 * type coercions are applied in one pass to each record. The resulting record is of the target type and is encoded
 * using the target protobuf {@link com.google.protobuf.Descriptors.Descriptor}. That record is then handed to an
 * abstract method that can implement a mutation to the store.
 * <br>
 * Example
 * <pre>
 * recordType:
 * {@code Restaurant(STRING as name, FLOAT as avg_review)}, descriptor from precompiled {@code RestaurantRecord}
 * incoming record:
 *   A {@link Message} based on a dynamically created protobuf descriptor which is wire-compatible with
 *   {@code RestaurantRecord}, {@code ('McDonald', 4i)} which is of type {@code (STRING as name, INT as avg_review)}.
 * modification:
 *   transformations: {@code name -> "Burger King"}
 *   promotions: {@code avg_review -> TREAT(avg_review AS FLOAT)}
 * result:
 *   A {@link com.google.protobuf.DynamicMessage} based on {@code RestaurantRecord},
 *   {@code ('Burger King', 4.0f)} which is of type {@code (STRING as name, FLOAT as avg_review)}.
 * </pre>
 */
@API(API.Status.INTERNAL)
public abstract class RecordQueryAbstractDataModificationPlan implements RecordQueryPlanWithChild, PlannerGraphRewritable {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryAbstractDataModificationPlan.class);
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Abstract-Data-Modification-Plan");
    private static final UUID CURRENT_MODIFIED_RECORD = UUID.randomUUID();
    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final Type innerFlowedType;
    @Nonnull
    private final String targetRecordType;
    @Nonnull
    private final Type.Record targetType;

    /**
     * A trie of transformations that is synthesized to perform a one-pass transformation of the incoming records.
     */
    @Nullable
    private final MessageHelpers.TransformationTrieNode transformationsTrie;

    @Nullable
    private final MessageHelpers.CoercionTrieNode coercionTrie;

    @Nonnull
    private final Value computationValue;

    @Nonnull
    private final Value resultValue;

    @Nonnull
    private final CorrelationIdentifier currentModifiedRecordAlias;

    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlatedToWithoutChildrenSupplier;

    @Nonnull
    private final Supplier<Integer> hashCodeWithoutChildrenSupplier;

    @Nonnull
    private final Supplier<Integer> planHashForContinuationSupplier;

    protected RecordQueryAbstractDataModificationPlan(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final PRecordQueryAbstractDataModificationPlan recordQueryAbstractDataModificationPlanProto) {
        this(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryAbstractDataModificationPlanProto.getInner())),
                Objects.requireNonNull(recordQueryAbstractDataModificationPlanProto.getTargetRecordType()),
                Type.Record.fromProto(serializationContext, Objects.requireNonNull(recordQueryAbstractDataModificationPlanProto.getTargetType())),
                PlanSerialization.getFieldOrNull(recordQueryAbstractDataModificationPlanProto,
                        PRecordQueryAbstractDataModificationPlan::hasTransformationsTrie,
                        m -> MessageHelpers.TransformationTrieNode.fromProto(serializationContext, Objects.requireNonNull(recordQueryAbstractDataModificationPlanProto.getTransformationsTrie()))),
                PlanSerialization.getFieldOrNull(recordQueryAbstractDataModificationPlanProto,
                        PRecordQueryAbstractDataModificationPlan::hasCoercionTrie,
                        m -> MessageHelpers.CoercionTrieNode.fromProto(serializationContext, Objects.requireNonNull(recordQueryAbstractDataModificationPlanProto.getCoercionTrie()))),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(recordQueryAbstractDataModificationPlanProto.getComputationValue())),
                CorrelationIdentifier.of(Objects.requireNonNull(recordQueryAbstractDataModificationPlanProto.getCurrentModifiedRecordAlias())));
    }

    protected RecordQueryAbstractDataModificationPlan(@Nonnull final Quantifier.Physical inner,
                                                      @Nonnull final String targetRecordType,
                                                      @Nonnull final Type.Record targetType,
                                                      @Nullable final MessageHelpers.TransformationTrieNode transformationsTrie,
                                                      @Nullable final MessageHelpers.CoercionTrieNode coercionTrie,
                                                      @Nonnull final Value computationValue,
                                                      @Nonnull final CorrelationIdentifier currentModifiedRecordAlias) {
        this.inner = inner;
        this.innerFlowedType = inner.getFlowedObjectType();
        this.targetRecordType = targetRecordType;
        this.targetType = targetType;
        this.transformationsTrie = transformationsTrie;
        this.coercionTrie = coercionTrie;
        this.computationValue = computationValue;
        this.resultValue = new QueriedValue(computationValue.getResultType());
        this.currentModifiedRecordAlias = currentModifiedRecordAlias;
        this.correlatedToWithoutChildrenSupplier = Suppliers.memoize(this::computeCorrelatedToWithoutChildren);
        this.hashCodeWithoutChildrenSupplier = Suppliers.memoize(this::computeHashCodeWithoutChildren);
        this.planHashForContinuationSupplier = Suppliers.memoize(this::computePlanHashForContinuation);
    }

    @Nonnull
    public Type.Record getTargetType() {
        return targetType;
    }

    @Nullable
    public MessageHelpers.TransformationTrieNode getTransformationsTrie() {
        return transformationsTrie;
    }

    @Nullable
    public MessageHelpers.CoercionTrieNode getCoercionTrie() {
        return coercionTrie;
    }

    @Nonnull
    @Override
    public Set<Type> getDynamicTypes() {
        final var dynamicTypesBuilder = ImmutableSet.<Type>builder();

        dynamicTypesBuilder.addAll(computationValue.getDynamicTypes());

        if (transformationsTrie != null) {
            transformationsTrie.values()
                    .forEach(value -> dynamicTypesBuilder.addAll(value.getDynamicTypes()));
        }

        return dynamicTypesBuilder.build();
    }

    @Nonnull
    @Override
    @SuppressWarnings({"PMD.CloseResource", "resource"})
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final RecordCursor<QueryResult> results =
                getInnerPlan().executePlan(store, context, continuation, executeProperties.clearSkipAndLimit());
        final var targetDescriptor = store.getRecordMetaData().getRecordType(targetRecordType).getDescriptor();
        return results
                .map(queryResult -> Pair.of(queryResult, mutateRecord(store, context, queryResult, targetDescriptor)))
                .mapPipelined(pair -> saveRecordAsync(store, pair.getRight(), executeProperties.isDryRun())
                                .thenApply(storedRecord -> {
                                    final var nestedContext = context.childBuilder()
                                            .setBinding(inner.getAlias(), pair.getLeft()) // pre-mutation
                                            .setBinding(currentModifiedRecordAlias, storedRecord.getRecord()) // post-mutation
                                            .build(context.getTypeRepository());
                                    final var result = computationValue.eval(store, nestedContext);
                                    return QueryResult.ofComputed(result, null, storedRecord.getPrimaryKey(), null);
                                }),
                        store.getPipelineSize(getPipelineOperation()));
    }

    public abstract PipelineOperation getPipelineOperation();

    @Nullable
    @SuppressWarnings("unchecked")
    public <M extends Message> M mutateRecord(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context,
                                              @Nonnull final QueryResult queryResult, @Nonnull final Descriptors.Descriptor targetDescriptor) {
        final var inRecord = (M)Preconditions.checkNotNull(queryResult.getMessage());
        return (M)MessageHelpers.transformMessage(store,
                context.withBinding(inner.getAlias(), queryResult),
                transformationsTrie,
                coercionTrie,
                targetType,
                targetDescriptor,
                innerFlowedType,
                inRecord.getDescriptorForType(),
                inRecord);
    }

    @Nonnull
    public abstract <M extends Message> CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull M message, boolean isDryRun);

    @Override
    public boolean isReverse() {
        return getInnerPlan().isReverse();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(this.inner);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return correlatedToWithoutChildrenSupplier.get();
    }

    @Nonnull
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        final var resultValueCorrelatedTo =
                Sets.filter(computationValue.getCorrelatedTo(),
                        alias -> !alias.equals(currentModifiedRecordAlias));
        if (transformationsTrie != null) {
            final var aliasesFromTransformationsTrieIterator =
                    transformationsTrie.values()
                            .stream()
                            .flatMap(value -> value.getCorrelatedTo().stream())
                            .iterator();
            return ImmutableSet.<CorrelationIdentifier>builder().addAll(aliasesFromTransformationsTrieIterator).addAll(resultValueCorrelatedTo).build();
        } else {
            return resultValueCorrelatedTo;
        }
    }

    @Nonnull
    public Value getComputationValue() {
        return computationValue;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
        if (this == other) {
            return true;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        final RecordQueryUpdatePlan otherUpdatePlan = (RecordQueryUpdatePlan)other;
        if (!getTargetRecordType().equals(otherUpdatePlan.getTargetRecordType())) {
            return false;
        }
        if (!getTargetType().equals(otherUpdatePlan.getTargetType())) {
            return false;
        }
        if (!getResultValue().semanticEquals(otherUpdatePlan.getResultValue(), equivalences)) {
            return false;
        }
        if (!Objects.equals(getTransformationsTrie(), otherUpdatePlan.getTransformationsTrie())) {
            return false;
        }
        return Objects.equals(getCoercionTrie(), otherUpdatePlan.getCoercionTrie());
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return hashCodeWithoutChildrenSupplier.get();
    }

    private int computeHashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), targetRecordType, targetType,
                transformationsTrie, coercionTrie, computationValue);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return planHashForContinuationSupplier.get();
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    private int computePlanHashForContinuation() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, getInnerPlan(),
                targetRecordType, transformationsTrie, coercionTrie);
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

    @Nonnull
    public String getTargetRecordType() {
        return targetRecordType;
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // TODO timer.increment(FDBStoreTimer.Counts.PLAN_TYPE_FILTER);
        getInnerPlan().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getInnerPlan().getComplexity();
    }

    @Nonnull
    public PRecordQueryAbstractDataModificationPlan toRecordQueryAbstractModificationPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PRecordQueryAbstractDataModificationPlan.Builder builder = PRecordQueryAbstractDataModificationPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setTargetRecordType(targetRecordType)
                .setTargetType(targetType.toProto(serializationContext));
        if (transformationsTrie != null) {
            builder.setTransformationsTrie(transformationsTrie.toProto(serializationContext));
        }
        if (coercionTrie != null) {
            builder.setCoercionTrie(coercionTrie.toProto(serializationContext));
        }
        builder.setComputationValue(computationValue.toValueProto(serializationContext));
        builder.setCurrentModifiedRecordAlias(currentModifiedRecordAlias.getId());
        return builder.build();
    }

    @Nonnull
    public static CorrelationIdentifier currentModifiedRecordAlias() {
        return CorrelationIdentifier.uniqueSingletonID(CURRENT_MODIFIED_RECORD, "𝓆");
    }
}
