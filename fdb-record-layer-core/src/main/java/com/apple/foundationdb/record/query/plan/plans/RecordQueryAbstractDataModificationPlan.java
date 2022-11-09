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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A query plan that filters out records from a child plan that are not of the designated record type(s).
 */
@API(API.Status.INTERNAL)
public abstract class RecordQueryAbstractDataModificationPlan implements RecordQueryPlanWithChild, PlannerGraphRewritable {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryAbstractDataModificationPlan.class);
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Abstract-Data-Modification-Plan");
    public static final CorrelationIdentifier CURRENT_MODIFIED_RECORD = CorrelationIdentifier.uniqueID(Quantifier.class);

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final Type innerFlowedType;
    @Nonnull
    private final String targetRecordType;
    @Nonnull
    private final Type.Record targetType;
    @Nonnull
    private final Descriptors.Descriptor targetDescriptor;

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
    private final Supplier<Set<CorrelationIdentifier>> correlatedToWithoutChildrenSupplier;

    @Nonnull
    private final Supplier<Integer> hashCodeWithoutChildrenSupplier;

    @Nonnull
    private final Supplier<Integer> planHashForContinuationSupplier;
    @Nonnull
    private final Supplier<Integer> planHashForWithoutLiteralsSupplier;

    protected RecordQueryAbstractDataModificationPlan(@Nonnull final Quantifier.Physical inner,
                                                      @Nonnull final String targetRecordType,
                                                      @Nonnull final Type.Record targetType,
                                                      @Nonnull final Descriptors.Descriptor targetDescriptor,
                                                      @Nullable final MessageHelpers.TransformationTrieNode transformationsTrie,
                                                      @Nullable final MessageHelpers.CoercionTrieNode coercionTrie,
                                                      @Nonnull final Value computationValue) {
        this.inner = inner;
        this.innerFlowedType = inner.getFlowedObjectType();
        this.targetRecordType = targetRecordType;
        this.targetType = targetType;
        this.targetDescriptor = targetDescriptor;
        this.transformationsTrie = transformationsTrie;
        this.coercionTrie = coercionTrie;
        this.computationValue = computationValue;
        this.resultValue = new QueriedValue(computationValue.getResultType());
        this.correlatedToWithoutChildrenSupplier = Suppliers.memoize(this::computeCorrelatedToWithoutChildren);
        this.hashCodeWithoutChildrenSupplier = Suppliers.memoize(this::computeHashCodeWithoutChildren);
        this.planHashForContinuationSupplier = Suppliers.memoize(this::computePlanHashForContinuation);
        this.planHashForWithoutLiteralsSupplier = Suppliers.memoize(this::computeRegularPlanHashWithoutLiterals);
    }

    @Nonnull
    public Type.Record getTargetType() {
        return targetType;
    }

    @Nonnull
    public Descriptors.Descriptor getTargetDescriptor() {
        return targetDescriptor;
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
        return computationValue.getDynamicTypes();
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

        return results
                .map(queryResult -> Pair.of(queryResult, mutateRecord(store, context, queryResult)))
                .mapPipelined(pair -> saveRecordAsync(store, pair.getRight())
                                .thenApply(storedRecord -> {
                                    final var nestedContext = context.childBuilder()
                                            .setBinding(inner.getAlias(), pair.getKey()) // pre-mutation
                                            .setBinding(CURRENT_MODIFIED_RECORD, storedRecord.getRecord()) // post-mutation
                                            .build(context.getTypeRepository());
                                    final var result = computationValue.eval(store, nestedContext);
                                    return QueryResult.ofComputed(result, null, storedRecord.getPrimaryKey(), null);
                                }),
                        store.getPipelineSize(PipelineOperation.UPDATE));
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public <M extends Message> M mutateRecord(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nonnull final QueryResult queryResult) {
        final var inRecord = (M)Preconditions.checkNotNull(queryResult.getMessage());
        return (M)MessageHelpers.transformMessage(store, context, transformationsTrie, coercionTrie, targetType, targetDescriptor, innerFlowedType, inRecord);
    }

    public abstract <M extends Message> CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull M message);

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
                        alias -> !alias.equals(CURRENT_MODIFIED_RECORD));
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

    @Nullable
    protected MessageHelpers.TransformationTrieNode translateTransformationsTrie(final @Nonnull TranslationMap translationMap) {
        final var transformationsTrie = getTransformationsTrie();
        if (transformationsTrie == null) {
            return null;
        }

        return transformationsTrie.<MessageHelpers.TransformationTrieNode>mapMaybe((current, childrenTries) -> {
            final var value = current.getValue();
            if (value != null) {
                Verify.verify(Iterables.isEmpty(childrenTries));
                return new MessageHelpers.TransformationTrieNode(value.translateCorrelations(translationMap), null);
            } else {
                final var oldChildrenMap = Verify.verifyNotNull(current.getChildrenMap());
                final var childrenTriesIterator = childrenTries.iterator();
                final var resultBuilder = ImmutableMap.<MessageHelpers.ResolvedAccessor, MessageHelpers.TransformationTrieNode>builder();
                for (final var oldEntry : oldChildrenMap.entrySet()) {
                    Verify.verify(childrenTriesIterator.hasNext());
                    final var childTrie = childrenTriesIterator.next();
                    resultBuilder.put(oldEntry.getKey(), childTrie);
                }
                return new MessageHelpers.TransformationTrieNode(null, resultBuilder.build());
            }
        }).orElseThrow(() -> new RecordCoreException("unable to translate correlations"));
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
        return Objects.hash(BASE_HASH.planHash(), targetRecordType, targetType, transformationsTrie, coercionTrie, computationValue);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case FOR_CONTINUATION:
                return planHashForContinuationSupplier.get();
            case STRUCTURAL_WITHOUT_LITERALS:
                return planHashForWithoutLiteralsSupplier.get();
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    private int computePlanHashForContinuation() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, getInnerPlan(), targetRecordType, targetType, transformationsTrie, coercionTrie);
    }

    private int computeRegularPlanHashWithoutLiterals() {
        return PlanHashable.objectsPlanHash(PlanHashKind.STRUCTURAL_WITHOUT_LITERALS, BASE_HASH, getInnerPlan(), targetRecordType, targetType, transformationsTrie, coercionTrie);
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
    public static List<FieldValue.FieldPath> checkAndPrepareOrderedFieldPaths(@Nonnull final Map<FieldValue.FieldPath, Value> transformMap) {
        // this brings together all paths that share the same prefixes
        final var orderedFieldPaths =
                transformMap.keySet()
                        .stream()
                        .sorted(FieldValue.FieldPath.comparator())
                        .collect(ImmutableList.toImmutableList());

        FieldValue.FieldPath currentFieldPath = null;
        for (final var fieldPath : orderedFieldPaths) {
            SemanticException.check(currentFieldPath == null || !currentFieldPath.isPrefixOf(fieldPath), SemanticException.ErrorCode.UPDATE_TRANSFORM_AMBIGUOUS);
            currentFieldPath = fieldPath;
        }
        return orderedFieldPaths;
    }

    /**
     * Method to compute a trie from a collection of lexicographically-ordered field paths. The trie is computed at
     * instantiation time (planning time). It serves to transform the input value in one pass.
     *
     * @param orderedFieldPaths a collection of field paths that must be lexicographically-ordered.
     * @param transformMap a map of transformations
     *
     * @return a {@link MessageHelpers.TransformationTrieNode}
     */
    @Nonnull
    public static MessageHelpers.TransformationTrieNode computeTrieForFieldPaths(@Nonnull final Collection<FieldValue.FieldPath> orderedFieldPaths,
                                                                                 @Nonnull final Map<FieldValue.FieldPath, Value> transformMap) {
        return computeTrieForFieldPaths(new FieldValue.FieldPath(ImmutableList.of()), transformMap, Iterators.peekingIterator(orderedFieldPaths.iterator()));
    }

    @Nonnull
    private static MessageHelpers.TransformationTrieNode computeTrieForFieldPaths(@Nonnull final FieldValue.FieldPath prefix,
                                                                                  @Nonnull final Map<FieldValue.FieldPath, Value> transformMap,
                                                                                  @Nonnull final PeekingIterator<FieldValue.FieldPath> orderedFieldPathIterator) {
        if (transformMap.containsKey(prefix)) {
            orderedFieldPathIterator.next();
            return new MessageHelpers.TransformationTrieNode(Verify.verifyNotNull(transformMap.get(prefix)), null);
        }
        final var childrenMapBuilder = ImmutableMap.<MessageHelpers.ResolvedAccessor, MessageHelpers.TransformationTrieNode>builder();
        while (orderedFieldPathIterator.hasNext()) {
            final var fieldPath = orderedFieldPathIterator.peek();
            if (!prefix.isPrefixOf(fieldPath)) {
                break;
            }

            final var prefixAccessors = prefix.getFieldAccessors();
            final var currentAccessor = fieldPath.getFieldAccessors().get(prefixAccessors.size());
            final var nestedPrefix = new FieldValue.FieldPath(ImmutableList.<MessageHelpers.ResolvedAccessor>builder()
                    .addAll(prefixAccessors)
                    .add(currentAccessor)
                    .build());

            final var currentTrie = computeTrieForFieldPaths(nestedPrefix, transformMap, orderedFieldPathIterator);
            childrenMapBuilder.put(currentAccessor, currentTrie);
        }

        return new MessageHelpers.TransformationTrieNode(null, childrenMapBuilder.build());
    }

    @Nullable
    public static MessageHelpers.CoercionTrieNode computePromotionsTrie(@Nonnull final Type targetType,
                                                                        @Nonnull Type currentType,
                                                                        @Nullable final MessageHelpers.TransformationTrieNode transformationsTrie) {
        if (transformationsTrie != null && transformationsTrie.getValue() != null) {
            currentType = transformationsTrie.getValue().getResultType();
        }

        if (currentType.isPrimitive()) {
            SemanticException.check(targetType.isPrimitive(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            if (!PromoteValue.isPromotionNeeded(currentType, targetType)) {
                return null;
            }
            // this is definitely a leaf; and we need to promote
            final var promotionFunction = PromoteValue.resolvePromotionFunction(currentType, targetType);
            SemanticException.check(promotionFunction != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            return new MessageHelpers.CoercionTrieNode(promotionFunction, null);
        }

        Verify.verify(targetType.getTypeCode() == currentType.getTypeCode());

        if (currentType.getTypeCode() == Type.TypeCode.ARRAY) {
            final var targetElementType = Verify.verifyNotNull(((Type.Array)targetType).getElementType());
            final var currentElementType = Verify.verifyNotNull(((Type.Array)currentType).getElementType());
            return computePromotionsTrie(targetElementType, currentElementType, null);
        }

        Verify.verify(currentType.getTypeCode() == Type.TypeCode.RECORD);
        final var targetRecordType = (Type.Record)targetType;
        final var currentRecordType = (Type.Record)currentType;
        SemanticException.check(targetRecordType.getFields().size() == currentRecordType.getFields().size(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);

        final var targetFields = targetRecordType.getFields();
        final var currentFields = currentRecordType.getFields();

        final var transformationsChildrenMap = transformationsTrie == null ? null : transformationsTrie.getChildrenMap();
        final var childrenMapBuilder = ImmutableMap.<MessageHelpers.ResolvedAccessor, MessageHelpers.CoercionTrieNode>builder();
        for (int i = 0; i < targetFields.size(); i++) {
            final var targetField = targetFields.get(i);
            final var currentAccessor = MessageHelpers.ResolvedAccessor.of(currentFields.get(i), i);

            final var transformationsFieldTrie = transformationsChildrenMap == null ? null : transformationsChildrenMap.get(currentAccessor);
            final var fieldTrie = computePromotionsTrie(targetField.getFieldType(), currentAccessor.getType(), transformationsFieldTrie);
            if (fieldTrie != null) {
                childrenMapBuilder.put(currentAccessor, fieldTrie);
            }
        }
        final var childrenMap = childrenMapBuilder.build();
        return childrenMap.isEmpty() ? null : new MessageHelpers.CoercionTrieNode(null, childrenMap);
    }
}
