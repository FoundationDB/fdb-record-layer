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
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.TreeLike;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
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
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
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
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

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
    private final String targetRecordType;
    @Nonnull
    private final Type.Record targetType;
    @Nonnull
    private final Descriptors.Descriptor targetDescriptor;

    /**
     * A trie of transformations that is synthesized to perform a one-pass transformation of the incoming records.
     */
    @Nullable
    private final TrieNode transformationsTrie;

    @Nullable
    private final TrieNode promotionsTrie;

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
                                                      @Nullable final TrieNode transformationsTrie,
                                                      @Nullable final TrieNode promotionsTrie,
                                                      @Nonnull final Value computationValue) {
        this.inner = inner;
        this.targetRecordType = targetRecordType;
        this.targetType = targetType;
        this.targetDescriptor = targetDescriptor;
        this.transformationsTrie = transformationsTrie;
        this.promotionsTrie = promotionsTrie;
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
    public TrieNode getTransformationsTrie() {
        return transformationsTrie;
    }

    @Nullable
    public TrieNode getPromotionsTrie() {
        return promotionsTrie;
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
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
        return (M)transformMessage(store, context, transformationsTrie, promotionsTrie, getResultValue().getResultType(), targetDescriptor, targetType, inRecord);
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
    protected TrieNode translateTransformationsTrie(final @Nonnull TranslationMap translationMap) {
        final var transformationsTrie = getTransformationsTrie();
        if (transformationsTrie == null) {
            return null;
        }

        return transformationsTrie.<TrieNode>mapMaybe((current, childrenTries) -> {
            final var value = current.getValue();
            if (value != null) {
                Verify.verify(Iterables.isEmpty(childrenTries));
                return new TrieNode(value.translateCorrelations(translationMap), null);
            } else {
                final var oldChildrenMap = current.getChildrenMap();
                final var childrenTriesIterator = childrenTries.iterator();
                final var resultBuilder = ImmutableMap.<Type.Record.Field, TrieNode>builder();
                for (final var oldEntry : oldChildrenMap.entrySet()) {
                    Verify.verify(childrenTriesIterator.hasNext());
                    final var childTrie = childrenTriesIterator.next();
                    resultBuilder.put(oldEntry.getKey(), childTrie);
                }
                return new TrieNode(null, resultBuilder.build());
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
        return Objects.equals(getPromotionsTrie(), otherUpdatePlan.getPromotionsTrie());
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
        return Objects.hash(BASE_HASH.planHash(), targetRecordType, targetType, transformationsTrie, promotionsTrie, computationValue);
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
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, getInnerPlan(), targetRecordType, targetType, transformationsTrie, promotionsTrie);
    }

    private int computeRegularPlanHashWithoutLiterals() {
        return PlanHashable.objectsPlanHash(PlanHashKind.STRUCTURAL_WITHOUT_LITERALS, BASE_HASH, getInnerPlan(), targetRecordType, targetType, transformationsTrie, promotionsTrie);
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
     * @param orderedFieldPaths a collection of field paths that must be lexicographically-ordered.
     * @param transformMap a map of transformations
     * @return a {@link TrieNode}
     */
    @Nonnull
    public static TrieNode computeTrieForFieldPaths(@Nonnull final Collection<FieldValue.FieldPath> orderedFieldPaths,
                                                    @Nonnull final Map<FieldValue.FieldPath, Value> transformMap) {
        return computeTrieForFieldPaths(new FieldValue.FieldPath(ImmutableList.of()), transformMap, Iterators.peekingIterator(orderedFieldPaths.iterator()));
    }

    @Nonnull
    private static TrieNode computeTrieForFieldPaths(@Nonnull final FieldValue.FieldPath prefix,
                                                     @Nonnull final Map<FieldValue.FieldPath, Value> transformMap,
                                                     @Nonnull final PeekingIterator<FieldValue.FieldPath> orderedFieldPathIterator) {
        if (transformMap.containsKey(prefix)) {
            orderedFieldPathIterator.next();
            return new TrieNode(Verify.verifyNotNull(transformMap.get(prefix)), null);
        }
        final var childrenMapBuilder = ImmutableMap.<Type.Record.Field, TrieNode>builder();
        while (orderedFieldPathIterator.hasNext()) {
            final var fieldPath = orderedFieldPathIterator.peek();
            if (!prefix.isPrefixOf(fieldPath)) {
                break;
            }

            final var prefixFields = prefix.getFields();
            final var currentField = fieldPath.getFields().get(prefixFields.size());
            final var nestedPrefix = new FieldValue.FieldPath(ImmutableList.<Type.Record.Field>builder()
                    .addAll(prefixFields)
                    .add(currentField)
                    .build());

            final var currentTrie = computeTrieForFieldPaths(nestedPrefix, transformMap, orderedFieldPathIterator);
            childrenMapBuilder.put(currentField, currentTrie);
        }

        return new TrieNode(null, childrenMapBuilder.build());
    }

    @Nullable
    public static TrieNode computePromotionsTrie(@Nonnull final Type targetType,
                                                 @Nonnull Type currentType,
                                                 @Nullable final TrieNode transformationsTrie) {
        if (transformationsTrie != null && transformationsTrie.getValue() != null) {
            currentType = transformationsTrie.getValue().getResultType();
        }

        if (currentType.isPrimitive()) {
            SemanticException.check(targetType.isPrimitive(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            if (!PromoteValue.isPromotionNeeded(currentType, targetType)) {
                return null;
            }
            // this is definitely a leaf; and we need to promote
            return new TrieNode(PromoteValue.inject(ObjectValue.of(Quantifier.CURRENT, currentType), targetType), null);
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
        final var childrenMapBuilder = ImmutableMap.<Type.Record.Field, TrieNode>builder();
        for (int i = 0; i < targetFields.size(); i++) {
            final Type.Record.Field targetField = targetFields.get(i);
            final Type.Record.Field currentField = currentFields.get(i);
            final var transformationsFieldTrie = transformationsChildrenMap == null ? null : transformationsChildrenMap.get(currentField);
            final var fieldTrie = computePromotionsTrie(targetField.getFieldType(), currentField.getFieldType(), transformationsFieldTrie);
            if (fieldTrie != null) {
                childrenMapBuilder.put(currentField, fieldTrie);
            }
        }
        final var childrenMap = childrenMapBuilder.build();
        return childrenMap.isEmpty() ? null : new TrieNode(null, childrenMap);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <M extends Message> Object transformMessage(@Nonnull final FDBRecordStoreBase<M> store,
                                                              @Nonnull final EvaluationContext context,
                                                              @Nullable final TrieNode transformationsTrie,
                                                              @Nullable final TrieNode promotionsTrie,
                                                              @Nonnull final Type targetType,
                                                              @Nullable Descriptors.Descriptor targetDescriptor,
                                                              @Nonnull final Type currentType,
                                                              @Nullable final Object current) {
        final var value = transformationsTrie == null ? null : transformationsTrie.getValue();
        Verify.verify(value == null);

        targetDescriptor = Verify.verifyNotNull(targetDescriptor);
        final var targetRecordType = (Type.Record)targetType;
        final var targetNameToFieldMap = Verify.verifyNotNull(targetRecordType.getFieldNameFieldMap());
        final var currentRecordType = (Type.Record)currentType;
        final var currentNameToFieldMap = Verify.verifyNotNull(currentRecordType.getFieldNameFieldMap());

        final var transformationsFieldIndexToFieldMap = transformationsTrie == null ? null : transformationsTrie.getFieldIndexToFieldMap();
        final var transformationsChildrenMap = transformationsTrie == null ? null : transformationsTrie.getChildrenMap();
        final var promotionsFieldIndexToFieldMap = promotionsTrie == null ? null : promotionsTrie.getFieldIndexToFieldMap();
        final var promotionsChildrenMap = promotionsTrie == null ? null : promotionsTrie.getChildrenMap();
        final var subRecord = (M)Verify.verifyNotNull(current);

        final var resultMessageBuilder = DynamicMessage.newBuilder(targetDescriptor);
        final var messageDescriptor = subRecord.getDescriptorForType();
        for (final var messageFieldDescriptor : messageDescriptor.getFields()) {
            final var transformationField = transformationsFieldIndexToFieldMap == null ? null : transformationsFieldIndexToFieldMap.get(messageFieldDescriptor.getNumber());
            final var targetFieldDescriptor = targetDescriptor.findFieldByName(messageFieldDescriptor.getName());
            final var promotionField = promotionsFieldIndexToFieldMap == null ? null : promotionsFieldIndexToFieldMap.get(messageFieldDescriptor.getNumber());
            final var promotionFieldTrie = (promotionsChildrenMap == null || promotionField == null) ? null : promotionsChildrenMap.get(promotionField);
            if (transformationField != null) {
                final var transformationFieldTrie = Verify.verifyNotNull(Verify.verifyNotNull(transformationsChildrenMap).get(transformationField));
                final var targetFieldType = targetNameToFieldMap.get(transformationField.getFieldName()).getFieldType();
                final var currentFieldType = transformationField.getFieldType();
                Object fieldResult;
                if (transformationFieldTrie.getValue() == null) {
                    fieldResult =
                            transformMessage(store,
                                    context,
                                    transformationFieldTrie,
                                    promotionFieldTrie,
                                    targetFieldType,
                                    targetFieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE ? targetFieldDescriptor.getMessageType() : null,
                                    currentFieldType,
                                    subRecord.getField(messageFieldDescriptor));
                } else {
                    final var transformationValue = transformationFieldTrie.getValue();
                    fieldResult = transformationValue.eval(store, context);
                    if (fieldResult !=  null) {
                        fieldResult = coerceField(store, context, promotionFieldTrie, targetFieldType, targetFieldDescriptor, currentFieldType, fieldResult);
                    }
                }
                Verify.verify(fieldResult != null || (currentFieldType.isNullable() && targetFieldType.isNullable()));
                if (fieldResult != null) {
                    resultMessageBuilder.setField(targetFieldDescriptor, fieldResult);
                }
            } else {
                var fieldResult = MessageValue.getFieldOnMessage(subRecord, messageFieldDescriptor);
                if (fieldResult != null) {
                    final var targetFieldType = Verify.verifyNotNull(targetNameToFieldMap.get(messageFieldDescriptor.getName())).getFieldType();
                    final var currentFieldType = Verify.verifyNotNull(currentNameToFieldMap.get(messageFieldDescriptor.getName())).getFieldType();
                    fieldResult = Verify.verifyNotNull(NullableArrayTypeUtils.unwrapIfArray(fieldResult, currentFieldType));
                    resultMessageBuilder.setField(targetFieldDescriptor, coerceField(store, context, promotionFieldTrie, targetFieldType, targetFieldDescriptor, currentFieldType, fieldResult));
                }
            }
        }
        return resultMessageBuilder.build();
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public static <M extends Message> Object coerceField(@Nonnull final FDBRecordStoreBase<M> store,
                                                         @Nonnull final EvaluationContext context,
                                                         @Nullable final TrieNode promotionsTrie,
                                                         @Nonnull final Type targetFieldType,
                                                         @Nonnull final Descriptors.FieldDescriptor targetFieldDescriptor,
                                                         @Nonnull final Type currentFieldType,
                                                         @Nonnull Object current) {
        if (currentFieldType.getTypeCode() == Type.TypeCode.ARRAY) {
            SemanticException.check(targetFieldType.getTypeCode() == Type.TypeCode.ARRAY, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            final var targetElementType = Verify.verifyNotNull(((Type.Array)targetFieldType).getElementType());
            final var currentElementType = Verify.verifyNotNull(((Type.Array)currentFieldType).getElementType());
            final var currentObjects = (List<?>)current;
            final var coercedObjectsBuilder = ImmutableList.builder();
            for (final var currentObject : currentObjects) {
                coercedObjectsBuilder.add(coerceField(store, context, promotionsTrie, targetElementType, targetFieldDescriptor, currentElementType, currentObject));
            }
            current = coercedObjectsBuilder.build();

            if (currentFieldType.isNullable()) {
                final var wrappedDescriptor = targetFieldDescriptor.getMessageType();
                final var wrapperBuilder = DynamicMessage.newBuilder(wrappedDescriptor);
                wrapperBuilder.setField(wrappedDescriptor.findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName()), current);
                return wrapperBuilder.build();
            }
        }

        switch (targetFieldDescriptor.getJavaType()) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case BYTE_STRING:
            case ENUM:
                if (promotionsTrie == null) {
                    return current;
                }

                final var promoteValue = Verify.verifyNotNull(promotionsTrie.getValue());
                return Verify.verifyNotNull(promoteValue.eval(store, context.withBinding(Quantifier.CURRENT, current)));
            case MESSAGE:
                return coerceMessage(store, context, promotionsTrie, targetFieldType, targetFieldDescriptor.getMessageType(), currentFieldType, (M)current);
            default:
                throw new IllegalStateException("unsupported java type for record field");
        }
    }

    @Nonnull
    public static <M extends Message> Message coerceMessage(@Nonnull final FDBRecordStoreBase<M> store,
                                                            @Nonnull final EvaluationContext context,
                                                            @Nullable final TrieNode promotionsTrie,
                                                            @Nonnull final Type targetType,
                                                            @Nonnull Descriptors.Descriptor targetDescriptor,
                                                            @Nonnull final Type currentType,
                                                            @Nonnull final M currentMessage) {
        targetDescriptor = Verify.verifyNotNull(targetDescriptor);
        final var promotionsChildrenMap = promotionsTrie == null ? null : promotionsTrie.getChildrenMap();
        final var targetRecordType = (Type.Record)targetType;
        final var targetNameToFieldMap = Verify.verifyNotNull(targetRecordType.getFieldNameFieldMap());
        final var currentRecordType = (Type.Record)currentType;
        final var currentNameToFieldMap = Verify.verifyNotNull(currentRecordType.getFieldNameFieldMap());
        final var resultMessageBuilder = DynamicMessage.newBuilder(targetDescriptor);
        final var messageDescriptor = currentMessage.getDescriptorForType();
        for (final var messageFieldDescriptor : messageDescriptor.getFields()) {
            if (currentMessage.hasField(messageFieldDescriptor)) {
                final var targetFieldDescriptor = Verify.verifyNotNull(targetDescriptor.findFieldByName(messageFieldDescriptor.getName()));
                final var targetFieldType = Verify.verifyNotNull(targetNameToFieldMap.get(messageFieldDescriptor.getName())).getFieldType();
                final var currentField = currentNameToFieldMap.get(messageFieldDescriptor.getName());
                final var currentFieldType = Verify.verifyNotNull(currentField).getFieldType();

                resultMessageBuilder.setField(targetFieldDescriptor,
                        coerceField(store,
                                context,
                                promotionsChildrenMap == null ? null : promotionsChildrenMap.get(currentField),
                                targetFieldType,
                                targetFieldDescriptor,
                                currentFieldType,
                                currentMessage.getField(messageFieldDescriptor)));
            }
        }
        return resultMessageBuilder.build();
    }

    /**
     * Bare-bone implementation of a trie data structure.
     */
    public static class TrieNode implements TreeLike<TrieNode> {
        @Nullable
        private final Value value;
        @Nullable
        private final Map<Type.Record.Field, TrieNode> childrenMap;
        @Nullable
        private final Map<Integer, Type.Record.Field> fieldIndexToFieldMap;

        public TrieNode(@Nullable final Value value, @Nullable final Map<Type.Record.Field, TrieNode> childrenMap) {
            this.value = value;
            this.childrenMap = childrenMap == null ? null : ImmutableMap.copyOf(childrenMap);
            this.fieldIndexToFieldMap = childrenMap == null ? null : computeFieldIndexToFieldMap(this.childrenMap);
        }

        @Nullable
        public Value getValue() {
            return value;
        }

        @Nullable
        public Map<Type.Record.Field, TrieNode> getChildrenMap() {
            return childrenMap;
        }

        @Nullable
        public Map<Integer, Type.Record.Field> getFieldIndexToFieldMap() {
            return fieldIndexToFieldMap;
        }

        @Nonnull
        @Override
        public TrieNode getThis() {
            return this;
        }

        @Nonnull
        @Override
        public Iterable<? extends TrieNode> getChildren() {
            return childrenMap == null ? ImmutableList.of() : childrenMap.values();
        }

        @Nonnull
        @Override
        public TrieNode withChildren(final Iterable<? extends TrieNode> newChildren) {
            throw new UnsupportedOperationException("trie does not define order among children");
        }

        @Nonnull
        public Collection<Value> values() {
            return Streams.stream(inPreOrder())
                    .flatMap(trie -> trie.getValue() == null ? Stream.of() : Stream.of(trie.getValue()))
                    .collect(ImmutableList.toImmutableList());
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TrieNode)) {
                return false;
            }
            final TrieNode trieNode = (TrieNode)o;
            return Objects.equals(getValue(), trieNode.getValue()) &&
                   Objects.equals(getChildrenMap(), trieNode.getChildrenMap()) &&
                   Objects.equals(getFieldIndexToFieldMap(), trieNode.getFieldIndexToFieldMap());
        }

        public boolean semanticEquals(final Object other, @Nonnull final AliasMap equivalencesMap) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TrieNode)) {
                return false;
            }
            final TrieNode otherTrieNode = (TrieNode)other;

            return equalsNullable(getValue(), otherTrieNode.getValue(), (t, o) -> t.semanticEquals(o, equivalencesMap)) &&
                   equalsNullable(getChildrenMap(), otherTrieNode.getChildrenMap(), (t, o) -> semanticEqualsForChildrenMap(t, o, equivalencesMap)) &&
                   Objects.equals(getFieldIndexToFieldMap(), otherTrieNode.getFieldIndexToFieldMap());
        }

        private static boolean semanticEqualsForChildrenMap(@Nonnull final Map<Type.Record.Field, TrieNode> self,
                                                            @Nonnull final Map<Type.Record.Field, TrieNode> other,
                                                            @Nonnull final AliasMap equivalencesMap) {
            if (self.size() != other.size()) {
                return false;
            }

            for (final var fieldPath : self.keySet()) {
                final var selfNestedTrie = self.get(fieldPath);
                final var otherNestedTrie = self.get(fieldPath);
                if (!selfNestedTrie.semanticEquals(otherNestedTrie, equivalencesMap)) {
                    return false;
                }
            }
            return true;
        }

        private static <T> boolean equalsNullable(@Nullable final T self,
                                                  @Nullable final T other,
                                                  @Nonnull final BiFunction<T, T, Boolean> nonNullableTest) {
            if (self == null && other == null) {
                return true;
            }
            if (self == null) {
                return false;
            }
            return nonNullableTest.apply(self, other);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getValue(), getChildrenMap(), getFieldIndexToFieldMap());
        }

        @Nonnull
        private static Map<Integer, Type.Record.Field> computeFieldIndexToFieldMap(@Nonnull final Map<Type.Record.Field, TrieNode> childrenMap) {
            final var resultBuilder = ImmutableMap.<Integer, Type.Record.Field>builder();
            for (final var entry : childrenMap.entrySet()) {
                resultBuilder.put(entry.getKey().getFieldIndex(), entry.getKey());
            }
            return resultBuilder.build();
        }
    }
}
