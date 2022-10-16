/*
 * RecordQueryTypeFilterPlan.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that filters out records from a child plan that are not of the designated record type(s).
 */
@API(API.Status.INTERNAL)
public class RecordQueryUpdatePlan implements RecordQueryPlanWithChild, PlannerGraphRewritable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Update-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryTypeFilterPlan.class);

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final String recordType;
    @Nonnull
    private final Descriptors.Descriptor targetDescriptor;
    
    /**
     * A trie of transformations that is synthesized to perform a one-pass transformation of the incoming records.
     */
    @Nonnull
    private final TrieNode transformTrie;

    private RecordQueryUpdatePlan(@Nonnull final Quantifier.Physical inner,
                                  @Nonnull final String recordType,
                                  @Nonnull final Descriptors.Descriptor targetDescriptor,
                                  @Nonnull final TrieNode transformTrie) {
        this.inner = inner;
        this.recordType = recordType;
        this.targetDescriptor = targetDescriptor;
        this.transformTrie = transformTrie;
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
                .map(queryResult -> updateRecord(store, context, queryResult))
                .mapPipelined(store::saveRecordAsync, store.getPipelineSize(PipelineOperation.UPDATE))
                .map(storedRecord -> QueryResult.fromQueriedRecord(FDBQueriedRecord.stored(storedRecord)));
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public <M extends Message> M updateRecord(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nonnull final QueryResult queryResult) {
        final var inRecord = (M)Preconditions.checkNotNull(queryResult.getMessage());
        return (M)transformMessage(store, context, transformTrie, targetDescriptor, inRecord);
    }

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
    public String toString() {
        return getInnerPlan() + " | " + "UPDATE " + recordType;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryUpdatePlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                       @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryUpdatePlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                getRecordType(),
                targetDescriptor,
                transformTrie);
    }

    @Nonnull
    @Override
    public RecordQueryUpdatePlan withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryUpdatePlan(Quantifier.physical(GroupExpressionRef.of(child)), getRecordType(), targetDescriptor, transformTrie);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
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
        if (targetDescriptor != otherUpdatePlan.targetDescriptor) {
            return false;
        }
        return transformTrie.equals(otherUpdatePlan.transformTrie);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        // TODO
        return Objects.hash(BASE_HASH.planHash(), recordType);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return getInnerPlan().planHash(hashKind) + recordType.hashCode();
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getInnerPlan(), recordType.hashCode());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
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
    public String getRecordType() {
        return recordType;
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

    /**
     * Rewrite the planner graph for better visualization of a query index plan.
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return the rewritten planner graph that models the filter as a node that uses the expression attribute
     *         to depict the record types this operator filters.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.ModificationOperatorNodeWithInfo(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("UPDATE {{recordType}}"),
                        ImmutableMap.of("recordType", Attribute.gml(getRecordType()))),
                childGraphs);
    }

    @Nonnull
    @VisibleForTesting
    public static Map<FieldValue.FieldPath, Value> checkAndPrepareTransformMap(@Nonnull final Map<FieldValue.FieldPath, Value> transformMap) {
        for (final var entry : transformMap.entrySet()) {
            // TODO check this using the type, not just the type code! For that to work we need isAssignableTo() checking
            //      in the type system, so we can account for e.g. differences in nullabilities between the types.
            SemanticException.check(entry.getKey()
                    .getLastField()
                    .getFieldType().getTypeCode().equals(entry.getValue().getResultType().getTypeCode()), SemanticException.ErrorCode.ASSIGNMENT_WRONG_TYPE);
        }
        return ImmutableMap.copyOf(transformMap);
    }

    @Nonnull
    @VisibleForTesting
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
     * Constructor.
     * Example:
     * <pre>
     * {@code
     *    RecordQueryUpdatePlan.forTransformMap(inner,
     *                                          ...,
     *                                          ImmutableMap.of(path1, new LiteralValue<>("1"),
     *                                                          path2, new LiteralValue<>(2),
     *                                                          path3, new LiteralValue<>(3)));
     * }
     * </pre>
     * transforms the current inner's object, according to the transform map, i.e. the data underneath {@code path1}
     * is transformed to the value {@code "1"}, the data underneath {@code path2} is transformed to the value {@code 2},
     * and the data underneath {@code path2} is transformed to the value {@code 3}.
     * @param inner an input value to transform
     * @param recordType the name of the record type this update modifies
     * @param targetDescriptor a protobuf descriptor to coerce the current record prior to the update
     * @param transformMap a map of field paths to values.
     */
    @Nonnull
    public static RecordQueryUpdatePlan forTransformMap(@Nonnull final Quantifier.Physical inner,
                                                        @Nonnull final String recordType,
                                                        @Nonnull final Descriptors.Descriptor targetDescriptor,
                                                        @Nonnull final Map<FieldValue.FieldPath, Value> transformMap) {
        return new RecordQueryUpdatePlan(inner, recordType, targetDescriptor, computeTrieForFieldPaths(checkAndPrepareOrderedFieldPaths(transformMap), transformMap));
    }

    /**
     * Method to compute a trie from a collection of lexicographically-ordered field paths. The trie is computed at
     * instantiation time (planning time). It serves to transform the input value in one pass.
     * @param orderedFieldPaths a collection of field paths that must be lexicographically-ordered.
     * @param transformMap a map of transformations
     * @return a {@link TrieNode}
     */
    @Nonnull
    @VisibleForTesting
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
    @SuppressWarnings("unchecked")
    public static <M extends Message> Object transformMessage(@Nonnull final FDBRecordStoreBase<M> store,
                                                              @Nonnull final EvaluationContext context,
                                                              @Nonnull final TrieNode trieNode,
                                                              @Nullable Descriptors.Descriptor targetDescriptor,
                                                              @Nullable final Object current) {
        final var value = trieNode.getValue();
        if (value != null) {
            return value.eval(store, context);
        } else {
            targetDescriptor = Verify.verifyNotNull(targetDescriptor);
            final var fieldIndexToFieldMap = Verify.verifyNotNull(trieNode.getFieldIndexToFieldMap());
            final var childrenMap = Verify.verifyNotNull(trieNode.getChildrenMap());
            final var subRecord = (M)Verify.verifyNotNull(current);

            final var resultMessageBuilder = DynamicMessage.newBuilder(targetDescriptor);
            final var messageDescriptor = subRecord.getDescriptorForType();
            for (final var messageFieldDescriptor : messageDescriptor.getFields()) {
                final var field = fieldIndexToFieldMap.get(messageFieldDescriptor.getNumber());
                final var targetFieldDescriptor = targetDescriptor.findFieldByName(messageFieldDescriptor.getName());
                if (field != null) {
                    final var fieldTrieNode = Verify.verifyNotNull(childrenMap.get(field));
                    var fieldResult = transformMessage(store,
                            context,
                            fieldTrieNode,
                            targetFieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE ? targetFieldDescriptor.getMessageType() : null,
                            subRecord.getField(messageFieldDescriptor));
                    final var fieldType = field.getFieldType();
                    if (fieldType.getTypeCode() == Type.TypeCode.ARRAY && fieldType.isNullable()) {
                        final var wrappedDescriptor = targetFieldDescriptor.getMessageType();
                        final var wrapperBuilder = DynamicMessage.newBuilder(wrappedDescriptor);
                        if (fieldResult != null) {
                            wrapperBuilder.setField(wrappedDescriptor.findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName()), fieldResult);
                        }
                        fieldResult = wrapperBuilder.build();
                    }
                    if (fieldResult != null) {
                        resultMessageBuilder.setField(targetFieldDescriptor, fieldResult);
                    }
                } else {
                    if (subRecord.hasField(messageFieldDescriptor)) {
                        final var fieldResult = coerceField(targetFieldDescriptor, subRecord);
                        resultMessageBuilder.setField(targetFieldDescriptor, fieldResult);
                    }
                }
            }
            return resultMessageBuilder.build();
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public static <M extends Message> Object coerceField(@Nonnull final Descriptors.FieldDescriptor targetFieldDescriptor,
                                                         @Nonnull final Object current) {
        switch (targetFieldDescriptor.getJavaType()) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case BYTE_STRING:
            case ENUM:
                return current;
            case MESSAGE:
                return coerceMessage(targetFieldDescriptor.getMessageType(), (M)current);
            default:
                throw new IllegalStateException("unsupported java type for record field");
        }
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <M extends Message> Message coerceMessage(@Nonnull final Descriptors.Descriptor targetDescriptor,
                                                            @Nonnull final M message) {
        Verify.verifyNotNull(targetDescriptor);
        final var resultMessageBuilder = DynamicMessage.newBuilder(targetDescriptor);
        final var messageDescriptor = message.getDescriptorForType();
        for (final var messageFieldDescriptor : messageDescriptor.getFields()) {
            final var targetFieldDescriptor = Verify.verifyNotNull(targetDescriptor.findFieldByName(messageFieldDescriptor.getName()));
            if (message.hasField(messageFieldDescriptor)) {
                resultMessageBuilder.setField(targetFieldDescriptor, coerceField(targetFieldDescriptor, message.getField(messageFieldDescriptor)));
            }
        }
        return resultMessageBuilder.build();
    }

    /**
     * Bare-bone implementation of a trie data structure.
     */
    public static class TrieNode {
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

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TrieNode)) {
                return false;
            }
            final TrieNode trieNode = (TrieNode)o;
            return Objects.equals(getValue(), trieNode.getValue()) && Objects.equals(getChildrenMap(), trieNode.getChildrenMap()) && Objects.equals(getFieldIndexToFieldMap(), trieNode.getFieldIndexToFieldMap());
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
