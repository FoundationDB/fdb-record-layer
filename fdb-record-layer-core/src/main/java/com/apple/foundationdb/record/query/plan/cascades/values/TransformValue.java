/*
 * TransformValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue.FieldPath;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link Value} that performs a templates set of transformations on an input value. This transformation can be
 * utilized to implement e.g. SQL update functionality.
 */
@API(API.Status.EXPERIMENTAL)
public class TransformValue implements Value {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Transform-Value");

    @Nonnull
    private final Value inValue;

    /**
     * All given field paths in lexicographical order.
     */
    @Nonnull
    private final List<FieldPath> orderedFieldPaths;

    /**
     * The map of transformations as given by the caller.
     */
    @Nonnull
    private final Map<FieldPath, Value> transformMap;

    /**
     * A trie of transformations synthesized from the transform map passed in to the constructor by the caller.
     */
    @Nonnull
    private final TrieNode transformTrie;

    /**
     * A supplier that computes the official children of this value lazily on demand. The children are composed of
     * the {@code inValue} followed by all right-hand {@link Value}s in the order defined by {@link #orderedFieldPaths}.
     */
    @Nonnull
    private final Supplier<List<? extends Value>> childrenSupplier;
    @Nonnull
    private final Supplier<Integer> hashCodeWithoutChildrenSupplier;

    /**
     * Constructor.
     * Example:
     * <pre>
     * {@code
     *    new TransformValue(inValue,
     *                       ImmutableMap.of(path1, new LiteralValue<>("1"),
     *                                       path2, new LiteralValue<>(2),
     *                                       path3, new LiteralValue<>(3)));
     * }
     * </pre>
     * transforms (when the value's {@link #eval(FDBRecordStoreBase, EvaluationContext)} is invoked) an input object
     * (result of evaluation of {@code inValue}), according to the transform map, i.e. the data underneath {@code path1}
     * is transformed to the value {@code "1"}, the data underneath {@code path2} is transformed to the value {@code 2},
     * and the data underneath {@code path2} is transformed to the value {@code 3}.
     * @param inValue an input value to transform
     * @param transformMap a map of field paths to values.
     */
    public TransformValue(@Nonnull final Value inValue, @Nonnull final Map<FieldPath, Value> transformMap) {
        Preconditions.checkArgument(inValue.getResultType() instanceof Type.Record);
        this.inValue = inValue;
        this.orderedFieldPaths = checkAndPrepareOrderedFieldPaths(transformMap);
        this.transformMap = checkAndPrepareTransformMap(transformMap);
        this.transformTrie = computeTrieForFieldPaths(this.orderedFieldPaths, this.transformMap);
        this.childrenSupplier = Suppliers.memoize(this::computeChildren);
        this.hashCodeWithoutChildrenSupplier = Suppliers.memoize(this::computeHashCodeWithoutChildren);
    }

    @Nonnull
    public Map<FieldPath, Value> getTransformMap() {
        return transformMap;
    }

    @Nonnull
    @VisibleForTesting
    public TrieNode getTransformTrie() {
        return transformTrie;
    }

    @Nonnull
    @Override
    public List<? extends Value> getChildren() {
        return childrenSupplier.get();
    }

    @Nonnull
    private List<? extends Value> computeChildren() {
        final var childrenBuilder = ImmutableList.<Value>builder();
        childrenBuilder.add(inValue);
        transformMap.values() // this is deterministically ordered by contract in ImmutableMap.
                .forEach(childrenBuilder::add);
        return childrenBuilder.build();
    }

    @Nonnull
    @Override
    public Type.Record getResultType() {
        // result type is equal to in type
        return (Type.Record)inValue.getResultType();
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var inRecord = (M)Preconditions.checkNotNull(inValue.eval(store, context));
        return evalSubtree(store, context, transformTrie, inRecord.getDescriptorForType(), inRecord);
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public <M extends Message> Object evalSubtree(@Nonnull final FDBRecordStoreBase<M> store,
                                                  @Nonnull final EvaluationContext context,
                                                  @Nonnull final TrieNode trieNode,
                                                  @Nullable final Descriptors.Descriptor descriptor,
                                                  @Nullable final Object current) {
        final var value = trieNode.getValue();
        if (value != null) {
            return value.eval(store, context);
        } else {
            Verify.verifyNotNull(descriptor);
            final var fieldIndexToFieldMap = Verify.verifyNotNull(trieNode.getFieldIndexToFieldMap());
            final var childrenMap = Verify.verifyNotNull(trieNode.getChildrenMap());
            final var subRecord = (M)current;

            final var fieldDescriptors = descriptor.getFields();
            final var resultMessageBuilder = DynamicMessage.newBuilder(descriptor);
            for (final var fieldDescriptor : fieldDescriptors) {
                final var field = fieldIndexToFieldMap.get(fieldDescriptor.getNumber());
                if (field != null) {
                    final var fieldTrieNode = Verify.verifyNotNull(childrenMap.get(field));
                    var fieldResult = evalSubtree(store,
                            context,
                            fieldTrieNode,
                            fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE ? fieldDescriptor.getMessageType() : null,
                            subRecord == null ? null : subRecord.getField(fieldDescriptor));
                    final var fieldType = field.getFieldType();
                    if (fieldType.getTypeCode() == Type.TypeCode.ARRAY && fieldType.isNullable()) {
                        final var wrappedDescriptor = fieldDescriptor.getMessageType();
                        final var wrapperBuilder = DynamicMessage.newBuilder(wrappedDescriptor);
                        if (fieldResult != null) {
                            wrapperBuilder.setField(wrappedDescriptor.findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName()), fieldResult);
                        }
                        fieldResult = wrapperBuilder.build();
                    }
                    if (fieldResult != null) {
                        resultMessageBuilder.setField(fieldDescriptor, fieldResult);
                    }
                } else {
                    if (subRecord != null && subRecord.hasField(fieldDescriptor)) {
                        final var fieldResult = subRecord.getField(fieldDescriptor);
                        resultMessageBuilder.setField(fieldDescriptor, fieldResult);
                    }
                }
            }
            return resultMessageBuilder.build();
        }
    }

    @Override
    public int hashCodeWithoutChildren() {
        return hashCodeWithoutChildrenSupplier.get();
    }

    private int computeHashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION,
                BASE_HASH,
                orderedFieldPaths);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, inValue, transformMap);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "(" +
               orderedFieldPaths.stream()
                       .map(fieldPath -> fieldPath + " = " + Verify.verifyNotNull(transformMap.get(fieldPath)).explain(formatter))
                       .collect(Collectors.joining("; ")) + ")";
    }

    @Override
    public String toString() {
        return "(" +
               orderedFieldPaths.stream()
                       .map(fieldPath -> fieldPath + " = " + Verify.verifyNotNull(transformMap.get(fieldPath)))
                       .collect(Collectors.joining("; ")) + ")";
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Nonnull
    private static Map<FieldPath, Value> checkAndPrepareTransformMap(@Nonnull final Map<FieldPath, Value> transformMap) {
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
    private static List<FieldPath> checkAndPrepareOrderedFieldPaths(@Nonnull final Map<FieldPath, Value> transformMap) {
        // this brings together all paths that share the same prefixes
        final var orderedFieldPaths =
                transformMap.keySet()
                        .stream()
                        .sorted(FieldPath.comparator())
                        .collect(ImmutableList.toImmutableList());

        FieldPath currentFieldPath = null;
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
    private static TrieNode computeTrieForFieldPaths(@Nonnull final Collection<FieldPath> orderedFieldPaths, @Nonnull final Map<FieldPath, Value> transformMap) {
        return computeTrieForFieldPaths(new FieldPath(ImmutableList.of()), transformMap, Iterators.peekingIterator(orderedFieldPaths.iterator()));
    }

    @Nonnull
    private static TrieNode computeTrieForFieldPaths(@Nonnull final FieldPath prefix,
                                                     @Nonnull final Map<FieldPath, Value> transformMap,
                                                     @Nonnull final PeekingIterator<FieldPath> orderedFieldPathIterator) {
        if (transformMap.containsKey(prefix)) {
            orderedFieldPathIterator.next();
            return new TrieNode(Verify.verifyNotNull(transformMap.get(prefix)), null);
        }
        final var childrenMapBuilder = ImmutableMap.<FieldValue.FieldDelegate, TrieNode>builder();
        while (orderedFieldPathIterator.hasNext()) {
            final var fieldPath = orderedFieldPathIterator.peek();
            if (!prefix.isPrefixOf(fieldPath)) {
                break;
            }

            final var prefixFields = prefix.getFields();
            final var currentField = fieldPath.getFields().get(prefixFields.size());
            final var nestedPrefix = new FieldPath(ImmutableList.<FieldValue.FieldDelegate>builder()
                    .addAll(prefixFields)
                    .add(currentField)
                    .build());

            final var currentTrie = computeTrieForFieldPaths(nestedPrefix, transformMap, orderedFieldPathIterator);
            childrenMapBuilder.put(currentField, currentTrie);
        }

        return new TrieNode(null, childrenMapBuilder.build());
    }

    @Nonnull
    @Override
    public TransformValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(getChildren().size() == Iterables.size(newChildren));

        final var newTransformMapBuilder = ImmutableMap.<FieldPath, Value>builder();

        final var newChildrenIterator = newChildren.iterator();

        Verify.verify(newChildrenIterator.hasNext());
        final var newInValue = newChildrenIterator.next();

        int i = 0;
        while (newChildrenIterator.hasNext()) {
            final var fieldPath = orderedFieldPaths.get(i);
            final var newChild = newChildrenIterator.next();
            newTransformMapBuilder.put(fieldPath, newChild);
            i ++;
        }
        Verify.verify(i == orderedFieldPaths.size());

        return new TransformValue(newInValue, newTransformMapBuilder.build());
    }

    /**
     * Bare-bone implementation of a trie data structure.
     */
    public static class TrieNode {
        @Nullable
        private final Value value;
        @Nullable
        private final Map<FieldValue.FieldDelegate, TrieNode> childrenMap;
        @Nullable
        private final Map<Integer, FieldValue.FieldDelegate> fieldIndexToFieldMap;

        public TrieNode(@Nullable final Value value, @Nullable final Map<FieldValue.FieldDelegate, TrieNode> childrenMap) {
            this.value = value;
            this.childrenMap = childrenMap == null ? null : ImmutableMap.copyOf(childrenMap);
            this.fieldIndexToFieldMap = childrenMap == null ? null : computeFieldIndexToFieldMap(this.childrenMap);
        }

        @Nullable
        public Value getValue() {
            return value;
        }

        @Nullable
        public Map<FieldValue.FieldDelegate, TrieNode> getChildrenMap() {
            return childrenMap;
        }

        @Nullable
        public Map<Integer, FieldValue.FieldDelegate> getFieldIndexToFieldMap() {
            return fieldIndexToFieldMap;
        }

        @Nonnull
        private static Map<Integer, FieldValue.FieldDelegate> computeFieldIndexToFieldMap(@Nonnull final Map<FieldValue.FieldDelegate, TrieNode> childrenMap) {
            final var resultBuilder = ImmutableMap.<Integer, FieldValue.FieldDelegate>builder();
            for (final var entry : childrenMap.entrySet()) {
                resultBuilder.put(entry.getKey().getFieldIndex(), entry.getKey());
            }
            return resultBuilder.build();
        }
    }
}
