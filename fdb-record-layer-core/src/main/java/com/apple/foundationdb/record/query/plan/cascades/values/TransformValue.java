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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue.FieldPath;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link Value} that encapsulates its children {@link Value}s into a single {@link Value} of a
 * {@link Type.Record} type.
 */
@API(API.Status.EXPERIMENTAL)
public class TransformValue implements Value {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Transform-Value");

    @Nonnull
    private final Value inValue;
    @Nonnull
    private final List<FieldPath> orderedFieldPaths;
    @Nonnull
    private final Map<FieldPath, Value> transformMap;
    @Nonnull
    private final TrieNode transformTrie;
    @Nonnull
    private final Supplier<List<? extends Value>> childrenSupplier;
    @Nonnull
    private final Supplier<Integer> hashCodeWithoutChildrenSupplier;

    public TransformValue(@Nonnull final Value inValue, @Nonnull Map<FieldPath, Value> transformMap) {
        Preconditions.checkArgument(inValue.getResultType() instanceof Type.Record);
        this.inValue = inValue;
        // this brings together all paths that share the same prefixes
        this.orderedFieldPaths = transformMap.keySet()
                .stream()
                .sorted(FieldPath.comparator())
                .collect(ImmutableList.toImmutableList());
        this.transformMap = ImmutableMap.copyOf(transformMap);
        this.transformTrie = computeTrieForFieldPaths(this.orderedFieldPaths, this.transformMap);
        this.childrenSupplier = Suppliers.memoize(this::computeChildren);
        this.hashCodeWithoutChildrenSupplier = Suppliers.memoize(this::computeHashCodeWithoutChildren);
    }

    @Nonnull
    @Override
    public List<? extends Value> getChildren() {
        return childrenSupplier.get();
    }

    @Nonnull
    public List<? extends Value> computeChildren() {
        final var childrenBuilder = ImmutableList.<Value>builder();
        childrenBuilder.add(inValue);
        transformMap.values() // this is deterministically ordered by contract in ImmutableMap.
                .forEach(childrenBuilder::add);
        return childrenBuilder.build();
    }

    @Nonnull
    @Override
    public Type.Record getResultType() {
        return (Type.Record)inValue.getResultType();
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var inRecord = (M)Preconditions.checkNotNull(inValue.eval(store, context));
        final var descriptorForType = inRecord.getDescriptorForType();
        final var resultMessageBuilder = DynamicMessage.newBuilder(descriptorForType);
        final var fields = Objects.requireNonNull(getResultType().getFields());

        return evalWithPrefix(store, context, inRecord, orderedFieldPaths.iterator(), 0);
    }

    @Nullable
    public <M extends Message> Object evalWithPrefix(@Nonnull final FDBRecordStoreBase<M> store,
                                                     @Nonnull final EvaluationContext context,
                                                     @Nonnull final M subRecord,
                                                     @Nonnull final Iterator<FieldPath> fieldPathIterator,
                                                     @Nonnull final int depth) {
        final var descriptorForType = subRecord.getDescriptorForType();
        final var resultMessageBuilder = DynamicMessage.newBuilder(descriptorForType);
        

        return null;
    }

    @Nonnull
    private DynamicMessage.Builder newMessageBuilderForType(@Nonnull TypeRepository typeRepository) {
        return Objects.requireNonNull(typeRepository.newMessageBuilder(getResultType()));
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

    private static TrieNode computeTrieForFieldPaths(@Nonnull final Collection<FieldPath> orderedFieldPaths, @Nonnull final Map<FieldPath, Value> transformMap) {
        return computeTrieForFieldPaths(new FieldPath(ImmutableList.of()), transformMap, Iterators.peekingIterator(orderedFieldPaths.iterator()));
    }

    private static TrieNode computeTrieForFieldPaths(@Nonnull final FieldPath prefix,
                                                     @Nonnull final Map<FieldPath, Value> transformMap,
                                                     @Nonnull final PeekingIterator<FieldPath> orderedFieldPathIterator) {
        if (transformMap.containsKey(prefix)) {
            orderedFieldPathIterator.next();
            return new TrieNode(Verify.verifyNotNull(transformMap.get(prefix)), null);
        }
        final var childrenMapBuilder = ImmutableMap.<Integer, TrieNode>builder();
        while (orderedFieldPathIterator.hasNext()) {
            final var fieldPath = orderedFieldPathIterator.peek();
            if (prefix != null && !prefix.isPrefixOf(fieldPath)) {
                break;
            }

            final var prefixFields = prefix == null ? ImmutableList.<Type.Record.Field>of() : prefix.getFields();
            final var currentField = fieldPath.getFields().get(prefixFields.size());
            final var nestedPrefix = new FieldPath(ImmutableList.<Type.Record.Field>builder()
                    .addAll(prefixFields)
                    .add(currentField)
                    .build());

            final var currentTrie = computeTrieForFieldPaths(nestedPrefix, transformMap, orderedFieldPathIterator);
            childrenMapBuilder.put(currentField.getFieldIndex(), currentTrie);
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

    private static class TrieNode {
        @Nullable
        private final Value value;
        @Nullable
        private final Map<Integer, TrieNode> childrenMap;

        public TrieNode(@Nullable final Value value, @Nullable final Map<Integer, TrieNode> childrenMap) {
            this.value = value;
            this.childrenMap = childrenMap == null ? null : ImmutableMap.copyOf(childrenMap);
        }

        @Nullable
        public Value getValue() {
            return value;
        }

        @Nullable
        public Map<Integer, TrieNode> getChildrenMap() {
            return childrenMap;
        }
    }
}
