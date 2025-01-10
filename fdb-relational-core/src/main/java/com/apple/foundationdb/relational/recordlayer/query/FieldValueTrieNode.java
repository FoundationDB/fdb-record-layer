/*
 * FieldValueTrieNode.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.values.EmptyValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.TrieNode;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * a {@link TrieNode} implementation having a {@link FieldValue.ResolvedAccessor} as key.
 */
@API(API.Status.EXPERIMENTAL)
public class FieldValueTrieNode extends TrieNode.AbstractTrieNode<FieldValue.ResolvedAccessor, Value, FieldValueTrieNode> {

    public FieldValueTrieNode(@Nonnull final Value value, @Nullable final Map<FieldValue.ResolvedAccessor, FieldValueTrieNode> childrenMap) {
        super(value, childrenMap);
    }

    public FieldValueTrieNode(@Nullable final Map<FieldValue.ResolvedAccessor, FieldValueTrieNode> childrenMap) {
        this(EmptyValue.empty(), childrenMap);
    }

    public FieldValueTrieNode(@Nonnull final Value value) {
        this(value, null);
    }

    public FieldValueTrieNode() {
        this(EmptyValue.empty(), null);
    }

    @Nonnull
    @Override
    public FieldValueTrieNode getThis() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FieldValueTrieNode)) {
            return false;
        }
        final FieldValueTrieNode transformationTrieNode = (FieldValueTrieNode) o;
        return Objects.equals(getValue(), transformationTrieNode.getValue()) &&
                Objects.equals(getChildrenMap(), transformationTrieNode.getChildrenMap());
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(final Object other, @Nonnull final AliasMap equivalencesMap) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FieldValueTrieNode)) {
            return false;
        }
        final FieldValueTrieNode otherFieldValueTrieNode = (FieldValueTrieNode) other;

        return equalsNullable(getValue(), otherFieldValueTrieNode.getValue(), (t, o) -> t.semanticEquals(o, equivalencesMap)) &&
                equalsNullable(getChildrenMap(), otherFieldValueTrieNode.getChildrenMap(), (t, o) -> semanticEqualsForChildrenMap(t, o, equivalencesMap));
    }

    private static boolean semanticEqualsForChildrenMap(@Nonnull final Map<FieldValue.ResolvedAccessor, FieldValueTrieNode> self,
                                                        @Nonnull final Map<FieldValue.ResolvedAccessor, FieldValueTrieNode> other,
                                                        @Nonnull final AliasMap equivalencesMap) {
        if (self.size() != other.size()) {
            return false;
        }

        for (final var entry : self.entrySet()) {
            final var ordinal = entry.getKey();
            final var selfNestedTrie = entry.getValue();
            final var otherNestedTrie = other.get(ordinal);
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
        return Objects.hash(getValue(), getChildrenMap());
    }

    public void validateNoOverlaps(@Nonnull Collection<FieldValueTrieNode> otherNodes) {
        // Make sure the same field path isn't referenced by other nodes
        Map<FieldValue.ResolvedAccessor, FieldValueTrieNode> children = getChildrenMap();
        if (children == null || children.isEmpty()) {
            return;
        }
        for (FieldValueTrieNode otherNode : otherNodes) {
            Map<FieldValue.ResolvedAccessor, FieldValueTrieNode> otherChildren = otherNode.getChildrenMap();
            if (otherChildren == null || otherChildren.isEmpty()) {
                continue;
            }
            for (Map.Entry<FieldValue.ResolvedAccessor, FieldValueTrieNode> otherEntry : otherChildren.entrySet()) {
                if (children.containsKey(otherEntry.getKey())) {
                    throw new RelationalException("Index with multiple disconnected references to the same column are not supported", ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException();
                }
            }
        }
    }

    public boolean isLeaf() {
        var children = getChildrenMap();
        return children == null || children.isEmpty();
    }

    /**
     * This method compresses an <i>ordered</i> set of prefixes into a trie data structure.
     *
     * @param orderedFieldPaths a list of ordered field paths.
     *
     * @return a trie representation of the ordered field paths.
     */
    @Nonnull
    public static FieldValueTrieNode computeTrieForFieldPaths(@Nonnull final Collection<FieldValue.FieldPath> orderedFieldPaths) {
        return computeTrieForFieldPaths(new FieldValue.FieldPath(ImmutableList.of()), orderedFieldPaths, Iterators.peekingIterator(orderedFieldPaths.iterator()));
    }

    @Nonnull
    private static FieldValueTrieNode computeTrieForFieldPaths(@Nonnull final FieldValue.FieldPath prefix,
                                                               @Nonnull final Collection<FieldValue.FieldPath> orderedFieldPaths,
                                                               @Nonnull final PeekingIterator<FieldValue.FieldPath> orderedFieldPathIterator) {
        if (orderedFieldPaths.contains(prefix)) {
            orderedFieldPathIterator.next();
            return new FieldValueTrieNode();
        }
        final var childrenMapBuilder = ImmutableMap.<FieldValue.ResolvedAccessor, FieldValueTrieNode>builder();
        while (orderedFieldPathIterator.hasNext()) {
            final var fieldPath = orderedFieldPathIterator.peek();
            if (!prefix.isPrefixOf(fieldPath)) {
                break;
            }

            final var prefixAccessors = prefix.getFieldAccessors();
            final var currentAccessor = fieldPath.getFieldAccessors().get(prefixAccessors.size());
            final var nestedPrefix = new FieldValue.FieldPath(ImmutableList.<FieldValue.ResolvedAccessor>builder()
                    .addAll(prefixAccessors)
                    .add(currentAccessor)
                    .build());

            final var currentTrie = computeTrieForFieldPaths(nestedPrefix, orderedFieldPaths, orderedFieldPathIterator);
            childrenMapBuilder.put(currentAccessor, currentTrie);
        }

        return new FieldValueTrieNode(childrenMapBuilder.build());
    }

    @Nonnull
    public static FieldValueTrieNode computeTrieForValues(@Nonnull final FieldValue.FieldPath prefix,
                                                          @Nonnull final PeekingIterator<Value> orderedValueIterator) {
        final List<FieldValue.ResolvedAccessor> prefixAccessors = prefix.getFieldAccessors();
        final var childrenMapBuilder = ImmutableMap.<FieldValue.ResolvedAccessor, FieldValueTrieNode>builder();
        while (orderedValueIterator.hasNext()) {
            final var value = orderedValueIterator.peek();
            if (!(value instanceof FieldValue)) {
                break;
            }

            final var fieldPath = ((FieldValue) value).getFieldPath();
            if (prefix.equals(fieldPath)) {
                orderedValueIterator.next();
                return new FieldValueTrieNode(value);
            } else if (!prefix.isPrefixOf(fieldPath)) {
                break;
            }

            final var currentAccessor = fieldPath.getFieldAccessors().get(prefixAccessors.size());
            final var nestedPrefix = new FieldValue.FieldPath(ImmutableList.<FieldValue.ResolvedAccessor>builderWithExpectedSize(prefixAccessors.size() + 1)
                    .addAll(prefixAccessors)
                    .add(currentAccessor)
                    .build()
            );

            final var currentTrie = computeTrieForValues(nestedPrefix, orderedValueIterator);
            childrenMapBuilder.put(currentAccessor, currentTrie);
        }

        try {
            return new FieldValueTrieNode(childrenMapBuilder.build());
        } catch (IllegalArgumentException e) {
            // Can happen if there are duplicate keys in the child map. This, in turn, indicates that the
            // same nested field path is being used multiple times (incorrectly)
            throw new RelationalException("Index with multiple disconnected references to the same column are not supported", ErrorCode.UNSUPPORTED_OPERATION, e).toUncheckedWrappedException();
        }
    }
}
