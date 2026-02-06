/*
 * TrieNode.java
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

package com.apple.foundationdb.record.util;

import com.apple.foundationdb.record.query.plan.cascades.TreeLike;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Basic trie implementation that stores values at the trie's leaves.
 *
 * @param <D> discriminator type parameter
 * @param <T> type parameter of objects stored at the leafs
 * @param <N> higher-order type to capture interior nodes
 */
public interface TrieNode<D, T, N extends TrieNode<D, T, N>> extends TreeLike<N> {
    @Nullable
    T getValue();

    @Nullable
    Map<D, N> getChildrenMap();

    @Nonnull
    @Override
    Iterable<N> getChildren();

    @Nonnull
    List<T> values();

    /**
     * Basic trie implementation.
     *
     * @param <D> discriminator type parameter
     * @param <T> type parameter of objects stored at the leafs
     * @param <N> higher-order type to capture interior nodes
     */
    abstract class AbstractTrieNode<D, T, N extends AbstractTrieNode<D, T, N>> implements TrieNode<D, T, N> {
        @Nullable
        private final T value;
        @Nullable
        private final Map<D, N> childrenMap;
        @Nonnull
        @SuppressWarnings("this-escape")
        private final Supplier<Iterable<N>> childrenSupplier = Suppliers.memoize(this::computeChildren);
        @Nonnull
        @SuppressWarnings("this-escape")
        private final Supplier<Integer> heightSupplier = Suppliers.memoize(TrieNode.super::height);

        public AbstractTrieNode(@Nullable final T value, @Nullable final Map<D, N> childrenMap) {
            this.value = value;
            this.childrenMap = childrenMap == null ? null : ImmutableMap.copyOf(childrenMap);
        }

        @Nullable
        @Override
        public T getValue() {
            return value;
        }

        @Nullable
        @Override
        public Map<D, N> getChildrenMap() {
            return childrenMap;
        }

        @Nonnull
        @Override
        public Iterable<N> getChildren() {
            return childrenSupplier.get();
        }

        @Nonnull
        private Iterable<N> computeChildren() {
            return childrenMap == null ? ImmutableList.of() : childrenMap.values();
        }

        @Override
        public int height() {
            return heightSupplier.get();
        }

        @Nonnull
        @Override
        public N withChildren(final Iterable<? extends N> newChildren) {
            throw new UnsupportedOperationException("trie does not define order among children");
        }

        @Nonnull
        @Override
        public List<T> values() {
            return preOrderStream()
                    .flatMap(trie -> trie.getValue() == null ? Stream.of() : Stream.of(trie.getValue()))
                    .collect(ImmutableList.toImmutableList());
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null) {
                return false;
            }
            if (getClass() != other.getClass()) {
                return false;
            }
            final AbstractTrieNode<?, ?, ?> otherTrieNode = (AbstractTrieNode<?, ?, ?>)other;
            return Objects.equals(getValue(), otherTrieNode.getValue()) &&
                    Objects.equals(getChildrenMap(), otherTrieNode.getChildrenMap());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getValue(), getChildrenMap());
        }
    }

    /**
     * Basic trie builder implementation.
     *
     * @param <D> discriminator type parameter
     * @param <T> type parameter of objects stored at the leafs
     * @param <N> higher-order type to capture interior nodes
     */
    abstract class AbstractTrieNodeBuilder<D, T, N extends AbstractTrieNodeBuilder<D, T, N>> implements TrieNode<D, T, N> {
        @Nullable
        private T value;
        @Nullable
        private Map<D, N> childrenMap;

        public AbstractTrieNodeBuilder(@Nullable final T value, @Nullable final Map<D, N> childrenMap) {
            this.value = value;
            this.childrenMap = childrenMap == null ? null : Maps.newLinkedHashMap(childrenMap);
        }

        @Nullable
        @Override
        public T getValue() {
            return value;
        }

        @Nonnull
        public N setValue(@Nullable final T value) {
            this.value = value;
            return getThis();
        }

        @Nullable
        @Override
        public Map<D, N> getChildrenMap() {
            return childrenMap;
        }

        @Nonnull
        public N computeIfAbsent(final D key,
                                 final Function<D, N> mappingFunction) {
            if (this.childrenMap == null) {
                this.childrenMap = Maps.newHashMap();
            }
            return childrenMap.computeIfAbsent(key, mappingFunction);
        }

        @Nonnull
        public N compute(final D key,
                         final BiFunction<D, N, N> mappingBiFunction) {
            if (this.childrenMap == null) {
                this.childrenMap = Maps.newHashMap();
            }
            return childrenMap.compute(key, mappingBiFunction);
        }

        @Nonnull
        @Override
        public Iterable<N> getChildren() {
            return childrenMap == null ? ImmutableList.of() : childrenMap.values();
        }

        @Nonnull
        @Override
        public N withChildren(final Iterable<? extends N> newChildren) {
            throw new UnsupportedOperationException("trie does not define order among children");
        }

        @Nonnull
        @Override
        public List<T> values() {
            return preOrderStream()
                    .flatMap(trie -> trie.getValue() == null ? Stream.of() : Stream.of(trie.getValue()))
                    .collect(ImmutableList.toImmutableList());
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null) {
                return false;
            }
            if (getClass() != other.getClass()) {
                return false;
            }
            final AbstractTrieNodeBuilder<?, ?, ?> otherTrieNode = (AbstractTrieNodeBuilder<?, ?, ?>)other;
            return Objects.equals(getValue(), otherTrieNode.getValue()) &&
                    Objects.equals(getChildrenMap(), otherTrieNode.getChildrenMap());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getValue(), getChildrenMap());
        }
    }
}
