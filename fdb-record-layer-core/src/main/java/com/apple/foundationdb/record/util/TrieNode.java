/*
 * TrieNode.java
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

package com.apple.foundationdb.record.util;

import com.apple.foundationdb.record.query.plan.cascades.TreeLike;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Basic trie implementation that stores values at the trie's leaves.
 *
 * @param <D> discriminator type parameter
 * @param <T> type parameter of objects stored at the leafs
 * @param <N> higher-order type to capture interior nodes
 */
public abstract class TrieNode<D, T, N extends TrieNode<D, T, N>> implements TreeLike<N> {
    @Nullable
    private final T value;
    @Nullable
    private final Map<D, N> childrenMap;

    public TrieNode(@Nullable final T value, @Nullable final Map<D, N> childrenMap) {
        this.value = value;
        this.childrenMap = childrenMap == null ? null : ImmutableMap.copyOf(childrenMap);
    }

    @Nullable
    public T getValue() {
        return value;
    }

    @Nullable
    public Map<D, N> getChildrenMap() {
        return childrenMap;
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
    public Collection<T> values() {
        return Streams.stream(inPreOrder())
                .flatMap(trie -> trie.getValue() == null ? Stream.of() : Stream.of(trie.getValue()))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TrieNode)) {
            return false;
        }
        final TrieNode<?, ?, ?> otherTrieNode = (TrieNode<?, ?, ?>)other;
        return Objects.equals(getValue(), otherTrieNode.getValue()) &&
               Objects.equals(getChildrenMap(), otherTrieNode.getChildrenMap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getValue(), getChildrenMap());
    }
}
