/*
 * BunchedMapScanEntry.java
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

package com.apple.foundationdb.map;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.subspace.Subspace;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Struct containing the results of scanning one or more {@link BunchedMap}s using the
 * {@link BunchedMap#scanMulti(com.apple.foundationdb.ReadTransaction, Subspace, SubspaceSplitter, byte[], byte[], byte[], int, boolean) scanMulti()}
 * function. This is the type returned by an {@link BunchedMapMultiIterator}. It includes, as one might
 * expect from a map entry object, a key and a value contained within a <code>BunchedMap</code>. However,
 * it also contains information about which subspace the key and value were contained within and which
 * "tag" the subspace was associated with. This is the same subspace and tag that is returned by a
 * {@link SubspaceSplitter}.
 *
 * @param <K> type of keys in the bunched map
 * @param <V> type of values in the bunched map
 * @param <T> type of the tag associated with each subspace
 */
@API(API.Status.EXPERIMENTAL)
public class BunchedMapScanEntry<K,V,T> {
    @Nonnull private final Subspace subspace;
    @Nullable private final T subspaceTag;
    @Nonnull private final K key;
    @Nonnull private final V value;

    BunchedMapScanEntry(@Nonnull final Subspace subspace, @Nullable final T subspaceTag,
                        @Nonnull final K key, @Nonnull final V value) {
        this.subspace = subspace;
        this.subspaceTag = subspaceTag;
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the subspace containing this entry. There should be one instance of a
     * {@link BunchedMap} within this {@link Subspace}.
     *
     * @return the subspace containing this entry
     */
    @Nonnull
    public Subspace getSubspace() {
        return subspace;
    }

    /**
     * Returns the tag associated with the subspace containing this entry. This will be consistent
     * with calling {@link SubspaceSplitter#subspaceTag(Subspace) splitter.subspaceTag(getSubspace())}
     * where <code>splitter</code> is the {@link SubspaceSplitter} that was used to distinguish
     * between different subspaces within a multi-map scan. However, this method does not
     * have to recalculate the tag. In particular, it can only be <code>null</code> if the
     * <code>SubspaceSplitter</code> can return <code>null</code>.
     *
     * @return the tag associated with the subspace containing this entry
     */
    @Nullable
    public T getSubspaceTag() {
        return subspaceTag;
    }

    /**
     * Returns the key from this entry.
     *
     * @return the map key associated with this entry
     */
    @Nonnull
    public K getKey() {
        return key;
    }

    /**
     * Returns the value from this entry.
     *
     * @return the map value associated with this entry
     */
    @Nonnull
    public V getValue() {
        return value;
    }
}
