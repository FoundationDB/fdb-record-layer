/*
 * RecordStoreState.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.Index;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * The <code>RecordStoreState</code> interface handles information that might differ between otherwise identical
 * record stores. In particular, it handles the data that might be different between two different record stores
 * that have the same meta-data.
 *
 * <p>
 * At the moment, this class tracks two pieces of store-specific meta-data:
 * </p>
 * <ul>
 *     <li>
 *         The store header. The header is a Protobuf message including information such as the
 *         store's format and meta-data version. This is used at store initialization time to validate
 *         the meta-data being used and take appropriate action on meta-data changes.
 *     </li>
 *     <li>
 *         Index state information. This includes whether each index is currently readable, disabled,
 *         or write-only. This information is used by the planner when selecting indexes and by the
 *         store when choosing which indexes to update upon record insertion.
 *     </li>
 * </ul>
 */
@API(API.Status.UNSTABLE)
public class RecordStoreState {

    @Nonnull
    protected final AtomicReference<RecordMetaDataProto.DataStoreInfo> storeHeader;
    @Nonnull
    protected final AtomicReference<Map<String, IndexState>> indexStateMap;

    /**
     * Creates a <code>RecordStoreState</code> with the given index states.
     * Only indexes that are not in the default state ({@link IndexState#READABLE IndexState.READABLE})
     * need to be included in the map.
     * @param storeHeader header information for the given store
     * @param indexStateMap mapping from index name to index state
     */
    @API(API.Status.INTERNAL)
    public RecordStoreState(@Nullable RecordMetaDataProto.DataStoreInfo storeHeader, @Nullable Map<String, IndexState> indexStateMap) {
        final Map<String, IndexState> copy;
        if (indexStateMap == null || indexStateMap.isEmpty()) {
            copy = Collections.emptyMap();
        } else {
            copy = ImmutableMap.copyOf(indexStateMap);
        }
        this.storeHeader = new AtomicReference<>(storeHeader == null ? RecordMetaDataProto.DataStoreInfo.getDefaultInstance() : storeHeader);
        this.indexStateMap = new AtomicReference<>(copy);
    }

    /**
     * Begin using this record store state.
     * Until {@link #endRead} is called, the state will not change.
     */
    public void beginRead() {
        // This implementation is immutable so there is nothing special to do at the beginning of a read.
        // Other implementations that inherit from this one might be mutable and need to do something.
    }

    /**
     * End using this record store state.
     * @see #beginRead()
     */
    public void endRead() {
        // This implementation is immutable so there is nothing special to do at the end of a read.
        // Other implementations that inherit from this one might be mutable and need to do something.
    }

    /**
     * Retrieve the mapping from index names to {@link IndexState} that is
     * underlying this <code>RecordStoreState</code>. This operation is
     * constant time as it does not return a copy, but the map that is
     * returned is immutable.
     * @return the underlying mapping of index names to their state
     */
    @Nonnull
    public Map<String, IndexState> getIndexStates() {
        return Collections.unmodifiableMap(indexStateMap.get());
    }

    /**
     * Determines whether the index provided is a write-only index.
     * This is determined by looking it up in the set.
     * @param index the index to check
     * @return <code>true</code> if the given index is write-only and <code>false</code> otherwise
     */
    public boolean isWriteOnly(@Nonnull Index index) {
        return isWriteOnly(index.getName());
    }

    /**
     * Determines whether the index of the given name is a write-only
     * index. This is determined by looking it up the set.
     * @param indexName the name of the index to check
     * @return <code>true</code> if the given name is the name of a write-only index and <code>false</code> otherwise
     */
    public boolean isWriteOnly(@Nonnull String indexName) {
        return getState(indexName).equals(IndexState.WRITE_ONLY);
    }

    /**
     * Determines whether the index is disabled. This is determined
     * by looking it up in the set.
     * @param index the index to check
     * @return <code>true</code> if the given index is disabled and <code>false</code> otherwise
     */
    public boolean isDisabled(@Nonnull Index index) {
        return isDisabled(index.getName());
    }

    /**
     * Determines whether the index of the given is disabled. This is determined
     * by looking it up in the set.
     * @param indexName the name of the index to check
     * @return <code>true</code> if the given index is disabled and <code>false</code> otherwise
     */
    public boolean isDisabled(@Nonnull String indexName) {
        return getState(indexName).equals(IndexState.DISABLED);
    }

    /**
     * Determines whether the index is readable. This is done just by process of
     * elimination, i.e., making sure it is not write-only and not disabled.
     * @param index the index to check
     * @return <code>true</code> if the given index is readable and <code>false</code> otherwise
     */
    public boolean isReadable(@Nonnull Index index) {
        return isReadable(index.getName());
    }

    /**
     * Determines whether the index of the given name is readable. This is done just by process of
     * elimination, i.e., making sure it is not write-only and not disabled.
     * @param indexName the name of the index to check
     * @return <code>true</code> if the given index is readable and <code>false</code> otherwise
     */
    public boolean isReadable(@Nonnull String indexName) {
        return getState(indexName).equals(IndexState.READABLE);
    }


    /**
     * Determines whether the index of the given name is in readable-unique-pending. This state means that
     * the index is fully indexed, but cannot be readable because of duplicated entries that contradict its
     * UNIQUE flag.
     * @param indexName the name of the index to check
     * @return <code>true</code> if the given index is readable-unique-pending and <code>false</code> otherwise
     */
    public boolean isReadableUniquePending(@Nonnull String indexName) {
        return getState(indexName).equals(IndexState.READABLE_UNIQUE_PENDING);
    }


    /**
     * Determines whether the index of the given name is in scannable. This state means that the index is
     * either readable or readable-unique-pending.
     * @param indexName the name of the index to check
     * @return <code>true</code> if the given index is scannable and <code>false</code> otherwise
     */
    public boolean isScannable(@Nonnull String indexName) {
        return getState(indexName).isScannable();
    }

    /**
     * Determine the state of an index. Note that all indexes are assumed to be
     * readable unless marked otherwise, i.e., this will return {@link IndexState#READABLE IndexState.READABLE}
     * if it is given an index that is not explicitly in its write-only or disabled lists.
     * @param index the index to check
     * @return the state of the given index
     */
    @Nonnull
    public IndexState getState(@Nonnull Index index) {
        return getState(index.getName());
    }

    /**
     * Determine the state of the index with the given name. Note that all indexes are assumed to be
     * readable unless marked otherwise, i.e., this will return {@link IndexState#READABLE IndexState.READABLE}
     * if it is given an index name that is not explicitly in its write-only or disabled lists.
     * @param indexName the name of the index to check
     * @return the state of the given index
     */
    @Nonnull
    public IndexState getState(@Nonnull String indexName) {
        return indexStateMap.get().getOrDefault(indexName, IndexState.READABLE);
    }

    /**
     * Determines whether all of the indexes in the store are currently readable. That is
     * to say, it makes sure there are no disabled indexes and no write-only indexes.
     * @return <code>true</code> if all of the indexes are readable and <code>false</code> otherwise
     */
    public boolean allIndexesReadable() {
        return indexStateMap.get().isEmpty() || indexStateMap.get().values().stream().allMatch(state -> state.equals(IndexState.READABLE));
    }

    /**
     * Determines if it is safe to use queries and other operations planned
     * with the passed <code>RecordStoreState</code> with a record store
     * that has the current state. It is possible that these operations will
     * be less efficient with the older state information, but they should
     * not cause correctness problems.
     * @param other the <code>RecordStoreState</code> to check compatibility with
     * @return whether operations planned with <code>other</code> will be correct
     * if the state is actually described by this <code>RecordStoreState</code>
     */
    public boolean compatibleWith(@Nonnull RecordStoreState other) {
        return indexStateMap.get().entrySet().stream().allMatch(entry -> {
            boolean readableInOther = other.getState(entry.getKey()).equals(IndexState.READABLE);
            return entry.getValue().equals(IndexState.READABLE) == readableInOther;
        });
    }

    /**
     * Get the names of any write-only indexes.
     * @return a set of indexes that are write-only for this store
     */
    public Set<String> getWriteOnlyIndexNames() {
        return indexStateMap.get().entrySet().stream()
                .filter(entry -> entry.getValue() == IndexState.WRITE_ONLY)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Get the names of any disabled indexes.
     * @return a set of indexes that are disabled for this store
     */
    public Set<String> getDisabledIndexNames() {
        return indexStateMap.get().entrySet().stream()
                .filter(entry -> entry.getValue() == IndexState.DISABLED)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Create a new version of this RecordStoreState, but with the specified indexes in the specified state,
     * ignoring their present state.
     * @param indexNames the names of the indexes to modify
     * @param state the new state for the given indexes
     * @return a new store state with the given indexes in the given state
     */
    @Nonnull
    public RecordStoreState withIndexesInState(@Nonnull final List<String> indexNames,
                                               @Nonnull IndexState state) {
        HashMap<String, IndexState> indexStateMapBuilder = new HashMap<>(getIndexStates());
        if (state == IndexState.READABLE) {
            indexNames.forEach(indexStateMapBuilder::remove);
        } else {
            indexNames.forEach(indexName -> indexStateMapBuilder.put(indexName, state));
        }
        return new RecordStoreState(storeHeader.get(), ImmutableMap.copyOf(indexStateMapBuilder));
    }

    /**
     * Create a new version of this {@code RecordStoreState}, but with additional {@link IndexState#WRITE_ONLY} indexes.
     *
     * @param writeOnlyIndexNames the indexes to be marked as WRITE_ONLY. If the index is already DISABLED, it will
     * stay disabled, but will otherwise be set to WRITE_ONLY.
     * @return a new version of this RecordStoreState, but with additional WRITE_ONLY indexes.
     */
    @Nonnull
    public RecordStoreState withWriteOnlyIndexes(@Nonnull final List<String> writeOnlyIndexNames) {
        return new RecordStoreState(storeHeader.get(), writeOnlyMap(writeOnlyIndexNames));
    }

    @Nonnull
    protected Map<String, IndexState> writeOnlyMap(@Nonnull final List<String> writeOnlyIndexNames) {
        Map<String, IndexState> map = new HashMap<>(getIndexStates());
        writeOnlyIndexNames.forEach(indexName ->
                map.compute(indexName, (name, state) -> {
                    // state may be null (which implies READABLE)
                    if (state == IndexState.DISABLED) {
                        return state;
                    } else {
                        return IndexState.WRITE_ONLY;
                    }
                }));
        return map;
    }

    /**
     * Get the store header associated with this record store state. This contains information like the
     * format version and meta-data version of the record store.
     *
     * @return the store header associated with the record store
     */
    @Nonnull
    public RecordMetaDataProto.DataStoreInfo getStoreHeader() {
        return storeHeader.get();
    }

    /**
     * Checks if this <code>RecordStoreState</code> specifies identical state
     * as the given object.
     * @param o the object to check for equality
     * @return <code>true</code> if the passed object is a <code>RecordStoreState</code>
     * instance and if the other state matches this <code>RecordStoreState</code>
     * and <code>false</code> otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (this == o) {
            return true;
        } else if (!(o instanceof RecordStoreState)) {
            return false;
        } else {
            RecordStoreState other = (RecordStoreState)o;
            return storeHeader.get().equals(other.storeHeader.get()) && indexStateMap.get().equals(other.indexStateMap.get());
        }
    }

    /**
     * Creates a valid hash code of this state based on the hashes of its members.
     * @return a hash code based off of the state's members hashes
     */
    @Override
    public int hashCode() {
        return Objects.hash(indexStateMap.get(), storeHeader.get());
    }

    /**
     * A human-readable representation of the state. This is essentially just
     * a print out of the member variables of the state.
     * @return the human-readable state representation
     */
    @Override
    public String toString() {
        return "RecordStoreState(" + indexStateMap.toString() + ")";
    }

    /**
     * Create an immutable version of this {@code RecordStoreState}. If the state object is already immutable,
     * this will return {@code this}. This version of the record store state is safe to cache as none of
     * its members can be mutated.
     *
     * @return an immutable version of this {@code RecordStoreState}
     */
    @Nonnull
    public RecordStoreState toImmutable() {
        return this;
    }

    /**
     * Create a mutable copy of this {@code RecordStoreState}. The returned object will contain the same information
     * as this record store state, but it will be mutable and will not share any mutable objects with this
     * object.
     *
     * @return a mutable copy of this {@code RecordStoreState}
     */
    @Nonnull
    public MutableRecordStoreState toMutable() {
        return new MutableRecordStoreState(this);
    }
}
