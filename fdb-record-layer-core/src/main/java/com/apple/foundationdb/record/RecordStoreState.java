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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
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
 * At the moment, this class tracks the following pieces of store-specific meta-data:
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
 *     <li>
 *         Index state meta-data information. This includes extra information about the state of the index ({@link IndexState}),
 *         such as the time an index entered the state, and the number of rebuild attempts.
 *     </li>
 * </ul>
 */
@API(API.Status.MAINTAINED)
public class RecordStoreState {
    /**
     * Empty <code>RecordStoreState</code>. This is the state of an empty record store that has not yet been
     * used. Calling the argument-less constructor of this class will produce a logically-equivalent object,
     * but having this code around avoids having to instantiate this class unnecessarily.
     *
     * <p>
     * When this object was initially introduced, the only information included in the record store state
     * was index readability information. This was the common case, and therefore sharing the same object
     * for most record stores was desirable. However, the store header information is not likely to be
     * the same for multiple record stores, so using this record store state is usually not recommended.
     * </p>
     *
     * @deprecated as this object usually has the wrong store header
     */
    @Deprecated
    @Nonnull
    public static final RecordStoreState EMPTY = new RecordStoreState();

    @Nonnull
    protected final AtomicReference<RecordMetaDataProto.DataStoreInfo> storeHeader;
    @Nonnull
    protected final AtomicReference<Map<String, IndexMetaDataProto.IndexMetaData>> indexMetaDataMap;

    /**
     * Creates a <code>RecordStoreState</code>.
     *
     * @param storeHeader header information for the given store
     * @param indexMetaDataMap mapping from index name to index state meta-data
     */
    @API(API.Status.INTERNAL)
    public RecordStoreState(@Nullable RecordMetaDataProto.DataStoreInfo storeHeader, @Nullable Map<String, IndexMetaDataProto.IndexMetaData> indexMetaDataMap) {
        final Map<String, IndexMetaDataProto.IndexMetaData> indexMetaDataCopy;
        if (indexMetaDataMap == null || indexMetaDataMap.isEmpty()) {
            indexMetaDataCopy = Collections.emptyMap();
        } else {
            indexMetaDataCopy = ImmutableMap.copyOf(indexMetaDataMap);
        }
        this.storeHeader = new AtomicReference<>(storeHeader == null ? RecordMetaDataProto.DataStoreInfo.getDefaultInstance() : storeHeader);
        this.indexMetaDataMap = new AtomicReference<>(indexMetaDataCopy);
    }

    /**
     * Creates a <code>RecordStoreState</code> with the given index states.
     * Only indexes that are not in the default state ({@link IndexState#READABLE IndexState.READABLE})
     * need to be included in the map. This initializes the record store state with a default store header, which
     * is not the expected state for most record stores. As a result, this constructor has been deprecated in favor
     * of the constructor where a store header must be provided.
     *
     * @param indexStateMap mapping from index name to index state
     * @deprecated as the default store header is incorrect for most record stores
     */
    @Deprecated
    public RecordStoreState(@Nullable Map<String, IndexState> indexStateMap) {
        this(null, indexStateMap == null ? null : indexStateMap.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toIndexMetaData())));
    }

    /**
     * Creates an empty <code>RecordStoreState</code> instance. This is the state that an empty {@link FDBRecordStoreBase}
     * would be expected to be in. All indexes are assumed to be readable with this constructor. This also
     * initializes the record store state with the default store header, which is not the expected state for
     * most record stores. As a result, this constructor has been deprecated in favor of the constructor where
     * a store header must be provided.
     *
     * @deprecated as the default store header is incorrect for most record stores
     */
    @Deprecated
    public RecordStoreState() {
        this(null);
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
     * underlying this <code>RecordStoreState</code>. The list only contains non-READABLE indexes.
     * @return the underlying mapping of index names to their state
     */
    @Nonnull
    public Map<String, IndexState> getIndexStates() {
        return Collections.unmodifiableMap(indexMetaDataMap.get()
                .entrySet()
                .stream()
                .filter(metadata -> metadata.getValue().getState() != IndexState.READABLE.ordinal())
                .collect(Collectors.toMap(e -> e.getKey(), e -> IndexState.fromCode((long)e.getValue().getState()))));
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
     * if it is given an index name that is not explicitly in the list.
     * @param indexName the name of the index to check
     * @return the state of the given index
     */
    @Nonnull
    public IndexState getState(@Nonnull String indexName) {
        return indexMetaDataMap.get().containsKey(indexName) ? IndexState.values()[indexMetaDataMap.get().get(indexName).getState()] : IndexState.READABLE;
    }

    /**
     * Determines whether all of the indexes in the store are currently readable. That is
     * to say, it makes sure there are no disabled indexes and no write-only indexes.
     * @return <code>true</code> if all of the indexes are readable and <code>false</code> otherwise
     */
    public boolean allIndexesReadable() {
        return indexMetaDataMap.get().isEmpty() || indexMetaDataMap.get().values().stream().allMatch(indexMetaData -> indexMetaData.getState() == IndexState.READABLE.ordinal());
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
        return indexMetaDataMap.get().entrySet().stream().allMatch(entry -> {
            boolean readable = entry.getValue().getState() == IndexState.READABLE.ordinal();
            boolean readableInOther = other.getIndexMetaData(entry.getKey()) == null || other.getIndexMetaData(entry.getKey()).getState() == IndexState.READABLE.ordinal();
            return readable == readableInOther;
        });
    }

    /**
     * Get the names of any write-only indexes.
     * @return a set of indexes that are write-only for this store
     */
    public Set<String> getWriteOnlyIndexNames() {
        return indexMetaDataMap.get().entrySet().stream()
                .filter(entry -> entry.getValue().getState() == IndexState.WRITE_ONLY.ordinal())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Get the names of any disabled indexes.
     * @return a set of indexes that are disabled for this store
     */
    public Set<String> getDisabledIndexNames() {
        return indexMetaDataMap.get().entrySet().stream()
                .filter(entry -> entry.getValue().getState() == IndexState.DISABLED.ordinal())
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
        HashMap<String, IndexMetaDataProto.IndexMetaData> indexStateMapBuilder = new HashMap<>(getIndexMetaData());
        indexNames.forEach(indexName -> indexStateMapBuilder.put(indexName, state.toIndexMetaData()));
        return new RecordStoreState(storeHeader.get(), ImmutableMap.copyOf(indexStateMapBuilder));
    }

    /**
     * Create a new version of this {@code RecordStoreState}, but with additional {@link IndexState#WRITE_ONLY} indexes.
     *
     * <p>
     *  If an index is already {@code DISABLED}, it will stay disabled, but will otherwise be set to WRITE_ONLY.
     * </p>
     *
     * @param writeOnlyIndexNames the indexes to be marked as WRITE_ONLY
     * @return a new version of this RecordStoreState, but with additional WRITE_ONLY indexes
     */
    @Nonnull
    public RecordStoreState withWriteOnlyIndexes(@Nonnull final List<String> writeOnlyIndexNames) {
        return new RecordStoreState(storeHeader.get(), writeOnlyMap(writeOnlyIndexNames));
    }

    @Nonnull
    protected Map<String, IndexMetaDataProto.IndexMetaData> writeOnlyMap(@Nonnull final List<String> writeOnlyIndexNames) {
        Map<String, IndexMetaDataProto.IndexMetaData> map = new HashMap<>(getIndexMetaData());
        writeOnlyIndexNames.forEach(indexName ->
                map.compute(indexName, (name, indexMetaData) -> {
                    if (indexMetaData == null || indexMetaData.getState() != IndexState.DISABLED.ordinal()) {
                        return IndexState.WRITE_ONLY.toIndexMetaData();
                    } else {
                        return indexMetaData;
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
     * Get the state meta-data of all indexes in the form of a mapping from index names to {@link com.apple.foundationdb.record.IndexMetaDataProto.IndexMetaData}.
     *
     * @return the state meta-data of all indexes.
     */
    @Nonnull
    public Map<String, IndexMetaDataProto.IndexMetaData> getIndexMetaData() {
        return Collections.unmodifiableMap(indexMetaDataMap.get());
    }

    /**
     * Get the state meta-data of the given index.
     *
     * @param indexName the name of the index
     * @return the meta-data of the index
     */
    @Nullable
    public IndexMetaDataProto.IndexMetaData getIndexMetaData(@Nonnull String indexName) {
        return indexMetaDataMap.get().getOrDefault(indexName, null);
    }

    /**
     * Get the state meta-data of the given index.
     *
     * @param indexName the name of the index
     * @param defaultIndexState the default state if the meta-data of this index does not exist yet.
     * @return the meta-data of the index
     */
    @Nullable
    public IndexMetaDataProto.IndexMetaData getIndexMetaData(@Nonnull String indexName, @Nonnull IndexState defaultIndexState) {
        return indexMetaDataMap.get().getOrDefault(indexName, defaultIndexState.toIndexMetaData());
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
            return storeHeader.get().equals(other.storeHeader.get()) && getIndexStates().equals(other.getIndexStates());
        }
    }

    /**
     * Creates a valid hash code of this state based on the hashes of its members.
     * @return a hash code based off of the state's members hashes
     */
    @Override
    public int hashCode() {
        return Objects.hash(storeHeader.get(), getIndexStates());
    }

    /**
     * A human-readable representation of the state. This is essentially just
     * a print out of the member variables of the state.
     * @return the human-readable state representation
     */
    @Override
    public String toString() {
        return "RecordStoreState(" + indexMetaDataMap.toString() + ")";
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
