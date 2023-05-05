/*
 * MutableRecordStoreState.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A record store that can be modified to reflect changes made to the database.
 * Such modifications are only allowed between {@link #beginWrite} and {@link #endWrite} and will conflict with
 * {@link #beginRead} ... {@link #endRead}.
 */
@API(API.Status.INTERNAL)
public class MutableRecordStoreState extends RecordStoreState {

    private static final long READ_MASK = 0xFFFF0000;
    private static final long WRITE_MASK = 0x0000FFFF;

    private final AtomicLong users = new AtomicLong();

    public MutableRecordStoreState(@Nullable RecordMetaDataProto.DataStoreInfo storeHeader,
                                   @Nullable Map<String, IndexState> indexStateMap,
                                   @Nullable RecordMetaData recordMetaData) {
        super(storeHeader, indexStateMap, recordMetaData);
    }

    public MutableRecordStoreState(@Nullable RecordMetaDataProto.DataStoreInfo storeHeader,
                                   @Nullable Map<String, IndexState> indexStateMap,
                                   @Nullable final BitSet readableIndexesBitSet) {
        super(storeHeader, indexStateMap, readableIndexesBitSet);
    }

    // Copy constructor
    MutableRecordStoreState(@Nonnull RecordStoreState recordStoreState) {
        this(recordStoreState.getStoreHeader(), recordStoreState.getIndexStates(), recordStoreState.readableIndexes.get());
    }

    private static long readIncrement(long u) {
        return ((((u >> 32) + 1) << 32) & READ_MASK) | (u & WRITE_MASK);
    }

    private static long readDecrement(long u) {
        return ((((u >> 32) - 1) << 32) & READ_MASK) | (u & WRITE_MASK);
    }

    private static long writeIncrement(long u) {
        return (u & READ_MASK) | (((u & WRITE_MASK) + 1) & WRITE_MASK);
    }

    private static long writeDecrement(long u) {
        return (u & READ_MASK) | (((u & WRITE_MASK) - 1) & WRITE_MASK);
    }

    private void verifyWritable() {
        if ((users.get() & WRITE_MASK) == 0) {
            throw new RecordCoreException("record store state is not enabled for modification");
        }
    }

    /**
     * Begin using this record store state.
     * Until {@link #endRead} is called, the state will not change.
     */
    @Override
    public void beginRead() {
        long inuse = users.updateAndGet(MutableRecordStoreState::readIncrement);
        if ((inuse & WRITE_MASK) != 0) {
            users.updateAndGet(MutableRecordStoreState::readDecrement);
            throw new RecordCoreException("record store state is being modified");
        }
    }

    /**
     * End using this record store state.
     * @see #beginRead()
     */
    @Override
    public void endRead() {
        users.updateAndGet(MutableRecordStoreState::readDecrement);
    }

    /**
     * Begin modifying this record store state.
     * Until {@link #endWrite} is called, the state will not change.
     */
    public void beginWrite() {
        // One-time transition to mutable map inside.
        Map<String, IndexState> current = indexStateMap.get();
        if (!(current instanceof ConcurrentHashMap<?, ?>)) {
            indexStateMap.compareAndSet(current, new ConcurrentHashMap<>(current));
        }

        long inuse = users.updateAndGet(MutableRecordStoreState::writeIncrement);
        if ((inuse & READ_MASK) != 0) {
            users.updateAndGet(MutableRecordStoreState::writeDecrement);
            throw new RecordCoreException("record store state is being used for queries");
        }
    }

    /**
     * End modifying this record store state.
     * @see #beginWrite()
     */
    public void endWrite() {
        users.updateAndGet(MutableRecordStoreState::writeDecrement);
    }

    /**
     * Modify the state of an index in this map. The caller should modify the database to match.
     *
     * @param indexName the index name
     * @param state the new state for the given index
     * @return the previous state of the given index
     */
    @Nonnull
    public IndexState setState(@Nonnull String indexName, @Nonnull IndexState state, @Nullable final RecordMetaData recordMetaData) {
        verifyWritable();
        IndexState previous;
        if (state == IndexState.READABLE) {
            previous = indexStateMap.get().remove(indexName);
            readableIndexes.set(getReadableIndexesBitset(recordMetaData, indexStateMap.get()));
        } else {
            previous = indexStateMap.get().put(indexName, state);
        }
        return previous == null ? IndexState.READABLE : previous;
    }

    @Nonnull
    @Override
    public MutableRecordStoreState withWriteOnlyIndexes(@Nonnull final List<String> writeOnlyIndexNames, @Nullable final RecordMetaData recordMetaData) {
        return new MutableRecordStoreState(getStoreHeader(), writeOnlyMap(writeOnlyIndexNames), recordMetaData);
    }

    /**
     * Update the store header in this record store state. The caller should modify the state in the database
     * to match.
     *
     * @param storeHeader the updated store header
     * @return the previous store header value
     */
    @Nonnull
    public RecordMetaDataProto.DataStoreInfo setStoreHeader(@Nonnull RecordMetaDataProto.DataStoreInfo storeHeader) {
        verifyWritable();
        return this.storeHeader.getAndSet(storeHeader);
    }

    // NOTE: Does not actually implement own equals. Is equal to immutable one, too.
    @Override
    @SuppressWarnings("PMD.UselessOverridingMethod")
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    @SuppressWarnings("PMD.UselessOverridingMethod")
    public int hashCode() {
        return super.hashCode();
    }

    @Nonnull
    @Override
    public RecordStoreState toImmutable() {
        return new RecordStoreState(storeHeader.get(), indexStateMap.get(), readableIndexes.get());
    }
}
