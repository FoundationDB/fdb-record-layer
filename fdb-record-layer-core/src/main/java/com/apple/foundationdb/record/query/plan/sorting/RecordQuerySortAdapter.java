/*
 * SortQueryKey.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.sorting;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.common.TransformedRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.SortedRecordSerializer;
import com.apple.foundationdb.record.sorting.FileSortAdapter;
import com.apple.foundationdb.record.sorting.MemorySortAdapter;
import com.apple.foundationdb.record.sorting.MemorySorter;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Suppliers;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.KeyGenerator;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.SecureRandom;
import java.util.function.Supplier;

/**
 * A {@link MemorySortAdapter} / {@link FileSortAdapter} for use with {@link RecordQuerySortPlan}.
 */
@API(API.Status.EXPERIMENTAL)
class RecordQuerySortAdapter<M extends Message> implements FileSortAdapter<Tuple, FDBStoredRecord<M>> {
    public static final int MAX_MEMORY_SIZE = 1000;
    public static final int MAX_FILES = 10;
    public static final int RECORDS_PER_SECTION = 100;

    private final int memoryLimit;
    private final boolean memoryOnly;
    @Nonnull
    private final RecordQuerySortKey key;
    @Nonnull
    private final SortedRecordSerializer<M> serializer;

    @Nullable
    private Key encryptionKey;
    private static final Supplier<SecureRandom> RANDOM = Suppliers.memoize(SecureRandom::new);

    RecordQuerySortAdapter(final int memoryLimit, boolean memoryOnly,
                           @Nonnull RecordQuerySortKey key, @Nonnull FDBRecordStoreBase<M> recordStore) {
        this.memoryLimit = memoryLimit;
        this.memoryOnly = memoryOnly;
        this.key = key;
        RecordSerializer<M> recordSerializer = recordStore.getSerializer();
        if (recordSerializer instanceof TransformedRecordSerializer) {
            // Nothing goes wrong without this, but it avoids double encryption / compression.
            recordSerializer = ((TransformedRecordSerializer<M>)recordSerializer).untransformed();
        }
        serializer = new SortedRecordSerializer<>(recordSerializer, recordStore.getRecordMetaData(), recordStore.getTimer());
    }

    public boolean isMemoryOnly() {
        return memoryOnly;
    }

    @Override
    public int compare(@Nonnull Tuple o1, @Nonnull Tuple o2) {
        return key.isReverse() ? o2.compareTo(o1) : o1.compareTo(o2);
    }

    @Nonnull
    @Override
    public Tuple generateKey(@Nonnull FDBStoredRecord<M> value) {
        return key.getKey().evaluateSingleton(value).toTuple();
    }

    @Nonnull
    @Override
    public byte[] serializeKey(final Tuple key) {
        return key.pack();
    }

    @Override
    public boolean isSerializedOrderReversed() {
        return key.isReverse();
    }

    @Nonnull
    @Override
    public Tuple deserializeKey(@Nonnull final byte[] key) {
        return Tuple.fromBytes(key);
    }

    @Nonnull
    @Override
    public byte[] serializeValue(final FDBStoredRecord<M> record) {
        return serializer.serialize(record);
    }

    @Nonnull
    @Override
    public FDBStoredRecord<M> deserializeValue(@Nonnull final byte[] bytes) {
        return serializer.deserialize(bytes).getStoredRecord();
    }

    @Override
    public int getMaxMapSize() {
        return memoryOnly ? memoryLimit : MAX_MEMORY_SIZE;
    }

    @Nonnull
    @Override
    public MemorySorter.SizeLimitMode getSizeLimitMode() {
        return memoryOnly ? MemorySorter.SizeLimitMode.DISCARD : MemorySorter.SizeLimitMode.STOP;
    }

    @Nonnull
    @Override
    public File generateFilename() throws IOException {
        return File.createTempFile("fdb", ".bin");
    }

    @Override
    public void writeValue(@Nonnull final FDBStoredRecord<M> record, @Nonnull final CodedOutputStream stream) throws IOException {
        serializer.write(record, stream);
    }

    @Override
    public FDBStoredRecord<M> readValue(@Nonnull final CodedInputStream stream) throws IOException {
        return serializer.read(stream).getStoredRecord();
    }

    @Override
    public int getMinFileSize() {
        return MAX_MEMORY_SIZE;
    }

    @Override
    public int getMaxNumFiles() {
        return MAX_FILES;
    }

    @Override
    public int getRecordsPerSection() {
        return RECORDS_PER_SECTION;
    }

    @Override
    public boolean isCompressed() {
        return true;
    }

    @Nullable
    @Override
    public synchronized Key getEncryptionKey() {
        if (encryptionKey == null) {
            try {
                final KeyGenerator keyGen = KeyGenerator.getInstance("AES");
                keyGen.init(128, RANDOM.get());
                encryptionKey = keyGen.generateKey();
            } catch (GeneralSecurityException ex) {
                throw new RecordCoreException(ex);
            }
        }
        return encryptionKey;
    }

    @Nullable
    @Override
    public SecureRandom getSecureRandom() {
        return RANDOM.get();
    }
}
