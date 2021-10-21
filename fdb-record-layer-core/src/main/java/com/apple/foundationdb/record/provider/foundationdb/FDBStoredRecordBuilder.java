/*
 * FDBStoredRecordBuilder.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A builder for {@link FDBStoredRecord}.
 * @param <M> type used to represent stored records
 */
@API(API.Status.MAINTAINED)
public class FDBStoredRecordBuilder<M extends Message> implements FDBRecord<M>, FDBStoredSizes {
    @Nullable
    private Tuple primaryKey;
    @Nullable
    private RecordType recordType;
    @Nullable
    private M protoRecord;
    @Nullable
    private FDBRecordVersion recordVersion;

    private int keyCount;
    private int keySize;
    private int valueSize;
    private boolean split;
    private boolean versionedInline;

    public FDBStoredRecordBuilder() {
        // Real initialization via set methods.
    }

    // Having this means the diamond operator can be used more often.
    public FDBStoredRecordBuilder(@Nonnull M protoRecord) {
        this.protoRecord = protoRecord;
    }

    @Override
    @Nonnull
    public Tuple getPrimaryKey() {
        if (primaryKey == null) {
            throw new RecordCoreException("primary key has not been set");
        }
        return primaryKey;
    }

    @Override
    @Nonnull
    public RecordType getRecordType() {
        if (recordType == null) {
            throw new RecordCoreException("record type has not been set");
        }
        return recordType;
    }

    @Override
    @Nonnull
    public M getRecord() {
        if (protoRecord == null) {
            throw new RecordCoreException("record has not been set");
        }
        return protoRecord;
    }

    @Override
    public boolean hasVersion() {
        return recordVersion != null;
    }

    @Nullable
    @Override
    public FDBRecordVersion getVersion() {
        return recordVersion;
    }

    @Override
    public int getKeyCount() {
        return keyCount;
    }

    @Override
    public int getKeySize() {
        return keySize;
    }

    @Override
    public int getValueSize() {
        return valueSize;
    }

    @Override
    public boolean isSplit() {
        return split;
    }

    @Override
    public boolean isVersionedInline() {
        return versionedInline;
    }

    public FDBStoredRecordBuilder<M> setPrimaryKey(Tuple primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public FDBStoredRecordBuilder<M> setRecordType(RecordType recordType) {
        this.recordType = recordType;
        return this;
    }

    public FDBStoredRecordBuilder<M> setRecord(M record) {
        this.protoRecord = record;
        return this;
    }

    public FDBStoredRecordBuilder<M> setVersion(FDBRecordVersion recordVersion) {
        this.recordVersion = recordVersion;
        return this;
    }

    public FDBStoredRecordBuilder<M> setKeyCount(int keyCount) {
        this.keyCount = keyCount;
        return this;
    }

    public FDBStoredRecordBuilder<M> setKeySize(int keySize) {
        this.keySize = keySize;
        return this;
    }

    public FDBStoredRecordBuilder<M> setValueSize(int valueSize) {
        this.valueSize = valueSize;
        return this;
    }

    public FDBStoredRecordBuilder<M> setSplit(boolean split) {
        this.split = split;
        return this;
    }

    public FDBStoredRecordBuilder<M> setVersionedInline(boolean versionedInline) {
        this.versionedInline = versionedInline;
        return this;
    }

    public FDBStoredRecordBuilder<M> setSize(@Nonnull FDBStoredSizes size) {
        this.keyCount = size.getKeyCount();
        this.keySize = size.getKeySize();
        this.valueSize = size.getValueSize();
        this.split = size.isSplit();
        this.versionedInline = size.isVersionedInline();
        return this;
    }

    public FDBStoredRecord<M> build() {
        return new FDBStoredRecord<>(getPrimaryKey(), getRecordType(), getRecord(),
                getKeyCount(), getKeySize(), getValueSize(), isSplit(), isVersionedInline(), getVersion());
    }
}
