/*
 * FDBStoredRecord.java
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
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A record stored in the database.
 *
 * Adds information about storage sizes from saving or retrieving.
 * @param <M> type used to represent stored records
 * @see FDBRecordStoreBase#saveRecord
 * @see FDBRecordStoreBase#loadRecord
 */
@API(API.Status.MAINTAINED)
public class FDBStoredRecord<M extends Message> implements FDBIndexableRecord<M> {
    @Nonnull
    private final Tuple primaryKey;
    @Nonnull
    private final RecordType recordType;
    @Nonnull
    private final M protoRecord;
    @Nullable
    private final FDBRecordVersion recordVersion;

    private final int keyCount;
    private final int keySize;
    private final int valueSize;
    private final boolean split;
    private final boolean versionedInline;

    public FDBStoredRecord(@Nonnull Tuple primaryKey, @Nonnull RecordType recordType, @Nonnull M protoRecord,
                           @Nonnull FDBStoredSizes size, @Nullable FDBRecordVersion recordVersion) {
        this(primaryKey, recordType, protoRecord, size.getKeyCount(), size.getKeySize(), size.getValueSize(), size.isSplit(), size.isVersionedInline(), recordVersion);
    }

    @API(API.Status.INTERNAL)
    @SuppressWarnings("squid:S00107")
    public FDBStoredRecord(@Nonnull Tuple primaryKey, @Nonnull RecordType recordType, @Nonnull M protoRecord,
                           int keyCount, int keySize, int valueSize, boolean split, boolean versionedInline, @Nullable FDBRecordVersion recordVersion) {

        this.primaryKey = primaryKey;
        this.recordType = recordType;
        this.protoRecord = protoRecord;

        this.keyCount = keyCount;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.split = split;
        this.recordVersion = recordVersion;
        this.versionedInline = versionedInline;
    }

    @Override
    @Nonnull
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    @Override
    @Nonnull
    public RecordType getRecordType() {
        return recordType;
    }

    @Override
    @Nonnull
    public M getRecord() {
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

    /**
     * Get a builder for a stored record.
     * @param <M> type used to represent stored records
     * @return a new uninitialized builder
     */
    @Nonnull
    public static <M extends Message> FDBStoredRecordBuilder<M> newBuilder() {
        return new FDBStoredRecordBuilder<>();
    }

    /**
     * Get a builder for a stored record.
     * @param protoRecord Protobuf record
     * @param <M> type used to represent stored records
     * @return a new builder initialized with the record
     */
    @Nonnull
    public static <M extends Message> FDBStoredRecordBuilder<M> newBuilder(@Nonnull M protoRecord) {
        return new FDBStoredRecordBuilder<>(protoRecord);
    }

    /**
     * Copy this record with a different version.
     * @param recordVersion new version
     * @return a new stored record with the given version
     */
    @Nonnull
    public FDBStoredRecord<M> withVersion(@Nullable FDBRecordVersion recordVersion) {
        return new FDBStoredRecord<>(primaryKey, recordType, protoRecord, this, recordVersion);
    }

    /**
     * Get this record with an updated version after committing.
     *
     * If this record has an incomplete version, it is completed with the given version stamp.
     * If the version is already complete or this record does not have a version, this record is returned.
     * @param committedVersion the result of {@link FDBRecordContext#getVersionStamp}
     * @return a stored record with the given version
     */
    @Nonnull
    public FDBStoredRecord<M> withCommittedVersion(@Nullable byte[] committedVersion) {
        if (recordVersion == null || recordVersion.isComplete()) {
            return this;
        }
        return withVersion(recordVersion.withCommittedVersion(committedVersion));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FDBStoredRecord<?> that = (FDBStoredRecord<?>) o;

        if (!primaryKey.equals(that.primaryKey)) {
            return false;
        }
        if (!recordType.getName().equals(that.recordType.getName())) {
            return false;
        }
        if (!protoRecord.equals(that.protoRecord)) {
            return false;
        }
        if (recordVersion == null && that.recordVersion != null || recordVersion != null && !recordVersion.equals(that.recordVersion)) {
            return false;
        }

        return this.keyCount == that.keyCount && this.keySize == that.keySize && this.valueSize == that.valueSize
               && this.split == that.split && this.versionedInline == that.versionedInline;
    }

    @Override
    public int hashCode() {
        int result = primaryKey.hashCode();
        result = 31 * result + recordType.getName().hashCode();
        result = 31 * result + protoRecord.hashCode();
        result = 31 * result + keyCount;
        result = 31 * result + keySize;
        result = 31 * result + valueSize;
        if (recordVersion != null) {
            result = 31 * result + recordVersion.hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(recordType.getName());
        str.append(primaryKey);
        if (hasVersion()) {
            str.append(recordVersion);
        }
        return str.toString();
    }
}
