/*
 * FDBSyntheticRecord.java
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

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A record synthesized from stored records.
 *
 * A {@code FDBSyntheticRecord} is {@link FDBIndexableRecord}, so that indexes can be defined on synthetic records,
 * but not {@link FDBStoredRecord}, since it is not stored separately.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class FDBSyntheticRecord implements FDBIndexableRecord<Message> {

    @Nonnull
    private final Tuple primaryKey;
    @Nonnull
    private final SyntheticRecordType<?> recordType;
    @Nonnull
    private final Message record;
    @Nonnull
    private final Map<String, FDBStoredRecord<? extends Message>> constituents;

    private final int keyCount;
    private final int keySize;
    private final int valueSize;

    protected FDBSyntheticRecord(@Nonnull Tuple primaryKey, @Nonnull SyntheticRecordType<?> recordType, @Nonnull Message record,
                                 @Nonnull FDBStoredSizes size, @Nonnull Map<String, FDBStoredRecord<? extends Message>> constituents) {
        this.primaryKey = primaryKey;
        this.recordType = recordType;
        this.record = record;
        this.constituents = constituents;
        this.keyCount = size.getKeyCount();
        this.keySize = size.getKeySize();
        this.valueSize = size.getValueSize();
    }
    
    @Nonnull
    public static FDBSyntheticRecord of(@Nonnull SyntheticRecordType<?> recordType,
                                        @Nonnull Map<String, FDBStoredRecord<? extends Message>> constituents) {
        // Note flattening and repeating nulls for outer-joined. Would nested Tuples always be superior?
        final List<Object> constituentPrimaryKeys = new ArrayList<>(recordType.getConstituents().size() + 1);
        constituentPrimaryKeys.add(recordType.getRecordTypeKey());
        DynamicMessage.Builder recordBuilder = DynamicMessage.newBuilder(recordType.getDescriptor());
        SplitHelper.SizeInfo size = new SplitHelper.SizeInfo();
        for (int i = 0; i < recordType.getConstituents().size(); i++) {
            final SyntheticRecordType.Constituent constituent = recordType.getConstituents().get(i);
            final FDBStoredRecord<? extends Message> constituentRecord = constituents.get(constituent.getName());
            if (constituentRecord == null) {
                final int nnulls = constituent.getRecordType().getPrimaryKey().getColumnSize();
                for (int j = 0; j < nnulls; j++) {
                    constituentPrimaryKeys.add(null);
                }
            } else {
                constituentPrimaryKeys.addAll(constituentRecord.getPrimaryKey().getItems());
                final Descriptors.FieldDescriptor field = recordType.getDescriptor().getFields().get(i);
                recordBuilder.setField(field, constituentRecord.getRecord());
                size.add(constituentRecord);
            }
        }
        return new FDBSyntheticRecord(Tuple.fromList(constituentPrimaryKeys), recordType, recordBuilder.build(), size, constituents);
    }

    @Nonnull
    @Override
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    @Nonnull
    @Override
    public RecordType getRecordType() {
        return recordType;
    }

    @Nonnull
    @Override
    public Message getRecord() {
        return record;
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
        return false;
    }

    @Override
    public boolean hasVersion() {
        return false;
    }

    @Nullable
    @Override
    public FDBRecordVersion getVersion() {
        return null;
    }

    @Override
    public boolean isVersionedInline() {
        return false;
    }

    @Nonnull
    @SuppressWarnings("squid:S1452")
    public Map<String, FDBStoredRecord<? extends Message>> getConstituents() {
        return constituents;
    }

    @Nullable
    @SuppressWarnings("squid:S1452")
    public FDBStoredRecord<? extends Message> getConstituent(@Nonnull String name) {
        return constituents.get(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FDBSyntheticRecord that = (FDBSyntheticRecord)o;
        if (!primaryKey.equals(that.primaryKey)) {
            return false;
        }
        if (!recordType.getName().equals(that.recordType.getName())) {
            return false;
        }
        if (!record.equals(that.record)) {
            return false;
        }

        return this.keyCount == that.keyCount && this.keySize == that.keySize && this.valueSize == that.valueSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKey, recordType, record, constituents, keyCount, keySize, valueSize);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getRecordType().getName());
        str.append(constituents);
        return str.toString();
    }

}
