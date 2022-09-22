/*
 * FDBIndexedRecord.java
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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A record that has been loaded via an index.
 * @param <M> type used to represent stored records
 */
@API(API.Status.MAINTAINED)
public class FDBIndexedRecord<M extends Message> implements FDBRecord<M>, FDBStoredSizes {
    @Nullable
    private final IndexEntry indexEntry;
    // When scanning for orphaned index entries (which can happen when index maintenance is too expensive to perform
    // in-line in a transaction), this will be null to indicate that the entry in the index has no corresponding record.
    @Nullable
    private final FDBStoredRecord<M> storedRecord;

    /**
     * Wrap a stored record with an index entry that pointed to it. This method is internal, and it generally
     * should not be called be external clients.
     *
     * @param indexEntry the index entry that produced this record
     * @param storedRecord the {@link FDBStoredRecord} containing the record's data
     */
    @API(API.Status.INTERNAL)
    public FDBIndexedRecord(@Nullable IndexEntry indexEntry, @Nullable FDBStoredRecord<M> storedRecord) {
        this.indexEntry = indexEntry;
        this.storedRecord = storedRecord;
    }

    /**
     * Get the index for this record.
     * @return the index that contained the entry pointing to this record
     */
    @Nullable
    public Index getIndex() {
        return (indexEntry != null) ? indexEntry.getIndex() : null;
    }

    /**
     * Get the index entry for this record.
     * @return the index entry that pointed to this record
     */
    @Nullable
    public IndexEntry getIndexEntry() {
        return indexEntry;
    }

    /**
     * When scanning for orphaned index entries, this method must be used to determine whether or not a record exists
     * before calling <code>getStoredRecord()</code>.
     * @return {@code true} if this record has an {@link FDBStoredRecord}
     */
    public boolean hasStoredRecord() {
        return storedRecord != null;
    }

    @Nonnull
    public FDBStoredRecord<M> getStoredRecord() {
        if (storedRecord == null) {
            RecordCoreException ex = new RecordCoreException("No record associated with index entry");
            if (indexEntry != null) {
                ex.addLogInfo(
                        LogMessageKeys.INDEX_NAME, indexEntry.getIndex().getName(),
                        LogMessageKeys.INDEX_KEY, indexEntry.getKey());
            }
            throw ex;
        }
        return storedRecord;
    }

    @Nonnull
    @Override
    public Tuple getPrimaryKey() {
        return getStoredRecord().getPrimaryKey();
    }

    @Nonnull
    @Override
    public RecordType getRecordType() {
        return getStoredRecord().getRecordType();
    }

    @Nonnull
    @Override
    public M getRecord() {
        return getStoredRecord().getRecord();
    }

    @Override
    public boolean hasVersion() {
        return getStoredRecord().hasVersion();
    }

    @Nullable
    @Override
    public FDBRecordVersion getVersion() {
        return getStoredRecord().getVersion();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FDBIndexedRecord<?> that = (FDBIndexedRecord<?>) o;

        if (!indexEntry.equals(that.indexEntry)) {
            return false;
        }
        return Objects.equals(this.storedRecord, that.storedRecord);
    }

    @Override
    public int hashCode() {
        int result = indexEntry.hashCode();
        result = 31 * result + Objects.hashCode(storedRecord);
        return result;
    }

    @Override
    public String toString() {
        return indexEntry + " -> " + storedRecord;
    }

    @Override
    public int getKeyCount() {
        return getStoredRecord().getKeyCount();
    }

    @Override
    public int getKeySize() {
        return getStoredRecord().getKeySize();
    }

    @Override
    public int getValueSize() {
        return getStoredRecord().getValueSize();
    }

    @Override
    public boolean isSplit() {
        return getStoredRecord().isSplit();
    }

    @Override
    public boolean isVersionedInline() {
        return getStoredRecord().isVersionedInline();
    }
}
