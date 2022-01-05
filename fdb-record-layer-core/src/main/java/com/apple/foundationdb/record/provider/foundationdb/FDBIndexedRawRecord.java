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
 * A raw record that has been loaded via an index. This is the case where the record was loaded via the
 * {@link FDBRecordStore#scanIndexPrefetch}. The raw record may have versions, splits and other raw record data.
 * @param <M> type used to represent stored records
 */
@API(API.Status.EXPERIMENTAL)
public class FDBIndexedRawRecord<M extends Message> implements FDBRecord<M>, FDBStoredSizes {
    @Nonnull
    private final IndexEntry indexEntry;
    @Nullable
    private final FDBRawRecord rawRecord;

    /**
     * Wrap a stored record with an index entry that pointed to it. This method is internal, and it generally
     * should not be called be external clients.
     *
     * @param indexEntry the index entry that produced this record
     * @param rawRecord the {@link FDBRawRecord} containing the record's data
     */
    @API(API.Status.INTERNAL)
    public FDBIndexedRawRecord(@Nonnull IndexEntry indexEntry, @Nullable FDBRawRecord rawRecord) {
        this.indexEntry = indexEntry;
        this.rawRecord = rawRecord;
    }

    /**
     * Get the index for this record.
     * @return the index that contained the entry pointing to this record
     */
    @Nonnull
    public Index getIndex() {
        return indexEntry.getIndex();
    }

    /**
     * Get the index entry for this record.
     * @return the index entry that pointed to this record
     */
    @Nonnull
    public IndexEntry getIndexEntry() {
        return indexEntry;
    }

    /**
     * When scanning for orphaned index entries, this method must be used to determine whether or not a record exists
     * before calling {@link #getRawRecord()}.
     * @return {@code true} if this record has an {@link FDBRawRecord}
     */
    public boolean hasRawRecord() {
        return rawRecord != null;
    }

    @Nonnull
    public FDBRawRecord getRawRecord() {
        if (rawRecord == null) {
            throw new RecordCoreException("No record associated with index entry").addLogInfo(
                    LogMessageKeys.INDEX_NAME, getIndex().getName(),
                    LogMessageKeys.INDEX_KEY, indexEntry.getKey());
        }
        return rawRecord;
    }

    @Nonnull
    @Override
    public Tuple getPrimaryKey() {
        return getRawRecord().getPrimaryKey();
    }

    @Nonnull
    @Override
    public RecordType getRecordType() {
        throw new UnsupportedOperationException("this record does not have an associated record type");
    }

    @Nonnull
    @Override
    public M getRecord() {
        throw new UnsupportedOperationException("this record does not have an associated record message");
    }

    @Override
    public boolean hasVersion() {
        return getRawRecord().hasVersion();
    }

    @Nullable
    @Override
    public FDBRecordVersion getVersion() {
        return getRawRecord().getVersion();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FDBIndexedRawRecord<?> that = (FDBIndexedRawRecord<?>) o;

        if (!indexEntry.equals(that.indexEntry)) {
            return false;
        }
        return Objects.equals(this.rawRecord, that.rawRecord);
    }

    @Override
    public int hashCode() {
        int result = indexEntry.hashCode();
        result = 31 * result + Objects.hashCode(rawRecord);
        return result;
    }

    @Override
    public String toString() {
        return indexEntry + " -> " + rawRecord;
    }

    @Override
    public int getKeyCount() {
        return getRawRecord().getKeyCount();
    }

    @Override
    public int getKeySize() {
        return getRawRecord().getKeySize();
    }

    @Override
    public int getValueSize() {
        return getRawRecord().getValueSize();
    }

    @Override
    public boolean isSplit() {
        return getRawRecord().isSplit();
    }

    @Override
    public boolean isVersionedInline() {
        return getRawRecord().isVersionedInline();
    }
}
