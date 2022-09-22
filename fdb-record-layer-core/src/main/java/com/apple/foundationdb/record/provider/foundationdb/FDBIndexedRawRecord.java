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

import com.apple.foundationdb.MappedKeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.metadata.Index;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A raw record that has been loaded via an index. This is the case where the record was loaded via the
 * {@link IndexMaintainer#scanRemoteFetch}. The raw record may contain versions, splits and other raw record data.
 */
@API(API.Status.EXPERIMENTAL)
public class FDBIndexedRawRecord {
    @Nullable
    private final IndexEntry indexEntry;
    @Nonnull
    private final MappedKeyValue rawRecord;

    /**
     * Wrap a stored record with an index entry that pointed to it. This method is internal, and it generally
     * should not be called be external clients.
     *
     * @param indexEntry the index entry that produced this record
     * @param rawRecord the {@link FDBRawRecord} containing the record's data
     */
    @API(API.Status.INTERNAL)
    public FDBIndexedRawRecord(@Nullable IndexEntry indexEntry, @Nonnull MappedKeyValue rawRecord) {
        this.indexEntry = indexEntry;
        this.rawRecord = rawRecord;
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

    @Nonnull
    public MappedKeyValue getRawRecord() {
        return rawRecord;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FDBIndexedRawRecord that = (FDBIndexedRawRecord)o;
        return Objects.equals(indexEntry, that.indexEntry) && rawRecord.equals(that.rawRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexEntry, rawRecord);
    }

    @Override
    public String toString() {
        return indexEntry + " -> " + rawRecord;
    }
}
