/*
 * FDBQueriedRecord.java
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A record returned by a query and therefore possibly associated with a particular entry in some index.
 * @param <M> type used to represent stored records
 */
@API(API.Status.MAINTAINED)
public abstract class FDBQueriedRecord<M extends Message> implements FDBRecord<M> {
    /**
     * Get the stored record, if any, that produced this query result record.
     * <code>null</code> if this query result record was assembled without loading the whole record,
     * for example, from a covering index.
     * @return the stored record form of this record
     */
    @Nullable
    public abstract FDBStoredRecord<M> getStoredRecord();

    /**
     * Get the index, if any, that produced this query result record.
     * <code>null</code> if this query result record was gotten by some other means than an index scan.
     * @return the index that was queried to produce this record
     */
    @Nullable
    public abstract Index getIndex();

    /**
     * Get the index entry, if any, that produced this query result record.
     * <code>null</code> if this query result record was gotten by direct lookup or scan and not an index.
     * @return this record's index entry
     */
    @Nullable
    public abstract IndexEntry getIndexEntry();

    public static <M extends Message> FDBQueriedRecord<M> indexed(@Nonnull FDBIndexedRecord<M> indexed) {
        return new Indexed<>(indexed);
    }

    public static <M extends Message> FDBQueriedRecord<M> stored(@Nonnull FDBStoredRecord<M> stored) {
        return new Stored<>(stored);
    }

    public static <M extends Message> FDBQueriedRecord<M> covered(@Nonnull Index index, @Nonnull IndexEntry indexEntry, @Nonnull Tuple primaryKey, @Nonnull RecordType recordType, @Nonnull M protoRecord) {
        return new Covered<>(index, indexEntry, primaryKey, recordType, protoRecord);
    }

    @SuppressWarnings("PMD.AvoidFieldNameMatchingTypeName")
    static class Indexed<M extends Message> extends FDBQueriedRecord<M> {
        private final FDBIndexedRecord<M> indexed;

        Indexed(FDBIndexedRecord<M> indexed) {
            this.indexed = indexed;
        }

        @Nonnull
        @Override
        public Tuple getPrimaryKey() {
            return indexed.getPrimaryKey();
        }

        @Nonnull
        @Override
        public RecordType getRecordType() {
            return indexed.getRecordType();
        }

        @Nonnull
        @Override
        public M getRecord() {
            return indexed.getRecord();
        }

        @Override
        public boolean hasVersion() {
            return indexed.hasVersion();
        }

        @Nullable
        @Override
        public FDBRecordVersion getVersion() {
            return indexed.getVersion();
        }

        @Nonnull
        @Override
        public FDBStoredRecord<M> getStoredRecord() {
            return indexed.getStoredRecord();
        }

        @Nullable
        @Override
        public Index getIndex() {
            return indexed.getIndex();
        }

        @Nullable
        @Override
        public IndexEntry getIndexEntry() {
            return indexed.getIndexEntry();
        }
    }

    @SuppressWarnings("PMD.AvoidFieldNameMatchingTypeName")
    static class Stored<M extends Message> extends FDBQueriedRecord<M> {
        private final FDBStoredRecord<M> stored;

        Stored(@Nonnull FDBStoredRecord<M> stored) {
            this.stored = stored;
        }

        @Nonnull
        @Override
        public Tuple getPrimaryKey() {
            return stored.getPrimaryKey();
        }

        @Nonnull
        @Override
        public RecordType getRecordType() {
            return stored.getRecordType();
        }

        @Nonnull
        @Override
        public M getRecord() {
            return stored.getRecord();
        }

        @Override
        public boolean hasVersion() {
            return stored.hasVersion();
        }

        @Nullable
        @Override
        public FDBRecordVersion getVersion() {
            return stored.getVersion();
        }

        @Nullable
        @Override
        public FDBStoredRecord<M> getStoredRecord() {
            return stored;
        }

        @Nullable
        @Override
        public Index getIndex() {
            return null;
        }

        @Nullable
        @Override
        public IndexEntry getIndexEntry() {
            return null;
        }
    }

    static class Covered<M extends Message> extends FDBQueriedRecord<M> {
        @Nonnull
        private final Index index;
        @Nonnull
        private final IndexEntry indexEntry;
        @Nonnull
        private final Tuple primaryKey;
        @Nonnull
        private final RecordType recordType;
        @Nonnull
        private final M protoRecord;

        public Covered(@Nonnull Index index, @Nonnull IndexEntry indexEntry, @Nonnull Tuple primaryKey, @Nonnull RecordType recordType, @Nonnull M protoRecord) {
            this.index = index;
            this.indexEntry = indexEntry;
            this.primaryKey = primaryKey;
            this.recordType = recordType;
            this.protoRecord = protoRecord;
        }

        @Nullable
        @Override
        public Index getIndex() {
            return index;
        }

        @Nonnull
        @Override
        public IndexEntry getIndexEntry() {
            return indexEntry;
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
        public M getRecord() {
            return protoRecord;
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

        @Nullable
        @Override
        public FDBStoredRecord<M> getStoredRecord() {
            return null;
        }
    }

}
