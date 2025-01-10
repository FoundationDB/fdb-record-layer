/*
 * RecordIndexUniquenessViolation.java
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
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An exception thrown when there is an attempt to store a duplicate value in a unique index.
 *
 * This exception is not necessarily thrown by the specific {@code saveRecord} that caused the violation,
 * but it will always be thrown before {@code commit} would have completed successfully. When this is thrown,
 * {@code commit} <em>will not</em> complete successfully, whether it was thrown earlier or from {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext#commit}
 * itself.
 */
@API(API.Status.UNSTABLE)
@SuppressWarnings({"serial", "squid:S1948", "squid:MaximumInheritanceDepth"})
public class RecordIndexUniquenessViolation extends RecordCoreException {
    @Nonnull private Index index;
    @Nullable private IndexEntry indexEntry;
    @Nullable private Tuple primaryKey;
    @Nullable private Tuple existingKey;

    public RecordIndexUniquenessViolation(@Nonnull Index index,
                                          IndexEntry indexEntry,
                                          Tuple primaryKey,
                                          Tuple existingKey) {
        super("Duplicate entry for unique index",
                LogMessageKeys.INDEX_NAME, index.getName(),
                LogMessageKeys.INDEX_KEY, indexEntry,
                "existing_primary_key", existingKey,
                LogMessageKeys.PRIMARY_KEY, primaryKey);

        this.index = index;
        this.indexEntry = indexEntry;
        this.primaryKey = primaryKey;
        this.existingKey = existingKey;
    }

    public RecordIndexUniquenessViolation(String message, RecordIndexUniquenessViolation cause) {
        super(message, cause);
        this.index = cause.index;
        this.indexEntry = cause.indexEntry;
        this.primaryKey = cause.primaryKey;
        this.existingKey = cause.existingKey;
    }

    /**
     * Get the index associated with this uniqueness violation.
     * @return the index associated with this uniqueness violation
     */
    @Nonnull
    public Index getIndex() {
        return index;
    }

    /**
     * Get the index value of this uniqueness violation. This is the value the
     * index took for some record that conflicted with the value
     * for a different record.
     * @return the index key of this uniqueness violation
     */
    @Nullable
    public IndexEntry getIndexEntry() {
        return indexEntry;
    }

    /**
     * Get the primary key of this uniqueness violation. This is the
     * record that was added that triggered the uniqueness
     * violation
     * @return the primary key of this uniqueness violation
     */
    @Nullable
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    /**
     * Get the existing key of this uniqueness violation. This is the
     * primary key of the record that was already within the database
     * at the time the second record was added.
     * @return the existing key of this uniqueness violation
     */
    @Nullable
    public Tuple getExistingKey() {
        return existingKey;
    }
}
