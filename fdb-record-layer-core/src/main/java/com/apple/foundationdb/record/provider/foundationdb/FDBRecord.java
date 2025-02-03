/*
 * FDBRecord.java
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
 * A record associated with the corresponding meta-data.
 *
 * Adds information about the primary key and record type.
 * @param <M> type used to represent stored records
 */
@API(API.Status.UNSTABLE)
public interface FDBRecord<M extends Message> {
    /**
     * Get the primary key for this record.
     * @return primary key for this record
     */
    @Nonnull
    Tuple getPrimaryKey();

    /**
     * Get the record type for this record.
     * @return record type for this record
     */
    @Nonnull
    RecordType getRecordType();

    /**
     * Get the Protobuf message form of this record.
     * @return the Protobuf message for this record
     */
    @Nonnull
    M getRecord();

    /**
     * Get whether a {@link FDBRecordVersion} has been set for this <code>StoredRecord</code>.
     * @return {@code true} if this record has a version
     */
    boolean hasVersion();

    /**
     * Get the {@link FDBRecordVersion} associated with this record (or <code>null</code>). 
     * @return the version for this record
     */
    @Nullable
    FDBRecordVersion getVersion();
}
