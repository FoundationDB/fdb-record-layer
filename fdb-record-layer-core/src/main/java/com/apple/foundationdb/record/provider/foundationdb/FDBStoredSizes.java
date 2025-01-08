/*
 * FDBStoredSizes.java
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
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;

import javax.annotation.Nonnull;

/**
 * Information about how a record is stored in the database.
 * @see FDBStoredRecord
 */
@API(API.Status.UNSTABLE)
public interface FDBStoredSizes {
    /**
     * Get the number of keys used to store this record.
     * @return number of keys
     */
    int getKeyCount();

    /**
     * Get the size in bytes of all keys used to store this record.
     * @return size in bytes
     */
    int getKeySize();

    /**
     * Get the size in bytes of all values used to store this record. 
     * @return size in bytes
     */
    int getValueSize();

    /**
     * Get whether this record is split between two or more key-value pairs.
     * @return {@code true} if split
     */
    boolean isSplit();

    /**
     * Get whether this record was stored with an associated version.
     * In particular, this states whether there was a version stored
     * directly with the record in the database, which should only be
     * true if the format version of the database is greater than or
     * equal to {@link FDBRecordStore#SAVE_VERSION_WITH_RECORD_FORMAT_VERSION}.
     * @return {@code true} if this record is stored with a version in-line
     */
    boolean isVersionedInline();

    /**
     * Add logging information about size information to a log message.
     *
     * @param msg the log message to add information to
     */
    @API(API.Status.EXPERIMENTAL)
    default void addSizeLogInfo(@Nonnull KeyValueLogMessage msg) {
        msg.addKeyAndValue(LogMessageKeys.KEY_COUNT, getKeyCount())
                .addKeyAndValue(LogMessageKeys.KEY_SIZE, getKeySize())
                .addKeyAndValue(LogMessageKeys.VALUE_SIZE, getValueSize())
                .addKeyAndValue(LogMessageKeys.IS_SPLIT, isSplit())
                .addKeyAndValue(LogMessageKeys.IS_VERSIONED_INLINE, isVersionedInline());
    }
}
