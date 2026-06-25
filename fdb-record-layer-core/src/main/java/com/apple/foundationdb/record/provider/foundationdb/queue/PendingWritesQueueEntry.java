/*
 * PendingQritesQueueEntry.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.queue;

import com.apple.foundationdb.record.PendingWritesQueueProto;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A single entry returned by {@link PendingWritesQueue#getQueueCursor(FDBRecordContext, ScanProperties, byte[])} Cursor.
 * Wraps the queue key (the {@code (incarnation, versionstamp)} tuple, needed for {@link PendingWritesQueue#clearEntry}) and the parsed
 * proto payload.
 */
public final class PendingWritesQueueEntry {
    @Nonnull
    private final Tuple keyTuple;
    @Nonnull
    private final PendingWritesQueueProto.PendingWriteItem item;

    PendingWritesQueueEntry(@Nonnull Tuple keyTuple, @Nonnull PendingWritesQueueProto.PendingWriteItem item) {
        // Sanity-check the key shape — the queue always writes (incarnation, versionstamp).
        if (keyTuple.size() != 2) {
            throw new RecordCoreStorageException("Unexpected queue key shape")
                    .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple);
        }
        this.keyTuple = keyTuple;
        this.item = item;
    }

    @Nonnull
    public Tuple getKeyTuple() {
        return keyTuple;
    }

    public int getIncarnation() {
        return (int)keyTuple.getLong(0);
    }

    public long getEnqueueTimestamp() {
        return item.getEnqueueTimestamp();
    }

    /**
     * @return the serialized "old" record bytes, or {@code null} if the entry represents an insert operation
     */
    @Nullable
    public byte[] getOldRecord() {
        return item.hasOldRecord() ? item.getOldRecord().toByteArray() : null;
    }

    /**
     * @return the serialized "new" record bytes, or {@code null} if the entry represents a delete operation
     */
    @Nullable
    public byte[] getNewRecord() {
        return item.hasNewRecord() ? item.getNewRecord().toByteArray() : null;
    }
}
