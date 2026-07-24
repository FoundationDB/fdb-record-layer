/*
 * PendingWritesQueueEntry.java
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

import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;

/**
 * A single entry returned by
 * {@link PendingWritesQueue#getQueueCursor(FDBRecordContext, ScanProperties, byte[])}.
 * Wraps the queue key (the {@code (incarnation, versionstamp)} tuple, needed for
 * {@link PendingWritesQueue#clearEntry}) and the already-unpacked, type-checked payload.
 *
 * @param <T> the payload message type of the queue that produced this entry
 */
public final class PendingWritesQueueEntry<T extends Message> {
    @Nonnull
    private final Tuple keyTuple;
    @Nonnull
    private final T payload;
    private final int version;
    @Nonnull
    private final String payloadTypeUrl;
    private final long enqueueTimestamp;

    PendingWritesQueueEntry(@Nonnull Tuple keyTuple,
                            @Nonnull T payload,
                            int version,
                            @Nonnull String payloadTypeUrl,
                            long enqueueTimestamp) {
        // Sanity-check the key shape — the queue always writes (incarnation, versionstamp).
        if (keyTuple.size() != 2) {
            throw new RecordCoreStorageException("Unexpected queue key shape")
                    .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple);
        }
        this.keyTuple = keyTuple;
        this.payload = payload;
        this.version = version;
        this.payloadTypeUrl = payloadTypeUrl;
        this.enqueueTimestamp = enqueueTimestamp;
    }

    @Nonnull
    public Tuple getKeyTuple() {
        return keyTuple;
    }

    public int getIncarnation() {
        return (int)keyTuple.getLong(0);
    }

    /**
     * Returns the on-disk schema version of this entry: the envelope {@code version} for a
     * current-format entry, or {@code 0} for an entry read via the legacy decoder (which has no
     * versioned envelope).
     *
     * @return the stored schema version, or {@code 0} for a legacy-format entry
     */
    public int getVersion() {
        return version;
    }

    public long getEnqueueTimestamp() {
        return enqueueTimestamp;
    }

    /**
     * Returns the entry's payload, already unpacked from the on-disk {@code Any} and
     * type-checked against the queue instance's bound message type.
     *
     * @return the unpacked, typed payload
     */
    @Nonnull
    public T getPayload() {
        return payload;
    }

    /**
     * Returns the type URL of the on-disk {@code Any} that held this entry's payload; useful
     * for diagnostics and logging.
     *
     * @return the on-disk {@code Any} type URL
     */
    @Nonnull
    public String getPayloadTypeUrl() {
        return payloadTypeUrl;
    }
}
