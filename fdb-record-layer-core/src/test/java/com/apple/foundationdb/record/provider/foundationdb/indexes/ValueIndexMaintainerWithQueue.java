/*
 * ValueIndexMaintainerWithQueue.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;
import com.google.protobuf.Any;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A test-only value index maintainer with simple pending-write-queue support.
 *
 */
public class ValueIndexMaintainerWithQueue extends ValueIndexMaintainer {
    public ValueIndexMaintainerWithQueue(final IndexMaintainerState state) {
        super(state);
    }

    @Override
    public boolean isPendingWriteQueueAllowed() {
        return StandardIndexMaintainerWithQueue.isPendingWriteQueueAllowed(this, state);
    }

    @Override
    @Nonnull
    public <M extends Message> Any serializePendingWriteQueue(@Nullable final FDBIndexableRecord<M> oldRecord,
                                                              @Nullable final FDBIndexableRecord<M> newRecord) {
        return StandardIndexMaintainerWithQueue.serializePendingWrites(state, oldRecord, newRecord);
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> updateFromQueue(@Nonnull final Any data) {
        // Calling updateWhileWriteOnly explicitly, lest this update be re-pushed to the queue.
        final IndexBuildProto.OldAndNewRecords records = StandardIndexMaintainerWithQueue.unpackPendingWrites(data);
        return updateWhileWriteOnly(
                records.hasOldRecords()
                        ? StandardIndexMaintainerWithQueue.deserializePendingRecord(state, records.getOldRecords()) : null,
                records.hasNewRecord()
                        ? StandardIndexMaintainerWithQueue.deserializePendingRecord(state, records.getNewRecord()) : null);
    }

    /**
     * Factory registering the {@link #INDEX_TYPE} index type, backed by {@link ValueIndexMaintainerWithQueue}.
     * Validation is delegated to the ordinary value-index validator.
     */
    @AutoService(IndexMaintainerFactory.class)
    public static class Factory implements IndexMaintainerFactory {
        /** Index type whose maintainer is an ordinary value index that also supports the pending write queue. */
        public static final String INDEX_TYPE = "value_with_queue";
        private static final Set<String> INDEX_TYPES = Collections.singleton(INDEX_TYPE);
        private static final ValueIndexMaintainerFactory underlying = new ValueIndexMaintainerFactory();

        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return INDEX_TYPES;
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(final Index index) {
            return underlying.getIndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
            return new ValueIndexMaintainerWithQueue(state);
        }
    }
}
