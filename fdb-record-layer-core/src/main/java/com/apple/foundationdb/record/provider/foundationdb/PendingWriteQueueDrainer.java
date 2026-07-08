/*
 * PendingWriteQueueDrainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueue;
import com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueueEntry;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.CursorFactory;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.ThrottledRetryingIterator;
import com.apple.foundationdb.util.CloseException;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.Serial;
import java.util.concurrent.CompletableFuture;

/**
 * Drain a pending writes queue. Used by the indexer.
 */
@ParametersAreNonnullByDefault
public class PendingWriteQueueDrainer {
    private final Index index;
    private final IndexingCommon common;

    public PendingWriteQueueDrainer(final Index index, IndexingCommon common) {
        this.index = index;
        this.common = common;
    }

    CompletableFuture<Boolean> isQueueEmpty(FDBRecordStore store) {
        return getQueue(store).isQueueEmpty(store.getContext());
    }

    @SuppressWarnings("PMD.CloseResource")
    CompletableFuture<Void> drainPendingQueue() {
        final ThrottledRetryingIterator<PendingWritesQueueEntry<IndexBuildProto.PendingWritesQueueEntry>> iterator =
                ThrottledRetryingIterator.builder(
                                common.getRunner().getDatabase(),
                                cursorFactory(),
                                this::handleOneItem)
                        .build();
        return iterator.iterateAll(common.getRecordStoreBuilder().copyBuilder())
                .whenComplete((v, e) -> {
                    try {
                        iterator.close();
                    } catch (CloseException closeEx) {
                        throw new PendingWriteQueueDrainException(closeEx);
                    }
                });
    }

    @Nonnull
    private CursorFactory<PendingWritesQueueEntry<IndexBuildProto.PendingWritesQueueEntry>> cursorFactory() {
        return (store, lastResult, rowLimit) -> {
            final byte[] continuation = lastResult == null ? null : lastResult.getContinuation().toBytes();
            final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(props -> props.setReturnedRowLimit(rowLimit));
            return getQueue(store).getQueueCursor(store.getContext(), scanProperties, continuation);
        };
    }

    @Nonnull
    private CompletableFuture<Void> handleOneItem(final FDBRecordStore store,
                                                  final RecordCursorResult<PendingWritesQueueEntry<IndexBuildProto.PendingWritesQueueEntry>> lastResult,
                                                  final ThrottledRetryingIterator.QuotaManager ignoredQuotaManager) {
        final PendingWritesQueueEntry<IndexBuildProto.PendingWritesQueueEntry> entry = lastResult.get();
        if (entry == null) {
            return AsyncUtil.DONE;
        }
        final IndexBuildProto.PendingWritesQueueEntry payload = entry.getPayload();
        return store.getIndexMaintainer(index)
                .updateWhileWriteOnly(
                        PendingWriteQueueIndexingFactory.getOldRecord(store, payload),
                        PendingWriteQueueIndexingFactory.getNewRecord(store, payload))
                .thenAccept(ignore ->
                        // The queue items deletes count in a single throttled transaction is unlimited (at least for now).
                        getQueue(store).clearEntry(store.getContext(), entry));
    }

    @Nonnull
    private PendingWritesQueue<IndexBuildProto.PendingWritesQueueEntry> getQueue(final FDBRecordStore store) {
        return PendingWriteQueueIndexingFactory.getIndexingQueue(store, index);
    }

    /**
     * thrown if pending queue drain had failed.
     */
    @SuppressWarnings("java:S110")
    public static class PendingWriteQueueDrainException extends RecordCoreException {
        @Serial
        private static final long serialVersionUID = 7;

        public PendingWriteQueueDrainException(final Throwable cause) {
            super("Pending write queue drain had failed", cause);
        }
    }
}
