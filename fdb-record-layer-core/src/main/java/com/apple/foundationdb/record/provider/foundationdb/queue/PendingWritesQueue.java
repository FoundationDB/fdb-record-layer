/*
 * PendingWritesQueue.java
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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PendingWritesQueueProto;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRawRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A persistent FDB-backed queue of pending entries, each carrying a typed Protobuf payload.
 *
 * <p>The queue is intended to hold pending foreground operations that arrive while a background worker
 * is running (and possibly holding resources). Front-end transactions enqueue items
 * conflict-free; the worker drains the queue and applies each entry.
 * The shape of the payload is determined by the caller — the queue is type-agnostic at the
 * proto layer but each {@code PendingWritesQueue} instance is homogeneous (bound to a single
 * {@code T extends Message} at construction).</p>
 *
 * <p>Design highlights:</p>
 * <ul>
 *   <li><b>Typed, extensible payload.</b> Each entry's on-disk payload is wrapped in a
 *       {@link com.google.protobuf.Any}. On read, the queue
 *       verifies that the stored type matches the message class bound to this queue
 *       instance, rejecting mismatches with a {@link RecordCoreStorageException}. Callers
 *       receive an already-unpacked {@code T} from
 *       {@link PendingWritesQueueEntry#getPayload()}.</li>
 *   <li><b>Conflict-free enqueue.</b> Each entry is keyed by a versionstamp,
 *       written through {@link SplitHelper#saveWithSplit} (which uses
 *       {@link MutationType#SET_VERSIONSTAMPED_KEY}). The size counter is mutated atomically
 *       with {@link MutationType#ADD}. The capacity check reads the counter via a snapshot
 *       read. None of these paths install a read conflict, so concurrent enqueuers and a
 *       concurrent background worker never block each other.</li>
 *   <li><b>Strict ordering.</b> Keys are {@code (incarnation, versionstamp)} tuples, so entries
 *       are ordered first by {@link FDBRecordStore#getIncarnation() incarnation} (older incarnations sort before newer ones) and then
 *       by commit version within an incarnation. The use of incarnation here is meant to support multi-cluster
 *       environments, so for a given store, all incarnations are expected to be the same until they are globally changed.</li>
 *   <li><b>Empty-queue invariant.</b> {@link #isQueueEmpty(FDBRecordContext)}
 *       performs a regular (non-snapshot) range read. FDB therefore installs a read-conflict
 *       range over the queue. A caller that drains the queue, calls
 *       {@link #isQueueEmpty} (asserting it is empty), and then mutates other
 *       state will conflict with any other transaction that enqueued an item, so once the
 *       caller's transaction commits the queue is provably empty. This is the core mechanism
 *       for "background work is done; no more writes are coming" close-out.</li>
 *   <li><b>Capacity.</b> A configurable maximum queue size; {@link #enqueue} fails with
 *       {@link PendingWritesQueueTooLargeException} once the size counter reaches the limit.
 *       Note that the limit is read at {@link IsolationLevel#SNAPSHOT} to prevent conflicts so the actual
 *       capacity can be above the maximum by the number of transactions enqueueing concurrently.
 *       A value of {@code 0} disables the cap.</li>
 *   <li><b>Schema versioning.</b> Each entry carries a {@code version} field on disk. Readers
 *       reject entries with a version newer than {@link #CURRENT_VERSION}, allowing forward
 *       migrations to evolve the payload safely.</li>
 * </ul>
 *
 * Note: The queue does <i>NOT</i> guarantee that an enqueue will fail with a conflict when a previously committed
 * transaction called {@link #isQueueEmpty(FDBRecordContext)}. The caller should ensure that happens via an external flag.
 *
 * <p>Callers supply two ready-made subspaces: one for the queue entries and one for the size
 * counter. The expected nesting is up to the caller (e.g. an indexer would typically place
 * both under its index-build subspace).</p>
 *
 * @param <T> the Protobuf message type of every entry's payload; bound at construction
 */
@API(API.Status.INTERNAL)
public class PendingWritesQueue<T extends Message> {
    /**
     * Current version of the on-disk entry payload. Increment when changing the proto schema in
     * a way readers must distinguish. Readers reject entries whose stored version exceeds this
     * constant.
     */
    public static final int CURRENT_VERSION = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(PendingWritesQueue.class);

    @Nonnull
    private final Subspace queueSubspace;
    @Nonnull
    private final Subspace queueSizeSubspace;
    private final long maxQueueSize;
    @Nonnull
    private final Class<T> payloadClass;

    /**
     * Construct a pending writes queue.
     *
     * <p>The queue only ever writes the current envelope format. The queue does support
     * reading of a previous encoded format through passing in a legacy encoder to
     * {@link #getQueueCursor(FDBRecordContext, ScanProperties, byte[], Function)}.</p>
     *
     * @param queueSubspace subspace under which queue entries live; the caller owns its layout
     * @param queueSizeSubspace subspace under which the atomic size counter lives; must not
     * overlap with {@code queueSubspace}
     * @param maxQueueSize maximum number of entries allowed in the queue; {@link #enqueue}
     * rejects further entries when the counter reaches this value. Pass {@code 0} to disable
     * the cap.
     * @param payloadClass class of the payload message type, e.g. {@code MyPayload.class}; used
     * to verify and unpack the payload on read
     */
    public PendingWritesQueue(@Nonnull Subspace queueSubspace,
                              @Nonnull Subspace queueSizeSubspace,
                              long maxQueueSize,
                              @Nonnull Class<T> payloadClass) {
        this.queueSubspace = queueSubspace;
        this.queueSizeSubspace = queueSizeSubspace;
        this.maxQueueSize = maxQueueSize;
        this.payloadClass = payloadClass;
    }

    /**
     * Enqueue a single item in the queue.
     *
     * <p>The entry is keyed with {@code (incarnation, versionstamp)}, so concurrent enqueues
     * across transactions get distinct keys without taking any read conflict. Incarnation is
     * meant to keep the order across FDB clusters, where the {@code versionStamp} ordering is
     * not sufficient. The capacity check is performed via a snapshot read on the size counter
     * — so it does not introduce a conflict either.</p>
     *
     * @param context the active record context
     * @param payload the typed payload to enqueue
     * @param incarnation incarnation prefix
     *
     * @return a future that completes once the operation is complete
     *
     * @throws PendingWritesQueueTooLargeException via the returned future if the queue is at
     * or beyond {@code maxQueueSize}
     */
    @Nonnull
    public CompletableFuture<Void> enqueue(@Nonnull FDBRecordContext context,
                                           @Nonnull T payload,
                                           int incarnation) {
        return capacityCheck(context).thenAccept(ignored -> writeEntry(context, payload, incarnation));
    }

    /**
     * Return a cursor that iterates through the queue entries in {@code (incarnation,
     * versionstamp)} order.
     *
     * <p>Equivalent to {@link #getQueueCursor(FDBRecordContext, ScanProperties, byte[], Function)}
     * with no legacy decoder.</p>
     *
     * @param context the record context to scan within
     * @param scanProperties scan properties; the isolation level is always forced to {@link IsolationLevel#SNAPSHOT}
     * @param continuation continuation from a previous cursor invocation, or {@code null} to
     * start from the beginning
     *
     * @return a cursor over {@link PendingWritesQueueEntry} values
     */
    @Nonnull
    public RecordCursor<PendingWritesQueueEntry<T>> getQueueCursor(@Nonnull FDBRecordContext context,
                                                                   @Nonnull ScanProperties scanProperties,
                                                                   @Nullable byte[] continuation) {
        return getQueueCursor(context, scanProperties, continuation, null);
    }

    /**
     * Return a cursor that iterates through the queue entries in {@code (incarnation,
     * versionstamp)} order.
     *
     * <p>The cursor always uses {@link IsolationLevel#SNAPSHOT} regardless of what the caller
     * passes in {@code scanProperties}. This is intentional: it lets a drain transaction
     * iterate the queue and clear entries without installing a read-conflict range that would
     * conflict with concurrent enqueues. Callers that want to fail-on-late-enqueue should
     * combine the drain with {@link #isQueueEmpty}.</p>
     *
     * @param context the record context to scan within
     * @param scanProperties scan properties; the isolation level is always forced to {@link IsolationLevel#SNAPSHOT}
     * @param continuation continuation from a previous cursor invocation, or {@code null} to
     * start from the beginning
     * @param legacyDecoder fallback that turns the raw stored bytes of a previous-format entry
     * (one that fails to parse as the current envelope) into a
     * payload {@code T}; {@code null} if there is no legacy format to read, in which case such an
     * entry is a storage error.
     *
     * @return a cursor over {@link PendingWritesQueueEntry} values
     */
    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    public RecordCursor<PendingWritesQueueEntry<T>> getQueueCursor(@Nonnull FDBRecordContext context,
                                                                   @Nonnull ScanProperties scanProperties,
                                                                   @Nullable byte[] continuation,
                                                                   @Nullable Function<byte[], T> legacyDecoder) {
        // Inner-cursor properties: clear per-row limits (the unsplitter applies entry-level
        // limits) and force snapshot isolation so the read never installs a read-conflict
        // range over the queue subspace. The unsplitter below still receives the ORIGINAL
        // scanProperties so the caller's row/time/byte/skip limits are honored at the entry
        // level (the unsplitter doesn't issue any FDB reads itself, so its isolation level is
        // irrelevant).
        ScanProperties innerScanProperties = scanProperties
                .with(ExecuteProperties::clearRowAndTimeLimits)
                .with(ExecuteProperties::clearSkipAndLimit)
                .with(ExecuteProperties::clearState)
                .with(executeProperties -> executeProperties.toBuilder()
                        .setIsolationLevel(IsolationLevel.SNAPSHOT)
                        .build());
        KeyValueCursor inner = KeyValueCursor.Builder.withSubspace(queueSubspace)
                .setContext(context)
                .setScanProperties(innerScanProperties)
                .setContinuation(continuation)
                .build();
        RecordCursor<FDBRawRecord> unsplitter = new SplitHelper.KeyValueUnsplitter(
                context, queueSubspace, inner,
                false, null, scanProperties)
                .limitRowsTo(scanProperties.getExecuteProperties().getReturnedRowLimit());
        return unsplitter.map(rawRecord -> toQueueEntry(rawRecord.getPrimaryKey(), rawRecord.getRawRecord(), legacyDecoder));
    }

    /**
     * Remove a queue entry that was previously read via {@link #getQueueCursor}.
     *
     * <p>The entry's key tuple must already have been resolved to a complete versionstamp,
     * which is the case for any entry returned by {@link #getQueueCursor} (since the read sees
     * only committed entries).</p>
     * <p>The {@link #clearEntry(FDBRecordContext, PendingWritesQueueEntry)} method installs a read conflict on the
     * range of keys that will be cleared. This is done to prevent cases of concurrent clearing of the same queue
     * elements that would then cause the queue size counter to drift. All but one of the concurrent clearing of the
     * same item would conflict.</p>
     *
     * @param context the record context
     * @param entry the entry to clear
     */
    public void clearEntry(@Nonnull FDBRecordContext context, @Nonnull PendingWritesQueueEntry<T> entry) {
        // Install a read-conflict range over the keys this entry occupies. Clears are blind
        // writes that don't conflict with each other, so without this two transactions could
        // concurrently clear the same entry and each decrement the size counter, making it
        // drift. The read conflict ensures that if another transaction clears (writes to) this
        // entry's keys first, this transaction conflicts on commit, so at most one clear of a
        // given entry succeeds.
        final Tuple key = entry.getKeyTuple();
        Range entryRange = Range.startsWith(queueSubspace.pack(key));
        context.ensureActive().addReadConflictRange(entryRange.begin, entryRange.end);
        SplitHelper.deleteSplit(context, queueSubspace, key, true, false, false, null);
        mutateQueueSizeCounter(context, -1);
        context.increment(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_CLEAR);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getLogMessage("Cleared queue entry").toString());
        }
    }

    /**
     * Check whether the queue is empty.
     *
     * <p>This uses a regular (non-snapshot) range read, which means FDB will install a
     * read-conflict range over the queue. As a result, a transaction that calls
     * {@code isQueueEmpty} while another transaction enqueues into the queue during the commit window will get a
     * conflict exception.</p>
     *
     * @param context the record context
     *
     * @return a future resolving to {@code true} when the queue range is empty
     */
    @Nonnull
    public CompletableFuture<Boolean> isQueueEmpty(@Nonnull FDBRecordContext context) {
        return context.ensureActive()
                .getRange(queueSubspace.range(), 1)
                .asList()
                .thenApply(List::isEmpty);
    }

    /**
     * Read the current value of the queue size counter via a snapshot read.
     *
     * <p>Returns {@code null} if the counter key has never been written (i.e. the queue has
     * never had an entry enqueued). The counter is maintained via atomic mutations.</p>
     *
     * @param context the record context
     *
     * @return a future resolving to the counter value, or {@code null} if uninitialized
     */
    @Nonnull
    public CompletableFuture<Long> getQueueSizeNoConflict(@Nonnull FDBRecordContext context) {
        return context.readTransaction(true).get(queueSizeSubspace.pack())
                .thenApply(bytes -> {
                    final Long size = bytes == null ? null : decodeQueueSize(bytes);
                    context.recordSize(FDBStoreTimer.SizeEvents.PENDING_WRITES_QUEUE_SIZE, size == null ? 0 : size);
                    return size;
                });
    }

    private void writeEntry(@Nonnull FDBRecordContext context, @Nonnull T payload, int incarnation) {
        Any packed = Any.pack(payload);
        PendingWritesQueueProto.PendingWriteItem item =
                PendingWritesQueueProto.PendingWriteItem.newBuilder()
                        .setVersion(CURRENT_VERSION)
                        .setPayload(packed)
                        .setEnqueueTimestamp(System.currentTimeMillis())
                        .build();
        FDBRecordVersion recordVersion = FDBRecordVersion.incomplete(context.claimLocalVersion());
        Tuple keyTuple = Tuple.from(incarnation, recordVersion.toVersionstamp());
        byte[] value = item.toByteArray();
        SplitHelper.saveWithSplit(context, queueSubspace, keyTuple, value, null, true, false, false, null, null);
        mutateQueueSizeCounter(context, 1);
        context.increment(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getLogMessage("Enqueued operation")
                    .addKeyAndValue(LogMessageKeys.INCARNATION, incarnation)
                    .addKeyAndValue(LogMessageKeys.VALUE_SIZE, value.length)
                    .toString());
        }
    }

    @Nonnull
    private CompletableFuture<Void> capacityCheck(@Nonnull FDBRecordContext context) {
        if (maxQueueSize <= 0) {
            return CompletableFuture.completedFuture(null);
        }
        return getQueueSizeNoConflict(context).thenAccept(currentSize -> {
            // A null counter means the queue has never been written; treat as size 0 (matches
            // FDB ADD semantics on a missing key).
            if (currentSize != null && currentSize >= maxQueueSize) {
                // TODO: replace with another "non-throwing-operation".
                // If the indexer had stopped, for any reason, and the queue becomes oversized, the
                // right thing to do - instead of preventing any user operations - will be to disable this
                // index. Some options:
                // 1. disable the index in the user IO (best option, but this may be a problem because of the way this IO transaction is constructed)
                // 2. avoid updating, trusting the indexer to disable the index when it detects a "MAX minus epsilon" size (epsilon should be big enough to prevent concurrency issues)
                // 3. user transaction, when avoiding queue update, can set size to a negative number. Indicating to the indexer that the queue is invalid and the index should be rebuilt from scratch.
                throw new PendingWritesQueueTooLargeException(currentSize, maxQueueSize);
            }
        });
    }

    @Nonnull
    private PendingWritesQueueEntry<T> toQueueEntry(@Nonnull Tuple keyTuple, @Nonnull byte[] valueBytes,
                                                    @Nullable Function<byte[], T> legacyDecoder) {
        final PendingWritesQueueProto.PendingWriteItem item = tryParseEnvelope(valueBytes);
        if (item != null && item.getVersion() > 0) {
            // Current format: a versioned envelope carrying an Any payload.
            return currentFormatEntry(keyTuple, item);
        }
        // Not the current format (parse failed, or the entry has no version): fall back to the
        // legacy decoder if one is configured.
        if (legacyDecoder == null) {
            throw new RecordCoreStorageException("Failed to parse pending writes queue entry and no legacy decoder is configured")
                    .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple)
                    .addLogInfo(LogMessageKeys.STORED_VERSION, item == null ? -1 : item.getVersion());
        }
        return legacyFormatEntry(keyTuple, valueBytes, legacyDecoder);
    }

    /**
     * Build an entry from a current-format envelope: validate the version and payload type, then
     * unpack the {@code Any} into the queue's bound message type.
     */
    @Nonnull
    private PendingWritesQueueEntry<T> currentFormatEntry(@Nonnull Tuple keyTuple,
                                                          @Nonnull PendingWritesQueueProto.PendingWriteItem item) {
        if (item.getVersion() > CURRENT_VERSION) {
            throw new RecordCoreStorageException("Pending writes queue entry version is newer than this reader supports")
                    .addLogInfo(LogMessageKeys.VERSION, CURRENT_VERSION)
                    .addLogInfo(LogMessageKeys.STORED_VERSION, item.getVersion())
                    .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple);
        }
        final Any storedPayload = item.getPayload();
        // Any#is derives the expected type URL from the bound message class internally and
        // compares it to the stored URL. If it doesn't match, we surface both URLs for
        // diagnostics before unpacking would itself throw.
        if (!storedPayload.is(payloadClass)) {
            throw new RecordCoreStorageException("Pending writes queue entry payload type does not match the queue's bound type")
                    .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple)
                    .addLogInfo(LogMessageKeys.EXPECTED_TYPE, payloadClass.getName())
                    .addLogInfo(LogMessageKeys.ACTUAL_TYPE, storedPayload.getTypeUrl());
        }
        final T payload;
        try {
            payload = storedPayload.unpack(payloadClass);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreStorageException("Failed to unpack pending writes queue entry payload", ex)
                    .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple)
                    .addLogInfo(LogMessageKeys.EXPECTED_TYPE, payloadClass.getName());
        }
        return new PendingWritesQueueEntry<>(keyTuple, payload, item.getVersion(), storedPayload.getTypeUrl(), item.getEnqueueTimestamp());
    }

    /**
     * Build an entry from a previous format value using the supplied {@code legacyDecoder}.
     *
     * @param keyTuple the key for the queue entry
     * @param valueBytes the serialized bytes to decode
     * @param legacyDecoder the read-path decoder for non-current-format entries
     */
    @Nonnull
    private PendingWritesQueueEntry<T> legacyFormatEntry(@Nonnull Tuple keyTuple,
                                                         @Nonnull byte[] valueBytes,
                                                         @Nonnull Function<byte[], T> legacyDecoder) {
        final T payload;
        try {
            payload = legacyDecoder.apply(valueBytes);
        } catch (RuntimeException ex) {
            throw new RecordCoreStorageException("Failed to decode legacy pending writes queue entry", ex)
                    .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple);
        }
        // Legacy entries were not Any-wrapped and carry no versioned envelope, so the version is
        // 0, the payload type URL is empty, and the enqueue timestamp is unavailable at this layer.
        return new PendingWritesQueueEntry<>(keyTuple, payload, 0, "", 0L);
    }

    /**
     * Parse the stored bytes as the current envelope, returning {@code null} if they are not a
     * valid envelope (e.g. an entry written by a predecessor implementation in a different
     * layout, which the read-path legacy decoder handles instead).
     */
    @Nullable
    private static PendingWritesQueueProto.PendingWriteItem tryParseEnvelope(@Nonnull byte[] valueBytes) {
        try {
            return PendingWritesQueueProto.PendingWriteItem.parseFrom(valueBytes);
        } catch (InvalidProtocolBufferException ex) {
            return null;
        }
    }

    private void mutateQueueSizeCounter(@Nonnull FDBRecordContext context, long delta) {
        context.ensureActive().mutate(MutationType.ADD, queueSizeSubspace.pack(), encodeQueueSize(delta));
    }

    private static byte[] encodeQueueSize(long count) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(count).array();
    }

    private static long decodeQueueSize(@Nonnull byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    @Nonnull
    private KeyValueLogMessage getLogMessage(@Nonnull String staticMsg) {
        return KeyValueLogMessage.build(staticMsg)
                .addKeyAndValue(LogMessageKeys.SUBSPACE, queueSubspace);
    }

    /**
     * Thrown when an enqueue is rejected because the queue is at or above its configured
     * maximum size.
     */
    @SuppressWarnings("serial")
    public static final class PendingWritesQueueTooLargeException extends RecordCoreException {
        PendingWritesQueueTooLargeException(long currentSize, long maxQueueSize) {
            super("Pending writes queue is full");
            addLogInfo(LogMessageKeys.RECORD_COUNT, currentSize);
            addLogInfo(LogMessageKeys.MAX_QUEUE_SIZE, maxQueueSize);
        }
    }
}
