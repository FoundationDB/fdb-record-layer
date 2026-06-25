/*
 * PendingWritesQueueTest.java
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
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PendingWritesQueueProto;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional tests for {@link PendingWritesQueue}.
 */
@Tag(Tags.RequiresFDB)
class PendingWritesQueueTest extends FDBRecordStoreTestBase {

    public static final int COUNT = 5;

    /**
     * Enqueue several entries in a single transaction, then iterate them in another
     * transaction. Verifies bytes round-trip and ordering is preserved by versionstamp.
     */
    @Test
    void testEnqueueAndIterate() {
        final int incarnation = 7;
        final List<byte[]> oldRecords = randomPayloads(COUNT, 32);
        final List<byte[]> newRecords = randomPayloads(COUNT, 48);

        PendingWritesQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, PendingWritesQueue.DEFAULT_MAX_QUEUE_SIZE);
            for (int i = 0; i < COUNT; i++) {
                queue.enqueue(context, oldRecords.get(i), newRecords.get(i), incarnation).join();
            }
            commit(context);
        }

        List<PendingWritesQueueEntry> entries = readAll(queue);
        assertEquals(COUNT, entries.size());
        for (int i = 0; i < COUNT; i++) {
            PendingWritesQueueEntry entry = entries.get(i);
            assertArrayEquals(oldRecords.get(i), entry.getOldRecord(), "old payload mismatch at " + i);
            assertArrayEquals(newRecords.get(i), entry.getNewRecord(), "new payload mismatch at " + i);
            assertEquals(incarnation, entry.getIncarnation());
            assertTrue(entry.getEnqueueTimestamp() > 0);
            // The key should be (incarnation, complete-versionstamp).
            assertEquals(2, entry.getKeyTuple().size());
            assertEquals((long)incarnation, entry.getKeyTuple().getLong(0));
            Versionstamp versionstamp = (Versionstamp)entry.getKeyTuple().get(1);
            assertTrue(versionstamp.isComplete());
        }
    }

    /**
     * Insert-shaped (newRecord only) and delete-shaped (oldRecord only) entries also round-trip.
     */
    @Test
    void testEnqueueInsertAndDeleteShapes() {
        PendingWritesQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueue(context, null, bytes("insert-1"), 0).join();
            queue.enqueue(context, bytes("delete-1"), null, 0).join();
            commit(context);
        }
        List<PendingWritesQueueEntry> entries = readAll(queue);
        assertEquals(2, entries.size());
        // First entry is the insert (no old payload).
        assertNull(entries.get(0).getOldRecord());
        assertArrayEquals(bytes("insert-1"), entries.get(0).getNewRecord());
        // Second is the delete (no new payload).
        assertArrayEquals(bytes("delete-1"), entries.get(1).getOldRecord());
        assertNull(entries.get(1).getNewRecord());
    }

    /**
     * Both bytes null is a programming error.
     */
    @Test
    void testEnqueueBothNullRejected() {
        try (FDBRecordContext context = openContext()) {
            PendingWritesQueue queue = getQueue(context, 100);
            assertThrows(RecordCoreArgumentException.class,
                    () -> queue.enqueue(context, null, null, 0));
        }
    }

    /**
     * Removing an entry via {@link PendingWritesQueue#clearEntry} actually removes it and
     * decrements the size counter.
     */
    @Test
    void testClearEntryRemovesItAndDecrementsCounter() {
        PendingWritesQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueue(context, null, bytes("a"), 0).join();
            queue.enqueue(context, null, bytes("b"), 0).join();
            queue.enqueue(context, null, bytes("c"), 0).join();
            commit(context);
        }
        List<PendingWritesQueueEntry> entries = readAll(queue);
        assertEquals(3, entries.size());

        try (FDBRecordContext context = openContext()) {
            queue.clearEntry(context, entries.get(1));
            commit(context);
        }
        List<PendingWritesQueueEntry> remaining = readAll(queue);
        assertEquals(2, remaining.size());
        assertArrayEquals(bytes("a"), remaining.get(0).getNewRecord());
        assertArrayEquals(bytes("c"), remaining.get(1).getNewRecord());

        try (FDBRecordContext context = openContext()) {
            assertEquals(2L, queue.getQueueSizeNoConflict(context).join());
        }
    }

    /**
     * Iterating in batches across multiple transactions via continuation tokens preserves
     * ordering and never repeats or skips an entry.
     */
    @Test
    void testIterateWithContinuations() {
        final int total = 25;
        PendingWritesQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 200);
            for (int i = 0; i < total; i++) {
                queue.enqueue(context, null, bytes("payload-" + i), 0).join();
            }
            commit(context);
        }

        List<PendingWritesQueueEntry> collected = new ArrayList<>();
        byte[] continuation = null;
        do {
            ScanProperties scanProps = ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(7)
                    .build()
                    .asScanProperties(false);
            try (FDBRecordContext context = openContext()) {
                RecordCursor<PendingWritesQueueEntry> cursor =
                        queue.getQueueCursor(context, scanProps, continuation);
                RecordCursorResult<PendingWritesQueueEntry> last = null;
                while (true) {
                    RecordCursorResult<PendingWritesQueueEntry> next = cursor.getNext();
                    if (!next.hasNext()) {
                        last = next;
                        break;
                    }
                    collected.add(next.get());
                }
                RecordCursorContinuation cont = last.getContinuation();
                continuation = cont.isEnd() ? null : cont.toBytes();
            }
        } while (continuation != null);

        assertEquals(total, collected.size());
        for (int i = 0; i < total; i++) {
            assertArrayEquals(bytes("payload-" + i), collected.get(i).getNewRecord(),
                    "ordering broke at " + i);
        }
    }

    /**
     * Older incarnations sort strictly before newer ones, regardless of which transaction
     * committed first.
     */
    @Test
    void testIncarnationOrdering() {
        PendingWritesQueue queue;
        // Commit the newer incarnation first ...
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueue(context, null, bytes("new-incarnation"), 5).join();
            commit(context);
        }
        // ... then commit the older incarnation.
        try (FDBRecordContext context = openContext()) {
            queue.enqueue(context, null, bytes("old-incarnation"), 1).join();
            commit(context);
        }

        List<PendingWritesQueueEntry> entries = readAll(queue);
        assertEquals(2, entries.size());
        // Despite being committed later, the smaller incarnation sorts first.
        assertEquals(1, entries.get(0).getIncarnation());
        assertArrayEquals(bytes("old-incarnation"), entries.get(0).getNewRecord());
        assertEquals(5, entries.get(1).getIncarnation());
        assertArrayEquals(bytes("new-incarnation"), entries.get(1).getNewRecord());
    }

    /**
     * Exercises the {@link SplitHelper} split path: an entry whose payload is well over the
     * FDB 100KB value-size limit.
     */
    @Test
    void testLargeEntryUsesSplitHelper() {
        // ~250KB old record + ~250KB new record → ~500KB serialized entry; well past 100KB.
        byte[] bigOld = randomBytes(250_000, 11L);
        byte[] bigNew = randomBytes(250_000, 13L);

        PendingWritesQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueue(context, bigOld, bigNew, 0).join();
            commit(context);
        }
        List<PendingWritesQueueEntry> entries = readAll(queue);
        assertEquals(1, entries.size());
        assertArrayEquals(bigOld, entries.get(0).getOldRecord());
        assertArrayEquals(bigNew, entries.get(0).getNewRecord());
    }

    /**
     * Two parallel transactions each enqueue several entries — neither should conflict with
     * the other.
     */
    @Test
    void testEnqueueAddsNoConflictsBetweenEnqueuers() {
        PendingWritesQueue queue;
        try (FDBRecordContext seed = openContext()) {
            queue = getQueue(seed, 100);
            commit(seed);
        }
        try (FDBRecordContext ctxA = openContext();
             FDBRecordContext ctxB = openContext()) {
            for (int i = 0; i < 4; i++) {
                queue.enqueue(ctxA, null, bytes("A-" + i), 0).join();
                queue.enqueue(ctxB, null, bytes("B-" + i), 0).join();
            }
            commit(ctxA);
            // Both transactions commit without conflict because each enqueue uses a
            // versionstamped key and an atomic counter mutation.
            commit(ctxB);
        }
        try (FDBRecordContext context = openContext()) {
            assertEquals(8L, queue.getQueueSizeNoConflict(context).join());
        }
    }

    /**
     * {@link PendingWritesQueue#isQueueEmptyAndFailOnConflict} returns true on a virgin queue and false once
     * something has been enqueued.
     */
    @Test
    void testIsQueueEmpty() {
        PendingWritesQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            assertTrue(queue.isQueueEmptyAndFailOnConflict(context).join());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            queue.enqueue(context, null, bytes("first"), 0).join();
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            assertFalse(queue.isQueueEmptyAndFailOnConflict(context).join());
        }
    }

    /**
     * The "fail if conflicting insert" close-out: TX_A calls {@code isQueueEmpty} (which uses a
     * non-snapshot read so FDB installs a read-conflict range over the queue). While TX_A is open, TX_B enqueues into the queue and commits
     * first. TX_A's commit must fail with a conflict.
     */
    @Test
    void testIsEmptyConflictsConcurrentEnqueue() {
        PendingWritesQueue queue;
        Subspace otherSubspace;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            otherSubspace = path.toSubspace(context).subspace(Tuple.from("other"));
            commit(context);
        }

        try (FDBRecordContext txA = openContext()) {
            // TX_A asserts the queue is empty and writes some close-out state.
            assertTrue(queue.isQueueEmptyAndFailOnConflict(txA).join());
            // make another write on the transaction such that it will conflict
            // this simulates the user of the queue marking the entire operation as "complete"
            txA.ensureActive().set(otherSubspace.pack(), bytes("done"));

            // While TX_A is open, TX_B enqueues into the queue and commits.
            try (FDBRecordContext txB = openContext()) {
                queue.enqueue(txB, null, bytes("late-arrival"), 0).join();
                commit(txB);
            }

            // TX_A's commit should now conflict because the (empty) range it scanned was
            // written into by TX_B.
            assertThrows(RecordCoreException.class, () -> commit(txA));
        }

        // The late-arrival enqueue did land.
        try (FDBRecordContext context = openContext()) {
            assertFalse(queue.isQueueEmptyAndFailOnConflict(context).join());
            assertEquals(1L, queue.getQueueSizeNoConflict(context).join());
        }
    }

    /**
     * The drain-in-a-single-transaction happy path: read every entry via the cursor, clear
     * each one, call {@code isQueueEmpty} (asserting true), commit. After commit, the queue is
     * empty and the size counter reads 0.
     */
    @Test
    void testIsEmptyDoesNotConflictWithDrainerInSameTx() {        PendingWritesQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            for (int i = 0; i < 4; i++) {
                queue.enqueue(context, null, bytes("payload-" + i), 0).join();
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            List<PendingWritesQueueEntry> entries =
                    queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
            assertEquals(4, entries.size());
            for (PendingWritesQueueEntry entry : entries) {
                queue.clearEntry(context, entry);
            }
            // Asserting emptiness in the same transaction that drained should be fine — there
            // is no other writer to conflict with.
            assertTrue(queue.isQueueEmptyAndFailOnConflict(context).join());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            assertTrue(queue.isQueueEmptyAndFailOnConflict(context).join());
            assertEquals(0L, queue.getQueueSizeNoConflict(context).join());
        }
    }

    /**
     * A drain transaction that reads via the cursor and clears every observed entry must NOT
     * conflict with a concurrent enqueue. Both transactions commit, and the queue ends up
     * holding exactly the entry that the concurrent enqueue added.
     *
     * <p>This is the normal in-progress drain pattern: the background indexer keeps draining
     * batches while front-end writers keep enqueuing, and neither side should block the other.
     * The conflict-freeness comes from {@link PendingWritesQueue#getQueueCursor} forcing
     * snapshot isolation regardless of what the caller passes in {@code scanProperties}. The
     * close-out emptiness assertion uses {@link
     * PendingWritesQueue#isQueueEmptyAndFailOnConflict} (which IS conflict-installing) and is
     * covered separately by {@link #testIsEmptyConflictsConcurrentEnqueue}.</p>
     */
    @Test
    void testDrainScanAndClearDoesNotConflictWithConcurrentEnqueue() {
        PendingWritesQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            for (int i = 0; i < 3; i++) {
                queue.enqueue(context, null, bytes("seed-" + i), 0).join();
            }
            commit(context);
        }

        try (FDBRecordContext drainTx = openContext()) {
            // getQueueCursor always uses snapshot isolation internally, so the caller can
            // pass plain FORWARD_SCAN and still avoid installing a read-conflict range.
            List<PendingWritesQueueEntry> drained =
                    queue.getQueueCursor(drainTx, ScanProperties.FORWARD_SCAN, null).asList().join();
            assertEquals(3, drained.size());
            for (PendingWritesQueueEntry entry : drained) {
                queue.clearEntry(drainTx, entry);
            }

            // While the drain transaction is still open, another transaction enqueues a fresh
            // entry. It must commit cleanly because enqueue never conflicts with anything.
            try (FDBRecordContext enqueueTx = openContext()) {
                queue.enqueue(enqueueTx, null, bytes("late-arrival"), 0).join();
                commit(enqueueTx);
            }

            // The drain commit must ALSO succeed — the snapshot scan installed no conflict
            // range, and clearEntry only touched the three seed keys which the concurrent
            // enqueue didn't touch.
            commit(drainTx);
        }

        // Final state: only the late-arrival entry remains.
        List<PendingWritesQueueEntry> remaining = readAll(queue);
        assertEquals(1, remaining.size());
        assertArrayEquals(bytes("late-arrival"), remaining.get(0).getNewRecord());
        try (FDBRecordContext context = openContext()) {
            assertEquals(1L, queue.getQueueSizeNoConflict(context).join());
        }
    }

    /**
     * An on-disk entry whose schema version exceeds {@link PendingWritesQueue#CURRENT_VERSION}
     * surfaces as a {@link RecordCoreStorageException} when read.
     */
    @Test
    void testVersionMismatchRejected() {
        PendingWritesQueue queue;
        // Hand-craft an entry whose proto version is CURRENT_VERSION + 1 and write it directly
        // under a (incarnation, versionstamp) key — bypassing PendingWritesQueue#enqueue.
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            PendingWritesQueueProto.PendingWriteItem item = PendingWritesQueueProto.PendingWriteItem.newBuilder()
                    .setVersion(PendingWritesQueue.CURRENT_VERSION + 1)
                    .setNewRecord(ByteString.copyFromUtf8("future"))
                    .setEnqueueTimestamp(System.currentTimeMillis())
                    .build();
            Subspace queueSubspace = queueSubspaceFor(context);
            FDBRecordVersion recordVersion = FDBRecordVersion.incomplete(context.claimLocalVersion());
            Tuple keyTuple = Tuple.from(0, recordVersion.toVersionstamp());
            SplitHelper.saveWithSplit(context, queueSubspace, keyTuple, item.toByteArray(),
                    null, true, false, false, null, null);
            // Bump the size counter so capacity bookkeeping stays consistent.
            context.ensureActive().mutate(MutationType.ADD,
                    counterSubspaceFor(context).pack(),
                    new byte[] {1, 0, 0, 0, 0, 0, 0, 0});
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            CompletableFuture<List<PendingWritesQueueEntry>> future =
                    queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList();
            CompletableFuture<List<PendingWritesQueueEntry>> caught = future.handle((entries, ex) -> {
                if (ex != null) {
                    Throwable root = ex;
                    while (root instanceof java.util.concurrent.CompletionException && root.getCause() != null) {
                        root = root.getCause();
                    }
                    throw (RuntimeException)root;
                }
                return entries;
            });
            Assertions.assertThatThrownBy(caught::join).hasCauseInstanceOf(RecordCoreStorageException.class);
        }
    }

    @Nonnull
    private PendingWritesQueue getQueue(@Nonnull FDBRecordContext context, long maxQueueSize) {
        return new PendingWritesQueue(queueSubspaceFor(context), counterSubspaceFor(context), maxQueueSize);
    }

    @Nonnull
    private Subspace queueSubspaceFor(@Nonnull FDBRecordContext context) {
        return path.toSubspace(context).subspace(Tuple.from("queue"));
    }

    @Nonnull
    private Subspace counterSubspaceFor(@Nonnull FDBRecordContext context) {
        return path.toSubspace(context).subspace(Tuple.from("counter"));
    }

    @Nonnull
    private List<PendingWritesQueueEntry> readAll(@Nonnull PendingWritesQueue queue) {
        try (FDBRecordContext context = openContext()) {
            return queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
        }
    }

    @Nonnull
    private static byte[] bytes(@Nonnull String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Nonnull
    private static List<byte[]> randomPayloads(int count, int size) {
        Random random = new Random(0xC0FFEEL + count + (long)size);
        return IntStream.range(0, count)
                .mapToObj(i -> {
                    byte[] b = new byte[size];
                    random.nextBytes(b);
                    return b;
                })
                .collect(Collectors.toList());
    }

    @Nonnull
    private static byte[] randomBytes(int size, long seed) {
        byte[] b = new byte[size];
        new Random(seed).nextBytes(b);
        return b;
    }
}
