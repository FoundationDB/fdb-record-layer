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
import com.apple.foundationdb.record.PendingWritesQueueTestProto.OtherTestQueuePayload;
import com.apple.foundationdb.record.PendingWritesQueueTestProto.TestQueuePayload;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.apple.test.Tags;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
     * transaction. Verifies payloads round-trip and ordering is preserved by versionstamp.
     */
    @Test
    void testEnqueueAndIterate() {
        final int incarnation = 7;
        final List<TestQueuePayload> payloads = randomPayloads(COUNT, 0xC0FFEEL + COUNT);

        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, PendingWritesQueue.DEFAULT_MAX_QUEUE_SIZE);
            for (int i = 0; i < COUNT; i++) {
                queue.enqueue(context, payloads.get(i), incarnation).join();
            }
            commit(context);
        }

        List<PendingWritesQueueEntry<TestQueuePayload>> entries = readAll(queue);
        assertEquals(COUNT, entries.size());
        for (int i = 0; i < COUNT; i++) {
            PendingWritesQueueEntry<TestQueuePayload> entry = entries.get(i);
            assertEquals(payloads.get(i), entry.getPayload(), "payload mismatch at " + i);
            assertEquals(incarnation, entry.getIncarnation());
            assertTrue(entry.getEnqueueTimestamp() > 0);
            // Type URL is exposed for diagnostics; it should be the expected one for our
            // bound payload type.
            assertEquals(Any.pack(TestQueuePayload.getDefaultInstance()).getTypeUrl(),
                    entry.getPayloadTypeUrl());
            // The key should be (incarnation, complete-versionstamp).
            assertEquals(2, entry.getKeyTuple().size());
            assertEquals((long) incarnation, entry.getKeyTuple().getLong(0));
            Versionstamp versionstamp = (Versionstamp) entry.getKeyTuple().get(1);
            assertTrue(versionstamp.isComplete());
        }
    }

    /**
     * Removing an entry via {@link PendingWritesQueue#clearEntry} actually removes it and
     * decrements the size counter.
     */
    @Test
    void testClearEntryRemovesItAndDecrementsCounter() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueue(context, payload("a"), 0).join();
            queue.enqueue(context, payload("b"), 0).join();
            queue.enqueue(context, payload("c"), 0).join();
            commit(context);
        }
        List<PendingWritesQueueEntry<TestQueuePayload>> entries = readAll(queue);
        assertEquals(3, entries.size());

        try (FDBRecordContext context = openContext()) {
            queue.clearEntry(context, entries.get(1));
            commit(context);
        }
        List<PendingWritesQueueEntry<TestQueuePayload>> remaining = readAll(queue);
        assertEquals(2, remaining.size());
        assertEquals("a", remaining.get(0).getPayload().getLabel());
        assertEquals("c", remaining.get(1).getPayload().getLabel());

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
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 200);
            for (int i = 0; i < total; i++) {
                queue.enqueue(context, payload("payload-" + i), 0).join();
            }
            commit(context);
        }

        List<PendingWritesQueueEntry<TestQueuePayload>> collected = new ArrayList<>();
        byte[] continuation = null;
        do {
            ScanProperties scanProps = ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(7)
                    .build()
                    .asScanProperties(false);
            try (FDBRecordContext context = openContext()) {
                RecordCursor<PendingWritesQueueEntry<TestQueuePayload>> cursor =
                        queue.getQueueCursor(context, scanProps, continuation);
                RecordCursorResult<PendingWritesQueueEntry<TestQueuePayload>> last = null;
                while (true) {
                    RecordCursorResult<PendingWritesQueueEntry<TestQueuePayload>> next = cursor.getNext();
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
            assertEquals("payload-" + i, collected.get(i).getPayload().getLabel(),
                    "ordering broke at " + i);
        }
    }

    /**
     * Older incarnations sort strictly before newer ones, regardless of which transaction
     * committed first.
     */
    @Test
    void testIncarnationOrdering() {
        PendingWritesQueue<TestQueuePayload> queue;
        // Commit the newer incarnation first ...
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueue(context, payload("new-incarnation"), 5).join();
            commit(context);
        }
        // ... then commit the older incarnation.
        try (FDBRecordContext context = openContext()) {
            queue.enqueue(context, payload("old-incarnation"), 1).join();
            commit(context);
        }

        List<PendingWritesQueueEntry<TestQueuePayload>> entries = readAll(queue);
        assertEquals(2, entries.size());
        // Despite being committed later, the smaller incarnation sorts first.
        assertEquals(1, entries.get(0).getIncarnation());
        assertEquals("old-incarnation", entries.get(0).getPayload().getLabel());
        assertEquals(5, entries.get(1).getIncarnation());
        assertEquals("new-incarnation", entries.get(1).getPayload().getLabel());
    }

    /**
     * Exercises the {@link SplitHelper} split path: a payload whose {@code blob} field is well
     * over the FDB 100KB value-size limit.
     */
    @Test
    void testLargeEntryUsesSplitHelper() {
        byte[] bigBlob = randomBytes(250_000, 11L);
        TestQueuePayload bigPayload = TestQueuePayload.newBuilder()
                .setLabel("big")
                .setValue(42L)
                .setBlob(ByteString.copyFrom(bigBlob))
                .build();

        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueue(context, bigPayload, 0).join();
            commit(context);
        }
        List<PendingWritesQueueEntry<TestQueuePayload>> entries = readAll(queue);
        assertEquals(1, entries.size());
        assertEquals(bigPayload, entries.get(0).getPayload());
        assertArrayEquals(bigBlob, entries.get(0).getPayload().getBlob().toByteArray());
    }

    /**
     * Two parallel transactions each enqueue several entries — neither should conflict with
     * the other.
     */
    @Test
    void testEnqueueAddsNoConflictsBetweenEnqueuers() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext seed = openContext()) {
            queue = getQueue(seed, 100);
            commit(seed);
        }
        try (FDBRecordContext ctxA = openContext();
                FDBRecordContext ctxB = openContext()) {
            for (int i = 0; i < 4; i++) {
                queue.enqueue(ctxA, payload("A-" + i), 0).join();
                queue.enqueue(ctxB, payload("B-" + i), 0).join();
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
     * {@link PendingWritesQueue#ensureQueueEmpty} returns true on a new queue
     * and false once something has been enqueued.
     */
    @Test
    void testEnsureQueueEmpty() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            assertTrue(queue.ensureQueueEmpty(context).join());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            queue.enqueue(context, payload("first"), 0).join();
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            assertFalse(queue.ensureQueueEmpty(context).join());
        }
    }

    /**
     * The "fail if conflicting insert" close-out: TX_A calls {@link  PendingWritesQueue#ensureQueueEmpty(FDBRecordContext)}
     * (which uses a non-snapshot read so FDB installs a read-conflict range over the queue). While TX_A is
     * open, TX_B enqueues into the queue and commits first. TX_A's commit must fail with a
     * conflict.
     */
    @Test
    void testIsEmptyConflictsConcurrentEnqueue() {
        PendingWritesQueue<TestQueuePayload> queue;
        Subspace otherSubspace;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            otherSubspace = path.toSubspace(context).subspace(Tuple.from("other"));
            commit(context);
        }

        try (FDBRecordContext txA = openContext()) {
            // TX_A asserts the queue is empty
            assertTrue(queue.ensureQueueEmpty(txA).join());
            // make another write on the transaction such that it will conflict
            // this simulates the user of the queue marking the entire operation as "complete"
            txA.ensureActive().set(otherSubspace.pack(), bytes("done"));

            // While TX_A is open, TX_B enqueues into the queue and commits.
            try (FDBRecordContext txB = openContext()) {
                queue.enqueue(txB, payload("late-arrival"), 0).join();
                commit(txB);
            }

            // TX_A's commit should now conflict because the (empty) range it scanned was
            // written into by TX_B.
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, () -> commit(txA));
        }

        // The late-arrival enqueue did land.
        try (FDBRecordContext context = openContext()) {
            assertFalse(queue.ensureQueueEmpty(context).join());
            assertEquals(1L, queue.getQueueSizeNoConflict(context).join());
        }
    }

    /**
     * The close-out path starting from a <em>full</em> queue: TX_A drains all 5 entries, then
     * calls {@link PendingWritesQueue#ensureQueueEmpty(FDBRecordContext)} (which now sees the
     * queue as empty within TX_A's own view, but installs a read-conflict range over the whole
     * queue), and writes a close-out marker. While TX_A is open, TX_B enqueues a new item and
     * commits. TX_A's commit must fail with a conflict — otherwise it would incorrectly conclude
     * the queue was permanently drained while a write slipped in behind it.
     */
    @Test
    void testDrainThenIsEmptyConflictsConcurrentEnqueue() {
        PendingWritesQueue<TestQueuePayload> queue;
        Subspace otherSubspace;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            otherSubspace = path.toSubspace(context).subspace(Tuple.from("other"));
            // Start the queue full.
            for (int i = 0; i < 5; i++) {
                queue.enqueue(context, payload("seed-" + i), 0).join();
            }
            commit(context);
        }

        try (FDBRecordContext txA = openContext()) {
            // TX_A drains every entry it can see ...
            List<PendingWritesQueueEntry<TestQueuePayload>> entries =
                    queue.getQueueCursor(txA, ScanProperties.FORWARD_SCAN, null).asList().join();
            assertEquals(5, entries.size());
            for (PendingWritesQueueEntry<TestQueuePayload> entry : entries) {
                queue.clearEntry(txA, entry);
            }
            // ... then asserts the queue is now empty. This read installs a read-conflict range
            // over the whole queue, since within TX_A's view the range is empty.
            assertTrue(queue.ensureQueueEmpty(txA).join());
            // Mark the operation complete (a write so the conflict matters at commit).
            txA.ensureActive().set(otherSubspace.pack(), bytes("done"));

            // While TX_A is open, TX_B enqueues a new item and commits.
            try (FDBRecordContext txB = openContext()) {
                queue.enqueue(txB, payload("late-arrival"), 0).join();
                commit(txB);
            }

            // TX_A's commit should now conflict: the (now-empty) range it scanned in
            // ensureQueueEmpty was written into by TX_B's enqueue.
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, () -> commit(txA));
        }

        // TX_A rolled back, so its drain never happened: the 5 seed entries plus the
        // late-arrival enqueue are all still present.
        try (FDBRecordContext context = openContext()) {
            assertFalse(queue.ensureQueueEmpty(context).join());
            assertEquals(6L, queue.getQueueSizeNoConflict(context).join());
        }
    }

    /**
     * The drain-in-a-single-transaction happy path: read every entry via the cursor, clear
     * each one, call {@code isQueueEmpty} (asserting true), commit. After commit, the queue is
     * empty and the size counter reads 0.
     */
    @Test
    void testIsEmptyDoesNotConflictWithDrainerInSameTx() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            for (int i = 0; i < 4; i++) {
                queue.enqueue(context, payload("payload-" + i), 0).join();
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            List<PendingWritesQueueEntry<TestQueuePayload>> entries =
                    queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
            assertEquals(4, entries.size());
            for (PendingWritesQueueEntry<TestQueuePayload> entry : entries) {
                queue.clearEntry(context, entry);
            }
            // Asserting emptiness in the same transaction that drained should be fine — there
            // is no other writer to conflict with.
            assertTrue(queue.ensureQueueEmpty(context).join());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            assertTrue(queue.ensureQueueEmpty(context).join());
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
     * PendingWritesQueue#ensureQueueEmpty} (which IS conflict-installing) and is
     * covered separately by {@link #testIsEmptyConflictsConcurrentEnqueue}.</p>
     */
    @Test
    void testDrainScanAndClearDoesNotConflictWithConcurrentEnqueue() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            for (int i = 0; i < 3; i++) {
                queue.enqueue(context, payload("seed-" + i), 0).join();
            }
            commit(context);
        }

        try (FDBRecordContext drainTx = openContext()) {
            // getQueueCursor always uses snapshot isolation internally, so the caller can
            // pass plain FORWARD_SCAN and still avoid installing a read-conflict range.
            List<PendingWritesQueueEntry<TestQueuePayload>> drained =
                    queue.getQueueCursor(drainTx, ScanProperties.FORWARD_SCAN, null).asList().join();
            assertEquals(3, drained.size());
            for (PendingWritesQueueEntry<TestQueuePayload> entry : drained) {
                queue.clearEntry(drainTx, entry);
            }

            // While the drain transaction is still open, another transaction enqueues a fresh
            // entry. It must commit cleanly because enqueue never conflicts with anything.
            try (FDBRecordContext enqueueTx = openContext()) {
                queue.enqueue(enqueueTx, payload("late-arrival"), 0).join();
                commit(enqueueTx);
            }

            // The drain commit must ALSO succeed — the snapshot scan installed no conflict
            // range, and clearEntry only touched the three seed keys which the concurrent
            // enqueue didn't touch.
            commit(drainTx);
        }

        // Final state: only the late-arrival entry remains.
        List<PendingWritesQueueEntry<TestQueuePayload>> remaining = readAll(queue);
        assertEquals(1, remaining.size());
        assertEquals("late-arrival", remaining.get(0).getPayload().getLabel());
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
        PendingWritesQueue<TestQueuePayload> queue;
        // Hand-craft an entry whose proto version is CURRENT_VERSION + 1 and write it directly
        // under a (incarnation, versionstamp) key — bypassing PendingWritesQueue#enqueue.
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            PendingWritesQueueProto.PendingWriteItem item = PendingWritesQueueProto.PendingWriteItem.newBuilder()
                    .setVersion(PendingWritesQueue.CURRENT_VERSION + 1)
                    .setPayload(Any.pack(TestQueuePayload.newBuilder().setLabel("future").build()))
                    .setEnqueueTimestamp(System.currentTimeMillis())
                    .build();
            writeRawEntry(context, item);
            commit(context);
        }

        assertScanThrows(queue, RecordCoreStorageException.class);
    }

    /**
     * An entry whose stored {@code Any} payload type URL doesn't match the queue's bound type
     * is rejected on read with a {@link RecordCoreStorageException}. Exercises the case where
     * the same subspace is reused (e.g. after a schema migration mistake) with a different
     * payload type.
     */
    @Test
    void testTypeUrlMismatchRejected() {
        // Write via a queue bound to TestQueuePayload.
        PendingWritesQueue<TestQueuePayload> writerQueue;
        try (FDBRecordContext context = openContext()) {
            writerQueue = getQueue(context, 100);
            writerQueue.enqueue(context, payload("hello"), 0).join();
            commit(context);
        }

        // Open a queue bound to a DIFFERENT payload type over the same subspaces. Reading must
        // throw, naming both expected and actual type URLs.
        PendingWritesQueue<OtherTestQueuePayload> readerQueue;
        try (FDBRecordContext context = openContext()) {
            readerQueue = new PendingWritesQueue<>(
                    queueSubspaceFor(context),
                    counterSubspaceFor(context),
                    100,
                    OtherTestQueuePayload.class);
            commit(context);
        }

        assertScanThrows(readerQueue, RecordCoreStorageException.class);
    }

    private <T extends com.google.protobuf.Message> void assertScanThrows(
            @Nonnull PendingWritesQueue<T> queue,
            @Nonnull Class<? extends Throwable> expected) {
        try (FDBRecordContext context = openContext()) {
            Assertions.assertThatThrownBy(() ->
                    queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join())
                    .hasCauseInstanceOf(expected);
        }
    }

    private void writeRawEntry(@Nonnull FDBRecordContext context,
                               @Nonnull PendingWritesQueueProto.PendingWriteItem item) {
        Subspace queueSubspace = queueSubspaceFor(context);
        FDBRecordVersion recordVersion = FDBRecordVersion.incomplete(context.claimLocalVersion());
        Tuple keyTuple = Tuple.from(0, recordVersion.toVersionstamp());
        SplitHelper.saveWithSplit(context, queueSubspace, keyTuple, item.toByteArray(),
                null, true, false, false, null, null);
        // Bump the size counter so capacity bookkeeping stays consistent.
        context.ensureActive().mutate(MutationType.ADD,
                counterSubspaceFor(context).pack(),
                new byte[] {1, 0, 0, 0, 0, 0, 0, 0});
    }

    @Nonnull
    private PendingWritesQueue<TestQueuePayload> getQueue(@Nonnull FDBRecordContext context, long maxQueueSize) {
        return new PendingWritesQueue<>(queueSubspaceFor(context), counterSubspaceFor(context),
                maxQueueSize, TestQueuePayload.class);
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
    private List<PendingWritesQueueEntry<TestQueuePayload>> readAll(@Nonnull PendingWritesQueue<TestQueuePayload> queue) {
        try (FDBRecordContext context = openContext()) {
            return queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
        }
    }

    @Nonnull
    private static TestQueuePayload payload(@Nonnull String label) {
        return TestQueuePayload.newBuilder().setLabel(label).build();
    }

    @Nonnull
    private static byte[] bytes(@Nonnull String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Nonnull
    private static List<TestQueuePayload> randomPayloads(int count, final long seed) {
        Random random = new Random(seed);
        return IntStream.range(0, count)
                .mapToObj(i -> TestQueuePayload.newBuilder()
                        .setLabel("p-" + i)
                        .setValue(random.nextLong())
                        .setBlob(ByteString.copyFrom(randomBytes(32, seed)))
                        .build())
                .collect(Collectors.toList());
    }

    @Nonnull
    private static byte[] randomBytes(int size, long seed) {
        byte[] b = new byte[size];
        new Random(seed).nextBytes(b);
        return b;
    }
}
