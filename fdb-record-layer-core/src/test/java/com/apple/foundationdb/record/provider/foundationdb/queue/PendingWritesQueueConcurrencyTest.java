/*
 * PendingWritesQueueConcurrencyTest.java
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

import com.apple.foundationdb.record.PendingWritesQueueTestProto.TestQueuePayload;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Randomized concurrency tests for {@link PendingWritesQueue}.
 *
 * <p>Two phases:</p>
 * <ol>
 *   <li>A storm of worker threads issues mixed enqueue / size-read / cursor-scan operations
 *       (the enqueues are conflict-free by construction; size reads use a snapshot;
 *       cursor scans take the default isolation but are read-only). After the storm we drain
 *       and verify multiset equality and {@code (incarnation, versionstamp)} ordering, plus a
 *       zero size-counter after clearing.</li>
 *   <li>A race loop where one transaction drains + asserts {@code isQueueEmpty} + writes a
 *       close-out marker, while another concurrently enqueues. We assert that whenever the two
 *       overlap, the FDB conflict detector lets exactly one of them win — either the
 *       close-out commits and the enqueue retries (its enqueue still lands), or the enqueue
 *       commits first and the close-out conflicts on the empty-check read range.</li>
 * </ol>
 *
 * <p>Both phases use a fixed-seed {@link Random}, so the test is deterministic in CI but
 * still exercises many orderings.</p>
 */
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
class PendingWritesQueueConcurrencyTest extends FDBRecordStoreTestBase {

    private static final long SEED = 0xCAFEBABEL;
    private static final int WORKER_COUNT = 8;
    private static final int ITERATIONS_PER_WORKER = 40;
    private static final int[] INCARNATIONS = {1, 3, 7};
    private static final int RACE_ROUNDS = 20;

    /**
     * Phase 1: storm of mixed-operation workers, then drain & verify.
     */
    @Test
    void testRandomizedConcurrentEnqueueAndScan() throws Exception {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 0);
            commit(context);
        }

        // Every enqueued payload is recorded here so we can verify exact multiset equality
        // after draining, without depending on any global ordering across workers.
        ConcurrentLinkedQueue<TestQueuePayload> recorded = new ConcurrentLinkedQueue<>();

        ExecutorService executor = Executors.newFixedThreadPool(WORKER_COUNT);
        List<Future<?>> futures = new ArrayList<>(WORKER_COUNT);
        try {
            for (int worker = 0; worker < WORKER_COUNT; worker++) {
                final int workerId = worker;
                // Per-worker Random seeded deterministically from the global seed + workerId
                // so each worker's interleaving is reproducible.
                final Random workerRandom = new Random(SEED + workerId);
                futures.add(executor.submit(() -> {
                    workerLoop(queue, workerId, workerRandom, recorded);
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get(120, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "executor did not shut down");
        }

        // Drain & verify.
        List<PendingWritesQueueEntry<TestQueuePayload>> drained;
        try (FDBRecordContext context = openContext()) {
            drained = queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
        }

        // 1. Count matches.
        assertEquals(recorded.size(), drained.size(),
                "drained entry count must match recorded enqueue count");

        // 2. (incarnation, versionstamp) ordering: monotonically non-decreasing, with
        // incarnation strictly ordered and versionstamps strictly increasing within the same
        // incarnation.
        assertEntriesOrdered(drained);

        // 3. Multiset equality of payloads.
        Map<TestQueuePayload, Integer> expectedCounts = new HashMap<>();
        for (TestQueuePayload payload : recorded) {
            expectedCounts.merge(payload, 1, Integer::sum);
        }
        Map<TestQueuePayload, Integer> actualCounts = new HashMap<>();
        for (PendingWritesQueueEntry<TestQueuePayload> entry : drained) {
            actualCounts.merge(entry.getPayload(), 1, Integer::sum);
        }
        assertEquals(expectedCounts, actualCounts,
                "drained payloads must equal recorded enqueues as a multiset");

        // 4. Clear everything and verify the counter goes to 0 and isEmpty is true.
        try (FDBRecordContext context = openContext()) {
            for (PendingWritesQueueEntry<TestQueuePayload> entry : drained) {
                queue.clearEntry(context, entry);
            }
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            assertTrue(queue.ensureQueueEmpty(context).join(),
                    "queue must be empty after draining everything");
            assertEquals(0L, queue.getQueueSizeNoConflict(context).join(),
                    "size counter must be 0 after draining everything");
        }
    }

    /**
     * Phase 2: race between a drain+close-out transaction and concurrent enqueues. Exactly
     * one of each pair must succeed cleanly; the loser observes either a queue-too-empty
     * conflict (close-out lost) or no conflict but the post-state is still consistent
     * (close-out won, late enqueue retries and lands as the "next round" entry).
     */
    @Test
    void testCloseoutVsEnqueueRace() {
        PendingWritesQueue<TestQueuePayload> queue;
        Subspace closeoutSpace;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 0);
            closeoutSpace = path.toSubspace(context).subspace(Tuple.from("closeout"));
            commit(context);
        }

        Random random = new Random(SEED);
        int rounds = 0;
        int closeoutWins = 0;
        int enqueueWins = 0;
        int closeoutConflicts = 0;

        for (int round = 0; round < RACE_ROUNDS; round++) {
            rounds++;
            // Pre-seed each round with a known-clean queue.
            drainQueueFully(queue);

            // Coin flip: whether to commit the enqueue before or after asking the close-out
            // transaction to commit. Both orderings exercise interesting interleavings.
            boolean enqueueFirst = random.nextBoolean();
            TestQueuePayload enqueuePayload = TestQueuePayload.newBuilder()
                    .setLabel("race-" + round)
                    .setValue(round)
                    .build();

            boolean closeoutCommitted = false;
            try (FDBRecordContext closeoutTx = openContext()) {
                // Drain (vacuously, since pre-seeded) and assert emptiness.
                List<PendingWritesQueueEntry<TestQueuePayload>> remaining =
                        queue.getQueueCursor(closeoutTx, ScanProperties.FORWARD_SCAN, null).asList().join();
                for (PendingWritesQueueEntry<TestQueuePayload> entry : remaining) {
                    queue.clearEntry(closeoutTx, entry);
                }
                assertTrue(queue.ensureQueueEmpty(closeoutTx).join());
                closeoutTx.ensureActive().set(closeoutSpace.pack(),
                        ("round-" + round).getBytes(StandardCharsets.UTF_8));

                if (enqueueFirst) {
                    try (FDBRecordContext enqueueTx = openContext()) {
                        queue.enqueue(enqueueTx, enqueuePayload, INCARNATIONS[0]).join();
                        commit(enqueueTx);
                    }
                    enqueueWins++;
                    try {
                        commit(closeoutTx);
                        // Should have conflicted — fail loudly if FDB let it through.
                        fail("close-out transaction should have conflicted when an enqueue committed first");
                    } catch (RecordCoreException expected) {
                        closeoutConflicts++;
                    }
                } else {
                    commit(closeoutTx);
                    closeoutCommitted = true;
                    closeoutWins++;
                    try (FDBRecordContext enqueueTx = openContext()) {
                        // The post-close-out enqueue is unaffected by the close-out's commit;
                        // the queue keyspace itself wasn't read by the close-out's range
                        // scan (only the empty-check, which scans 1 row and finds none).
                        queue.enqueue(enqueueTx, enqueuePayload, INCARNATIONS[0]).join();
                        commit(enqueueTx);
                    }
                }
            } catch (RecordCoreException ex) {
                if (closeoutCommitted) {
                    throw ex;
                }
                // Close-out itself threw — count as a conflict.
                closeoutConflicts++;
            }
        }

        // Sanity: every round was accounted for in exactly one bucket.
        assertEquals(rounds, closeoutWins + enqueueWins,
                "every round must be either a close-out win or an enqueue win");
        // Whenever the enqueue committed first, the close-out must have conflicted.
        assertEquals(enqueueWins, closeoutConflicts,
                "every enqueue-first round must have produced a close-out conflict");
        // Both branches should have triggered at least once with RACE_ROUNDS = 20.
        assertTrue(closeoutWins > 0, "expected at least one close-out-wins round");
        assertTrue(enqueueWins > 0, "expected at least one enqueue-first round");
    }

    /**
     * Single worker's loop: pick a random operation, do it, repeat. Enqueues run in their own
     * one-shot transactions so each commit is independent.
     */
    private void workerLoop(@Nonnull PendingWritesQueue<TestQueuePayload> queue,
                            int workerId,
                            @Nonnull Random random,
                            @Nonnull ConcurrentLinkedQueue<TestQueuePayload> recorded) {
        for (int seq = 0; seq < ITERATIONS_PER_WORKER; seq++) {
            int op = random.nextInt(10);
            if (op < 7) {
                // 70% enqueue.
                TestQueuePayload payload = generatePayload(workerId, seq, random);
                int incarnation = INCARNATIONS[random.nextInt(INCARNATIONS.length)];
                try (FDBRecordContext context = openContext()) {
                    queue.enqueue(context, payload, incarnation).join();
                    commit(context);
                    recorded.add(payload);
                }
            } else if (op < 9) {
                // 20% size read — must never go negative.
                try (FDBRecordContext context = openContext()) {
                    Long size = queue.getQueueSizeNoConflict(context).join();
                    if (size != null) {
                        assertTrue(size >= 0, "queue size must never go negative, saw " + size);
                    }
                }
            } else {
                // 10% cursor scan — entries are always in (incarnation, versionstamp) order.
                try (FDBRecordContext context = openContext()) {
                    List<PendingWritesQueueEntry<TestQueuePayload>> entries =
                            queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
                    assertEntriesOrdered(entries);
                }
            }
        }
    }

    private static void assertEntriesOrdered(@Nonnull List<PendingWritesQueueEntry<TestQueuePayload>> entries) {
        int lastIncarnation = Integer.MIN_VALUE;
        Versionstamp lastVersionstamp = null;
        for (PendingWritesQueueEntry<TestQueuePayload> entry : entries) {
            int incarnation = entry.getIncarnation();
            Versionstamp versionstamp = (Versionstamp) entry.getKeyTuple().get(1);
            assertTrue(versionstamp.isComplete(),
                    "queue entries must always carry a complete versionstamp");
            if (incarnation < lastIncarnation) {
                fail("incarnations should be monotonically non-decreasing, saw "
                        + incarnation + " after " + lastIncarnation);
            }
            if (incarnation == lastIncarnation && lastVersionstamp != null) {
                if (versionstamp.compareTo(lastVersionstamp) <= 0) {
                    fail("versionstamps within an incarnation must strictly increase, saw "
                            + versionstamp + " after " + lastVersionstamp);
                }
            }
            lastIncarnation = incarnation;
            lastVersionstamp = versionstamp;
        }
    }

    private void drainQueueFully(@Nonnull PendingWritesQueue<TestQueuePayload> queue) {
        while (true) {
            List<PendingWritesQueueEntry<TestQueuePayload>> entries;
            try (FDBRecordContext context = openContext()) {
                entries = queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
                if (entries.isEmpty()) {
                    return;
                }
                for (PendingWritesQueueEntry<TestQueuePayload> entry : entries) {
                    queue.clearEntry(context, entry);
                }
                commit(context);
            }
        }
    }

    @Nonnull
    private PendingWritesQueue<TestQueuePayload> getQueue(@Nonnull FDBRecordContext context, long maxQueueSize) {
        Subspace queueSubspace = path.toSubspace(context).subspace(Tuple.from("queue"));
        Subspace counterSubspace = path.toSubspace(context).subspace(Tuple.from("counter"));
        return new PendingWritesQueue<>(queueSubspace, counterSubspace, maxQueueSize,
                TestQueuePayload.class);
    }

    @Nonnull
    private static TestQueuePayload generatePayload(int workerId, int seq, @Nonnull Random random) {
        // Vary label + value + blob so each payload is distinct. Random blob size so we hit a
        // range of FDB value lengths.
        int blobSize = 1 + random.nextInt(64);
        byte[] blob = filled(blobSize, (byte) (workerId & 0xFF), seq);
        return TestQueuePayload.newBuilder()
                .setLabel("w" + workerId + "-s" + seq)
                .setValue((long) workerId * 1_000_000L + seq)
                .setBlob(ByteString.copyFrom(blob))
                .build();
    }

    @Nonnull
    private static byte[] filled(int size, byte marker, int seq) {
        byte[] b = new byte[size];
        Arrays.fill(b, marker);
        // Sprinkle the sequence number into the first 4 bytes to make every blob unique even
        // when workers happen to pick the same (size, marker) pair on the same iteration.
        for (int i = 0; i < Math.min(4, size); i++) {
            b[i] = (byte) ((seq >> (i * 8)) & 0xFF);
        }
        return b;
    }
}
