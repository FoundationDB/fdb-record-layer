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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PendingWritesQueueTestProto.TestQueuePayload;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Randomized concurrency tests for {@link PendingWritesQueue}.
 *
 *  A storm of worker threads issues mixed enqueue / size-read / cursor-scan / drain
 *  operations. Enqueues are conflict-free by construction; size reads use a snapshot;
 *  cursor scans are read-only; drain workers clear a batch and record the cleared
 *  payloads into a shared result set. After the storm we drain whatever is left into the
 *  same result set, then verify that the union (drained-during-run + drained-after)
 *  equals the recorded enqueues as a multiset, that the leftover snapshot was ordered by
 *  {@code (incarnation, versionstamp)}, and that the size counter is zero once empty.s
 */
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
class PendingWritesQueueConcurrencyTest extends FDBRecordStoreTestBase {
    private static final int WORKER_COUNT = 8;
    private static final int ITERATIONS_PER_WORKER = 400;
    private static final int INCARNATION = 1;

    /**
     * mixed-operation workers, then drain & verify.
     */
    @ParameterizedTest
    @RandomSeedSource(0xCAFEBABEL)
    void testRandomizedConcurrentEnqueueAndScan(long seed) throws Exception {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 0);
            commit(context);
        }

        // Every enqueued payload is recorded here, in commit order. Each worker performs its
        // commit and its append to this list together under `commitLock`, so the list order
        // matches the FDB commit order (= versionstamp order = queue scan order). Drainers do
        // the same with `drained`, so the two lists line up entry-for-entry.
        ConcurrentLinkedQueue<TestQueuePayload> recorded = new ConcurrentLinkedQueue<>();
        // Payloads cleared by drain workers during the run land here, in clear order. The
        // post-test drain appends whatever is left, so this ends up equal to `recorded`.
        ConcurrentLinkedQueue<TestQueuePayload> drained = new ConcurrentLinkedQueue<>();
        // Serializes every committing transaction (enqueue and drain) with its corresponding
        // list append, so commit order and list order agree.
        final Object commitLock = new Object();

        try (ExecutorService executor = Executors.newFixedThreadPool(WORKER_COUNT)) {
            List<Future<?>> futures = new ArrayList<>(WORKER_COUNT);
            try {
                for (int worker = 0; worker < WORKER_COUNT; worker++) {
                    final int workerId = worker;
                    // Per-worker Random seeded deterministically from the global seed + workerId
                    futures.add(executor.submit(() -> {
                        workerLoop(queue, workerId, new Random(seed + workerId), recorded, drained, commitLock);
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
        }

        // Drain whatever the workers left behind, in a single consistent snapshot, recording
        // the cleared payloads into the same result set. The workers have stopped, so there is
        // no concurrency here: the leftover scan reflects one consistent ordering.
        try (FDBRecordContext context = openContext()) {
            List<PendingWritesQueueEntry<TestQueuePayload>> leftover =
                    queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
            for (PendingWritesQueueEntry<TestQueuePayload> entry : leftover) {
                queue.clearEntry(context, entry);
                drained.add(entry.getPayload());
            }
            commit(context);
        }

        // Every enqueued item was drained exactly once, and the queue is FIFO: items come out
        // in the exact order they were committed. Because both lists are appended in commit
        // order (under commitLock) and drains always take from the front, the drained sequence
        // must equal the recorded sequence element-for-element.
        assertEquals(new ArrayList<>(recorded), new ArrayList<>(drained),
                "items must be drained in the same order they were enqueued");

        // The queue is now empty and the size counter has settled at 0.
        try (FDBRecordContext context = openContext()) {
            assertTrue(queue.isQueueEmpty(context).join(),
                    "queue must be empty after draining everything");
            assertEquals(0L, queue.getQueueSizeNoConflict(context).join(),
                    "size counter must be 0 after draining everything");
        }
    }

    /**
     * Single worker's loop: pick a random operation, do it, repeat. Enqueues and drains run in
     * their own one-shot transactions. The commit and the corresponding list append happen
     * together under {@code commitLock} so that list order matches FDB commit order.
     */
    private void workerLoop(@Nonnull PendingWritesQueue<TestQueuePayload> queue,
                            int workerId,
                            @Nonnull Random random,
                            @Nonnull ConcurrentLinkedQueue<TestQueuePayload> recorded,
                            @Nonnull ConcurrentLinkedQueue<TestQueuePayload> drained,
                            @Nonnull Object commitLock) {
        for (int seq = 0; seq < ITERATIONS_PER_WORKER; seq++) {
            int op = random.nextInt(10);
            if (op < 7) {
                // 70% enqueue.
                TestQueuePayload payload = generatePayload(workerId, seq, random);
                try (FDBRecordContext context = openContext()) {
                    queue.enqueue(context, payload, INCARNATION).join();
                    // Commit and record under the lock so that the recorded order matches the
                    // commit order (= versionstamp order).
                    synchronized (commitLock) {
                        commit(context);
                        recorded.add(payload);
                    }
                }
            } else if (op < 8) {
                // 10% size read — must never go negative.
                try (FDBRecordContext context = openContext()) {
                    Long size = queue.getQueueSizeNoConflict(context).join();
                    if (size != null) {
                        assertTrue(size >= 0, "queue size must never go negative, saw " + size);
                    }
                }
            } else if (op < 9) {
                // 10% drain — clear a batch and record the cleared payloads.
                drainSomeIntoResult(queue, drained, commitLock);
            } else {
                // 10% cursor scan
                try (FDBRecordContext context = openContext()) {
                    List<PendingWritesQueueEntry<TestQueuePayload>> entries =
                            queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
                    Long size = queue.getQueueSizeNoConflict(context).join();
                    assertEquals(size == null ? 0 : size, entries.size());
                    commit(context);
                }
            }
        }
    }

    /**
     * Drain a small batch from the front of the queue and record the cleared payloads into
     * {@code drained}, but only after a successful commit.
     *
     * <p>The cursor forces snapshot isolation, so two drainers could observe the same entry. Since clearEntry will
     * conflict on two drains to the same entry, only one drainer can commit. The loser rolls back and
     * records nothing. This keeps draining exactly-once. Enqueues are not affected (different key)</p>
     *
     * <p>Like the enqueue path, the commit and the append to {@code drained} happen together
     * under {@code commitLock}. Because drains always take from the front (lowest versionstamps
     * first), this makes the drained sequence match the enqueue/commit order.</p>
     */
    private void drainSomeIntoResult(@Nonnull PendingWritesQueue<TestQueuePayload> queue,
                                     @Nonnull ConcurrentLinkedQueue<TestQueuePayload> drained,
                                     @Nonnull Object commitLock) {
        // Limit the batch so drains stay small and interleave with each other and with enqueues.
        ScanProperties limitedScan = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(5)
                .build()
                .asScanProperties(false);
        List<TestQueuePayload> clearedThisTx = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            List<PendingWritesQueueEntry<TestQueuePayload>> entries =
                    queue.getQueueCursor(context, limitedScan, null).asList().join();
            for (PendingWritesQueueEntry<TestQueuePayload> entry : entries) {
                // Since the clearEntry will conflict with another drainer, this may be rolled back
                queue.clearEntry(context, entry);
                clearedThisTx.add(entry.getPayload());
            }

            // Commit and record under the lock so that the drained order matches the commit
            // order. Only count these as drained once the commit actually succeeded.
            synchronized (commitLock) {
                commit(context);
                drained.addAll(clearedThisTx);
            }
        } catch (FDBExceptions.FDBStoreTransactionConflictException conflict) {
            // Another drainer committed against the queue first; the whole transaction
            // rolled back, so nothing here was cleared. Leave the entries for a later drain.
        }
    }

    @Nonnull
    private PendingWritesQueue<TestQueuePayload> getQueue(@Nonnull FDBRecordContext context, long maxQueueSize) {
        return new PendingWritesQueue<>(queueSubspaceFor(context), counterSubspaceFor(context), maxQueueSize,
                TestQueuePayload.class);
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
    private static TestQueuePayload generatePayload(int workerId, int seq, @Nonnull Random random) {
        // Vary label + value + blob so each payload is distinct. Random blob size so we hit a
        // range of FDB value lengths.
        int blobSize = 1 + random.nextInt(64);
        if (random.nextInt(20) < 1) {
            // at 5% make the size more than a split size
            blobSize = 200_000;
        }
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
