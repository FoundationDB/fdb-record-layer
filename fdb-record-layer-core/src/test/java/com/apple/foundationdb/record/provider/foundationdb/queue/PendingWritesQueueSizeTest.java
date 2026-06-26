/*
 * PendingWritesQueueSizeTest.java
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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Tests for {@link PendingWritesQueue}'s size counter and capacity enforcement.
 */
@Tag(Tags.RequiresFDB)
class PendingWritesQueueSizeTest extends FDBRecordStoreTestBase {

    /**
     * On a brand-new queue, the counter key has never been written, so {@code getQueueSize}
     * returns {@code null}. {@code isQueueEmptyAndFailOnConflict} still correctly returns
     * {@code true} because it goes to the queue keyspace.
     */
    @Test
    void testQueueSizeUninitialized() {
        try (FDBRecordContext context = openContext()) {
            PendingWritesQueue<TestQueuePayload> queue = getQueue(context, 100);
            Assertions.assertThat(queue.getQueueSizeNoConflict(context).join()).isNull();
            Assertions.assertThat(queue.ensureQueueEmpty(context).join()).isTrue();
            commit(context);
        }
    }

    /**
     * Each {@code enqueue} bumps the atomic counter; the counter persists across transactions.
     */
    @Test
    void testQueueSizeIncrementOnEnqueue() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueue(context, payload("a"), 0).join();
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 1L);
        }

        try (FDBRecordContext context = openContext()) {
            queue.enqueue(context, payload("b"), 0).join();
            queue.enqueue(context, payload("c"), 0).join();
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 3L);
        }
    }

    /**
     * Each {@code clearEntry} decrements the counter; once everything is cleared the counter
     * reads 0 (not null).
     */
    @Test
    void testQueueSizeDecrementOnClear() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueue(context, payload("a"), 0).join();
            queue.enqueue(context, payload("b"), 0).join();
            queue.enqueue(context, payload("c"), 0).join();
            commit(context);
        }

        List<PendingWritesQueueEntry<TestQueuePayload>> entries;
        try (FDBRecordContext context = openContext()) {
            entries = queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
            Assertions.assertThat(entries.size()).isEqualTo(3);
        }

        try (FDBRecordContext context = openContext()) {
            queue.clearEntry(context, entries.get(0));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 2L);
        }

        try (FDBRecordContext context = openContext()) {
            queue.clearEntry(context, entries.get(1));
            queue.clearEntry(context, entries.get(2));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 0L);
            Assertions.assertThat(queue.ensureQueueEmpty(context).join()).isTrue();
        }
    }

    /**
     * Once the queue counter has reached the cap, a subsequent {@code enqueue} fails with
     * {@link PendingWritesQueue.PendingWritesQueueTooLargeException}.
     */
    @Test
    void testEnqueueExceedsMaxQueueSize() {
        final int maxSize = 5;
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, maxSize);
            for (int i = 0; i < maxSize; i++) {
                queue.enqueue(context, payload("p-" + i), 0).join();
            }
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, (long) maxSize);
        }

        try (FDBRecordContext context = openContext()) {
            // The capacity check runs as part of enqueue's returned future, so the failure
            // surfaces when we .join() it.
            Assertions.assertThatThrownBy(() -> queue.enqueue(context, payload("overflow"), 0).join())
                    .hasCauseInstanceOf(PendingWritesQueue.PendingWritesQueueTooLargeException.class);
        }
    }

    /**
     * {@code maxQueueSize == 0} disables the cap; many thousands of entries enqueue fine.
     */
    @Test
    void testUnlimitedQueueSize() {
        final int itemsToEnqueue = 11_000;
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 0);
            for (int i = 0; i < itemsToEnqueue; i++) {
                queue.enqueue(context, payload("p-" + i), 0).join();
            }
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, (long) itemsToEnqueue);
            List<PendingWritesQueueEntry<TestQueuePayload>> entries =
                    queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
            Assertions.assertThat(entries.size()).isEqualTo(itemsToEnqueue);
        }
    }

    /**
     * The counter is cumulative across separate transactions; the counter mutation is
     * atomic so neither side conflicts with the other.
     */
    @Test
    void testQueueSizeAcrossMultipleTransactions() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            commit(context);
        }

        try (FDBRecordContext ctx1 = openContext();
                FDBRecordContext ctx2 = openContext()) {
            queue.enqueue(ctx1, payload("1"), 0).join();
            queue.enqueue(ctx1, payload("2"), 0).join();
            queue.enqueue(ctx2, payload("3"), 0).join();
            queue.enqueue(ctx2, payload("4"), 0).join();
            queue.enqueue(ctx2, payload("5"), 0).join();
            commit(ctx1);
            commit(ctx2);
        }
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 5L);
        }
    }

    @Nonnull
    private PendingWritesQueue<TestQueuePayload> getQueue(@Nonnull FDBRecordContext context, long maxQueueSize) {
        Subspace queueSubspace = path.toSubspace(context).subspace(Tuple.from("queue"));
        Subspace counterSubspace = path.toSubspace(context).subspace(Tuple.from("counter"));
        return new PendingWritesQueue<>(queueSubspace, counterSubspace, maxQueueSize,
                TestQueuePayload.class);
    }

    private static void assertQueueSize(@Nonnull PendingWritesQueue<TestQueuePayload> queue,
                                        @Nonnull FDBRecordContext context,
                                        long expected) {
        Long size = queue.getQueueSizeNoConflict(context).join();
        Assertions.assertThat(size).isNotNull();
        Assertions.assertThat((long) size).isEqualTo(expected);
        // The exact-count from the cursor should match the counter.
        List<PendingWritesQueueEntry<TestQueuePayload>> entries =
                queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
        Assertions.assertThat((long) entries.size()).isEqualTo((long) size);
    }

    @Nonnull
    private static TestQueuePayload payload(@Nonnull String label) {
        return TestQueuePayload.newBuilder().setLabel(label).build();
    }
}
