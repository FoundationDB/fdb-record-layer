/*
 * PendingWriteQueueSizeTest.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.lucene.LuceneDocumentFromRecord;
import com.apple.foundationdb.record.lucene.LuceneIndexExpressions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PendingWriteQueue} size limiting functionality.
 * These tests assert the functionality of the PendingWriteQueue limits with no other system components in play.
 */
@Tag(Tags.RequiresFDB)
class PendingWriteQueueSizeTest extends FDBRecordStoreTestBase {
    @Test
    void testQueueSizeUninitialized() {
        // Test that getQueueSize returns null when counter is uninitialized
        PendingWriteQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);

            // Before any operations, counter should be null (uninitialized)
            Long size = queue.getQueueSize(context).join();
            assertNull(size, "Uninitialized queue size should return null");

            // isEmpty should still work correctly
            assertTrue(queue.isQueueEmpty(context).join());

            commit(context);
        }
    }

    @Test
    void testQueueSizeIncrementOnEnqueue() {
        // Test that queue size counter increments when items are enqueued
        PendingWriteQueue queue;

        // Enqueue first item
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueueInsert(context, Tuple.from(1), createSingleField());
            commit(context);
        }

        // Counter should be initialized to 1 (ADD mutation on non-existent key treats original as 0)
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 1L);
        }

        // Enqueue more items
        try (FDBRecordContext context = openContext()) {
            queue.enqueueInsert(context, Tuple.from(2), createSingleField());
            queue.enqueueInsert(context, Tuple.from(3), createSingleField());
            commit(context);
        }

        // Counter should be 3
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 3L);
        }
    }

    @Test
    void testQueueSizeDecrementOnClear() {
        // Test that queue size counter decrements when items are cleared
        PendingWriteQueue queue;

        // Enqueue 3 items
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 100);
            queue.enqueueInsert(context, Tuple.from(1), createSingleField());
            queue.enqueueInsert(context, Tuple.from(2), createSingleField());
            queue.enqueueDelete(context, Tuple.from(2));
            commit(context);
        }

        // Get entries
        List<PendingWriteQueue.QueueEntry> entries;
        try (FDBRecordContext context = openContext()) {
            entries = queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
            assertEquals(3, entries.size());
        }

        // Clear one entry
        try (FDBRecordContext context = openContext()) {
            queue.clearEntry(context, entries.get(0));
            commit(context);
        }

        // Counter should be decremented
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 2L);
        }

        // Clear remaining entries
        try (FDBRecordContext context = openContext()) {
            queue.clearEntry(context, entries.get(1));
            queue.clearEntry(context, entries.get(2));
            commit(context);
        }

        // Counter should be 0
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 0L);
            assertTrue(queue.isQueueEmpty(context).join());
        }
    }

    @Test
    void testEnqueueExceedsMaxQueueSize() {
        // Test that enqueue fails when queue size exceeds limit
        PendingWriteQueue queue;
        final int maxSize = 5;

        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, maxSize);

            // Enqueue items up to the limit
            for (int i = 0; i < maxSize; i++) {
                queue.enqueueInsert(context, Tuple.from(i), createSingleField());
            }
            commit(context);
        }

        // Verify size is at limit
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, (long)maxSize);
        }

        // Attempt to enqueue one more item should fail
        try (FDBRecordContext context = openContext()) {
            PendingWriteQueue.PendingWritesQueueTooLargeException exception = assertThrows(
                    PendingWriteQueue.PendingWritesQueueTooLargeException.class,
                    () -> queue.enqueueInsert(context, Tuple.from(999), createSingleField())
            );
            assertNotNull(exception);
            assertTrue(exception.getMessage().contains("Queue size too large"));
        }
    }

    @Test
    void testUnlimitedQueueSize() {
        // Test that maxQueueSize = 0 means unlimited
        PendingWriteQueue queue;
        final int itemsToEnqueue = 11000;

        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, 0); // 0 = unlimited

            // Enqueue many items
            for (int i = 0; i < itemsToEnqueue; i++) {
                queue.enqueueInsert(context, Tuple.from(i), createSingleField());
            }
            commit(context);
        }

        // All items should be enqueued successfully
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, (long)itemsToEnqueue);

            List<PendingWriteQueue.QueueEntry> entries =
                    queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
            assertEquals(itemsToEnqueue, entries.size());
        }
    }

    @Test
    void testQueueSizeAcrossMultipleTransactions() {
        // Test that counter is maintained correctly across multiple transactions
        // This should verify that the mutation and reads are conflict-free
        PendingWriteQueue queue;

        // Transaction 1: enqueue 2 items
        try (FDBRecordContext context1 = openContext()) {
            queue = getQueue(context1, 100);

            queue.enqueueInsert(context1, Tuple.from(1), createSingleField());
            queue.enqueueInsert(context1, Tuple.from(2), createSingleField());

            // Transaction 2: enqueue 3 more items
            try (FDBRecordContext context2 = openContext()) {
                queue.enqueueInsert(context2, Tuple.from(3), createSingleField());
                queue.enqueueInsert(context2, Tuple.from(4), createSingleField());
                queue.enqueueInsert(context2, Tuple.from(5), createSingleField());

                commit(context1);
                commit(context2);
            }
        }
        // Counter should be cumulative
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 5L);
        }

        // Transaction 3,4: clear 1 item
        List<PendingWriteQueue.QueueEntry> entries;
        try (FDBRecordContext context = openContext()) {
            entries = queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join();
        }

        try (FDBRecordContext context3 = openContext()) {
            queue.clearEntry(context3, entries.get(0));

            try (FDBRecordContext context4 = openContext()) {
                queue.clearEntry(context4, entries.get(1));

                commit(context3);
                commit(context4);
            }
        }
        // Counter should be decremented
        try (FDBRecordContext context = openContext()) {
            assertQueueSize(queue, context, 3L);
        }
    }

    @Test
    void testExceedLimitInSingleTransaction() {
        // Test adding more than the max number of entries in a single transaction.
        PendingWriteQueue queue;
        final int maxSize = 5;

        // Transaction 1: Add MORE than maxSize items in a single transaction
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, maxSize);

            // Add 10 items, which exceeds the limit of 5
            // This works because the counter reads as null (uninitialized) for all enqueues in this transaction
            for (int i = 0; i < maxSize; i++) {
                queue.enqueueInsert(context, Tuple.from(i), createSingleField());
            }

            assertThrows(PendingWriteQueue.PendingWritesQueueTooLargeException.class,
                    () -> queue.enqueueInsert(context, Tuple.from(999), createSingleField()));
            // The index is now in an inconsistent state, do don't commit
        }
    }

    private PendingWriteQueue getQueue(FDBRecordContext context, int maxQueueSize) {
        Subspace queueSpace = path.toSubspace(context).subspace(Tuple.from("queue"));
        Subspace counterSpace = path.toSubspace(context).subspace(Tuple.from("counter"));
        return new PendingWriteQueue(queueSpace, counterSpace,
                PendingWriteQueue.DEFAULT_MAX_PENDING_ENTRIES_TO_REPLAY, maxQueueSize);
    }

    private List<LuceneDocumentFromRecord.DocumentField> createSingleField() {
        return List.of(new LuceneDocumentFromRecord.DocumentField(
                "testField", "testValue", LuceneIndexExpressions.DocumentFieldType.STRING,
                true, false, Collections.emptyMap()));
    }

    private static void assertQueueSize(final PendingWriteQueue queue, final FDBRecordContext context, final long expected) {
        Long size = queue.getQueueSize(context).join();
        assertNotNull(size);
        assertEquals(expected, size);
    }
}
