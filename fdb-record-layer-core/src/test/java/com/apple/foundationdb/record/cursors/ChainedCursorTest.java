/*
 * ChainedCursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ChainedCursor}.
 */
@Tag(Tags.RequiresFDB)
public class ChainedCursorTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    @Test
    public void testChainedCursor() {
        RecordCursorIterator<Long> cursor = newCursor(null).asIterator();

        long i = 0L;
        while (cursor.hasNext()) {
            assertEquals((Long) i, cursor.next());
            ++i;
        }
        assertEquals(25, i);
    }

    @Test
    public void testChainedCursorContinuation() {
        RecordCursorIterator<Long> cursor = newCursor(null).asIterator();

        long i = 0L;
        while (cursor.hasNext()) {
            assertEquals((Long) i, cursor.next());
            ++i;
            if ((i % 2) == 0) {
                final byte[] continuation = cursor.getContinuation();
                cursor = newCursor(continuation).asIterator();
            }
        }
        assertEquals(25, i);
    }

    @Test
    public void testObeysReturnedRowLimit() {
        limitBy(5, Integer.MAX_VALUE, RecordCursor.NoNextReason.RETURN_LIMIT_REACHED);
    }

    @Test
    public void testObeysScanLimit() {
        limitBy(Integer.MAX_VALUE, 5, RecordCursor.NoNextReason.SCAN_LIMIT_REACHED);
    }


    private void limitBy(int returnedRowLimit, int recordScanLimit, RecordCursor.NoNextReason noNextReason) {
        // Note that this test only requires a database and a context because the ChainedCursor API requires
        // a context to be passed in when you are applying scan limits.
        FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            ScanProperties props = new ScanProperties(ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(returnedRowLimit)
                    .setScannedRecordsLimit(recordScanLimit)
                    .setFailOnScanLimitReached(false)
                    .build());

            RecordCursorIterator<Long> cursor = new ChainedCursor<>(
                    context,
                    ChainedCursorTest::nextKey,
                    (key) -> Tuple.from(key).pack(),
                    (prevContinuation) -> Tuple.fromBytes(prevContinuation).getLong(0),
                    null,
                    props)
                    .asIterator();

            int count = 0;
            while (cursor.hasNext()) {
                assertEquals(Long.valueOf(count), cursor.next());
                ++count;
            }

            assertEquals(Math.min(returnedRowLimit, recordScanLimit), count);
            assertEquals(cursor.getNoNextReason(), noNextReason);
        }
    }

    @Test
    public void testObeysTimeLimit() {
        FDBDatabase database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            ScanProperties props = new ScanProperties(ExecuteProperties.newBuilder()
                    .setTimeLimit(4L)
                    .setFailOnScanLimitReached(false)
                    .build());

            RecordCursorIterator<Long> cursor = new ChainedCursor<>(
                    context,
                    (lastKey) -> nextKey(lastKey).thenApply(value -> {
                        sleep(1L);
                        return value;
                    }),
                    (key) -> Tuple.from(key).pack(),
                    (prevContinuation) -> Tuple.fromBytes(prevContinuation).getLong(0),
                    null,
                    props).asIterator();

            int count = 0;
            while (cursor.hasNext()) {
                assertEquals(Long.valueOf(count), cursor.next());
                ++count;
            }

            assertEquals(cursor.getNoNextReason(), RecordCursor.NoNextReason.TIME_LIMIT_REACHED);
            assertTrue(count < 5, "Too many values returned");
        }
    }

    @Test
    public void testHatesReverse() {
        // The chained cursor cannot implement a reverse scan
        assertThrows(RecordCoreArgumentException.class, () -> {
            FDBDatabase database = dbExtension.getDatabase();
            try (FDBRecordContext context = database.openContext()) {
                ScanProperties props = new ScanProperties(ExecuteProperties.newBuilder().build(), true);
                new ChainedCursor<>(context,
                        (lastKey) -> CompletableFuture.completedFuture(Optional.of(10L)),
                        (key) -> new byte[0],
                        (prevContinuation) -> 10L,
                        null,
                        props);
            }
        });
    }

    private RecordCursor<Long> newCursor(byte[] continuation) {
        return new ChainedCursor<>(
                (lastKey) -> nextKey(lastKey),
                (key) -> Tuple.from(key).pack(),
                (prevContinuation) -> Tuple.fromBytes(prevContinuation).getLong(0),
                continuation,
                null
        );
    }

    private static CompletableFuture<Optional<Long>> nextKey(Optional<Long> currentKey) {
        final Optional<Long> ret;
        if (currentKey.isPresent()) {
            if (currentKey.get() >= 24) {
                ret = Optional.empty();
            } else {
                ret = Optional.of(currentKey.get() + 1L);
            }
        } else {
            ret = Optional.of(0L);
        }

        return CompletableFuture.completedFuture(ret);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
