/*
 * ThrottledIteratorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.cursors.throttled;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

final class ThrottledIteratorTest extends FDBRecordStoreTestBase {

    @ParameterizedTest
    @CsvSource({
            "1000,100,0,0",     // less than max
            "2000,100,180,0",
            "100,10,1,0",
            "105,10,1,0",

            "1000,100,100,0",    // just right
            "100,10,1,0",

            "1000,100,200,1000", // delay required - the more interesting cases...
            "2000,100,210,100",
            "250,100,100,750",
            "250,50,100,1750",  // 100 events should take two seconds, wait what it takes to get there
            "1,50,100,1999",
            "1999,50,100,1",
            "10,10,1,90",       // 10 events per second, require 100ms per one event

            "500,100,49,0",     // consecutive
            "500,100,50,0",
            "500,100,51,10",

    })
    void testThrottledIteratorGetDelay(long rangeProcessingTimeMillis, int maxPerSec, int eventsCount, long expectedResult) {
        long ret = ThrottledRetryingIterator.throttlePerSecGetDelayMillis(rangeProcessingTimeMillis, maxPerSec, eventsCount);
        assertThat(ret).isEqualTo(expectedResult);
    }

    @CsvSource({"-1, -1", "0, 0", "-1, 0", "0,-1", "100, -1", "100, 0", "100, 1", "-1, 1", "0, 1", "100, 3", "100, 100"})
    @ParameterizedTest
    void testThrottleIteratorTestSimpleRowLimit(int maxRowsLimit, int initialRowLimit) throws Exception {
        // Iterate range, verify that the number of items scanned matches the number of records
        final int numRecords = 42; // mostly harmless
        AtomicInteger iteratedCount = new AtomicInteger(0); // total number of scanned items
        AtomicInteger deletedCount = new AtomicInteger(0); // total number of "deleted" items
        AtomicInteger successRangeCount = new AtomicInteger(0); // number of invocations of RangeSuccess callback
        AtomicInteger limitRef = new AtomicInteger(-1);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator = ThrottledRetryingIterator
                    .builder(fdb.newRunner(),
                            intCursor(numRecords, limitRef),
                            (store, item, quotaManager) -> {
                                quotaManager.deleteCountAdd(1);
                                return AsyncUtil.DONE;
                            })
                    .withRangeSuccessNotification(quotaManager -> {
                        successRangeCount.incrementAndGet();
                        iteratedCount.addAndGet(quotaManager.getScannedCount());
                        deletedCount.addAndGet(quotaManager.getDeletesCount());
                    });
            if (maxRowsLimit != -1) {
                throttledIterator.withMaxRecordsScannedPerTransaction(maxRowsLimit);
            }
            if (initialRowLimit != -1) {
                throttledIterator.withInitialRecordsScannedPerTransaction(initialRowLimit);
            }
            throttledIterator.build().iterateAll(recordStore).join();
        }
        assertThat(iteratedCount.get()).isEqualTo(numRecords);
        assertThat(deletedCount.get()).isEqualTo(numRecords);
        if (limitRef.get() == 0) {
            assertThat(successRangeCount.get()).isOne();
        }
        if (maxRowsLimit <= 0) {
            assertThat(limitRef.get()).isZero();
        } else {
            assertThat(limitRef.get()).isGreaterThanOrEqualTo(initialRowLimit).isLessThanOrEqualTo(maxRowsLimit);
        }
    }

    @CsvSource({"-1", "0", "50", "100"})
    @ParameterizedTest
    void testThrottleIteratorTestSimpleSecondsLimit(int maxPerSecLimit) throws Exception {
        // Iterate range, verify that the number of items scanned matches the number of records
        final int numRecords = 50;
        AtomicInteger iteratedCount = new AtomicInteger(0); // total number of scanned items
        AtomicInteger deletedCount = new AtomicInteger(0); // total number of "deleted" items
        AtomicInteger successRangeCount = new AtomicInteger(0); // number of invocations of RangeSuccess callback
        AtomicInteger limitRef = new AtomicInteger(-1);

        long startTimeMillis = System.currentTimeMillis();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator = ThrottledRetryingIterator
                    .builder(fdb.newRunner(),
                            intCursor(numRecords, limitRef),
                            (store, item, quotaManager) -> {
                                quotaManager.deleteCountAdd(1);
                                return AsyncUtil.DONE;
                            })
                    .withRangeSuccessNotification(quotaManager -> {
                        successRangeCount.incrementAndGet();
                        iteratedCount.addAndGet(quotaManager.getScannedCount());
                        deletedCount.addAndGet(quotaManager.getDeletesCount());
                    })
                    .withMaxRecordsScannedPerTransaction(10);
            if (maxPerSecLimit != -1) {
                throttledIterator.withMaxRecordsScannedPerSec(maxPerSecLimit);
            }
            throttledIterator.build().iterateAll(recordStore).join();
        }
        long totalTimeMillis = System.currentTimeMillis() - startTimeMillis;
        assertThat(iteratedCount.get()).isEqualTo(numRecords);
        assertThat(deletedCount.get()).isEqualTo(numRecords);
        if (maxPerSecLimit > 0) {
            assertThat(totalTimeMillis).isGreaterThan(TimeUnit.SECONDS.toMillis(50 / maxPerSecLimit));
        }
    }

    @CsvSource({"-1, -1", "0, 0", "-1, 0", "0,-1", "100, -1", "100, 0", "100, 1", "-1, 1", "0, 1", "100, 3", "100, 100"})
    @ParameterizedTest
    void testThrottleIteratorWithFailuresRowLimit(int maxRowsLimit, int initialRowLimit) throws Exception {
        final int numRecords = 43;
        AtomicInteger totalScanned = new AtomicInteger(0); // number of items scanned
        AtomicInteger failCount = new AtomicInteger(0); // number of exception thrown
        AtomicInteger transactionStartCount = new AtomicInteger(0); // number of invocations of RangeInit callback
        AtomicInteger transactionCommitCount = new AtomicInteger(0); // number of invocations of RangeSuccess callback
        AtomicInteger lastFailedItem = new AtomicInteger(0); // last item that triggered a failure

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator = ThrottledRetryingIterator
                    .builder(fdb.newRunner(),
                            intCursor(numRecords, null),
                            (store, item, quotaManager) -> CompletableFuture.supplyAsync(() -> {
                                // fail 5 times
                                if (failCount.get() < 5) {
                                    int itemNumber = item.get();
                                    // fail every other item starting at item 3
                                    if ((itemNumber > 2) && (itemNumber >= lastFailedItem.get() + 2)) {
                                        failCount.incrementAndGet();
                                        lastFailedItem.set(itemNumber);
                                        throw new RuntimeException("intentionally failed while testing item " + item.get());
                                    }
                                }
                                return null;
                            }))
                    .withRangeInitNotification(quotaManager -> {
                        transactionStartCount.incrementAndGet();
                    })
                    .withRangeSuccessNotification(quotaManager -> {
                        transactionCommitCount.incrementAndGet();
                        totalScanned.addAndGet(quotaManager.getScannedCount());
                    });
            if (maxRowsLimit != -1) {
                throttledIterator.withMaxRecordsScannedPerTransaction(maxRowsLimit);
            }
            if (initialRowLimit != -1) {
                throttledIterator.withInitialRecordsScannedPerTransaction(initialRowLimit);
            }

            throttledIterator.build().iterateAll(recordStore).join();
        }
        assertThat(totalScanned.get()).isEqualTo(numRecords); // this is the count of the last invocation of rangeSuccess
        assertThat(failCount.get()).isEqualTo(5);
        assertThat(transactionStartCount.get()).isEqualTo(transactionCommitCount.get() + failCount.get()); //
    }

    // TODO: transaction max time
    // TODO: initial scan when max scan is 0

    @CsvSource({"-1", "0", "50", "100"})
    @ParameterizedTest
    void testThrottleIteratorWithFailuresSecondsLimit(int maxPerSecLimit) throws Exception {
        final int numRecords = 43;
        AtomicInteger totalScanned = new AtomicInteger(0); // number of items scanned
        AtomicInteger failCount = new AtomicInteger(0); // number of exception thrown
        AtomicInteger transactionStartCount = new AtomicInteger(0); // number of invocations of RangeInit callback
        AtomicInteger transactionCommitCount = new AtomicInteger(0); // number of invocations of RangeSuccess callback
        AtomicInteger lastFailedItem = new AtomicInteger(0); // last item that triggered a failure

        long startTime = System.currentTimeMillis();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator = ThrottledRetryingIterator
                    .builder(fdb.newRunner(),
                            intCursor(numRecords, null),
                            (store, item, quotaManager) -> CompletableFuture.supplyAsync(() -> {
                                // fail 5 times
                                if (failCount.get() < 5) {
                                    int itemNumber = item.get();
                                    // fail every other item starting at item 3
                                    if ((itemNumber > 2) && (itemNumber >= lastFailedItem.get() + 2)) {
                                        failCount.incrementAndGet();
                                        lastFailedItem.set(itemNumber);
                                        throw new RuntimeException("intentionally failed while testing item " + item.get());
                                    }
                                }
                                return null;
                            }))
                    .withRangeInitNotification(quotaManager -> {
                        transactionStartCount.incrementAndGet();
                    })
                    .withRangeSuccessNotification(quotaManager -> {
                        transactionCommitCount.incrementAndGet();
                        totalScanned.addAndGet(quotaManager.getScannedCount());
                    })
                    .withMaxRecordsScannedPerTransaction(10);
            if (maxPerSecLimit != -1) {
                throttledIterator.withMaxRecordsScannedPerSec(maxPerSecLimit);
            }

            throttledIterator.build().iterateAll(recordStore).join();
        }
        long totalTimeMillis = System.currentTimeMillis() - startTime;

        assertThat(totalScanned.get()).isEqualTo(numRecords); // this is the count of the last invocation of rangeSuccess
        assertThat(failCount.get()).isEqualTo(5);
        assertThat(transactionStartCount.get()).isEqualTo(transactionCommitCount.get() + failCount.get()); //
        if (maxPerSecLimit > 0) {
            assertThat(totalTimeMillis).isGreaterThan(TimeUnit.SECONDS.toMillis(50 / maxPerSecLimit));
        }
    }

    @Test
    void testConstantFailures() throws Exception {
        final String failureMessage = "intentionally failed while testing";
        try (FDBRecordContext context = openContext()) {
            AtomicInteger transactionStart = new AtomicInteger(0);
            AtomicBoolean success = new AtomicBoolean(false);
            openSimpleRecordStore(context);
            ThrottledRetryingIterator<Integer> throttledIterator = ThrottledRetryingIterator
                    .builder(fdb.newRunner(),
                            intCursor(500, null),
                            (store, item, quotaManager) -> futureFailure())
                    .withMaxRecordsScannedPerTransaction(10)
                    .withMaxRecordsScannedPerSec(1)
                    .withRangeInitNotification(quotaManager -> {
                        transactionStart.incrementAndGet();
                    })
                    .withRangeSuccessNotification(quotaManager -> {
                        success.set(true);
                    })
                    .build();

            Throwable ex = Assertions.catchThrowableOfType(RuntimeException.class, () -> throttledIterator.iterateAll(recordStore).join());
            assertThat(ex.getMessage()).contains(failureMessage);
            assertThat(transactionStart.get()).isEqualTo(101);
            assertThat(success.get()).isFalse();
            // TODO: control the number of retries?
        }

    }

    @Test
    void testLimitHandlingOnFailure() throws Exception {
        final String failureMessage = "intentionally failed while testing";
        final AtomicInteger limitRef = new AtomicInteger(0);
        final AtomicInteger failCount = new AtomicInteger(0);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator<Integer> throttledIterator = ThrottledRetryingIterator
                    .builder(fdb.newRunner(),
                            intCursor(999, limitRef),
                            (store, item, quotaManager) -> {
                                int limit = limitRef.get();
                                int scannedCount = quotaManager.getScannedCount();
                                switch (failCount.get()) {
                                    case 0:
                                        assertThat(limit).isEqualTo(200);
                                        if (scannedCount == 100) {
                                            failCount.incrementAndGet();
                                            return futureFailure();
                                        }
                                        return AsyncUtil.DONE;
                                    case 1:
                                        assertThat(limit).isEqualTo(90);
                                        if (scannedCount == 50) {
                                            failCount.incrementAndGet();
                                            return futureFailure();
                                        }
                                        return AsyncUtil.DONE;
                                    case 2:
                                        assertThat(limit).isEqualTo(45);
                                        // from now on: fail at first item
                                        break;
                                    default:
                                        assertThat(failCount.get()).isLessThanOrEqualTo(100);
                                        break;
                                }
                                failCount.incrementAndGet();
                                return futureFailure();
                            })
                    .withMaxRecordsScannedPerTransaction(200)
                    .withInitialRecordsScannedPerTransaction(200)
                    .withMaxRecordsScannedPerSec(1000) // todo test to see this actually work
                    .build();

            Throwable ex = Assertions.catchThrowableOfType(RuntimeException.class, () -> throttledIterator.iterateAll(recordStore).join());
            assertThat(ex.getMessage()).contains(failureMessage);
            assertThat(limitRef.get()).isOne();
        }
    }

    @Test
    void testLimitHandlingOnSuccess() throws Exception {
        final AtomicInteger limitRef = new AtomicInteger(0);
        final AtomicInteger fullCount = new AtomicInteger(0);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator<Integer> throttledIterator = ThrottledRetryingIterator
                    .builder(fdb.newRunner(),
                            intCursor(2000, limitRef),
                            (store, item, quotaManager) -> {
                                int limit = limitRef.get();
                                int scannedCount = quotaManager.getScannedCount();
                                int count = fullCount.incrementAndGet();
                                System.out.println(":: jezra :: " + count + " scanned: " + scannedCount + " limit: " + limit);
                                if (count <= 400) {         // 10 * 40 (limit * successes) before change
                                    assertThat(limit).isEqualTo(10);
                                } else if (count <= 880) {  // 400 + (12 * 40)
                                    assertThat(limit).isEqualTo(12);
                                } else if (count <= 1480) { // 880 + (15 * 40)
                                    assertThat(limit).isEqualTo(15);
                                } else {
                                    // end all iterations
                                    quotaManager.markExhausted();
                                }
                                return AsyncUtil.DONE;
                            })
                    .withMaxRecordsScannedPerTransaction(100)
                    .withInitialRecordsScannedPerTransaction(10)
                    .withMaxRecordsScannedPerSec(100000)
                    .build();
            throttledIterator.iterateAll(recordStore).join();
        }
    }

    private CursorFactory<Integer> intCursor(int numInts, AtomicInteger limitRef) {
        return listCursor(IntStream.range(0, numInts).boxed().collect(Collectors.toList()), limitRef);
    }

    private <T> CursorFactory<T> listCursor(List<T> items, AtomicInteger limitRef) {
        return (store, cont, limit) -> {
            if (limitRef != null) {
                limitRef.set(limit);
            }
            final byte[] continuation = cont == null ? null : cont.getContinuation().toBytes();
            return RecordCursor.fromList(items, continuation).limitRowsTo(limit);
        };
    }

    private CompletableFuture<Void> futureFailure() {
        return CompletableFuture.failedFuture(new RuntimeException("intentionally failed while testing"));
    }
}
