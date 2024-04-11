/*
 * FDBStoreTimerTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.StoreTimerSnapshot;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FDBStoreTimer}.
 */
@Tag(Tags.RequiresFDB)
@Execution(ExecutionMode.CONCURRENT)
public class FDBStoreTimerTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    FDBDatabase fdb;
    KeySpacePath path;
    FDBRecordContext context;
    private Subspace subspace;

    enum DummySizeEvents implements StoreTimer.SizeEvent {
        SIZE_EVENT_1,
        SIZE_EVENT_2
        ;

        @Override
        public String title() {
            return null;
        }
    }

    @BeforeEach
    void setup() throws Exception {
        fdb = dbExtension.getDatabase();
        path = pathManager.createPath(TestKeySpace.RAW_DATA);
        FDBStoreTimer timer = new FDBStoreTimer();
        context = fdb.openContext(null, timer);
        setupBaseData();
    }

    @AfterEach
    void tearDown() {
        context.close();
        fdb.close();
    }

    @Test
    void counterDifferenceTest() {
        RecordCursor<KeyValue> kvc = KeyValueCursor.Builder.withSubspace(subspace).setContext(context).setScanProperties(ScanProperties.FORWARD_SCAN).setRange(TupleRange.ALL).build();

        // see the timer counts from some onNext calls
        FDBStoreTimer latestTimer = context.getTimer();
        StoreTimerSnapshot savedTimer;
        StoreTimer diffTimer;

        // get a snapshot from latestTimer before advancing cursor
        savedTimer = StoreTimerSnapshot.from(latestTimer);

        // advance the cursor once
        kvc.onNext().join();

        // the diff from latestTimer minus savedTimer will have the timer cost from the single cursor advance
        diffTimer = StoreTimer.getDifference(latestTimer, savedTimer);
        Map<String, Number> diffKVs;
        diffKVs = diffTimer.getKeysAndValues();
        assertThat(diffKVs, hasKey("load_scan_entry_count"));
        assertEquals(1, diffKVs.get("load_scan_entry_count").intValue());
        assertThat(diffKVs, hasKey("load_key_value_count"));
        assertEquals(1, diffKVs.get("load_key_value_count").intValue());

        // get a snapshot from latestTimer after the single cursor advance
        savedTimer = StoreTimerSnapshot.from(latestTimer);

        // advance the cursor more times
        final int numAdvances = 5;
        for (int i = 0; i < numAdvances; i++) {
            kvc.onNext().join();
        }

        // the diff from latestTimer and savedTimer will have the timer cost from the subsequent cursor advances
        diffTimer = StoreTimer.getDifference(latestTimer, savedTimer);
        diffKVs = diffTimer.getKeysAndValues();
        assertThat(diffKVs, hasKey("load_scan_entry_count"));
        assertEquals(numAdvances, diffKVs.get("load_scan_entry_count").intValue());
        assertThat(diffKVs, hasKey("load_key_value_count"));
        assertEquals(numAdvances, diffKVs.get("load_key_value_count").intValue(), numAdvances);
    }

    @Test
    void timeoutCounterDifferenceTest() {
        RecordCursor<KeyValue> kvc = KeyValueCursor.Builder.withSubspace(subspace).setContext(context).setScanProperties(ScanProperties.FORWARD_SCAN).setRange(TupleRange.ALL).build();

        FDBStoreTimer latestTimer = context.getTimer();
        CompletableFuture<RecordCursorResult<KeyValue>> fkvr;
        RecordCursorResult<KeyValue> kvr;

        // record timeout
        latestTimer.recordTimeout(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, System.nanoTime() - 5000);

        // the latest timer should have recorded the one timeout event
        Map<String, Number> diffKVs;
        diffKVs = latestTimer.getKeysAndValues();
        assertEquals(1, diffKVs.get("wait_advance_cursor_timeout_count").intValue());
        assertTrue(diffKVs.get("wait_advance_cursor_timeout_micros").intValue() > 0);
        assertThat(diffKVs.get("wait_advance_cursor_timeout_micros").intValue(), greaterThan(0));

        // advance the cursor without timing out
        latestTimer.record(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, System.nanoTime());
        latestTimer.record(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, System.nanoTime());

        // record the state after the first timeout event and generate some more timeout events
        StoreTimerSnapshot savedTimer;
        savedTimer = StoreTimerSnapshot.from(latestTimer);
        final int numTimeouts = 3;
        for (int i = 0; i < numTimeouts; i++) {
            latestTimer.recordTimeout(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, System.nanoTime() - 5000);
        }

        // should have the additional timeout events in latestTimer
        diffKVs = latestTimer.getKeysAndValues();
        assertEquals(numTimeouts + 1, diffKVs.get("wait_advance_cursor_timeout_count").intValue());
        assertThat(diffKVs.get("wait_advance_cursor_timeout_micros").intValue(), greaterThan(0));

        // the savedTimer should only have recorded the first timeout event and hence the difference is the numTimeout events that occurred after that
        StoreTimer diffTimer;
        diffTimer = StoreTimer.getDifference(latestTimer, savedTimer);
        diffKVs = diffTimer.getKeysAndValues();
        assertEquals(numTimeouts, diffKVs.get("wait_advance_cursor_timeout_count").intValue());
        assertThat(diffKVs.get("wait_advance_cursor_timeout_micros").intValue(), greaterThan(0));
    }

    @Test
    void timerConstraintChecks() {
        // invalid to subtract a snapshot timer from a timer that has been reset after the snapshot was taken
        FDBStoreTimer latestTimer = context.getTimer();
        final StoreTimerSnapshot savedTimer;
        savedTimer = StoreTimerSnapshot.from(latestTimer);
        latestTimer.reset();
        assertThrows(RecordCoreArgumentException.class, () -> StoreTimer.getDifference(latestTimer, savedTimer));

        // invalid to subtract a snapshot timer from a timer it was not derived from
        StoreTimer anotherStoreTimer = new StoreTimer();
        assertThrows(RecordCoreArgumentException.class, () -> StoreTimer.getDifference(anotherStoreTimer, savedTimer));
    }

    @Test
    void unchangedMetricsExcludedFromSnapshotDifference() {
        StoreTimer timer = new FDBStoreTimer();

        timer.increment(FDBStoreTimer.Counts.CREATE_RECORD_STORE);
        timer.increment(FDBStoreTimer.Counts.DELETE_RECORD_KEY);
        timer.record(FDBStoreTimer.Events.CHECK_VERSION, 1L);
        timer.record(FDBStoreTimer.Events.DIRECTORY_READ, 3L);
        timer.recordSize(DummySizeEvents.SIZE_EVENT_1, 10L);
        timer.record(DummySizeEvents.SIZE_EVENT_2, 20L);

        StoreTimerSnapshot snapshot = StoreTimerSnapshot.from(timer);

        timer.increment(FDBStoreTimer.Counts.DELETE_RECORD_KEY);
        timer.record(FDBStoreTimer.Events.DIRECTORY_READ, 7L);
        timer.recordSize(DummySizeEvents.SIZE_EVENT_2, 30L);

        StoreTimer diff = StoreTimer.getDifference(timer, snapshot);
        assertThat(diff.getCounter(FDBStoreTimer.Counts.CREATE_RECORD_STORE), Matchers.nullValue());
        assertThat(diff.getCounter(FDBStoreTimer.Events.CHECK_VERSION), Matchers.nullValue());
        assertThat(diff.getCounter(DummySizeEvents.SIZE_EVENT_1), Matchers.nullValue());
        assertThat(diff.getCounter(FDBStoreTimer.Counts.DELETE_RECORD_KEY).getCount(), Matchers.is(1));
        assertThat(diff.getCounter(FDBStoreTimer.Counts.DELETE_RECORD_KEY).getTimeNanos(), Matchers.is(0L));
        assertThat(diff.getCounter(FDBStoreTimer.Events.DIRECTORY_READ).getCount(), Matchers.is(1));
        assertThat(diff.getCounter(FDBStoreTimer.Events.DIRECTORY_READ).getTimeNanos(), Matchers.is(7L));
        assertThat(diff.getCounter(DummySizeEvents.SIZE_EVENT_2).getCount(), Matchers.is(1));
        assertThat(diff.getCounter(DummySizeEvents.SIZE_EVENT_2).getCumulativeValue(), Matchers.is(30L));
    }

    @Test
    void newMetricsAddedToSnapshotDifference() {
        StoreTimer timer = new FDBStoreTimer();

        timer.increment(FDBStoreTimer.Counts.DELETE_RECORD_KEY);

        StoreTimerSnapshot snapshot = StoreTimerSnapshot.from(timer);

        timer.increment(FDBStoreTimer.Counts.DELETE_RECORD_KEY);
        timer.record(FDBStoreTimer.Events.DIRECTORY_READ, 7L);
        timer.recordSize(DummySizeEvents.SIZE_EVENT_2, 2L);

        StoreTimer diff = StoreTimer.getDifference(timer, snapshot);
        assertThat(diff.getCounter(FDBStoreTimer.Counts.DELETE_RECORD_KEY).getCount(), Matchers.is(1));
        assertThat(diff.getCounter(FDBStoreTimer.Counts.DELETE_RECORD_KEY).getTimeNanos(), Matchers.is(0L));
        assertThat(diff.getCounter(FDBStoreTimer.Events.DIRECTORY_READ).getCount(), Matchers.is(1));
        assertThat(diff.getCounter(FDBStoreTimer.Events.DIRECTORY_READ).getTimeNanos(), Matchers.is(7L));
        assertThat(diff.getCounter(DummySizeEvents.SIZE_EVENT_2).getCount(), Matchers.is(1));
        assertThat(diff.getCounter(DummySizeEvents.SIZE_EVENT_2).getCumulativeValue(), Matchers.is(2L));
    }

    @Test
    void metricsInSnapshots() {
        StoreTimer timer = new FDBStoreTimer();

        timer.increment(FDBStoreTimer.Counts.CREATE_RECORD_STORE);
        timer.record(FDBStoreTimer.Events.CHECK_VERSION, 2L);
        timer.recordSize(DummySizeEvents.SIZE_EVENT_1, 10L);

        StoreTimerSnapshot snapshot = StoreTimerSnapshot.from(timer);

        assertThat(snapshot.getCounterSnapshot(FDBStoreTimer.Counts.DELETE_RECORD_KEY), Matchers.nullValue());
        assertThat(snapshot.getCounterSnapshot(FDBStoreTimer.Counts.CREATE_RECORD_STORE), Matchers.notNullValue());
        StoreTimerSnapshot.CounterSnapshot counterSnapshot1 = snapshot.getCounterSnapshot(FDBStoreTimer.Counts.CREATE_RECORD_STORE);
        assertThat(counterSnapshot1.getCount(), Matchers.is(1));
        assertThat(snapshot.getCounterSnapshot(FDBStoreTimer.Events.DIRECTORY_READ), Matchers.nullValue());
        assertThat(snapshot.getCounterSnapshot(FDBStoreTimer.Events.CHECK_VERSION), Matchers.notNullValue());
        StoreTimerSnapshot.CounterSnapshot counterSnapshot2 = snapshot.getCounterSnapshot(FDBStoreTimer.Events.CHECK_VERSION);
        assertThat(counterSnapshot2.getCount(), Matchers.is(1));
        assertThat(counterSnapshot2.getTimeNanos(), Matchers.is(2L));
        assertThat(snapshot.getCounterSnapshot(DummySizeEvents.SIZE_EVENT_2), Matchers.nullValue());
        assertThat(snapshot.getCounterSnapshot(DummySizeEvents.SIZE_EVENT_1), Matchers.notNullValue());
        StoreTimerSnapshot.CounterSnapshot counterSnapshot3 = snapshot.getCounterSnapshot(DummySizeEvents.SIZE_EVENT_1);
        assertThat(counterSnapshot3.getCount(), Matchers.is(1));
        assertThat(counterSnapshot3.getCumulativeValue(), Matchers.is(10L));
    }

    @Test
    void noMetricsAfterReset() {
        StoreTimer timer = new FDBStoreTimer();

        timer.increment(FDBStoreTimer.Counts.CREATE_RECORD_STORE);
        timer.record(FDBStoreTimer.Events.CHECK_VERSION, 2L);
        timer.recordSize(DummySizeEvents.SIZE_EVENT_1, 10L);

        assertThat(timer.getCounter(FDBStoreTimer.Counts.CREATE_RECORD_STORE), Matchers.notNullValue());
        assertThat(timer.getCounter(FDBStoreTimer.Events.CHECK_VERSION), Matchers.notNullValue());
        assertThat(timer.getCounter(DummySizeEvents.SIZE_EVENT_1), Matchers.notNullValue());

        timer.reset();

        assertThat(timer.getCounter(FDBStoreTimer.Counts.CREATE_RECORD_STORE), Matchers.nullValue());
        assertThat(timer.getCounter(FDBStoreTimer.Events.CHECK_VERSION), Matchers.nullValue());
        assertThat(timer.getCounter(DummySizeEvents.SIZE_EVENT_1), Matchers.nullValue());
    }

    private enum TestEvent implements StoreTimer.Event {

        EVENT_WITH_LONG_NAME("An event with a very long name", "ShorterName"),
        EVENT_WITH_SHORT_NAME("An event with a very short name", null);

        private final String title;
        private final String logKey;

        TestEvent(@Nonnull String title, String logKey) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : StoreTimer.Event.super.logKey();
        }
        
        @Override
        public String title() {
            return this.title;
        }

        @Override
        public String logKey() {
            return this.logKey;
        }
    }

    @Test
    void logKeyTest() {
        // If log key has not been specified, 'logKey()' should return then '.name()' of the enum.
        assertEquals(TestEvent.EVENT_WITH_SHORT_NAME.logKey(), "event_with_short_name");

        // If log key has been specified, 'logKey()' should return the specified log key.
        assertEquals(TestEvent.EVENT_WITH_LONG_NAME.logKey(), "ShorterName");
    }

    @Test
    void testAggregateMetrics() {
        FDBStoreTimer storeTimer = new FDBStoreTimer();

        // I don't want this test to fail if new aggregates are added, but do want to verify that the
        // getAggregates() at least does return some of the expected aggregates.
        assertTrue(storeTimer.getAggregates().contains(FDBStoreTimer.CountAggregates.BYTES_DELETED));

        storeTimer.increment(FDBStoreTimer.Counts.DELETE_RECORD_KEY_BYTES, 203);
        storeTimer.increment(FDBStoreTimer.Counts.DELETE_RECORD_VALUE_BYTES, 1000);
        storeTimer.increment(FDBStoreTimer.Counts.DELETE_INDEX_KEY_BYTES, 85);
        storeTimer.increment(FDBStoreTimer.Counts.DELETE_INDEX_VALUE_BYTES, 234);
        storeTimer.increment(FDBStoreTimer.Counts.REPLACE_RECORD_VALUE_BYTES, 100);

        assertNotNull(storeTimer.getCounter(FDBStoreTimer.CountAggregates.BYTES_DELETED));
        assertEquals(1622, storeTimer.getCount(FDBStoreTimer.CountAggregates.BYTES_DELETED),
                "Incorrect aggregate count for BYTES_DELETED");

        // Aggregate counters are immutable.
        assertThrows(RecordCoreException.class, () -> {
            storeTimer.getCounter(FDBStoreTimer.CountAggregates.BYTES_DELETED).increment(44);
        });
    }

    @Test
    void testLowLevelIoMetrics() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            Transaction tr = context.ensureActive();
            tr.clear(subspace.range());
            tr.commit().join();
        }

        assertThat(timer.getCount(FDBStoreTimer.Counts.DELETES), equalTo(1));
        assertThat(timer.getCount(FDBStoreTimer.Events.COMMITS), equalTo(1));

        timer.reset();

        int writeBytes = 0;
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            Transaction tr = context.ensureActive();
            for (int i = 0; i < 5; i++) {
                byte[] key = subspace.pack(Tuple.from(i));
                byte[] value = subspace.pack(Tuple.from("foo", i));
                tr.set(key, value);
                writeBytes += (key.length + value.length);
            }

            ReadTransaction rtr = tr.snapshot();
            List<KeyValue> values = rtr.getRange(subspace.range()).asList().join();
            assertThat(values.size(), equalTo(5));
            tr.commit().join();
        }

        assertThat(timer.getCount(FDBStoreTimer.Counts.WRITES), equalTo(5));
        assertThat(timer.getCount(FDBStoreTimer.Counts.BYTES_WRITTEN), equalTo(writeBytes));
        assertThat(timer.getCount(FDBStoreTimer.Counts.READS), equalTo(1));
        assertThat(timer.getCount(FDBStoreTimer.Counts.BYTES_READ), equalTo(writeBytes));
        assertThat(timer.getCount(FDBStoreTimer.Events.COMMITS), equalTo(1));
    }

    @Test
    void testTransactionMetricListener() {
        try (FDBRecordContext context = fdb.openContext(null, null)) {
            Transaction tr = context.ensureActive();
            tr.clear(subspace.range());
            tr.commit().join();
        }


        final TestTransactionListener listener = new TestTransactionListener();

        final FDBStoreTimer timer = new FDBStoreTimer();
        final Tuple t = Tuple.from(1L);
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setTransactionListener(listener);
        for (int i = 0; i < 3; i++) {
            try (FDBRecordContext context = fdb.openContext(null, timer)) {
                Transaction tr = context.ensureActive();
                tr.set(subspace.pack(t), t.pack());
                tr.get(subspace.pack(t)).join();
                tr.get(subspace.pack(t)).join();

                // Make sure we get metrics even if there is no commit
                if (i != 1) {
                    context.commit();
                }
            }
        }
        assertThat(listener.transactions, equalTo(3));
        assertThat(listener.reads, equalTo(6));
        assertThat(timer.getCount(FDBStoreTimer.Counts.READS), equalTo(listener.reads));
        assertThat(timer.getCount(FDBStoreTimer.Counts.BYTES_READ), equalTo(listener.reads * t.getPackedSize()));
        assertThat(listener.writes, equalTo(2));
        assertThat(timer.getCount(FDBStoreTimer.Counts.WRITES), equalTo(listener.writes));
        assertThat(timer.getCount(FDBStoreTimer.Counts.BYTES_WRITTEN), equalTo((t.getPackedSize() * 2 + subspace.getKey().length) * listener.writes));
        assertThat(listener.commits, equalTo(2));
        assertThat(timer.getCount(FDBStoreTimer.Events.COMMIT), equalTo(listener.commits));
        assertThat(listener.closes, equalTo(3));
    }

    @Test
    void testDelayedForCommit() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            Transaction tr = context.ensureActive();
            tr.clear(subspace.range());
            assertEquals(0, timer.getCount(FDBStoreTimer.Counts.RANGE_DELETES));
            context.commit();
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.RANGE_DELETES));
        }
    }

    @Test
    void testMultipleTransactionsShareTimerSomeCommit() {
        final int contextCount = 20;
        final FDBStoreTimer timer = new FDBStoreTimer();
        final List<FDBRecordContext> contexts = new ArrayList<>(contextCount);
        try {
            for (int i = 0; i < contextCount; i++) {
                FDBRecordContext context = fdb.openContext(null, timer);
                context.getReadVersion();
                contexts.add(context);
            }
            assertEquals(contextCount, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION));

            int bytesRead = 0;
            int bytesWritten = 0;
            for (int i = 0; i < contextCount; i++) {
                FDBRecordContext context = contexts.get(i);
                byte[] key = subspace.pack(Tuple.from(i / 5, i % 5));
                byte[] value = context.ensureActive().get(key).join();
                context.ensureActive().set(key, value);

                bytesRead += value.length;
                if (i % 2 == 0) {
                    bytesWritten += key.length + value.length;
                }
            }
            assertEquals(contextCount, timer.getCount(FDBStoreTimer.Counts.READS));
            assertEquals(bytesRead, timer.getCount(FDBStoreTimer.Counts.BYTES_READ));
            assertEquals(0, timer.getCount(FDBStoreTimer.Counts.WRITES));
            assertEquals(0, timer.getCount(FDBStoreTimer.Counts.BYTES_WRITTEN));

            // Only commit half the transactions (note: i += 2, not i++)
            for (int i = 0; i < contextCount; i += 2) {
                contexts.get(i).commit();
            }

            assertEquals(contextCount, timer.getCount(FDBStoreTimer.Counts.READS));
            assertEquals(bytesRead, timer.getCount(FDBStoreTimer.Counts.BYTES_READ));
            assertEquals(contextCount / 2, timer.getCount(FDBStoreTimer.Counts.WRITES));
            assertEquals(bytesWritten, timer.getCount(FDBStoreTimer.Counts.BYTES_WRITTEN));
        } finally {
            contexts.forEach(FDBRecordContext::close);
        }
    }

    @Test
    void doNotAddDelayedMetricsOnFailedCommit() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context1 = fdb.openContext(null, timer);
                FDBRecordContext context2 = fdb.openContext(null, timer)) {
            context1.getReadVersion();
            context2.getReadVersion();

            // Use context1 to read all keys beginning with 1
            final List<KeyValue> beginsWith1 = context1.ensureActive()
                    .getRange(subspace.range(Tuple.from(1)))
                    .asList()
                    .join();

            // Use context2 to read all keys beginning with 2
            final List<KeyValue> beginsWith2 = context2.ensureActive()
                    .getRange(subspace.range(Tuple.from(2)))
                    .asList()
                    .join();

            byte[] value = Tuple.from("blah").pack();
            // Update a key beginning with 2 using context 1
            byte[] key1 = subspace.pack(Tuple.from(2, 3));
            context1.ensureActive().set(key1, value);

            // Update a key beginning with 1 using context 2
            byte[] key2 = subspace.pack(Tuple.from(1, 4));
            context2.ensureActive().set(key2, value);

            context1.commit();
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context2::commit);

            // Reads contain updates from both transactions
            int bytesRead = beginsWith1.stream().mapToInt(kv -> kv.getKey().length + kv.getValue().length).sum()
                    + beginsWith2.stream().mapToInt(kv -> kv.getKey().length + kv.getValue().length).sum();
            assertEquals(bytesRead, timer.getCount(FDBStoreTimer.Counts.BYTES_READ));

            // Writes only include the transaction that successfully committed
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.WRITES));
            int bytesWritten = key1.length + value.length;
            assertEquals(bytesWritten, timer.getCount(FDBStoreTimer.Counts.BYTES_WRITTEN));
        }

    }

    @Test
    void testEmptyScans() throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            Transaction tr = context.ensureActive();
            tr.clear(subspace.range());

            // Reading an empty range should be registered in the counter
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.EMPTY_SCANS));
            final AsyncIterable<KeyValue> iterable = tr.getRange(subspace.pack(1L), subspace.pack(2L));
            assertThat(iterable.asList().get(), empty());
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.EMPTY_SCANS));
            assertFalse(iterable.iterator().hasNext());
            assertEquals(2L, timer.getCount(FDBStoreTimer.Counts.EMPTY_SCANS));
            assertFalse(iterable.iterator().onHasNext().get());
            assertEquals(3L, timer.getCount(FDBStoreTimer.Counts.EMPTY_SCANS));

            final AsyncIterator<KeyValue> itr1 = iterable.iterator();
            assertFalse(itr1.hasNext()); // Should increment EMPTY_SCANS
            assertFalse(itr1.hasNext()); // Should not double-count the same empty scan
            assertEquals(4L, timer.getCount(FDBStoreTimer.Counts.EMPTY_SCANS));

            // Set a key in the range. From now on, the counter shouldn't get incremented
            tr.set(subspace.pack(Tuple.from(1L, "foo")), Tuple.from("bar").pack());
            final AsyncIterable<KeyValue> iterable2 = tr.getRange(subspace.pack(1L), subspace.pack(2L));
            assertThat(iterable2.asList().get(), hasSize(1));
            assertEquals(4L, timer.getCount(FDBStoreTimer.Counts.EMPTY_SCANS));

            final AsyncIterator<KeyValue> itr2a = iterable2.iterator();
            assertTrue(itr2a.hasNext());
            assertEquals(Tuple.from("bar"), Tuple.fromBytes(itr2a.next().getValue()));
            assertFalse(itr2a.hasNext());
            assertEquals(4L, timer.getCount(FDBStoreTimer.Counts.EMPTY_SCANS));

            final AsyncIterator<KeyValue> itr2b = iterable2.iterator();
            assertTrue(itr2b.onHasNext().get());
            assertEquals(Tuple.from("bar"), Tuple.fromBytes(itr2b.next().getValue()));
            assertFalse(itr2b.onHasNext().get());
            assertEquals(4L, timer.getCount(FDBStoreTimer.Counts.EMPTY_SCANS));
        }
    }

    @Test
    void testReadsDoNotCountUntilStarted() throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            Transaction tr = context.ensureActive();
            final AsyncIterable<KeyValue> iterable = tr.getRange(subspace.range());

            // Calling getRange does not start a read yet, so the timers shouldn't have started
            assertEquals(0, timer.getCount(FDBStoreTimer.Counts.READS));
            assertEquals(0, timer.getCount(FDBStoreTimer.Counts.RANGE_READS));

            // Run the read and validate metrics are updated
            iterable.asList().get();
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.READS));
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.RANGE_READS));

            // Creating an iterator does start the read, so make sure the reads metrics are updated
            final AsyncIterator<KeyValue> iterator = iterable.iterator();
            assertEquals(2, timer.getCount(FDBStoreTimer.Counts.READS));
            assertEquals(2, timer.getCount(FDBStoreTimer.Counts.RANGE_READS));

            // Moving an iterator forward does not count as another read
            iterator.next();
            assertEquals(2, timer.getCount(FDBStoreTimer.Counts.READS));
            assertEquals(2, timer.getCount(FDBStoreTimer.Counts.RANGE_READS));

            // Creating a new iterator starts a new read
            final AsyncIterator<KeyValue> iterator2 = iterable.iterator();
            assertEquals(3, timer.getCount(FDBStoreTimer.Counts.READS));
            assertEquals(3, timer.getCount(FDBStoreTimer.Counts.RANGE_READS));

            iterator.cancel();
            iterator2.cancel();
        }
    }

    private static class TestTransactionListener implements TransactionListener {
        int transactions;
        int reads;
        int writes;
        int commits;
        int closes;

        @Override
        public void create(@Nonnull final FDBDatabase database, @Nonnull final Transaction transaction) {
            ++transactions;
        }

        @Override
        public void commit(@Nonnull final FDBDatabase database, @Nonnull final Transaction transaction,
                           @Nullable final StoreTimer storeTimer, @Nullable final Throwable exception) {
            reads += storeTimer.getCount(FDBStoreTimer.Counts.READS);
            writes += storeTimer.getCount(FDBStoreTimer.Counts.WRITES);
            storeTimer.reset();
            commits++;
        }

        @Override
        public void close(@Nonnull final FDBDatabase database, @Nonnull final Transaction transaction, @Nullable final StoreTimer storeTimer) {
            reads += storeTimer.getCount(FDBStoreTimer.Counts.READS);
            writes += storeTimer.getCount(FDBStoreTimer.Counts.WRITES);
            storeTimer.reset();
            ++closes;
        }
    }

    private void setupBaseData() {
        subspace = fdb.run(path::toSubspace);

        // Populate with data.
        fdb.database().run(tr -> {
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < 5; j++) {
                    tr.set(subspace.pack(Tuple.from(i, j)), Tuple.from(i, j).pack());
                }
            }
            return null;
        });
    }
}
