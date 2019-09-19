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
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.StoreTimerSnapshot;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FDBStoreTimer}.
 */
@Tag(Tags.RequiresFDB)
public class FDBStoreTimerTest {
    FDBDatabase fdb;
    FDBRecordContext context;
    private Subspace subspace;
    ExecuteProperties ep;

    @BeforeEach
    public void setup() throws Exception {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        context = fdb.openContext();
        setupBaseData();
        FDBStoreTimer timer = new FDBStoreTimer();
        context.setTimer(timer);
    }

    @AfterEach
    public void teardown() throws Exception {
        context.close();
        fdb.close();
    }

    @Test
    public void counterDifferenceTest() {
        RecordCursor<KeyValue> kvc = KeyValueCursor.Builder.withSubspace(subspace).setContext(context).setScanProperties(ScanProperties.FORWARD_SCAN).setRange(TupleRange.ALL).build();

        // see the timer counts from some onNext calls
        FDBStoreTimer latestTimer = context.getTimer();
        StoreTimerSnapshot savedTimer;
        CompletableFuture<RecordCursorResult<KeyValue>> fkvr;
        RecordCursorResult<KeyValue> kvr;
        StoreTimer diffTimer;

        // get a snapshot from latestTimer before advancing cursor
        savedTimer = StoreTimerSnapshot.from(latestTimer);

        // advance the cursor once
        kvr = kvc.onNext().join();

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
            kvr = kvc.onNext().join();
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
    public void timeoutCounterDifferenceTest() {
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
    public void timerConstraintChecks() {

        // invalid to substract a snapshot timer from a timer that has been reset after the snapshot was taken
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
    public void logKeyTest() {
        // If log key has not been specified, 'logKey()' should return then '.name()' of the enum.
        assertEquals(FDBStoreTimer.Events.COMMIT.logKey(), "COMMIT");

        // If log key has been specified, 'logKey()' should return the specified log key.
        assertEquals(FDBStoreTimer.Events.LOAD_RECORD_STORE_INDEX_META_DATA.logKey(), "ld_rs_idx_meta");
    }

    private void setupBaseData() {
        subspace = fdb.run(context -> {
            KeySpacePath path = TestKeySpace.getKeyspacePath("record-test", "unit");
            FDBRecordStore.deleteStore(context, path);
            return path.toSubspace(context);
        });

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
