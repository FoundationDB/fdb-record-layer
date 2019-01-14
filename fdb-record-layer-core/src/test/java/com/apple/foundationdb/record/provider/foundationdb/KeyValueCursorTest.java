/*
 * KeyValueCursorTest.java
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
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordScanLimiter;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link KeyValueCursor}.
 */
@Tag(Tags.RequiresFDB)
public class KeyValueCursorTest extends FDBTestBase {
    private FDBDatabase fdb;
    private Subspace subspace;

    @BeforeEach
    public void runBefore() {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        subspace = fdb.run(context -> {
            KeySpacePath path = TestKeySpace.getKeyspacePath("record-test", "unit", "keyvaluecursor");
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

    @Test
    public void all() {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.ALL)
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < 5; j++) {
                    KeyValue kv = cursor.next();
                    assertArrayEquals(subspace.pack(Tuple.from(i, j)), kv.getKey());
                    assertArrayEquals(Tuple.from(i, j).pack(), kv.getValue());
                }
            }
            assertThat(cursor.hasNext(), is(false));

            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.ALL)
                    .setContinuation(null)
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()))
                    .build();
            assertEquals(10, (int)cursor.getCount().join());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.ALL)
                    .setContinuation(cursor.getContinuation())
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            assertEquals(15, (int)cursor.getCount().join());

            return null;
        });
    }

    @Test
    public void beginsWith() {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(3)))
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            for (int j = 0; j < 5; j++) {
                KeyValue kv = cursor.next();
                assertArrayEquals(subspace.pack(Tuple.from(3, j)), kv.getKey());
                assertArrayEquals(Tuple.from(3, j).pack(), kv.getValue());
            }
            assertThat(cursor.hasNext(), is(false));

            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(3)))
                    .setContinuation(null)
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(2).build()))
                    .build();
            assertEquals(2, (int)cursor.getCount().join());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(3)))
                    .setContinuation(cursor.getContinuation())
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(3).build()))
                    .build();
            assertEquals(3, (int)cursor.getCount().join());

            return null;
        });
    }

    @Test
    public void inclusiveRange() {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_INCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_INCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 3L), Tuple.from(3L, 4L), Tuple.from(4L, 0L), Tuple.from(4L, 1L), Tuple.from(4L, 2L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());

            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_INCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_INCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(2).build()))
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 3L), Tuple.from(3L, 4L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_INCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_INCLUSIVE)
                    .setContinuation(cursor.getContinuation())
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            assertEquals(Arrays.asList(Tuple.from(4L, 0L), Tuple.from(4L, 1L), Tuple.from(4L, 2L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());

            return null;
        });
    }

    @Test
    public void exclusiveRange() {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 4L), Tuple.from(4L, 0L), Tuple.from(4L, 1L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());

            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(2).build()))
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 4L), Tuple.from(4L, 0L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setContinuation(cursor.getContinuation())
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            assertEquals(Collections.singletonList(Tuple.from(4L, 1L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());

            return null;
        });
    }

    @Test
    public void inclusiveNull() {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(4), EndpointType.RANGE_INCLUSIVE)
                    .setHigh((Tuple) null, EndpointType.RANGE_INCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            for (int j = 0; j < 5; j++) {
                KeyValue kv = cursor.next();
                assertArrayEquals(subspace.pack(Tuple.from(4, j)), kv.getKey());
                assertArrayEquals(Tuple.from(4, j).pack(), kv.getValue());
            }
            assertThat(cursor.hasNext(), is(false));

            return null;
        });
    }

    @Test
    public void exclusiveNull() {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(4, 0), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh((Tuple) null, EndpointType.RANGE_EXCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            assertThat(cursor.hasNext(), is(false));

            return null;
        });
    }

    @Test
    public void noNextReasons() {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(3)))
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN.with(props -> props.setReturnedRowLimit(3)))
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 0L), Tuple.from(3L, 1L), Tuple.from(3L, 2L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, cursor.getNoNextReason());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(3)))
                    .setContinuation(cursor.getContinuation())
                    .setScanProperties(ScanProperties.FORWARD_SCAN.with(props -> props.setReturnedRowLimit(3)))
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 3L), Tuple.from(3L, 4L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
            assertNull(cursor.getContinuation());

            return null;
        });
    }

    private ScanProperties forwardScanWithLimiter(RecordScanLimiter limiter) {
        return new ScanProperties(ExecuteProperties.SERIAL_EXECUTE.setState(new ExecuteState(limiter)));
    }

    @Test
    public void simpleScanLimit() {
        fdb.run(context -> {
            RecordScanLimiter limiter = new RecordScanLimiter(2);
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.ALL)
                    .setScanProperties(forwardScanWithLimiter(limiter))
                    .build();
            assertEquals(2, (int) cursor.getCount().join());
            assertThat("no next reason should be SCAN_LIMIT_REACHED", cursor.getNoNextReason(),
                    equalTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));

            return null;
        });
    }

    @Test
    public void limitNotReached() {
        fdb.run(context -> {
            RecordScanLimiter limiter = new RecordScanLimiter(4);
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setScanProperties(forwardScanWithLimiter(limiter))
                    .build();
            assertEquals(3, (int) cursor.getCount().join());
            assertThat("no next reason should be SOURCE_EXHAUSTED", cursor.getNoNextReason(),
                    equalTo(RecordCursor.NoNextReason.SOURCE_EXHAUSTED));

            return null;
        });
    }

    private boolean hasNextAndAdvance(KeyValueCursor cursor) {
        boolean hasNext = cursor.hasNext();
        cursor.next();
        return hasNext;
    }

    @Test
    public void sharedLimiter() {
        fdb.run(context -> {
            RecordScanLimiter limiter = new RecordScanLimiter(4);
            KeyValueCursor.Builder builder =  KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setScanProperties(forwardScanWithLimiter(limiter));
            KeyValueCursor cursor1 = builder.build();
            KeyValueCursor cursor2 = builder.build();

            assertThat(hasNextAndAdvance(cursor1), is(true));
            assertThat(hasNextAndAdvance(cursor2), is(true));
            assertThat(hasNextAndAdvance(cursor1), is(true));
            assertThat(hasNextAndAdvance(cursor2), is(true));
            assertThat(cursor1.hasNext(), is(false));
            assertThat(cursor2.hasNext(), is(false));
            assertThat("no next reason should be SCAN_LIMIT_REACHED", cursor1.getNoNextReason(),
                    equalTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));
            assertThat("no next reason should be SCAN_LIMIT_REACHED", cursor2.getNoNextReason(),
                    equalTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));

            return null;
        });
    }

    @Test
    public void limiterWithLookahead() {
        fdb.run(context -> {
            RecordScanLimiter limiter = new RecordScanLimiter(1);
            KeyValueCursor kvCursor =  KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setScanProperties(forwardScanWithLimiter(limiter))
                    .build();
            RecordCursor<KeyValue> cursor = kvCursor.skip(2); // should exhaust limit first
            assertThat("skipped items should exhaust limit", cursor.hasNext(), is(false));
            assertThat("no next reason should be SCAN_LIMIT_REACHED", cursor.getNoNextReason(),
                    equalTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));
            return null;
        });
    }

    @Test
    public void emptyScan() {
        fdb.run(context -> {
            RecordCursor<KeyValue> cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(9)))
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            assertFalse(cursor.hasNext());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
            assertNull(cursor.getContinuation());

            return null;
        });
    }

    @Test
    public void emptyScanSplit() {
        fdb.run(context -> {
            RecordCursor<KeyValue> kvCursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(9)))
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            RecordCursor<?> cursor = new SplitHelper.KeyValueUnsplitter(context, subspace, kvCursor, false, null, false,
                    new CursorLimitManager(context, ScanProperties.FORWARD_SCAN));
            assertFalse(cursor.hasNext());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
            assertNull(cursor.getContinuation());

            return null;
        });
    }
}
