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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordScanLimiter;
import com.apple.foundationdb.record.RecordScanLimiterFactory;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeyValueCursor}.
 */
@Tag(Tags.RequiresFDB)
public class KeyValueCursorTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private FDBDatabase fdb;
    private KeySpacePath path;
    private Subspace subspace;


    @BeforeEach
    public void runBefore() {
        fdb = dbExtension.getDatabase();
        path = pathManager.createPath(TestKeySpace.RAW_DATA);
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

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void all(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            byte[] continuation = null;
            KeyValueCursor cursor = null;
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < 5; j++) {
                    cursor = KeyValueCursor.Builder.withSubspace(subspace)
                            .setContext(context)
                            .setRange(TupleRange.ALL)
                            .setContinuation(continuation)
                            .setScanProperties(ScanProperties.FORWARD_SCAN)
                            .setSerializationMode(serializationMode)
                            .build();
                    RecordCursorResult<KeyValue> cursorResult = cursor.getNext();
                    KeyValue kv = cursorResult.get();
                    continuation = cursorResult.getContinuation().toBytes();
                    assertArrayEquals(subspace.pack(Tuple.from(i, j)), kv.getKey());
                    assertArrayEquals(Tuple.from(i, j).pack(), kv.getValue());
                }
            }
            assertThat(cursor.getNext().hasNext(), is(false));

            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.ALL)
                    .setContinuation(null)
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()))
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(10, (int)cursor.getCount().join());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.ALL)
                    .setContinuation(cursor.getNext().getContinuation().toBytes())
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(15, (int)cursor.getCount().join());

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void allInARange(KeyValueCursorBase.SerializationMode serializationMode) throws InvalidProtocolBufferException {
        // pick 2 examples that can be serialized as RecordCursorProto.KeyValueCursorContinuation, but was correctly rejected by the magic number check
        byte[] lowBytes = new byte[]{ 0x11, (byte) 0xac,  (byte) 0xcd, (byte) 0x73, 0x01, (byte) 0xdd, 0x42, (byte) 0x98, 0x5e, 0x0A, 0x04, 0x0f, (byte) 0xdb, 0x00, 0x14 };
        byte[] highBytes = new byte[]{ 0x18, 0x01, 0x0A, 0x02, 0x01, 0x14 };
        RecordCursorProto.KeyValueCursorContinuation lowProto = RecordCursorProto.KeyValueCursorContinuation.parseFrom(lowBytes);
        Assertions.assertEquals(0x5e9842dd0173cdacL, lowProto.getMagicNumber());
        Assertions.assertEquals(ByteString.copyFrom(new byte[] { 0x0F, (byte)0xDB, 0x00, 0x14 }), lowProto.getInnerContinuation());
        RecordCursorProto.KeyValueCursorContinuation highProto = RecordCursorProto.KeyValueCursorContinuation.parseFrom(highBytes);
        Assertions.assertEquals(ByteString.copyFrom(new byte[] {(byte) 1, (byte) 20}), highProto.getInnerContinuation());

        Tuple low = Tuple.fromBytes(lowBytes);  // should set the magic_number to 0x5e9842dd0173cdacL and the inner continuation to `\x0f\xdb\x00\x14`
        Tuple high = Tuple.fromBytes(highBytes); // should set an unknown field 3 to 1 and the inner_continuation to `\x01\x14`
        TupleRange tupleRange = new TupleRange(low, high, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);

        // clear data populated by BeforeEach
        fdb.database().run(transaction -> {
            transaction.clear(subspace.range());
            return null;
        });

        // Populate with data.
        fdb.database().run(tr -> {
            tr.set(subspace.pack(low), low.pack());
            tr.set(subspace.pack(high), high.pack());
            return null;
        });


        fdb.run(context -> {
            byte[] continuation = null;
            KeyValueCursor cursor = null;
            for (int k = 0; k < 2; k++) {
                cursor = KeyValueCursor.Builder.withSubspace(subspace)
                        .setContext(context)
                        .setRange(tupleRange)
                        .setContinuation(continuation)
                        .setScanProperties(ScanProperties.FORWARD_SCAN)
                        .setSerializationMode(serializationMode)
                        .build();
                RecordCursorResult<KeyValue> cursorResult = cursor.getNext();
                KeyValue kv = cursorResult.get();
                continuation = cursorResult.getContinuation().toBytes();
                if (k == 0) {
                    assertArrayEquals(subspace.pack(low), kv.getKey());
                    assertArrayEquals(low.pack(), kv.getValue());
                } else {
                    assertArrayEquals(subspace.pack(high), kv.getKey());
                    assertArrayEquals(high.pack(), kv.getValue());
                }
            }

            assertThat(cursor.getNext().hasNext(), is(false));


            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(tupleRange)
                    .setContinuation(null)
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(1).build()))
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(1, (int)cursor.getCount().join());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(tupleRange)
                    .setContinuation(cursor.getNext().getContinuation().toBytes())
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(1, (int)cursor.getCount().join());

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void beginsWith(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(3)))
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build();
            for (int j = 0; j < 5; j++) {
                KeyValue kv = cursor.getNext().get();
                assertArrayEquals(subspace.pack(Tuple.from(3, j)), kv.getKey());
                assertArrayEquals(Tuple.from(3, j).pack(), kv.getValue());
            }
            assertThat(cursor.getNext().hasNext(), is(false));

            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(3)))
                    .setContinuation(null)
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(2).build()))
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(2, (int)cursor.getCount().join());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(3)))
                    .setContinuation(cursor.getNext().getContinuation().toBytes())
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(3).build()))
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(3, (int)cursor.getCount().join());

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void inclusiveRange(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_INCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_INCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 3L), Tuple.from(3L, 4L), Tuple.from(4L, 0L), Tuple.from(4L, 1L), Tuple.from(4L, 2L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());

            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_INCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_INCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(2).build()))
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 3L), Tuple.from(3L, 4L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_INCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_INCLUSIVE)
                    .setContinuation(cursor.getNext().getContinuation().toBytes())
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(Arrays.asList(Tuple.from(4L, 0L), Tuple.from(4L, 1L), Tuple.from(4L, 2L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());

            return null;
        });
    }

    private KeyValueCursor scanPrefixString(FDBRecordContext context, Tuple prefixRange, ScanProperties scanProperties, byte[] continuation) {
        TupleRange range = new TupleRange(
                prefixRange,
                prefixRange,
                EndpointType.PREFIX_STRING,
                EndpointType.PREFIX_STRING
        );
        KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                .setContext(context)
                .setRange(range)
                .setContinuation(continuation)
                .setScanProperties(scanProperties)
                .build();
        return cursor;
    }

    private void assertKeyValue(KeyValue kv, Tuple key, Tuple value) {
        assertArrayEquals(subspace.pack(key), kv.getKey());
        assertArrayEquals(value.pack(), kv.getValue());
    }

    @Test
    public void prefixString() {
        // Populate data
        fdb.database().run(tr -> {
            for (int i = 0; i < 5; i++) {
                tr.set(subspace.pack(Tuple.from("apple", i)), Tuple.from("apple", i).pack());
                tr.set(subspace.pack(Tuple.from("banana", i)), Tuple.from("banana", i).pack());
                tr.set(subspace.pack(Tuple.from("app", i)), Tuple.from("app", i).pack());
                tr.set(subspace.pack(Tuple.from("a", i)), Tuple.from("a", i).pack());
            }
            return null;
        });
        fdb.run(context -> {
            // Scan by empty prefix string, limit 10 rows
            KeyValueCursor cursor = scanPrefixString(context, Tuple.from(""), new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()), null);
            for (int i = 0; i < 5; i++) {
                assertKeyValue(cursor.getNext().get(), Tuple.from("a", i), Tuple.from("a", i));
            }
            for (int i = 0; i < 5; i++) {
                assertKeyValue(cursor.getNext().get(), Tuple.from("app", i), Tuple.from("app", i));
            }
            // Continue scan next 10 rows
            cursor = scanPrefixString(context, Tuple.from(""), new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()), cursor.getNext().getContinuation().toBytes());
            for (int i = 0; i < 5; i++) {
                assertKeyValue(cursor.getNext().get(), Tuple.from("apple", i), Tuple.from("apple", i));
            }
            for (int i = 0; i < 5; i++) {
                assertKeyValue(cursor.getNext().get(), Tuple.from("banana", i), Tuple.from("banana", i));
            }
            // Reverse scan with "app" prefix
            cursor = scanPrefixString(context, Tuple.from("app"), new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(5).build(), true), null);
            for (int i = 4; i >= 0; i--) {
                assertKeyValue(cursor.getNext().get(), Tuple.from("apple", i), Tuple.from("apple", i));
            }
            cursor = scanPrefixString(context, Tuple.from("app"), ScanProperties.REVERSE_SCAN, cursor.getNext().getContinuation().toBytes());
            for (int i = 4; i >= 0; i--) {
                assertKeyValue(cursor.getNext().get(), Tuple.from("app", i), Tuple.from("app", i));
            }
            // Scan with null character
            cursor = scanPrefixString(context, Tuple.from("a\0"), ScanProperties.FORWARD_SCAN, null);
            assertEquals(0, cursor.getCount().join());
            cursor = scanPrefixString(context, Tuple.from("ap\0le"), ScanProperties.FORWARD_SCAN, null);
            assertEquals(0, cursor.getCount().join());

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void exclusiveRange(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 4L), Tuple.from(4L, 0L), Tuple.from(4L, 1L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());

            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(2).build()))
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 4L), Tuple.from(4L, 0L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setContinuation(cursor.getNext().getContinuation().toBytes())
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(Collections.singletonList(Tuple.from(4L, 1L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void inclusiveNull(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            RecordCursorIterator<KeyValue> cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(4), EndpointType.RANGE_INCLUSIVE)
                    .setHigh((Tuple) null, EndpointType.RANGE_INCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build()
                    .asIterator();
            for (int j = 0; j < 5; j++) {
                KeyValue kv = cursor.next();
                assertArrayEquals(subspace.pack(Tuple.from(4, j)), kv.getKey());
                assertArrayEquals(Tuple.from(4, j).pack(), kv.getValue());
            }
            assertThat(cursor.hasNext(), is(false));

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void exclusiveNull(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            RecordCursorIterator<KeyValue> cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(4, 0), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh((Tuple) null, EndpointType.RANGE_EXCLUSIVE)
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build()
                    .asIterator();
            assertThat(cursor.hasNext(), is(false));

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void noNextReasons(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(3)))
                    .setContinuation(null)
                    .setSerializationMode(serializationMode)
                    .setScanProperties(ScanProperties.FORWARD_SCAN.with(props -> props.setReturnedRowLimit(3)))
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 0L), Tuple.from(3L, 1L), Tuple.from(3L, 2L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());
            RecordCursorResult<KeyValue> result = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, result.getNoNextReason());
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(3)))
                    .setContinuation(result.getContinuation().toBytes())
                    .setSerializationMode(serializationMode)
                    .setScanProperties(ScanProperties.FORWARD_SCAN.with(props -> props.setReturnedRowLimit(3)))
                    .build();
            assertEquals(Arrays.asList(Tuple.from(3L, 3L), Tuple.from(3L, 4L)),
                    cursor.map(KeyValue::getValue).map(Tuple::fromBytes).asList().join());
            result = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
            assertNull(result.getContinuation().toBytes());

            return null;
        });
    }

    private ScanProperties forwardScanWithLimiter(RecordScanLimiter limiter) {
        return new ScanProperties(ExecuteProperties.SERIAL_EXECUTE.setState(new ExecuteState(limiter, null)));
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void simpleScanLimit(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            RecordScanLimiter limiter = RecordScanLimiterFactory.enforce(2);
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.ALL)
                    .setScanProperties(forwardScanWithLimiter(limiter))
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(2, (int) cursor.getCount().join());
            RecordCursorResult<KeyValue> result = cursor.getNext();
            assertThat("no next reason should be SCAN_LIMIT_REACHED", result.getNoNextReason(),
                    equalTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void limitNotReached(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            RecordScanLimiter limiter = RecordScanLimiterFactory.enforce(4);
            KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setScanProperties(forwardScanWithLimiter(limiter))
                    .setSerializationMode(serializationMode)
                    .build();
            assertEquals(3, (int) cursor.getCount().join());
            RecordCursorResult<?> result = cursor.getNext();
            assertThat("no next reason should be SOURCE_EXHAUSTED", result.getNoNextReason(),
                    equalTo(RecordCursor.NoNextReason.SOURCE_EXHAUSTED));

            return null;
        });
    }

    private boolean hasNextAndAdvance(KeyValueCursor cursor) {
        return cursor.getNext().hasNext();
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void sharedLimiter(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            RecordScanLimiter limiter = RecordScanLimiterFactory.enforce(4);
            KeyValueCursor.Builder builder =  KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setScanProperties(forwardScanWithLimiter(limiter))
                    .setSerializationMode(serializationMode);
            KeyValueCursor cursor1 = builder.build();
            KeyValueCursor cursor2 = builder.build();

            assertThat(hasNextAndAdvance(cursor1), is(true));
            assertThat(hasNextAndAdvance(cursor2), is(true));
            assertThat(hasNextAndAdvance(cursor1), is(true));
            assertThat(hasNextAndAdvance(cursor2), is(true));
            assertThat(cursor1.getNext().hasNext(), is(false));
            assertThat(cursor2.getNext().hasNext(), is(false));
            assertThat("no next reason should be SCAN_LIMIT_REACHED", cursor1.getNext().getNoNextReason(),
                    equalTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));
            assertThat("no next reason should be SCAN_LIMIT_REACHED", cursor2.getNext().getNoNextReason(),
                    equalTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void limiterWithLookahead(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            RecordScanLimiter limiter = RecordScanLimiterFactory.enforce(1);
            KeyValueCursor kvCursor =  KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setLow(Tuple.from(3, 3), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(Tuple.from(4, 2), EndpointType.RANGE_EXCLUSIVE)
                    .setScanProperties(forwardScanWithLimiter(limiter))
                    .setSerializationMode(serializationMode)
                    .build();
            RecordCursor<KeyValue> cursor = kvCursor.skip(2); // should exhaust limit first
            RecordCursorResult<KeyValue> result = cursor.getNext();
            assertThat("skipped items should exhaust limit", result.hasNext(), is(false));
            assertThat("no next reason should be SCAN_LIMIT_REACHED", result.getNoNextReason(),
                    equalTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));
            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void emptyScan(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            RecordCursor<KeyValue> cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(9)))
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build();
            RecordCursorResult<KeyValue> result = cursor.getNext();
            assertFalse(result.hasNext());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
            assertTrue(result.getContinuation().isEnd());

            return null;
        });
    }

    @ParameterizedTest
    @EnumSource(KeyValueCursorBase.SerializationMode.class)
    public void emptyScanSplit(KeyValueCursorBase.SerializationMode serializationMode) {
        fdb.run(context -> {
            RecordCursor<KeyValue> kvCursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.allOf(Tuple.from(9)))
                    .setContinuation(null)
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .setSerializationMode(serializationMode)
                    .build();
            RecordCursor<?> cursor = new SplitHelper.KeyValueUnsplitter(context, subspace, kvCursor, false, null, false,
                    new CursorLimitManager(context, ScanProperties.FORWARD_SCAN));
            RecordCursorResult<?> result = cursor.getNext();
            assertFalse(result.hasNext());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
            assertTrue(result.getContinuation().isEnd());

            return null;
        });
    }

    @Test
    public void buildWithoutRequiredProperties() {
        assertThrows(RecordCoreException.class, () -> KeyValueCursor.Builder.withSubspace(subspace)
                                                        .build());
    }

    @Test
    public void buildWithoutScanProperties() {
        fdb.run(context -> {
            assertThrows(RecordCoreException.class, () -> KeyValueCursor.Builder.withSubspace(subspace)
                                                            .setContext(context)
                                                            .build());

            return null;
        });
    }

    @Test
    public void buildWithRequiredProperties() {
        fdb.run(context -> {
            try {
                RecordCursor<KeyValue> kvCursor = KeyValueCursor.Builder.withSubspace(subspace)
                                                    .setContext(context)
                                                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                                                    .build();
                assertTrue(true);
            } catch (RecordCoreException ex) {
                // Program flow should not reach here.
                assertTrue(false);
            }

            return null;
        });
    }

    @Test
    void emptyByteStringSerDesTest() throws InvalidProtocolBufferException {
        // not setting continuation -> continuation.hasContinuation() = false
        byte[] continuation = RecordCursorProto.KeyValueCursorContinuation.newBuilder().build().toByteArray();
        RecordCursorProto.KeyValueCursorContinuation continuationProto = RecordCursorProto.KeyValueCursorContinuation.parseFrom(continuation);
        Assertions.assertFalse(continuationProto.hasInnerContinuation());

        // setting continuation = ByteString.EMPTY -> continuation.hasContinuation() = true
        byte[] continuation2 = RecordCursorProto.KeyValueCursorContinuation.newBuilder().setInnerContinuation(ByteString.EMPTY).build().toByteArray();
        RecordCursorProto.KeyValueCursorContinuation continuationProto2 = RecordCursorProto.KeyValueCursorContinuation.parseFrom(continuation2);
        Assertions.assertTrue(continuationProto2.hasInnerContinuation());
        Assertions.assertEquals(ByteString.EMPTY, continuationProto2.getInnerContinuation());
    }

    @Test
    void emptyInnerContinuationTest() {
        // ensure that when innerContinuation is empty, the wrapped continuation is not at beginning
        int prefixLength = 50;
        byte[] randomBytes = new byte[prefixLength];
        new SecureRandom().nextBytes(randomBytes);
        RecordCursorContinuation oldContinuation = new KeyValueCursorBase.Continuation(randomBytes, prefixLength, KeyValueCursorBase.SerializationMode.TO_OLD);
        Assertions.assertFalse(oldContinuation.isEnd());
        Assertions.assertEquals(ByteString.EMPTY, oldContinuation.toByteString());

        KeyValueCursorBase.Continuation newContinuation = new KeyValueCursorBase.Continuation(randomBytes, prefixLength, KeyValueCursorBase.SerializationMode.TO_NEW);
        Assertions.assertFalse(newContinuation.isEnd());
        // inner continuation is empty
        Assertions.assertEquals(0, Objects.requireNonNull(KeyValueCursorBase.Continuation.getInnerContinuation(newContinuation.toBytes())).length);
        // wrapped continuation is not empty
        Assertions.assertNotEquals(ByteString.EMPTY, newContinuation.toByteString());
    }
}
