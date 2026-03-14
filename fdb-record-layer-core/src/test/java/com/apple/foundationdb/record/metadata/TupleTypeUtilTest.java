/*
 * TupleTypeUtilTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.tuple.Versionstamp;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests of the {@link TupleTypeUtil} class. These tests require FDB because an API version must be set in order to
 * pack {@link Tuple}s with incomplete versionstamps due to the difference in format starting with API version 520.
 */
@Tag(Tags.RequiresFDB)
public class TupleTypeUtilTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    @BeforeEach
    void ensureAPIVersionIsSet() {
        // This test does not actually use the database, but the FDB API version needs to be initialized
        // for proper serialization of versionstamp types, hence the inclusion of this extension
        dbExtension.getDatabase();
    }

    @Nonnull
    private static final List<Object> VALUES = Arrays.asList(
            null,
            Key.Evaluated.NullStandin.NULL,
            Key.Evaluated.NullStandin.NULL_UNIQUE,
            Key.Evaluated.NullStandin.NOT_NULL,
            "hello",
            String.copyValueOf(new char[]{'h', 'e', 'l', 'l', 'o'}),
            "hello".getBytes(StandardCharsets.UTF_8),
            new byte[]{(byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o'},
            ByteString.copyFromUtf8("hello"),
            (byte)42,
            (short)42,
            42,
            42L,
            BigInteger.valueOf(42L),
            BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN),
            BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.TEN),
            3.14f,
            3.14,
            Float.NaN,
            Double.NaN,
            true,
            false,
            UUID.randomUUID(),
            TestRecordsEnumProto.MyShapeRecord.Size.SMALL,
            TestRecordsEnumProto.MyShapeRecord.Size.SMALL.getValueDescriptor(),
            TestRecordsEnumProto.MyShapeRecord.Size.SMALL.getNumber(),
            (long)TestRecordsEnumProto.MyShapeRecord.Size.SMALL.getNumber(),
            Tuple.from("hello", null),
            Arrays.asList("hello", null),
            FDBRecordVersion.firstInDBVersion(1066L),
            Versionstamp.fromBytes(FDBRecordVersion.firstInDBVersion(1066L).toBytes()),
            FDBRecordVersion.incomplete(1415),
            Versionstamp.incomplete(1415)
    );

    @Nonnull
    private byte[] toBytes(@Nullable Object value) {
        Object tupleValue = TupleTypeUtil.toTupleAppropriateValue(value);
        if (tupleValue instanceof Versionstamp && !((Versionstamp)tupleValue).isComplete()) {
            return Tuple.from(tupleValue).packWithVersionstamp();
        } else {
            return Tuple.from(tupleValue).pack();
        }
    }

    @Nonnull
    public static Stream<Object> valueEquivalenceSource() {
        return VALUES.stream();
    }

    @ParameterizedTest(name = "valueEquivalenceSource [value = {0}]")
    @MethodSource("valueEquivalenceSource")
    public void valueEquivalence(Object value) {
        Object normalizedValue = TupleTypeUtil.toTupleEquivalentValue(value);
        byte[] tupleRep = toBytes(value);

        if (normalizedValue == null) {
            if (value != null) {
                assertThat(value, instanceOf(Key.Evaluated.NullStandin.class));
            }
        } else {
            assertNotNull(value);
            if (value.getClass().equals(normalizedValue.getClass()) && !(value instanceof List<?>) && !(value instanceof Tuple)) {
                assertSame(value, normalizedValue);
            }
        }

        for (Object otherValue : VALUES) {
            Object otherNormalizedValue = TupleTypeUtil.toTupleEquivalentValue(otherValue);
            byte[] otherTupleRep = toBytes(otherValue);

            if (Arrays.equals(tupleRep, otherTupleRep)) {
                assertEquals(normalizedValue, otherNormalizedValue);
                if (normalizedValue != null) {
                    assertNotNull(otherNormalizedValue);
                    assertEquals(normalizedValue.hashCode(), otherNormalizedValue.hashCode());
                }
            } else {
                assertNotEquals(normalizedValue, otherNormalizedValue);
            }
        }
    }

    @Test
    public void listEquivalence() {
        List<Object> packableValues = VALUES.stream().filter(value -> {
            if (value instanceof FDBRecordVersion) {
                return ((FDBRecordVersion)value).isComplete();
            } else if (value instanceof Versionstamp) {
                return ((Versionstamp)value).isComplete();
            } else {
                return true;
            }
        }).collect(Collectors.toList());

        List<Object> equivalentList = TupleTypeUtil.toTupleEquivalentList(packableValues);
        Tuple tupleWithNormalizing = Tuple.fromList(TupleTypeUtil.toTupleAppropriateList(equivalentList));
        Tuple tupleWithoutNormalizing = Tuple.fromList(TupleTypeUtil.toTupleAppropriateList(packableValues));
        assertThat(TupleHelpers.equals(tupleWithNormalizing, tupleWithoutNormalizing), is(true));
        assertArrayEquals(tupleWithoutNormalizing.pack(), tupleWithNormalizing.pack());

        Tuple deserializedTuple = Tuple.fromBytes(tupleWithNormalizing.pack());
        assertThat(TupleHelpers.equals(tupleWithNormalizing, deserializedTuple), is(true));
    }
}
