/*
 * TupleTest.java
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

package com.apple.foundationdb.tuple;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests related to {@link Tuple} beyond those in the FDB Java binding itself.
 */
public class TupleTest {

    private static final char zeroByteCharacter = 0;
    private static final char weirdCharacter = 3;
    private static final char ffCharacter = 0xff;

    private static List<ExpectedTupleEncoding<?>> tests = ImmutableList.<ExpectedTupleEncoding<?>>builder()
            .add(new ExpectedTupleEncoding<>(null, "\\x00"))
            .add(new ExpectedTupleEncoding<>(new UUID(0, 0),
                    "0\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(new UUID(Long.MIN_VALUE, Long.MIN_VALUE),
                    "0\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(new UUID(Long.MAX_VALUE, Long.MAX_VALUE),
                    "0\\x7f\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\x7f\\xff\\xff\\xff\\xff\\xff\\xff\\xff"))
            .add(new ExpectedTupleEncoding<>(UUID.fromString("21ce5312-52a1-40f4-9bd8-22f6138b31a4"),
                    "0!\\xceS\\x12R\\xa1@\\xf4\\x9b\\xd8\\x22\\xf6\\x13\\x8b1\\xa4"))
            .add(new ExpectedTupleEncoding<>(new byte[] {}, "\\x01\\x00"))
            .add(new ExpectedTupleEncoding<>(new byte[] {0}, "\\x01\\x00\\xff\\x00"))
            .add(new ExpectedTupleEncoding<>(new byte[] {1}, "\\x01\\x01\\x00"))
            .add(new ExpectedTupleEncoding<>(new byte[] {-1}, "\\x01\\xff\\x00"))
            .add(new ExpectedTupleEncoding<>(new byte[] {(byte)0xff, 0x03, 0x04, 0x00},
                    "\\x01\\xff\\x03\\x04\\x00\\xff\\x00"))
            .add(new ExpectedTupleEncoding<>("", "\\x02\\x00"))
            .add(new ExpectedTupleEncoding<>("aoesnuth", "\\x02aoesnuth\\x00"))
            .add(new ExpectedTupleEncoding<>(Character.toString(zeroByteCharacter), "\\x02\\x00\\xff\\x00"))
            .add(new ExpectedTupleEncoding<>(Character.toString(ffCharacter), "\\x02\\xc3\\xbf\\x00"))
            .add(new ExpectedTupleEncoding<>(Character.toString(weirdCharacter), "\\x02\\x03\\x00"))
            .add(new ExpectedTupleEncoding<>("数据库", "\\x02\\xe6\\x95\\xb0\\xe6\\x8d\\xae\\xe5\\xba\\x93\\x00"))
            .add(new ExpectedTupleEncoding<>("💯🔥", "\\x02\\xf0\\x9f\\x92\\xaf\\xf0\\x9f\\x94\\xa5\\x00"))
            .add(new ExpectedTupleEncoding<>(0f, " \\x80\\x00\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(Float.MAX_VALUE, " \\xff\\x7f\\xff\\xff"))
            .add(new ExpectedTupleEncoding<>(Float.MIN_VALUE, " \\x80\\x00\\x00\\x01"))
            .add(new ExpectedTupleEncoding<>(Float.NEGATIVE_INFINITY, " \\x00\\x7f\\xff\\xff"))
            .add(new ExpectedTupleEncoding<>(Float.NaN, " \\xff\\xc0\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(Float.intBitsToFloat(0xffffffff), " \\x00\\x00\\x00\\x00")) // also NaN
            .add(new ExpectedTupleEncoding<>(Float.POSITIVE_INFINITY, " \\xff\\x80\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(Float.MIN_NORMAL, " \\x80\\x80\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(0.15230012f, " \\xbe\\x1b\\xf4\\x90"))
            .add(new ExpectedTupleEncoding<>(0d, "!\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(Double.MAX_VALUE, "!\\xff\\xef\\xff\\xff\\xff\\xff\\xff\\xff"))
            .add(new ExpectedTupleEncoding<>(Double.MIN_VALUE, "!\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\x01"))
            .add(new ExpectedTupleEncoding<>(Double.NEGATIVE_INFINITY, "!\\x00\\x0f\\xff\\xff\\xff\\xff\\xff\\xff"))
            .add(new ExpectedTupleEncoding<>(Double.NaN, "!\\xff\\xf8\\x00\\x00\\x00\\x00\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(Double.longBitsToDouble(0xffffffffffffffffL), "!\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00")) // also NaN
            .add(new ExpectedTupleEncoding<>(Double.POSITIVE_INFINITY, "!\\xff\\xf0\\x00\\x00\\x00\\x00\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(Double.MIN_NORMAL, "!\\x80\\x10\\x00\\x00\\x00\\x00\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(0.11577446748173348d, "!\\xbf\\xbd\\xa3e?\\x8b\\xbd\\x88"))
            // TODO: Add tests for Tuple encoding of BigIntegers (https://github.com/FoundationDB/fdb-record-layer/issues/18)
            .add(new ExpectedTupleEncoding<>(true, "'"))
            .add(new ExpectedTupleEncoding<>(false, "&"))
            .add(new ExpectedTupleEncoding<>(Versionstamp.complete(
                    ByteArrayUtil2.unprint("\\x93\\x82\\x8db\\x97;\\xc5\\xfbt\\x9a"), 5180),
                    "3\\x93\\x82\\x8db\\x97;\\xc5\\xfbt\\x9a\\x14<"))
            // Note: nested lists changed between Tuple2 and Tuple (i.e. with the transition to fdb 5.1)
            // These checks will fail if you were to redo them with Tuple2
            .add(new ExpectedTupleEncoding<>(new ArrayList<>(0), "\\x05\\x00"))
            .add(new ExpectedTupleEncoding<>(
                    Arrays.asList(0, 1, "String " + zeroByteCharacter + " andZero", true, null, new byte[] {0, 0x03}),
                    "\\x05\\x14\\x15\\x01\\x02String \\x00\\xff andZero\\x00'\\x00\\xff\\x01\\x00\\xff\\x03\\x00\\x00"))
            .add(new ExpectedTupleEncoding<>(Tuple.from(), "\\x05\\x00"))
            .add(new ExpectedTupleEncoding<>(
                    Tuple.fromList(Arrays.asList(0, 1, "String " + zeroByteCharacter + " andZero", true, null, new byte[] {0, 0x03})),
                    "\\x05\\x14\\x15\\x01\\x02String \\x00\\xff andZero\\x00'\\x00\\xff\\x01\\x00\\xff\\x03\\x00\\x00"))
            .add()
            .build();

    @Nonnull
    static Stream<ExpectedTupleEncoding<?>> testTuple() {
        return tests.stream();
    }

    @ParameterizedTest
    @MethodSource
    void testTuple(ExpectedTupleEncoding<?> test) {
        test.check();
    }

    @Test
    void testAllTuples() {
        for (ExpectedTupleEncoding<?> test : tests) {
            assertNotNull(String.valueOf(test.obj), test.encodedLoggable);
        }

        // Construct a single large tuple of all the items
        final Tuple objects = Tuple.fromList(tests.stream()
                .map(expectedTupleEncoding -> expectedTupleEncoding.obj)
                .collect(Collectors.toList()));
        // The expected encoding of concatenating all the items into a large tuple should be the same
        // as concatenating the expected encodings
        String expected = tests.stream()
                .map(expectedTupleEncoding -> expectedTupleEncoding.encodedLoggable)
                .collect(Collectors.joining());
        assertEquals(expected, ByteArrayUtil2.loggable(objects.pack()));
    }

    private static class ExpectedTupleEncoding<T> {
        @Nullable
        private T obj;
        @Nullable
        private String encodedLoggable;

        public ExpectedTupleEncoding(@Nullable T o, @Nullable String encodedLoggable) {
            obj = o;
            this.encodedLoggable = encodedLoggable;
        }

        public void check() {
            byte[] actualAlone = Tuple.from(obj).pack();
            if (encodedLoggable == null) {
                // This is used to generate new test cases.
                // To add a new test case, create a new ExpectedTupleEncoding with a null string, then run
                // testTuple above. Then copy the encoding into the test case from standard output
                if (actualAlone != null) {
                    System.out.println("\"" +
                            ByteArrayUtil2.loggable(actualAlone).replaceAll("\\\\", "\\\\\\\\") + "\"");
                }
            } else {
                assertEquals(encodedLoggable, ByteArrayUtil2.loggable(actualAlone));
            }
        }

        @Override
        public String toString() {
            return encodedLoggable == null ? Objects.toString(obj) : encodedLoggable;
        }
    }
}
