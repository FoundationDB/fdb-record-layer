/*
 * TupleHelpersTest.java
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

package com.apple.foundationdb.tuple;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TupleHelpersTest {

    static Stream<Arguments> isPrefixTrueCases() {
        UUID uuid1 = UUID.fromString("12345678-1234-1234-1234-123456789abc");
        byte[] data1 = {0x01, 0x02, 0x03};
        byte[] data2 = {0x01, 0x02, 0x03};

        return Stream.of(
                Arguments.of("empty tuples", 
                        Tuple.from(), 
                        Tuple.from()),
                Arguments.of("empty prefix and non-empty target", 
                        Tuple.from(), 
                        Tuple.from("test", 42)),
                Arguments.of("single element match", 
                        Tuple.from("test"), 
                        Tuple.from("test", 42, true)),
                Arguments.of("multiple elements match", 
                        Tuple.from("test", 42, true), 
                        Tuple.from("test", 42, true, "more", "data")),
                Arguments.of("equal tuples", 
                        Tuple.from("test", 42, true), 
                        Tuple.from("test", 42, true)),
                Arguments.of("different data types", 
                        Tuple.from("string", 123L, 4.5f, 6.7d, true), 
                        Tuple.from("string", 123L, 4.5f, 6.7d, true, "more", false)),
                Arguments.of("mixed type match", 
                        Tuple.from("string", 123), 
                        Tuple.from("string", 123L)),
                Arguments.of("binary data", 
                        Tuple.from("test", data1), 
                        Tuple.from("test", data2, "more")),
                Arguments.of("uuids", 
                        Tuple.from("test", uuid1), 
                        Tuple.from("test", uuid1, UUID.randomUUID())),
                Arguments.of("null values", 
                        Tuple.from("test", null), 
                        Tuple.from("test", null, "more")),
                Arguments.of("number variations", 
                        Tuple.from(42, 3.14, -7L), 
                        Tuple.from(42, 3.14, -7L, "suffix")),
                Arguments.of("complex data",
                        Tuple.from("company", 1L, "engineering", 100L),
                        Tuple.from("company", 1L, "engineering", 100L, "employee_data", true, 4.5f)),
                Arguments.of("nested tuple match", 
                        Tuple.from("outer", Tuple.from("inner", 42)), 
                        Tuple.from("outer", Tuple.from("inner", 42), "more")),
                Arguments.of("nested tuple with different depths", 
                        Tuple.from("root", Tuple.from("level1", Tuple.from("level2", "value"))), 
                        Tuple.from("root", Tuple.from("level1", Tuple.from("level2", "value")), "extra", "data")),
                Arguments.of("empty nested tuple", 
                        Tuple.from("outer", Tuple.from()), 
                        Tuple.from("outer", Tuple.from(), "suffix"))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("isPrefixTrueCases")
    void isPrefixShouldReturnTrue(String description, Tuple prefix, Tuple target) {
        assertTrue(TupleHelpers.isPrefix(prefix, target));
    }

    static Stream<Arguments> isPrefixFalseCases() {
        byte[] data1 = {0x01, 0x02, 0x03};
        byte[] data2 = {0x01, 0x02, 0x04};
        byte[] data3 = {0x01, 0x02};
        
        return Stream.of(
                Arguments.of("non-empty prefix and empty target", 
                        Tuple.from("test"), 
                        Tuple.from()),
                Arguments.of("single element mismatch", 
                        Tuple.from("test"), 
                        Tuple.from("different", 42)),
                Arguments.of("multiple elements partial match", 
                        Tuple.from("test", 42, false), 
                        Tuple.from("test", 42, true, "more")),
                Arguments.of("prefix longer than target", 
                        Tuple.from("test", 42, true, "extra"), 
                        Tuple.from("test", 42)),
                Arguments.of("different binary data", 
                        Tuple.from("test", data1), 
                        Tuple.from("test", data2, "more")),
                Arguments.of("null mismatch", 
                        Tuple.from("test", null), 
                        Tuple.from("test", "not-null")),
                Arguments.of("null vs empty string",
                        Tuple.from("test", null),
                        Tuple.from("test", "")),
                Arguments.of("null vs 0",
                        Tuple.from("test", null),
                        Tuple.from("test", 0)),
                Arguments.of("null vs false",
                        Tuple.from("test", null),
                        Tuple.from("test", false)),
                Arguments.of("null vs byte[0]",
                        Tuple.from("test", null),
                        Tuple.from("test", new byte[0])),
                Arguments.of("number mismatch", 
                        Tuple.from(42, 3.14), 
                        Tuple.from(42, 3.15, "suffix")),
                Arguments.of("byte[] prefix of other", 
                        Tuple.from().add(data3), 
                        Tuple.from().add(data2)),
                Arguments.of("string prefix of other", 
                        Tuple.from().add("ap"), 
                        Tuple.from().add("apple")),
                Arguments.of("empty string vs empty byte",
                        Tuple.from().add(""),
                        Tuple.from().add(new byte[0])),
                Arguments.of("nested tuple mismatch", 
                        Tuple.from("outer", Tuple.from("inner", 42)), 
                        Tuple.from("outer", Tuple.from("inner", 43), "more")),
                Arguments.of("nested tuple different structure", 
                        Tuple.from("root", Tuple.from("level1", "value")), 
                        Tuple.from("root", Tuple.from("level2", "value"), "extra")),
                Arguments.of("nested vs non-nested", 
                        Tuple.from("outer", Tuple.from("inner")), 
                        Tuple.from("outer", "inner", "more"))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("isPrefixFalseCases")
    void isPrefixShouldReturnFalse(String description, Tuple prefix, Tuple target) {
        assertFalse(TupleHelpers.isPrefix(prefix, target));
    }
}
