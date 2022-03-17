/*
 * ValueTupleTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ValueTupleTest {
    static final ValueTuple testLong = new ValueTuple(5L);
    static final ValueTuple testInt = new ValueTuple(5);
    static final ValueTuple testDouble = new ValueTuple(5.0);
    static final ValueTuple testFloat = new ValueTuple(5.0f);
    static final ValueTuple testString = new ValueTuple("five");
    static final ValueTuple testBytes = new ValueTuple("five".getBytes(StandardCharsets.UTF_8));
    static final ValueTuple testNestableTuple = new ValueTuple(EmptyTuple.INSTANCE);
    static final Tuple fdbTuple = new Tuple().add("five").add(5L).add(5.0f);
    static final ValueTuple testFdbTuple = new ValueTuple(fdbTuple);
    static final ValueTuple testArray = new ValueTuple(List.of("five", 5.0, fdbTuple, EmptyTuple.INSTANCE));

    @Test
    void getLong() throws InvalidTypeException, InvalidColumnReferenceException {
        for (ValueTuple valid : List.of(testLong, testInt, testDouble, testFloat)) {
            assertThat(valid.getLong(0)).isEqualTo(5L);
        }
        for (ValueTuple invalid : List.of(testString, testBytes, testNestableTuple, testFdbTuple, testArray)) {
            assertThatExceptionOfType(InvalidTypeException.class).isThrownBy(
                    () -> invalid.getLong(0)
            );
        }
    }

    @Test
    void getFloat() throws InvalidTypeException, InvalidColumnReferenceException {
        for (ValueTuple valid : List.of(testLong, testInt, testDouble, testFloat)) {
            assertThat(valid.getFloat(0)).isEqualTo(5.0f);
        }
        for (ValueTuple invalid : List.of(testString, testBytes, testNestableTuple, testFdbTuple, testArray)) {
            assertThatExceptionOfType(InvalidTypeException.class).isThrownBy(
                    () -> invalid.getFloat(0)
            );
        }
    }

    @Test
    void getDouble() throws InvalidTypeException, InvalidColumnReferenceException {
        for (ValueTuple valid : List.of(testLong, testInt, testDouble, testFloat)) {
            assertThat(valid.getDouble(0)).isEqualTo(5.0);
        }
        for (ValueTuple invalid : List.of(testString, testBytes, testNestableTuple, testFdbTuple, testArray)) {
            assertThatExceptionOfType(InvalidTypeException.class).isThrownBy(
                    () -> invalid.getDouble(0)
            );
        }
    }

    @Test
    void getString() throws InvalidTypeException, InvalidColumnReferenceException {
        assertThat(testString.getString(0)).isEqualTo("five");
        for (ValueTuple invalid : List.of(testLong, testInt, testDouble, testFloat, testBytes, testNestableTuple, testFdbTuple, testArray)) {
            assertThatExceptionOfType(InvalidTypeException.class).isThrownBy(
                    () -> invalid.getString(0)
            );
        }
    }

    @Test
    void getBytes() throws InvalidTypeException, InvalidColumnReferenceException {
        assertThat(testBytes.getBytes(0)).isEqualTo("five".getBytes(StandardCharsets.UTF_8));
        for (ValueTuple invalid : List.of(testLong, testInt, testDouble, testFloat, testString, testNestableTuple, testFdbTuple, testArray)) {
            assertThatExceptionOfType(InvalidTypeException.class).isThrownBy(
                    () -> invalid.getBytes(0)
            );
        }
    }

    @Test
    void getTuple() throws InvalidTypeException, InvalidColumnReferenceException {
        assertThat(testNestableTuple.getTuple(0)).isEqualTo(EmptyTuple.INSTANCE);
        assertThat(testFdbTuple.getTuple(0)).isEqualTo(new FDBTuple(fdbTuple));
        for (ValueTuple invalid : List.of(testLong, testInt, testDouble, testFloat, testString, testBytes, testArray)) {
            assertThatExceptionOfType(InvalidTypeException.class).isThrownBy(
                    () -> invalid.getTuple(0)
            );
        }
    }

    @Test
    void getArray() throws InvalidTypeException, InvalidColumnReferenceException {
        assertThat(testArray.getArray(0)).containsExactly(new ValueTuple("five"), new ValueTuple(5.0), new FDBTuple(fdbTuple), EmptyTuple.INSTANCE);
        assertThat(testFdbTuple.getArray(0)).containsExactly(new ValueTuple("five"), new ValueTuple(5L), new ValueTuple(5.0f));
        for (ValueTuple invalid : List.of(testLong, testInt, testDouble, testFloat, testString, testBytes, testNestableTuple)) {
            assertThatExceptionOfType(InvalidTypeException.class).isThrownBy(
                    () -> invalid.getArray(0)
            );
        }
    }

    @Test
    void testEquals() {
        assertThat(testLong.equals(null)).isFalse();
        assertThat(testLong.equals(5)).isFalse();
        assertThat(testLong.equals(EmptyTuple.INSTANCE)).isFalse();
        assertThat(testLong.equals(new ValueTuple(6L))).isFalse();
        assertThat(testLong.equals(new ValueTuple(5L))).isTrue();
    }

    @Test
    void testHashCode() {
        assertThat(testLong.hashCode()).isEqualTo(new ValueTuple(5L).hashCode());
    }

    @Test
    void setObject() {
        ValueTuple v = new ValueTuple(5);
        assertThat(v.getObject(0)).isEqualTo(5);
        v.setObject("five");
        assertThat(v.getObject(0)).isEqualTo("five");
    }

    @Test
    void getNumFields() {
        assertThat(testArray.getNumFields()).isOne();
        assertThat(testLong.getNumFields()).isOne();
        assertThat(testNestableTuple.getNumFields()).isOne();
    }

    @Test
    void getOutOfBounds() {
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testLong.getLong(-1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testLong.getLong(1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testFloat.getFloat(-1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testFloat.getFloat(1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testDouble.getDouble(-1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testDouble.getDouble(1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testString.getString(-1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testString.getString(1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testBytes.getBytes(-1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testBytes.getBytes(1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testNestableTuple.getTuple(-1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testNestableTuple.getTuple(1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testArray.getArray(-1));
        assertThatExceptionOfType(InvalidColumnReferenceException.class).isThrownBy(
                () -> testArray.getArray(1));
    }

    @Test
    void testToString() {
        assertThat(testLong.toString()).isEqualTo("(5)");
    }
}
