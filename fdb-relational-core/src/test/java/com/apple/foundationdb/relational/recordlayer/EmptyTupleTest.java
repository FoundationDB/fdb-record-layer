/*
 * EmptyTupleTest.java
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

import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;


class EmptyTupleTest {

    @Test
    void getNumFields() {
        assertThat(EmptyTuple.INSTANCE.getNumFields()).isZero();
    }

    @Test
    void getObject() {
        assertThat(EmptyTuple.INSTANCE.getObject(-1)).isNull();
        assertThat(EmptyTuple.INSTANCE.getObject(0)).isNull();
        assertThat(EmptyTuple.INSTANCE.getObject(10)).isNull();
    }

    @Test
    void getOtherTypes() {
        assertThatExceptionOfType(InvalidColumnReferenceException.class)
                .isThrownBy(() -> EmptyTuple.INSTANCE.getFloat(0));
        assertThatExceptionOfType(InvalidColumnReferenceException.class)
                .isThrownBy(() -> EmptyTuple.INSTANCE.getLong(0));
        assertThatExceptionOfType(InvalidColumnReferenceException.class)
                .isThrownBy(() -> EmptyTuple.INSTANCE.getDouble(0));
        assertThatExceptionOfType(InvalidColumnReferenceException.class)
                .isThrownBy(() -> EmptyTuple.INSTANCE.getBytes(0));
        assertThatExceptionOfType(InvalidColumnReferenceException.class)
                .isThrownBy(() -> EmptyTuple.INSTANCE.getTuple(0));
        assertThatExceptionOfType(InvalidColumnReferenceException.class)
                .isThrownBy(() -> EmptyTuple.INSTANCE.getArray(0));
        assertThatExceptionOfType(InvalidColumnReferenceException.class)
                .isThrownBy(() -> EmptyTuple.INSTANCE.getString(0));
    }

    @Test
    void testEqualTo() {
        assertThat(EmptyTuple.INSTANCE).isEqualTo(new EmptyTuple());
    }

    @Test
    void testToString() {
        assertThat(EmptyTuple.INSTANCE.toString()).isEqualTo("()");
    }

    @Test
    void testHashCode() {
        assertThat(EmptyTuple.INSTANCE.hashCode()).isEqualTo(0);
    }

}
