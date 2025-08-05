/*
 * FDBTupleTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FDBTupleTest {
    @Nonnull
    static final FDBTuple emptyTuple;
    @Nonnull
    static final FDBTuple longTuple;
    static final Tuple fdbUnderlyingTuple =
            Tuple.from("five", 5L, 5.0f,
                    Tuple.from("six", 6L, 6.0f,
                            Tuple.from("seven", 7L, 7.0f)
                    )
            );
    static final FDBTuple fdbTuple = new FDBTuple(fdbUnderlyingTuple);

    static {
        try {
            emptyTuple = FDBTuple.fromRow(EmptyTuple.INSTANCE);
            longTuple = FDBTuple.fromRow(new ValueTuple(5L));
        } catch (RelationalException err) {
            throw err.toUncheckedWrappedException();
        }
    }

    @Test
    void setTuple() throws RelationalException {
        FDBTuple tuple = FDBTuple.fromRow(emptyTuple);
        tuple.setTuple(fdbUnderlyingTuple);
        Assertions.assertEquals(fdbUnderlyingTuple, tuple.fdbTuple());
    }

    @Test
    void getNumFields() {
        assertThat(emptyTuple.getNumFields()).isZero();
        assertThat(longTuple.getNumFields()).isOne();
        assertThat(fdbTuple.getNumFields()).isEqualTo(4);
    }

    @Test
    void getObject() throws InvalidColumnReferenceException {
        assertThat(longTuple.getObject(0)).isEqualTo(5L);
        assertThat(fdbTuple.getObject(0)).isEqualTo("five");
        assertThat(fdbTuple.getObject(1)).isEqualTo(5L);
        assertThat(fdbTuple.getObject(2)).isEqualTo(5.0f);
        RelationalAssertions.assertThrows(
                () -> fdbTuple.getObject(-1))
                .hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
        RelationalAssertions.assertThrows(
                () -> fdbTuple.getObject(10))
                .hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
    }

    @Test
    void getTuple() throws InvalidTypeException, InvalidColumnReferenceException {
        assertThatThrownBy(() -> emptyTuple.getRow(-1))
                .isInstanceOf(InvalidColumnReferenceException.class);
        assertThatThrownBy(() -> fdbTuple.getRow(4))
                .isInstanceOf(InvalidColumnReferenceException.class);
        assertThatThrownBy(() -> longTuple.getRow(0))
                .isInstanceOf(InvalidTypeException.class);

        assertThat(fdbTuple.getRow(3))
                .isEqualTo(new FDBTuple(new Tuple().add("six").add(6L).add(6.0f).add(new Tuple().add("seven").add(7L).add(7.0f))));
    }

    @Test
    void getArray() throws InvalidTypeException, InvalidColumnReferenceException {
        assertThatThrownBy(() -> emptyTuple.getArray(-1))
                .isInstanceOf(InvalidColumnReferenceException.class);
        assertThatThrownBy(() -> fdbTuple.getArray(4))
                .isInstanceOf(InvalidColumnReferenceException.class);
        assertThatThrownBy(() -> longTuple.getArray(0))
                .isInstanceOf(InvalidTypeException.class);

        assertThat(fdbTuple.getArray(3))
                .containsExactly(new ValueTuple("six"), new ValueTuple(6L), new ValueTuple(6.0f), new FDBTuple(new Tuple().add("seven").add(7L).add(7.0f)));
    }

    @Test
    void fdbTuple() {
        Tuple t = fdbTuple.fdbTuple();
        Assertions.assertEquals(fdbUnderlyingTuple, t);
    }

    @Test
    void testToString() {
        assertThat(emptyTuple.toString()).isEqualTo("()");
        assertThat(longTuple.toString()).isEqualTo("(5)");
        assertThat(fdbTuple.toString()).isEqualTo("(\"five\", 5, 5.0, (\"six\", 6, 6.0, (\"seven\", 7, 7.0)))");
    }
}
