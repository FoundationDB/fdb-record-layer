/*
 * ImmutableKeyValueTest.java
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
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ImmutableKeyValueTest {

    @Test
    void getNumFields() {
        assertEquals(2, new ImmutableKeyValue(new ValueTuple(1), new ValueTuple(2)).getNumFields());
        assertEquals(3, new ImmutableKeyValue(new ValueTuple(1), new ImmutableKeyValue(new ValueTuple(2), new ValueTuple(3))).getNumFields());
        assertEquals(4, new ImmutableKeyValue(new ValueTuple(1), new FDBTuple(new Tuple().add(2).add(3).add(4))).getNumFields());
        assertEquals(4, new ImmutableKeyValue(new FDBTuple(new Tuple().add(2).add(3).add(4)), new ValueTuple(2)).getNumFields());
    }

    @Test
    void getObject() throws InvalidColumnReferenceException {
        Row row = new ImmutableKeyValue(new FDBTuple(new Tuple().add(2).add(3).add(4)), new ImmutableKeyValue(new ValueTuple(5), new ValueTuple(6)));
        assertThat(row.getObject(0)).isEqualTo(2L);
        assertThat(row.getObject(1)).isEqualTo(3L);
        assertThat(row.getObject(2)).isEqualTo(4L);
        assertThat(row.getObject(3)).isEqualTo(5);
        assertThat(row.getObject(4)).isEqualTo(6);

        RelationalAssertions.assertThrows(
                () -> row.getObject(-1))
                .hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
        RelationalAssertions.assertThrows(
                () -> row.getObject(10))
                .hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
    }
}
