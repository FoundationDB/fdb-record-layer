/*
 * RowStructTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.ImmutableRowStruct;
import com.apple.foundationdb.relational.api.MutableRowStruct;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.RowStruct;
import com.apple.foundationdb.relational.api.metadata.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.List;

public class RowStructTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void wasNullWorks(boolean mutable) throws SQLException {
        final var struct = createStruct(mutable);
        struct.getObject(1);
        Assertions.assertTrue(struct.wasNull());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void wasNullWorksWithToString(boolean mutable) throws SQLException {
        final var struct = createStruct(mutable);
        struct.getObject(1);
        Assertions.assertFalse(struct.toString().isEmpty());
        Assertions.assertTrue(struct.wasNull());
    }

    private static RowStruct createStruct(boolean mutable) {
        final var type = DataType.StructType.from("BLAH", List.of(
                DataType.StructType.Field.from("fInt", DataType.Primitives.NULLABLE_INTEGER.type(), 0),
                DataType.StructType.Field.from("fInt", DataType.Primitives.NULLABLE_LONG.type(), 1)
        ), true);
        final var metadata = RelationalStructMetaData.of(type);
        final var row = new ArrayRow(null, 1L);
        if (mutable) {
            final var toReturn = new MutableRowStruct(metadata);
            toReturn.setRow(row);
            return toReturn;
        } else {
            return new ImmutableRowStruct(row, metadata);
        }
    }
}
