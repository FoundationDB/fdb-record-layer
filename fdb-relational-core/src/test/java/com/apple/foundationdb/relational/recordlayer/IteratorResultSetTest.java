/*
 * IteratorResultSetTest.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.metadata.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

class IteratorResultSetTest {

    @Test
    void continuationAtBeginning() throws SQLException {
        final var type = DataType.StructType.from("STRUCT", List.of(
                DataType.StructType.Field.from("testField", DataType.Primitives.STRING.type(), 0)
        ), true);
        try (IteratorResultSet irs = new IteratorResultSet(RelationalStructMetaData.of(type), Collections.singleton((Row) (new ArrayRow(new Object[]{"test"}))).iterator(), 0)) {
            Assertions.assertThrows(SQLException.class, irs::getContinuation);

            //now iterate
            irs.next();

            //now the continuation should be at the end
            Continuation end = irs.getContinuation();

            Assertions.assertTrue(end.atEnd(), "Is not at end!");
            Assertions.assertArrayEquals(new byte[]{}, end.getExecutionState());
        }
    }
}
