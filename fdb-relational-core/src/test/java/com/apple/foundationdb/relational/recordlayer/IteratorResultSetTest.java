/*
 * IteratorResultSetTest.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;

class IteratorResultSetTest {

    @Test
    void continuationAtBeginning() throws RelationalException, SQLException {
        FieldDescription[] fields = new FieldDescription[]{
                FieldDescription.primitive("testField", Types.VARCHAR, DatabaseMetaData.columnNoNulls)
        };
        try (IteratorResultSet irs = new IteratorResultSet(new RelationalStructMetaData(fields), Collections.singleton((Row) (new ArrayRow(new Object[]{"test"}))).iterator(), 0)) {
            Continuation shouldBeStart = irs.getContinuation();
            Assertions.assertTrue(shouldBeStart.atBeginning(), "Is not at beginning!");
            Assertions.assertNull(shouldBeStart.getBytes(), "Incorrect byte[] for continuation!");

            //now iterate
            irs.next();

            //now the continuation should be at the end
            Continuation end = irs.getContinuation();

            Assertions.assertTrue(end.atEnd(), "Is not at end!");
            Assertions.assertArrayEquals(new byte[]{}, end.getBytes());
        }
    }
}
