/*
 * QueryTestUtils.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;

import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.sql.SQLException;

public class QueryTestUtils {

    public static RelationalStruct insertT1Record(@Nonnull final RelationalStatement statement, long pk, long a, long b, long c) throws SQLException {
        var result = EmbeddedRelationalStruct.newBuilder()
                .addLong("PK", pk)
                .addLong("A", a)
                .addLong("B", b)
                .addLong("C", c)
                .build();
        int cnt = statement.executeInsert("T1", result);
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return result;
    }

    public static RelationalStruct insertT1RecordColAIsNull(@Nonnull final RelationalStatement statement, long pk, long b, long c) throws SQLException {
        var result = EmbeddedRelationalStruct.newBuilder()
                .addLong("PK", pk)
                .addLong("B", b)
                .addLong("C", c)
                .build();
        int cnt = statement.executeInsert("T1", result);
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return result;
    }
}
