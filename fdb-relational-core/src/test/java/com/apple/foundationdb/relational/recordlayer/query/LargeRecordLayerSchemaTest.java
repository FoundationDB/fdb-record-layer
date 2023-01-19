/*
 * LargeRecordLayerSchemaTest.java
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

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.ResultSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LargeRecordLayerSchemaTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public LargeRecordLayerSchemaTest() {
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 100, 1000})
    void canCreateColumns(int colCount) throws Exception {
        StringBuilder template = new StringBuilder("CREATE TABLE T1(");
        template.append(IntStream.range(0, colCount).mapToObj(LargeRecordLayerSchemaTest::column).map(c -> c + " int64").collect(Collectors.joining(",")));
        template.append(", PRIMARY KEY(").append(column(0)).append("))");
        try (var ddl = Ddl.builder().database(LargeRecordLayerSchemaTest.class.getSimpleName()).relationalExtension(relationalExtension).schemaTemplate(template.toString()).build()) {
            try (RelationalConnection conn = ddl.setSchemaAndGetConnection()) {
                try (var statement = conn.createStatement()) {
                    DynamicMessageBuilder toInsert = statement.getDataBuilder("T1");
                    for (long i = 0; i < colCount; i++) {
                        toInsert.setField(column(i), i);
                    }
                    statement.executeInsert("T1", toInsert.build());
                    try (ResultSet rs = statement.executeQuery("SELECT * from T1")) {
                        Assertions.assertTrue(rs.next());
                        for (long i = 0; i < colCount; i++) {
                            Assertions.assertEquals(i, rs.getLong((int) i + 1));
                        }
                        Assertions.assertFalse(rs.next());
                    }
                }
            }
        }
    }

    @Disabled
    @Test
    void tooManyColumns() {
        int colCount = 10000;
        StringBuilder template = new StringBuilder("CREATE TABLE T1(");
        template.append(IntStream.range(0, colCount).mapToObj(LargeRecordLayerSchemaTest::column).map(c -> c + " int64").collect(Collectors.joining(",")));
        template.append(", PRIMARY KEY(").append(column(0)).append("))");
        RelationalAssertions.assertThrowsSqlException(() ->
                Ddl.builder().database(LargeRecordLayerSchemaTest.class.getSimpleName()).relationalExtension(relationalExtension).schemaTemplate(template.toString()).build())
                .hasErrorCode(ErrorCode.TOO_MANY_COLUMNS);
    }

    private static String column(long colNumber) {
        return "COL" + colNumber;
    }
}
