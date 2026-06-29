/*
 * TableSerDeTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

/**
 * Tests for serializing and deserializing {@link RecordTypeTable}s.
 */
class RecordTypeTableSerDeTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(relationalExtension, RecordTypeTableSerDeTest.class,
            """
            CREATE TABLE t1(a bigint, b string, c bytes, d boolean, e integer, f float, g double, h uuid, PRIMARY KEY(a))
            """);

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(relationalExtension, db::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema(db.getSchemaName());

    private void roundTripT1(Map<String, Object> fieldData) throws SQLException {
        try (RelationalStatement statement = connection.createStatement()) {
            var builder = EmbeddedRelationalStruct.newBuilder();
            for (Map.Entry<String, Object> entry : fieldData.entrySet()) {
                builder.addObject(entry.getKey(), entry.getValue());
            }
            statement.executeInsert("T1", builder.build());
            try (RelationalResultSet getRes = statement.executeGet("T1", new KeySet().setKeyColumn("A", fieldData.get("A")), Options.NONE)) {
                ResultSetAssert.assertThat(getRes)
                        .hasNextRow()
                        .row()
                        .containsColumnsByName(fieldData);
                ResultSetAssert.assertThat(getRes).hasNoNextRow();
            }
        }
    }

    @Test
    void setLongKey() throws SQLException {
        roundTripT1(Map.of("A", 100L));
    }

    @Test
    void setString() throws SQLException {
        roundTripT1(Map.of("A", 101L, "B", "hello"));
    }

    @Test
    void setBytes() throws SQLException {
        roundTripT1(Map.of("A", 102L, "C", new byte[]{0x01, 0x02}));
    }

    @Test
    void setBoolean() throws SQLException {
        roundTripT1(Map.of("A", 103L, "D", true));
    }

    @Test
    void setInt() throws SQLException {
        roundTripT1(Map.of("A", 104L, "E", 42));
    }

    @Test
    void setFloat() throws SQLException {
        roundTripT1(Map.of("A", 105L, "F", 3.14f));
    }

    @Test
    void setDouble() throws SQLException {
        roundTripT1(Map.of("A", 106L, "G", 2.72d));
    }

    @Test
    void setUuid() throws SQLException {
        roundTripT1(Map.of("A", 107L, "H", UUID.randomUUID()));
    }
}
