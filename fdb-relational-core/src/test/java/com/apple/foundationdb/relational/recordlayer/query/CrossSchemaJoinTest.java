/*
 * CrossSchemaJoinTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.RelationalStatementRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SchemaRule;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CrossSchemaJoinTest {

    private static final String PRIMARY_TEMPLATE_DEF =
            "CREATE TABLE ITEMS (id BIGINT, name STRING, PRIMARY KEY(id))";

    private static final String SECONDARY_TEMPLATE_NAME = "CrossSchemaJoinTest_SECONDARY_TEMPLATE";
    private static final String SECONDARY_TEMPLATE_DEF =
            "CREATE TABLE TAGS (item_id BIGINT, tag STRING, PRIMARY KEY(item_id))";
    private static final String SECONDARY_SCHEMA_NAME = "SECONDARY_SCHEMA";

    private static final String TERTIARY_TEMPLATE_NAME = "CrossSchemaJoinTest_TERTIARY_TEMPLATE";
    private static final String TERTIARY_TEMPLATE_DEF =
            "CREATE TABLE PRICES (item_id BIGINT, price BIGINT, PRIMARY KEY(item_id))";
    private static final String TERTIARY_SCHEMA_NAME = "TERTIARY_SCHEMA";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(CrossSchemaJoinTest.class, PRIMARY_TEMPLATE_DEF);

    @RegisterExtension
    @Order(2)
    public final SchemaTemplateRule secondaryTemplateRule = new SchemaTemplateRule(
            SECONDARY_TEMPLATE_NAME, Options.none(), null, SECONDARY_TEMPLATE_DEF);

    @RegisterExtension
    @Order(3)
    public final SchemaRule secondarySchemaRule = new SchemaRule(
            SECONDARY_SCHEMA_NAME, URI.create("/TEST/CrossSchemaJoinTest"), SECONDARY_TEMPLATE_NAME, Options.none());

    @RegisterExtension
    @Order(4)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(db::getConnectionUri)
            .withSchema(db.getSchemaName());

    @RegisterExtension
    @Order(5)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @RegisterExtension
    @Order(6)
    public final SchemaTemplateRule tertiaryTemplateRule = new SchemaTemplateRule(
            TERTIARY_TEMPLATE_NAME, Options.none(), null, TERTIARY_TEMPLATE_DEF);

    @RegisterExtension
    @Order(7)
    public final SchemaRule tertiarySchemaRule = new SchemaRule(
            TERTIARY_SCHEMA_NAME, URI.create("/TEST/CrossSchemaJoinTest"), TERTIARY_TEMPLATE_NAME, Options.none());

    public CrossSchemaJoinTest() throws SQLException {
    }

    @BeforeEach
    void setup() throws Exception {
        Utils.enableCascadesDebugger();
        statement.execute("INSERT INTO ITEMS VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry')");
        try (var secondaryConn = DriverManager.getConnection(db.getConnectionUri().toString())) {
            secondaryConn.setSchema(SECONDARY_SCHEMA_NAME);
            try (var stmt = secondaryConn.createStatement()) {
                stmt.execute("INSERT INTO TAGS VALUES (1, 'fruit'), (2, 'yellow'), (3, 'red')");
            }
        }
        try (var tertiaryConn = DriverManager.getConnection(db.getConnectionUri().toString())) {
            tertiaryConn.setSchema(TERTIARY_SCHEMA_NAME);
            try (var stmt = tertiaryConn.createStatement()) {
                stmt.execute("INSERT INTO PRICES VALUES (1, 100), (2, 200), (3, 300)");
            }
        }
    }

    @Test
    void innerJoinAcrossSchemas() throws Exception {
        try (var rs = statement.executeQuery(
                "SELECT a.id, a.name, b.tag FROM ITEMS AS a JOIN " + SECONDARY_SCHEMA_NAME + ".TAGS AS b ON a.id = b.item_id ORDER BY a.id")) {
            ResultSetAssert.assertThat(rs)
                    .hasNextRow().hasColumns(Map.of("id", 1L, "name", "Apple",  "tag", "fruit"))
                    .hasNextRow().hasColumns(Map.of("id", 2L, "name", "Banana", "tag", "yellow"))
                    .hasNextRow().hasColumns(Map.of("id", 3L, "name", "Cherry", "tag", "red"))
                    .hasNoNextRow();
        }
    }

    @Test
    void innerJoinAcrossSchemasWithFilter() throws Exception {
        try (var rs = statement.executeQuery(
                "SELECT a.id, a.name, b.tag FROM ITEMS AS a JOIN " + SECONDARY_SCHEMA_NAME + ".TAGS AS b ON a.id = b.item_id WHERE a.id = 2")) {
            ResultSetAssert.assertThat(rs)
                    .hasNextRow().hasColumns(Map.of("id", 2L, "name", "Banana", "tag", "yellow"))
                    .hasNoNextRow();
        }
    }

    @Test
    void queryFromSecondarySchemaTableAlone() throws Exception {
        try (var rs = statement.executeQuery(
                "SELECT tag FROM " + SECONDARY_SCHEMA_NAME + ".TAGS ORDER BY item_id")) {
            ResultSetAssert.assertThat(rs)
                    .hasNextRow().hasColumns(Map.of("tag", "fruit"))
                    .hasNextRow().hasColumns(Map.of("tag", "yellow"))
                    .hasNextRow().hasColumns(Map.of("tag", "red"))
                    .hasNoNextRow();
        }
    }

    @Test
    void innerJoinThreeSchemas() throws Exception {
        try (var rs = statement.executeQuery(
                "SELECT a.id, a.name, b.tag, c.price FROM ITEMS AS a" +
                " JOIN " + SECONDARY_SCHEMA_NAME + ".TAGS AS b ON a.id = b.item_id" +
                " JOIN " + TERTIARY_SCHEMA_NAME + ".PRICES AS c ON a.id = c.item_id" +
                " ORDER BY a.id")) {
            ResultSetAssert.assertThat(rs)
                    .hasNextRow().hasColumns(Map.of("id", 1L, "name", "Apple",  "tag", "fruit",  "price", 100L))
                    .hasNextRow().hasColumns(Map.of("id", 2L, "name", "Banana", "tag", "yellow", "price", 200L))
                    .hasNextRow().hasColumns(Map.of("id", 3L, "name", "Cherry", "tag", "red",    "price", 300L))
                    .hasNoNextRow();
        }
    }

    @Test
    void innerJoinWithContinuation() throws Exception {
        final Continuation continuation;
        statement.setMaxRows(2);
        try (var rs = statement.executeQuery(
                "SELECT a.id, a.name, b.tag FROM ITEMS AS a JOIN " + SECONDARY_SCHEMA_NAME + ".TAGS AS b ON a.id = b.item_id ORDER BY a.id")) {
            ResultSetAssert.assertThat(rs)
                    .hasNextRow().hasColumns(Map.of("id", 1L, "name", "Apple",  "tag", "fruit"))
                    .hasNextRow().hasColumns(Map.of("id", 2L, "name", "Banana", "tag", "yellow"))
                    .hasNoNextRow();
            continuation = rs.getContinuation();
        }
        Assertions.assertThat(continuation.atEnd()).isFalse();

        try (var ps = connection.prepareStatement("EXECUTE CONTINUATION ?c")) {
            ps.setBytes("c", continuation.serialize());
            try (var rs = ps.executeQuery()) {
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumns(Map.of("id", 3L, "name", "Cherry", "tag", "red"))
                        .hasNoNextRow();
            }
        }
    }

    @Test
    void crossSchemaCatalogGetTables() throws Exception {
        final String database = db.getDatabasePath().getPath();
        final Set<String> tables = new HashSet<>();
        try (var rs = connection.getMetaData().getTables(database, null, null, null)) {
            while (rs.next()) {
                tables.add(rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME"));
            }
        }
        Assertions.assertThat(tables)
                .contains(db.getSchemaName() + ".ITEMS")
                .contains(SECONDARY_SCHEMA_NAME + ".TAGS")
                .contains(TERTIARY_SCHEMA_NAME + ".PRICES");
    }
}
