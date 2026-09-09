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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.RelationalStatementRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SchemaRule;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class CrossSchemaJoinTest {

    private static final String PRIMARY_TEMPLATE_DEF =
            "CREATE TABLE ITEMS (id BIGINT, name STRING, PRIMARY KEY(id))";

    private static final String SECONDARY_TEMPLATE_NAME = "CrossSchemaJoinTest_SECONDARY_TEMPLATE";
    private static final String SECONDARY_TEMPLATE_DEF =
            "CREATE TABLE TAGS (item_id BIGINT, tag STRING, PRIMARY KEY(item_id))";
    private static final String SECONDARY_SCHEMA_NAME = "SECONDARY_SCHEMA";

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
}
