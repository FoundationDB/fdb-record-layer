/*
 * CrossSchemaTypeCollisionTest.java
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

/**
 * Tests that two schemas defining a table with the same name but different columns do not
 * cause TypeRepository collisions. The type namespacing fix in LogicalOperator prefixes the
 * proto type name with the schema name (e.g. "SECONDARY.THINGS" vs "THINGS") so that
 * TypeRepository can distinguish them.
 */
public class CrossSchemaTypeCollisionTest {

    // Primary schema: THINGS(id, description) — note: different columns from secondary
    private static final String PRIMARY_TEMPLATE_DEF =
            "CREATE TABLE THINGS (id BIGINT, description STRING, PRIMARY KEY(id))";

    // Secondary schema: also named THINGS, but with a different column (quantity instead of description)
    private static final String SECONDARY_TEMPLATE_NAME = "CrossSchemaTypeCollisionTest_SECONDARY_TEMPLATE";
    private static final String SECONDARY_TEMPLATE_DEF =
            "CREATE TABLE THINGS (id BIGINT, quantity BIGINT, PRIMARY KEY(id))";
    private static final String SECONDARY_SCHEMA_NAME = "SECONDARY_SCHEMA";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(CrossSchemaTypeCollisionTest.class, PRIMARY_TEMPLATE_DEF);

    @RegisterExtension
    @Order(2)
    public final SchemaTemplateRule secondaryTemplateRule = new SchemaTemplateRule(
            SECONDARY_TEMPLATE_NAME, Options.none(), null, SECONDARY_TEMPLATE_DEF);

    @RegisterExtension
    @Order(3)
    public final SchemaRule secondarySchemaRule = new SchemaRule(
            SECONDARY_SCHEMA_NAME, URI.create("/TEST/CrossSchemaTypeCollisionTest"), SECONDARY_TEMPLATE_NAME, Options.none());

    @RegisterExtension
    @Order(4)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(db::getConnectionUri)
            .withSchema(db.getSchemaName());

    @RegisterExtension
    @Order(5)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    public CrossSchemaTypeCollisionTest() throws SQLException {
    }

    @BeforeEach
    void setup() throws Exception {
        Utils.enableCascadesDebugger();
        statement.execute("INSERT INTO THINGS VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey')");
        try (var secondaryConn = DriverManager.getConnection(db.getConnectionUri().toString())) {
            secondaryConn.setSchema(SECONDARY_SCHEMA_NAME);
            try (var stmt = secondaryConn.createStatement()) {
                stmt.execute("INSERT INTO THINGS VALUES (1, 10), (2, 20), (3, 30)");
            }
        }
    }

    @Test
    void joinSameNamedTablesFromDifferentSchemas() throws Exception {
        // Both schemas define THINGS but with different columns.
        // Without type namespacing, TypeRepository would throw an IllegalArgumentException
        // because two Type.Record objects with different fields but the same storageName ("THINGS")
        // cannot both be registered.
        try (var rs = statement.executeQuery(
                "SELECT a.description, b.quantity FROM THINGS AS a" +
                " JOIN " + SECONDARY_SCHEMA_NAME + ".THINGS AS b ON a.id = b.id ORDER BY a.id")) {
            ResultSetAssert.assertThat(rs)
                    .hasNextRow().hasColumns(Map.of("description", "Widget",    "quantity", 10L))
                    .hasNextRow().hasColumns(Map.of("description", "Gadget",    "quantity", 20L))
                    .hasNextRow().hasColumns(Map.of("description", "Doohickey", "quantity", 30L))
                    .hasNoNextRow();
        }
    }

    @Test
    void querySameNamedTableInSecondarySchemaAlone() throws Exception {
        try (var rs = statement.executeQuery(
                "SELECT quantity FROM " + SECONDARY_SCHEMA_NAME + ".THINGS ORDER BY id")) {
            ResultSetAssert.assertThat(rs)
                    .hasNextRow().hasColumns(Map.of("quantity", 10L))
                    .hasNextRow().hasColumns(Map.of("quantity", 20L))
                    .hasNextRow().hasColumns(Map.of("quantity", 30L))
                    .hasNoNextRow();
        }
    }
}
