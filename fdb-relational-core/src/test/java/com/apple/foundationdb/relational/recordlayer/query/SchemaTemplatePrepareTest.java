/*
 * SchemaTemplatePrepareTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.cache.QueryCacheKey;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.utils.Ddl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;

public class SchemaTemplatePrepareTest {

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " PREPARE by_col1 FROM 'select * from t1 where col1 = 10'" +
                    " PREPARE by_id FROM 'select * from t1 where id = 1'";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    private long countCachedPlans(RelationalConnection connection, String templateName) throws SQLException {
        final var embeddedConnection = connection.unwrap(EmbeddedRelationalConnection.class);
        final RelationalPlanCache cache = embeddedConnection.getRecordLayerDatabase().getPlanCache();
        if (cache == null) {
            return 0;
        }
        final Long count = cache.getStats().numSecondaryEntries(templateName);
        return count != null ? count : 0;
    }

    private void showCache(RelationalConnection connection) throws SQLException {
        final var embeddedConnection = connection.unwrap(EmbeddedRelationalConnection.class);
        final RelationalPlanCache cache = embeddedConnection.getRecordLayerDatabase().getPlanCache();
        if (cache == null) {
            System.out.println("[CACHE] no plan cache");
            return;
        }
        for (String key : cache.getStats().getAllKeys()) {
            System.out.println("[CACHE] template: " + key);
            for (QueryCacheKey secondaryKey : cache.getStats().getAllSecondaryKeys(key)) {
                System.out.println("[CACHE]   query: " + secondaryKey.getCanonicalQueryString()
                        + " (version=" + secondaryKey.getSchemaTemplateVersion()
                        + ", userVersion=" + secondaryKey.getUserVersion() + ")");
                var tertiaryMappings = cache.getStats().getAllTertiaryMappings(key, secondaryKey);
                for (var entry : tertiaryMappings.entrySet()) {
                    System.out.println("[CACHE]     plan: " + entry.getValue().explain());
                }
            }
        }
    }

    @Test
    void prepareStatementsStoredInTemplate() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/PREPARE_DB"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final var embeddedConnection = connection.unwrap(EmbeddedRelationalConnection.class);
            embeddedConnection.setAutoCommit(false);
            embeddedConnection.createNewTransaction();
            final var schemaTemplate = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class);
            embeddedConnection.rollback();
            embeddedConnection.setAutoCommit(true);
            final var prepareStatements = schemaTemplate.getPrepareStatements();
            Assertions.assertEquals(2, prepareStatements.size());
            Assertions.assertEquals("select * from t1 where col1 = 10", prepareStatements.get("BY_COL1"));
            Assertions.assertEquals("select * from t1 where id = 1", prepareStatements.get("BY_ID"));
            Assertions.assertEquals(0, countCachedPlans(connection, ddl.getSchemaTemplateName())); // this is not good
        }
    }

    @Test
    void prepareStatementsAfterFirstQuery() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/PREPARE_DB3"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            Assertions.assertEquals(0, countCachedPlans(connection, ddl.getSchemaTemplateName()));

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
            }
            Assertions.assertEquals(2, countCachedPlans(connection, ddl.getSchemaTemplateName()));
        }
    }

    @Test
    void prepareStatementsUsage() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/PREPARE_DB2"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            Assertions.assertEquals(0, countCachedPlans(connection, ddl.getSchemaTemplateName()));

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
            }
            Assertions.assertEquals(2, countCachedPlans(connection, ddl.getSchemaTemplateName()));

            try (var stmt = connection.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where col1 = 10")) {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals(1, rs.getLong("ID"));
                Assertions.assertFalse(rs.next());
            }
            Assertions.assertEquals(2, countCachedPlans(connection, ddl.getSchemaTemplateName()));

            try (var stmt = connection.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where id = 1")) {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals(1, rs.getLong("ID"));
                Assertions.assertEquals(10, rs.getLong("COL1"));
                Assertions.assertEquals(1, rs.getLong("COL2"));
                Assertions.assertFalse(rs.next());
            }
            Assertions.assertEquals(2, countCachedPlans(connection, ddl.getSchemaTemplateName()));
        }
    }

    @Test
    void prepareStatementsUsageParams() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/PREPARE_DB2"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            Assertions.assertEquals(0, countCachedPlans(connection, ddl.getSchemaTemplateName()));

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
            }
            Assertions.assertEquals(2, countCachedPlans(connection, ddl.getSchemaTemplateName()));

            try (var stmt = connection.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where col1 = 20")) {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals(2, rs.getLong("ID"));
                Assertions.assertFalse(rs.next());
            }
            Assertions.assertEquals(2, countCachedPlans(connection, ddl.getSchemaTemplateName()));

            try (var stmt = connection.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where id = 2")) {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals(2, rs.getLong("ID"));
                Assertions.assertEquals(20, rs.getLong("COL1"));
                Assertions.assertEquals(2, rs.getLong("COL2"));
                Assertions.assertFalse(rs.next());
            }
            Assertions.assertEquals(2, countCachedPlans(connection, ddl.getSchemaTemplateName()));
        }
    }

    @Test
    void prepareStatementsUsageJdbcPrepare() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/PREPARE_DB4"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
            }
            Assertions.assertEquals(2, countCachedPlans(connection, ddl.getSchemaTemplateName()));

            try (var ps = connection.prepareStatement("select * from t1 where col1 = ?")) {
                ps.setLong(1, 20);
                try (RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2, rs.getLong("ID"));
                    Assertions.assertFalse(rs.next());
                }
            }
            Assertions.assertEquals(2, countCachedPlans(connection, ddl.getSchemaTemplateName()));

            try (var ps = connection.prepareStatement("select * from t1 where id = ?")) {
                ps.setLong(1, 2);
                try (RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2, rs.getLong("ID"));
                    Assertions.assertEquals(20, rs.getLong("COL1"));
                    Assertions.assertEquals(2, rs.getLong("COL2"));
                    Assertions.assertFalse(rs.next());
                }
            }
            Assertions.assertEquals(2, countCachedPlans(connection, ddl.getSchemaTemplateName()));
        }
    }
}
