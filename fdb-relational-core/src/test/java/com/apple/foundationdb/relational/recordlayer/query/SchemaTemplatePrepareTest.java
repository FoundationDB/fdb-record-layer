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

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.utils.Ddl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

public class SchemaTemplatePrepareTest {
    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " PREPARE by_col1 FROM 'select * from t1 where col1 = 1'" +
                    " PREPARE by_id FROM 'select * from t1 where id = 1'";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

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
            Assertions.assertEquals("select * from t1 where col1 = 1", prepareStatements.get("BY_COL1"));
            Assertions.assertEquals("select * from t1 where id = 1", prepareStatements.get("BY_ID"));
        }
    }

    @Test
    void queryWithPrepareStatements() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/PREPARE_DB2"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1), (2, 10, 2), (3, 20, 3), (4, 20, 4), (5, 30, 5)");
            }

            try (var ps = connection.prepareStatement("SELECT * FROM T1 WHERE col1 = ?")) {
                ps.setLong(1, 10);
                try (RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(1, rs.getLong("ID"));
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2, rs.getLong("ID"));
                    Assertions.assertFalse(rs.next());
                }
            }

            try (var ps = connection.prepareStatement("SELECT * FROM T1 WHERE id = ?")) {
                ps.setLong(1, 3);
                try (RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(3, rs.getLong("ID"));
                    Assertions.assertEquals(20, rs.getLong("COL1"));
                    Assertions.assertEquals(3, rs.getLong("COL2"));
                    Assertions.assertFalse(rs.next());
                }
            }
        }
    }
}
