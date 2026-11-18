/*
 * DdlRecordLayerSchemaTemplateAutoQATest.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.ThrowingConsumer;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;

/**
 * Additional end-to-end tests for the schema template DDL in the Record Layer.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class DdlRecordLayerSchemaTemplateAutoQATest {
    @RegisterExtension
    public final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    private void run(@Nonnull ThrowingConsumer<? super RelationalStatement> operation) throws Throwable {
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try (RelationalStatement statement = conn.createStatement()) {
                operation.accept(statement);
            }
        }
    }

    @Nonnull
    private static RecordLayerSchemaTemplate simpleTemplate(boolean storeRowVersions) {
        return RecordLayerSchemaTemplate.newBuilder()
                .setName("AUTO_QA_TEMPLATE")
                .setVersion(3)
                .setStoreRowVersions(storeRowVersions)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("T1")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("ID")
                                .setDataType(DataType.Primitives.NULLABLE_LONG.type())
                                .build())
                        .build())
                .build();
    }

    @Test
    void toBuilderPreservesStoreRowVersions() {
        // BUG AQA-18: RecordLayerSchemaTemplate.toBuilder() does not copy storeRowVersions, so CREATE/DROP TEMPORARY
        // FUNCTION rebinds a template without row versions and __ROW_VERSION becomes unknown. Spec: SchemaTemplate
        // isStoreRowVersions javadoc; TEMPORARY_FUNCTION.rst does not say template options change.
        // toBuilder().build() must give a template with the same options, including store_row_versions
        final RecordLayerSchemaTemplate template = simpleTemplate(true);
        final RecordLayerSchemaTemplate copy = template.toBuilder().build();
        Assertions.assertThat(copy.getName()).isEqualTo(template.getName());
        Assertions.assertThat(copy.getVersion()).isEqualTo(template.getVersion());
        Assertions.assertThat(copy.isEnableLongRows()).isEqualTo(template.isEnableLongRows());
        Assertions.assertThat(copy.isStoreRowVersions()).isTrue();
        Assertions.assertThat(copy.toRecordMetadata().isStoreRecordVersions()).isTrue();
    }

    @Test
    void createTemporaryFunctionKeepsRowVersionColumn() throws Exception {
        // BUG AQA-18: RecordLayerSchemaTemplate.toBuilder() does not copy storeRowVersions, so CREATE/DROP TEMPORARY
        // FUNCTION rebinds a template without row versions and __ROW_VERSION becomes unknown. Spec: SchemaTemplate
        // isStoreRowVersions javadoc; TEMPORARY_FUNCTION.rst does not say template options change.
        // creating a temporary function must not drop the store_row_versions option of the bound template
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk)) " +
                "with options (store_row_versions=true)";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/AUTO_QA_DDL_RV")).relationalExtension(relational)
                .schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10)");
            }
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                try (var rs = statement.executeQuery("select pk, \"__ROW_VERSION\" from t1")) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.getObject(2)).isNotNull();
                }
                statement.execute("create temporary function f1() on commit drop function as select * from t1");
                try (var rs = statement.executeQuery("select pk, \"__ROW_VERSION\" from t1")) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.getLong(1)).isEqualTo(1L);
                    Assertions.assertThat(rs.getObject(2)).isNotNull();
                    Assertions.assertThat(rs.next()).isFalse();
                }
            }
            connection.rollback();
        }
    }

    @Test
    void createSchemaTemplateDuplicateIndexNameAcrossTablesFails() throws Throwable {
        // two indexes with the same name on different tables must be rejected
        final String template = "CREATE SCHEMA TEMPLATE auto_qa_dup_index " +
                "CREATE TABLE t1 (id bigint, a bigint, PRIMARY KEY(id)) " +
                "CREATE TABLE t2 (id bigint, b bigint, PRIMARY KEY(id)) " +
                "CREATE INDEX idx AS SELECT a FROM t1 ORDER BY a " +
                "CREATE INDEX idx AS SELECT b FROM t2 ORDER BY b";
        run(statement -> {
            RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(template))
                    .hasMessageContaining("IDX");
            RelationalAssertions.assertThrowsSqlException(() -> statement.executeQuery("DESCRIBE SCHEMA TEMPLATE auto_qa_dup_index"))
                    .hasErrorCode(ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        });
    }

    @Test
    void createSchemaTemplateQuotedNamesDifferingInCaseAreDistinct() throws Throwable {
        // quoted identifiers keep their case, so "t" and "T" are two different tables
        final String template = "CREATE SCHEMA TEMPLATE auto_qa_quoted_case " +
                "CREATE TABLE \"t\" (id bigint, PRIMARY KEY(id)) " +
                "CREATE TABLE \"T\" (id bigint, PRIMARY KEY(id))";
        run(statement -> {
            statement.executeUpdate(template);
            try (var rs = statement.executeQuery("DESCRIBE SCHEMA TEMPLATE auto_qa_quoted_case")) {
                Assertions.assertThat(rs.next()).isTrue();
                final var tables = rs.getArray("TABLES").getResultSet();
                int count = 0;
                while (tables.next()) {
                    count++;
                }
                Assertions.assertThat(count).isEqualTo(2);
            }
            statement.executeUpdate("DROP SCHEMA TEMPLATE auto_qa_quoted_case");
        });
    }
}
