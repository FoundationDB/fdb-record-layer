/*
 * SqlVisitorTests.java
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

package com.apple.foundationdb.relational.recordlayer.structuredsql;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.fluentsql.statement.StructuredQuery;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.utils.Ddl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class SqlVisitorTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public SqlVisitorTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    public void addExtraExpressionsToSetAndWhereClause() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42 where pk = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.addSetClause(ef.field("T1", "B"), ef.literal(55).add(ef.literal(44)));
            ef.literal(42).add(ef.field("T1", "B").asInt());
            var generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42,\"B\" = 55 + 44 WHERE pk = 444", generatedQuery);
            updateBuilder.addWhereClause(ef.field("T1", "B").asInt().lessThan(ef.literal(42)));
            generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42,\"B\" = 55 + 44 WHERE pk = 444 AND \"B\" < 42", generatedQuery);
            updateBuilder.addWhereClause(ef.field("T1", "B").isNotNull().or(ef.field("T1", "C").isNull()));
            generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42,\"B\" = 55 + 44 WHERE pk = 444 AND \"B\" < 42 AND \"B\" IS NOT NULL OR \"C\" IS NULL", generatedQuery);
            isValidStatement(ddl.getConnection(), generatedQuery);
        }
    }

    @Test
    public void addReturning() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42 where pk = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.addReturning(ef.parseFragment("\"new\".B"));
            var generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42 WHERE pk = 444 RETURNING \"new\".B", generatedQuery);
            isValidStatement(ddl.getConnection(), generatedQuery);
        }
    }

    @Test
    public void addNestedWhereClause() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42 where pk = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.addWhereClause(ef.field("T1", "B").asInt().lessThan(ef.literal(42)).nested().nested());
            var generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42 WHERE pk = 444 AND ( ( \"B\" < 42 ) )", generatedQuery);
            isValidStatement(ddl.getConnection(), generatedQuery);
        }
    }

    @Test
    public void addQueryOptions() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42 where pk = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            updateBuilder.withOption(StructuredQuery.QueryOptions.DRY_RUN);
            Assertions.assertEquals(Set.of(StructuredQuery.QueryOptions.DRY_RUN), updateBuilder.getOptions());
            var generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42 WHERE pk = 444 OPTIONS (DRY RUN)", generatedQuery);
            isValidStatement(ddl.getConnection(), generatedQuery);
        }
    }

    @Test
    public void rewriteColumnAliases() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set customer_facing_a = 42 where CUSTOMER_FACING_PK = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement, Map.of("CUSTOMER_FACING_A", List.of("a"), "CUSTOMER_FACING_PK", List.of("pk")));
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.addWhereClause(ef.field("T1", "B").asInt().lessThan(ef.literal(42)).nested());
            var generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42 WHERE \"PK\" = 444 AND ( \"B\" < 42 )", generatedQuery);
            isValidStatement(ddl.getConnection(), generatedQuery);
        }
    }

    @Test
    public void rewriteColumnAliasesQuotes() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            connection.setOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true);
            final var updateStatement = "update T1 set \"customer_facing_a\" = 42 where \"CUSTOMER_FACING_PK\" = 444";
            final var updateBuilder = connection.createStatementBuilderFactory().updateStatementBuilder(updateStatement, Map.of("customer_facing_a", List.of("A"), "CUSTOMER_FACING_PK", List.of("PK")));
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.addWhereClause(ef.field("T1", "B").asInt().lessThan(ef.literal(42)).nested());
            var generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42 WHERE \"PK\" = 444 AND ( \"B\" < 42 )", generatedQuery);
            isValidStatement(ddl.getConnection(), generatedQuery);
        }
    }

    @Test
    public void rewriteColumnAliasesInsideUdf() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set customer_facing_a = 42 where CUSTOMER_FACING_PK = greatest(42, customer_FACING_B)";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement,
                    Map.of("CUSTOMER_FACING_A", List.of("a"),
                            "CUSTOMER_FACING_PK", List.of("pk"),
                            "CUSTOMER_FACING_B", List.of("b")));
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.addWhereClause(ef.field("T1", "B").asInt().lessThan(ef.literal(42)).nested());
            var generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42 WHERE \"PK\" = greatest ( 42 , \"B\" ) AND ( \"B\" < 42 )", generatedQuery);
            isValidStatement(ddl.getConnection(), generatedQuery);
        }
    }

    private void isValidStatement(@Nonnull final RelationalConnection connection, @Nonnull final String query) throws Exception {
        final String schemaName = connection.getSchema();
        final EmbeddedRelationalConnection embeddedConnection = (EmbeddedRelationalConnection) connection;
        embeddedConnection.createNewTransaction();
        final AbstractDatabase database = embeddedConnection.getRecordLayerDatabase();
        final FDBRecordStoreBase<?> store = database.loadSchema(schemaName).loadStore().unwrap(FDBRecordStoreBase.class);
        final PlanContext planContext = PlanContext.Builder
                .create()
                .fromDatabase(database)
                .fromRecordStore(store, Options.none())
                .withSchemaTemplate(embeddedConnection.getSchemaTemplate())
                .withMetricsCollector(embeddedConnection.getMetricCollector())
                .build();
        final PlanGenerator planGenerator = PlanGenerator.create(Optional.empty(), planContext, store, Options.NONE);
        Assertions.assertDoesNotThrow(() -> planGenerator.getPlan(query));
    }
}
