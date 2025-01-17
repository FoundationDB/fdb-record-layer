/*
 * StatementBuilderTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.fluentsql.expression.Field;
import com.apple.foundationdb.relational.api.fluentsql.statement.StructuredQuery;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StatementBuilderTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public StatementBuilderTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    public void addExtraSetClauseToUpdate() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42 where pk = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.addSetClause(ef.field("T1", "B"), ef.literal(55).add(ef.literal(44)));
            var generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42,\"B\" = 55 + 44 WHERE pk = 444", generatedQuery);
        }
    }

    @Test
    public void setSameFieldMultipleTimes() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42 where pk = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.addSetClause(ef.field("T1", "B"), ef.literal(55).add(ef.literal(44)));
            // case-sensitivity rule is followed for set fields.
            updateBuilder.addSetClause(ef.field("T1", "b"), ef.literal(55).add(ef.literal(44)));
            var generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42,\"B\" = 55 + 44 WHERE pk = 444", generatedQuery);
        }
    }

    @Test
    public void removeSetFieldClause() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42, b = 44 where pk = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.removeSetClause(ef.field("T1", "B"));
            var generatedQuery = updateBuilder.build().getSqlQuery();
            Assertions.assertEquals("UPDATE \"T1\" SET \"A\" = 42 WHERE pk = 444", generatedQuery);
        }
    }

    @Test
    public void removeAllSetFieldClausesThrows() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set b = 44 where pk = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.removeSetClause(ef.field("T1", "B"));
            final var ex = Assertions.assertThrows(RelationalException.class, updateBuilder::build);
            Assertions.assertEquals(ex.getMessage(), "update set clauses is empty");
        }
    }

    @Test
    public void examineSetFields() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42, c = 'bla' where pk = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            var setFields = updateBuilder.getSetClauses().keySet();
            Assertions.assertEquals(Set.of("A", "C"), setFields.stream().map(Field::getName).collect(Collectors.toUnmodifiableSet()));
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.addSetClause(ef.field("T1", "B"), ef.literal(55).add(ef.literal(44)));
            setFields = updateBuilder.getSetClauses().keySet();
            Assertions.assertEquals(Set.of("A", "C", "B"), setFields.stream().map(Field::getName).collect(Collectors.toUnmodifiableSet()));
        }
    }

    @Test
    public void examineWhereClause() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42, c = 'bla' where pk = 444";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            var whereClause = updateBuilder.getWhereClause();
            Assertions.assertNotNull(whereClause);
            Assertions.assertTrue(whereClause.toString().contains("{pk = 444}"));
        }
    }

    @Test
    public void examineMultipleWhereClauses() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42, c = 'bla' where pk = 444 AND (a < 42)";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            var whereClause = updateBuilder.getWhereClause();
            Assertions.assertNotNull(whereClause);
            Assertions.assertTrue(whereClause.toString().contains("{pk = 444 AND ( a < 42 )}"));
        }
    }

    @Nonnull
    static Stream<Arguments> queryOptionsParameters() {
        return Stream.of(
                Arguments.of("options()", Set.of()),
                Arguments.of("options (dry run)", Set.of(StructuredQuery.QueryOptions.DRY_RUN)),
                Arguments.of("options (log query)", Set.of(StructuredQuery.QueryOptions.LOG_QUERY)),
                Arguments.of("options (nocache)", Set.of(StructuredQuery.QueryOptions.NOCACHE)),
                Arguments.of("options (nocache, dry run)", Set.of(StructuredQuery.QueryOptions.NOCACHE, StructuredQuery.QueryOptions.DRY_RUN)),
                Arguments.of("options (nocache, dry run, log query)", Set.of(StructuredQuery.QueryOptions.NOCACHE, StructuredQuery.QueryOptions.DRY_RUN, StructuredQuery.QueryOptions.LOG_QUERY)));
    }

    @DisplayName("examining query options")
    @ParameterizedTest(name = "{index}: {0} should parse into {1}")
    @MethodSource("queryOptionsParameters")
    public void examineQueryOptions(@Nonnull final String queryOptionsClause, @Nonnull final Set<StructuredQuery.QueryOptions> expectedOptions) throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            var updateStatement = "update T1 set a = 42, c = 'bla' where pk = 444 AND (a < 42) " + queryOptionsClause;
            var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            Assertions.assertEquals(expectedOptions, updateBuilder.getOptions());
        }
    }

    @Test
    public void addWhereClauses() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42, c = 'bla' where pk = 444 AND (a < 42)";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            updateBuilder.addWhereClause(ef.field("T1", "B").asLong().lessThan(ef.literal(42L)));
            var whereClause = updateBuilder.getWhereClause();
            Assertions.assertNotNull(whereClause);
            // this is not very nice output, but I don't want to use the SQL visitor to check the string, i.e. I want the test to focus on one API call at a time if possible.
            Assertions.assertTrue(whereClause.toString().contains("AND(({pk = 444 AND ( a < 42 )} : ???) : boolean ∪ ∅,LESS_THAN((B : long ∪ ∅) : long ∪ ∅,42 : long) : boolean ∪ ∅) : boolean ∪ ∅"));
        }
    }

    @Test
    public void examineReturning() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42, c = 'bla' returning *";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            final var returning = updateBuilder.getReturning();
            Assertions.assertEquals(1, returning.size());
            Assertions.assertTrue(returning.get(0).toString().contains("{*} : ???"));
        }
    }

    @Test
    public void examineReturningMultipleColumns() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42, c = 'bla' returning \"old\".a, b, *, c+1, d + (5 + 4)";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            final var returning = updateBuilder.getReturning();
            Assertions.assertEquals(5, returning.size());
            Assertions.assertTrue(returning.get(0).toString().contains("{\"old\" . a} : ???"));
            Assertions.assertTrue(returning.get(1).toString().contains("{b} : ???"));
            Assertions.assertTrue(returning.get(2).toString().contains("{*} : ???"));
            Assertions.assertTrue(returning.get(3).toString().contains("{c + 1} : ???"));
            Assertions.assertTrue(returning.get(4).toString().contains("{d + ( 5 + 4 )} : ???"));
        }
    }

    @Test
    public void setReturningClause() throws Exception {
        final String schemaTemplateString = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateString).build()) {
            final var updateStatement = "update T1 set a = 42, c = 'bla' returning \"old\".a, b, *, c+1, d + (5 + 4)";
            final var updateBuilder = ddl.setSchemaAndGetConnection().createStatementBuilderFactory().updateStatementBuilder(updateStatement);
            var returning = updateBuilder.getReturning();
            Assertions.assertEquals(5, returning.size());
            Assertions.assertTrue(returning.get(0).toString().contains("{\"old\" . a} : ???"));
            Assertions.assertTrue(returning.get(1).toString().contains("{b} : ???"));
            Assertions.assertTrue(returning.get(2).toString().contains("{*} : ???"));
            Assertions.assertTrue(returning.get(3).toString().contains("{c + 1} : ???"));
            Assertions.assertTrue(returning.get(4).toString().contains("{d + ( 5 + 4 )} : ???"));
            updateBuilder.clearReturning();
            final var ef = ddl.getConnection().createExpressionBuilderFactory();
            // old and new psuedo identifiers are special, they can't be used in field resolution.
            updateBuilder.addReturning(ef.parseFragment("\"old\".*"));
            returning = updateBuilder.getReturning();
            Assertions.assertEquals(1, returning.size());
            Assertions.assertTrue(returning.get(0).toString().contains("{\"old\".*} : ???"));
        }
    }
}
