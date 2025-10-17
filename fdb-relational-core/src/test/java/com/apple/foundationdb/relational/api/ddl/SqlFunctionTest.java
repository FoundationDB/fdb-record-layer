/*
 * SqlFunctionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.AbstractMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.utils.PermutationIterator;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.google.common.collect.Streams;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.relational.matchers.SchemaTemplateMatchers.containsRoutinesInAnyOrder;
import static com.apple.foundationdb.relational.matchers.SchemaTemplateMatchers.routine;
import static com.apple.foundationdb.relational.utils.RelationalAssertions.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

/**
 * Contains a number of tests for creating SQL functions.
 */
public class SqlFunctionTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(DdlStatementParsingTest.class, TestSchemas.books());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @BeforeAll
    public static void setup() {
        Utils.enableCascadesDebugger();
    }

    private static final String[] validPrimitiveDataTypes = new String[]{
            "integer", "bigint", "double", "boolean", "string", "bytes"
    };

    @Nonnull
    public static Stream<Arguments> columnTypePermutations() {
        int numColumns = 2;
        final List<String> items = List.of(validPrimitiveDataTypes);

        final PermutationIterator<String> permutations = PermutationIterator.generatePermutations(items, numColumns);
        return permutations.stream().map(Arguments::of);
    }

    @ParameterizedTest(name = "create sql function with " + ARGUMENTS_PLACEHOLDER + " parameters")
    @MethodSource("columnTypePermutations")
    void createSqlFunctionWorksVariousTypes(List<String> types) throws Exception {
        final var parametersString = Streams.mapWithIndex(types.stream(), (type, idx) -> "IN PARAM" + idx + " " + type)
                .collect(Collectors.joining(", ", "(", ")"));
        final var columnsString = Streams.mapWithIndex(types.stream(), (type, idx) -> "COL" + idx + " " + type)
                .collect(Collectors.joining(", ", "(", ", PRIMARY KEY (COL0))"));
        final var conditionString = Streams.mapWithIndex(types.stream(), (type, idx) -> "COL" + idx + " = " + " PARAM" + idx)
                .collect(Collectors.joining(" AND "));
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T " + columnsString +
                        "CREATE FUNCTION SQ" + parametersString + " AS SELECT * FROM T WHERE " + conditionString),
                containsRoutinesInAnyOrder(routine("SQ", "CREATE FUNCTION SQ" + parametersString +
                        " AS SELECT * FROM T WHERE " + conditionString)));
    }

    void shouldWorkWithInjectedFactory(@Nonnull final String query, @Nonnull final MetadataOperationsFactory metadataOperationsFactory)
            throws Exception {
        shouldWorkWithInjectedFactory(query, PreparedParams.empty(), metadataOperationsFactory);
    }

    void shouldWorkWithInjectedFactory(@Nonnull final String query, @Nonnull final PreparedParams preparedParams,
                                       @Nonnull final MetadataOperationsFactory metadataOperationsFactory)
            throws Exception {
        connection.setAutoCommit(false);
        connection.getUnderlyingEmbeddedConnection().createNewTransaction();
        DdlTestUtil.getPlanGenerator(connection.getUnderlyingEmbeddedConnection(), database.getSchemaTemplateName(),
                "/SqlFunctionTest", metadataOperationsFactory, preparedParams).getPlan(query);
        connection.rollback();
        connection.setAutoCommit(true);
    }

    @Nonnull
    SchemaTemplate ddl(@Nonnull final String sql) throws Exception {
        return ddl(sql, PreparedParams.empty());
    }

    @Nonnull
    SchemaTemplate ddl(@Nonnull final String sql, @Nonnull final PreparedParams preparedParams) throws Exception {
        final AtomicReference<SchemaTemplate> t = new AtomicReference<>();
        shouldWorkWithInjectedFactory(sql, preparedParams, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull final SchemaTemplate template,
                                                                      @Nonnull final Options ignored) {
                t.set(template);
                return txn -> {
                };
            }
        });
        return t.get();
    }

    @Test
    void createSqlFunctionWorks() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                "CREATE FUNCTION SQ(IN S BIGINT) AS SELECT * FROM T WHERE b < S"),
                containsRoutinesInAnyOrder(routine("SQ", "CREATE FUNCTION SQ(IN S BIGINT) AS SELECT * FROM T WHERE b < S")));
    }

    @Test
    void createTwoSqlFunctionWorks() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION SQ1(IN S BIGINT) AS SELECT * FROM T WHERE b < S " +
                        "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM T WHERE b < S"),
                containsRoutinesInAnyOrder(routine("SQ1", "CREATE FUNCTION SQ1(IN S BIGINT) AS SELECT * FROM T WHERE b < S"),
                        routine("SQ2", "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM T WHERE b < S")));
    }

    @Test
    void createSqlFunctionBeforeReferencedTableWorks() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE FUNCTION SQ1(IN S BIGINT) AS SELECT * FROM T WHERE b < S " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM T WHERE b < S"),
                containsRoutinesInAnyOrder(routine("SQ1", "CREATE FUNCTION SQ1(IN S BIGINT) AS SELECT * FROM T WHERE b < S"),
                        routine("SQ2", "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM T WHERE b < S")));
    }

    @Test
    void createSqlFunctionsWithDependenciesOnOtherFunctionsWork() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION SQ1() AS SELECT * FROM T WHERE b < 100 " +
                        "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM SQ1() WHERE b < S"),
                containsRoutinesInAnyOrder(routine("SQ1", "CREATE FUNCTION SQ1() AS SELECT * FROM T WHERE b < 100"),
                        routine("SQ2", "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM SQ1() WHERE b < S")));
    }

    @Test
    void createSqlFunctionsWithDeepDependenciesOnOtherFunctionsWithParameters() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION SQ1(IN Q BIGINT) AS SELECT * FROM T WHERE b < Q " +
                        "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM SQ1(Q => 100 + S) WHERE b < S"),
                containsRoutinesInAnyOrder(routine("SQ1", "CREATE FUNCTION SQ1(IN Q BIGINT) AS SELECT * FROM T WHERE b < Q"),
                        routine("SQ2", "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM SQ1(Q => 100 + S) WHERE b < S")));
    }

    @Test
    void recursiveSqlFunctionsAreNotSupported() {
        // not the best error, but it is failing, as expected, in resolving self-referencing functions as a natural
        // side effect of resolution logic.
        assertThrows(() -> ddl("CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                "CREATE FUNCTION RecFunc(IN Q BIGINT) AS SELECT * FROM RecFunc(0) WHERE b < Q "))
                .hasErrorCode(ErrorCode.UNDEFINED_FUNCTION);
    }

    @Test
    void indirectlyRecursiveSqlFunctionsAreNotSupported() {
        // not the best error, but it is failing, as expected, in resolving self-referencing functions as a natural
        // side effect of resolution logic.
        assertThrows(() -> ddl("CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                "CREATE FUNCTION SQ1(IN Q BIGINT) AS SELECT * FROM SQ2(0) WHERE b < Q " +
                "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM SQ1(Q => 100 + S) WHERE b < S"))
                .hasErrorCode(ErrorCode.UNDEFINED_FUNCTION);
    }

    @Test
    void functionWithDefaultValues() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION SQ1(IN Q BIGINT DEFAULT 42) AS SELECT * FROM T WHERE b < Q " +
                        "CREATE FUNCTION SQ2(IN S BIGINT DEFAULT 100) AS SELECT * FROM SQ1(Q => 100 + S) WHERE b < S"),
                containsRoutinesInAnyOrder(routine("SQ1", "CREATE FUNCTION SQ1(IN Q BIGINT DEFAULT 42) AS SELECT * FROM T WHERE b < Q"),
                        routine("SQ2", "CREATE FUNCTION SQ2(IN S BIGINT DEFAULT 100) AS SELECT * FROM SQ1(Q => 100 + S) WHERE b < S")));
    }

    @Test
    void functionWithDefaultValuesInvocation() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION SQ1(IN Q BIGINT DEFAULT 42) AS SELECT * FROM T WHERE b < Q " +
                        "CREATE FUNCTION SQ2(IN S BIGINT DEFAULT 100) AS SELECT * FROM SQ1() WHERE b < S"),
                containsRoutinesInAnyOrder(routine("SQ1", "CREATE FUNCTION SQ1(IN Q BIGINT DEFAULT 42) AS SELECT * FROM T WHERE b < Q"),
                        routine("SQ2", "CREATE FUNCTION SQ2(IN S BIGINT DEFAULT 100) AS SELECT * FROM SQ1() WHERE b < S")));
    }

    @Test
    void nestedFunctionInvocationUnnamedArguments() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION SQ1(IN Q BIGINT) AS SELECT * FROM T WHERE b < Q " +
                        "CREATE FUNCTION SQ2(IN S BIGINT DEFAULT 100) AS SELECT * FROM SQ1(100) WHERE b < S"),
                containsRoutinesInAnyOrder(routine("SQ1", "CREATE FUNCTION SQ1(IN Q BIGINT) AS SELECT * FROM T WHERE b < Q"),
                        routine("SQ2", "CREATE FUNCTION SQ2(IN S BIGINT DEFAULT 100) AS SELECT * FROM SQ1(100) WHERE b < S")));
    }

    @Test
    void nestedFunctionInvocationUnnamedArgumentsMismatch() {
        assertThrows(() -> ddl("CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                "CREATE FUNCTION SQ1(IN Q BIGINT, IN R BIGINT) AS SELECT * FROM T WHERE b < Q " +
                "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM SQ1(100) WHERE b < S"))
                .hasErrorCode(ErrorCode.UNDEFINED_FUNCTION);
    }

    @Test
    void nestedFunctionInvocationUnnamedArgumentsTypeMismatch() {
        assertThrows(() -> ddl("CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                "CREATE FUNCTION SQ1(IN Q BIGINT, IN R BIGINT) AS SELECT * FROM T WHERE b < Q " +
                "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM SQ1('a', 'b') WHERE b < S"))
                .hasErrorCode(ErrorCode.UNDEFINED_FUNCTION);
    }

    @Test
    void createFunctionWithAmbiguousParameterNamesCase1() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION SQ(IN a BIGINT) AS SELECT * FROM T WHERE b < a"),
                containsRoutinesInAnyOrder(routine("SQ", "CREATE FUNCTION SQ(IN a BIGINT) AS SELECT * FROM T WHERE b < a")));
    }

    @Test
    void createFunctionWithAmbiguousParameterNamesCase2() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION SQ(IN a BIGINT) AS SELECT * FROM T WHERE T.a < a"),
                containsRoutinesInAnyOrder(routine("SQ", "CREATE FUNCTION SQ(IN a BIGINT) AS SELECT * FROM T WHERE T.a < a")));
    }

    @Test
    void createFunctionWithAmbiguousParameterNamesCase3() throws Exception {
        assertThrows(() -> ddl("CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                "CREATE FUNCTION SQ1(IN A BIGINT, IN A BIGINT) AS SELECT * FROM T WHERE b < 42 "))
                .hasErrorCode(ErrorCode.INVALID_FUNCTION_DEFINITION);
    }

    @Test
    void createFunctionWithAmbiguousParameterNamesCase4() throws Exception {
        assertThrows(() -> ddl("CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                "CREATE FUNCTION SQ1(IN A BIGINT, IN B STRING, IN A BIGINT) AS SELECT * FROM T WHERE b < 42 "))
                .hasErrorCode(ErrorCode.INVALID_FUNCTION_DEFINITION);
    }

    @Test
    void createFunctionWithoutExplicitParmeterMode() throws Exception {
        assertThat(ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION SQ1(S BIGINT) AS SELECT * FROM T WHERE b < S " +
                        "CREATE FUNCTION SQ2(S BIGINT) AS SELECT * FROM T WHERE b < S"),
                containsRoutinesInAnyOrder(routine("SQ1", "CREATE FUNCTION SQ1(S BIGINT) AS SELECT * FROM T WHERE b < S"),
                        routine("SQ2", "CREATE FUNCTION SQ2(S BIGINT) AS SELECT * FROM T WHERE b < S")));
    }

    @Test
    void definingTempFunctionInSchemaTemplateDefinitionThrows() throws Exception {
        assertThrows(() -> ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE TEMPORARY FUNCTION SQ1(S BIGINT) ON COMMIT DROP FUNCTION AS SELECT * FROM T WHERE b < S " +
                        "CREATE FUNCTION SQ2(S BIGINT) AS SELECT * FROM T WHERE b < S"))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void definingNonTemporaryFunctionWithPreparedParametersThrows() {
        assertThrows(() -> ddl("CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                "CREATE FUNCTION SQ1(IN Q BIGINT, IN R BIGINT) AS SELECT * FROM T WHERE b < Q " +
                "CREATE FUNCTION SQ2(IN S BIGINT) AS SELECT * FROM SQ1(100) WHERE b < ?",
                PreparedParams.ofUnnamed(Map.of(1, 42L))))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void definingFunctionThatConflictsWithATableDefinition() {
        assertThrows(() -> ddl("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                        "CREATE FUNCTION T(IN Q BIGINT, IN R BIGINT) AS SELECT * FROM T WHERE b < Q "))
                .hasErrorCode(ErrorCode.INVALID_SCHEMA_TEMPLATE)
                .containsInMessage("table with name 'T' already exists");
    }

    @Test
    void definingTableThatConflictsWithAFunctionDefinition() {
        assertThrows(() -> ddl("CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(a BIGINT, b BIGINT, primary key(a)) " +
                "CREATE FUNCTION U(IN Q BIGINT DEFAULT 0, IN R BIGINT DEFAULT 0) AS SELECT * FROM T WHERE b < Q " +
                "CREATE TABLE U(a BIGINT, b BIGINT, primary key(a))"))
                .hasErrorCode(ErrorCode.INVALID_SCHEMA_TEMPLATE)
                .containsInMessage("table with name 'U' already exists");
    }
}
