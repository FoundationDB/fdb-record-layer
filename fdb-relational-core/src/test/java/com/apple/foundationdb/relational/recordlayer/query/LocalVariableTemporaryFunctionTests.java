/*
 * LocalVariableTemporaryFunctionTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.LogAppenderRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

/**
 * Tests for the interaction between transaction-scoped local variables (SET TRANSACTION VARIABLE /
 * GET_VARIABLE(name), see {@link LocalVariableTests}) and table-valued functions -- both
 * temporary ({@code CREATE TEMPORARY FUNCTION}) and permanent ({@code CREATE FUNCTION}).
 */
public class LocalVariableTemporaryFunctionTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public LocalVariableTemporaryFunctionTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void schemaTemplateFunctionWithVariableInBody() throws Exception {
        // Passing GET_VARIABLE(var) at the call site works today; using it directly inside a TVF
        // body requires lazy compilation support, which is a separate work item.
        final String schemaTemplate =
                "create table schfoo(pk bigint, name string, primary key(pk)) " +
                "create function find_names(in target string) as select pk, name from schfoo where name = target";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into schfoo values (1, 'alice'), (2, 'bob'), (3, 'carol')");
            }
            final var conn = ddl.getConnection();

            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select pk, name from find_names(target => GET_VARIABLE(varname))"))
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
            }
            conn.rollback();

            conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable varname = 'alice'");
                try (var rs = stmt.executeQuery("select pk, name from find_names(target => GET_VARIABLE(varname))")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L, "alice").hasNoNextRow();
                }
            }
            conn.commit();

            // Different value, same function call site
            conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable varname = 'bob'");
                try (var rs = stmt.executeQuery("select pk, name from find_names(target => GET_VARIABLE(varname))")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(2L, "bob").hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void filteredIndexSelectedByVariableWithPlanCacheConstraints() throws Exception {
        // A filtered index covers only active=true rows. The TVF is called with GET_VARIABLE(filter)
        // as its argument, so the Cascades planner picks a different plan variant (filtered-index vs.
        // full-scan) depending on the variable's value -- verified via plan-cache hit/miss log messages.
        final String schemaTemplate =
                "create table schbar(pk bigint, active boolean, val bigint, primary key(pk)) " +
                "create index idx_active as select pk, val from schbar where active = true order by pk " +
                "create function active_items(in active_filter boolean) as select pk, val from schbar where active = active_filter";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into schbar values (1, true, 10), (2, false, 20), (3, true, 30)");
            }
            final var conn = ddl.getConnection();

            try (var logAppender = LogAppenderRule.of("LocalVariableTests_idx", PlanGenerator.class, Level.INFO)) {

                // filter=true satisfies the filtered index's predicate -> filtered-index plan, first seen -> miss
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set transaction variable filter = true");
                    try (var rs = stmt.executeQuery("select pk, val from active_items(active_filter => GET_VARIABLE(filter)) options (log query)")) {
                        ResultSetAssert.assertThat(rs)
                                .hasNextRow().isRowExactly(1L, 10L)
                                .hasNextRow().isRowExactly(3L, 30L)
                                .hasNoNextRow();
                    }
                }
                conn.commit();
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss(), "first call (filter=true) should be a cache miss");

                conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set transaction variable filter = true");
                    try (var rs = stmt.executeQuery("select pk, val from active_items(active_filter => GET_VARIABLE(filter)) options (log query)")) {
                        ResultSetAssert.assertThat(rs)
                                .hasNextRow().isRowExactly(1L, 10L)
                                .hasNextRow().isRowExactly(3L, 30L)
                                .hasNoNextRow();
                    }
                }
                conn.commit();
                Assertions.assertTrue(logAppender.lastMessageIsCacheHit(), "second call (filter=true) should be a cache hit");

                // filter=false violates the filtered index's predicate -> full-scan plan, a different variant -> miss
                conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set transaction variable filter = false");
                    try (var rs = stmt.executeQuery("select pk, val from active_items(active_filter => GET_VARIABLE(filter)) options (log query)")) {
                        ResultSetAssert.assertThat(rs)
                                .hasNextRow().isRowExactly(2L, 20L)
                                .hasNoNextRow();
                    }
                }
                conn.commit();
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss(), "call with filter=false should be a cache miss (different plan variant)");

                conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set transaction variable filter = false");
                    try (var rs = stmt.executeQuery("select pk, val from active_items(active_filter => GET_VARIABLE(filter)) options (log query)")) {
                        ResultSetAssert.assertThat(rs)
                                .hasNextRow().isRowExactly(2L, 20L)
                                .hasNoNextRow();
                    }
                }
                conn.rollback();
                Assertions.assertTrue(logAppender.lastMessageIsCacheHit(), "fourth call (filter=false again) should be a cache hit");
            }
        }
    }

    @Test
    void variableInTempFunctionBodyResolvesAtCallTime() throws Exception {
        // GET_VARIABLE(body_var) directly inside a temp TVF body is resolved at invocation time, not at CREATE
        // TEMPORARY FUNCTION time. The function can be created before the variable exists; the error
        // surfaces only if GET_VARIABLE(body_var) is still absent when the function is actually called.
        final String schemaTemplate = "create table tvfbody(pk bigint, name string, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into tvfbody values (1, 'alice'), (2, 'bob'), (3, 'carol')");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                // CREATE succeeds even though GET_VARIABLE(body_var) is not set yet
                stmt.execute("create temporary function find_by_body_var() on commit drop function " +
                        "as select pk, name from tvfbody where name = GET_VARIABLE(body_var)");

                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select pk, name from find_by_body_var()"))
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);

                stmt.execute("set transaction variable body_var = 'alice'");
                try (var rs = stmt.executeQuery("select pk, name from find_by_body_var()")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L, "alice").hasNoNextRow();
                }

                // Overwriting the variable updates what the same compiled function resolves to
                stmt.execute("set transaction variable body_var = 'bob'");
                try (var rs = stmt.executeQuery("select pk, name from find_by_body_var()")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(2L, "bob").hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void tempFunctionAndLocalVariableCoexist() throws Exception {
        // Verifies that SET TRANSACTION VARIABLE and CREATE TEMPORARY FUNCTION are independent:
        // - the temp function survives a SET TRANSACTION VARIABLE in the same transaction
        // - the local variable survives a CREATE TEMPORARY FUNCTION in the same transaction
        // - both can be used together in the same query (variable passed as a function argument)
        final String schemaTemplate = "create table coexist(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into coexist values (1, 10), (2, 20), (3, 30)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("create temporary function find_val(in threshold bigint) on commit drop function as select pk from coexist where val > threshold");
                stmt.execute("set transaction variable limit_val = 15");

                try (var rs = stmt.executeQuery("select pk from find_val(threshold => 15)")) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(2L)
                            .hasNextRow().isRowExactly(3L)
                            .hasNoNextRow();
                }

                try (var rs = stmt.executeQuery("select pk from coexist where val > GET_VARIABLE(limit_val)")) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(2L)
                            .hasNextRow().isRowExactly(3L)
                            .hasNoNextRow();
                }

                try (var rs = stmt.executeQuery("select pk from find_val(threshold => GET_VARIABLE(limit_val))")) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(2L)
                            .hasNextRow().isRowExactly(3L)
                            .hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void nullLocalVariableDoesNotBreakFunctionInvocation() throws Exception {
        // Regression: a local variable bound to NULL must not crash a query that invokes a
        // user-defined/table function. The function-compilation memoize cache previously keyed on
        // ImmutableMap.copyOf(localVars), which throws NullPointerException on null values, so
        // merely having a null-valued local variable in scope while resolving any function crashed.
        final String schemaTemplate =
                "create table nulltvf(pk bigint, name string, primary key(pk)) " +
                "create function names_like(in target string) as select pk, name from nulltvf where name = target";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into nulltvf values (1, 'alice'), (2, 'bob')");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                // A null-valued local variable is in scope (unrelated to the function argument).
                stmt.execute("set transaction variable unused = null");
                try (var rs = stmt.executeQuery("select pk, name from names_like(target => 'alice')")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L, "alice").hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void permanentFunctionBodyCannotReferenceLocalVariable() throws Exception {
        // Unlike temporary functions (see variableInTempFunctionBodyResolvesAtCallTime), a permanent
        // (schema-template) function body is compiled against an EMPTY local-variable scope:
        //  - DdlVisitor.getInvokedRoutineMetadata compiles non-temporary bodies eagerly at CREATE
        //    time (only table-valued TEMPORARY functions defer and thread the vars in), and
        //  - the deserialization path (RecordMetadataDeserializer.getSqlFunctionCompiler ->
        //    RoutineParser.parse) builds a fresh MutablePlanGenerationContext with no local
        //    variables and ignores the localVars argument entirely.
        // So a GET_VARIABLE(var) reference in a permanent function body is never resolved from the calling
        // transaction and surfaces as UNDEFINED_PARAMETER. Here it fails when the schema template
        // (which eagerly compiles the body) is created; the whole flow is wrapped so the assertion
        // still holds if compilation is ever deferred to invocation time.
        final String schemaTemplate =
                "create table permfoo(pk bigint, name string, primary key(pk)) " +
                "create function names_matching() as select pk, name from permfoo where name = GET_VARIABLE(body_var)";
        RelationalAssertions.assertThrowsSqlException(() -> {
            try (var ddl = Ddl.builder().database(URI.create("/TEST/LVPERM")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
                final var conn = ddl.setSchemaAndGetConnection();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.executeUpdate("insert into permfoo values (1, 'alice')");
                    stmt.execute("set transaction variable body_var = 'alice'");
                    try (var rs = stmt.executeQuery("select pk, name from names_matching()")) {
                        rs.next();
                    }
                }
            }
        }).hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
    }
}
