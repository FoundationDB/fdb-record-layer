/*
 * LocalVariableTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.Options;
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
import java.util.Base64;

/**
 * Tests for transaction-scoped local variables (SET LOCAL / @variable).
 */
public class LocalVariableTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public LocalVariableTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void setLocalStringVariableAndRead() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, name string, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t1 values (1, 'alice'), (2, 'bob'), (3, 'carol')");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local myname = 'bob'");
                try (var rs = stmt.executeQuery("select pk, name from t1 where name = @myname")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(2L, "bob").hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void setLocalLongVariableAndFilter() throws Exception {
        final String schemaTemplate = "create table t2(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t2 values (1, 10), (2, 20), (3, 30), (4, 40)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local target = 20");
                try (var rs = stmt.executeQuery("select pk from t2 where val = @target")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(2L).hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void variableNotVisibleInNextTransaction() throws Exception {
        final String schemaTemplate = "create table t3(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var conn = ddl.setSchemaAndGetConnection();
            conn.setAutoCommit(false);

            // Set variable in first transaction
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local x = 42");
            }
            conn.commit();

            // New transaction — variable must not be visible
            conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select @x from t3"))
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
            }
            conn.rollback();
        }
    }

    @Test
    void undefinedVariableThrows() throws Exception {
        final String schemaTemplate = "create table t4(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var conn = ddl.setSchemaAndGetConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select @undefined from t4"))
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
            }
            conn.rollback();
        }
    }

    @Test
    void planCacheReusedForDifferentValues() throws Exception {
        final String schemaTemplate = "create table t5(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t5 values (1, 100), (2, 200), (3, 300)");
            }
            final var conn = ddl.getConnection();

            // First value
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local v = 100");
                try (var rs = stmt.executeQuery("select pk from t5 where val = @v")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L).hasNoNextRow();
                }
            }
            conn.commit();

            // Second value in a new transaction — same plan should be reused
            conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local v = 200");
                try (var rs = stmt.executeQuery("select pk from t5 where val = @v")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(2L).hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void setLocalBooleanVariable() throws Exception {
        final String schemaTemplate = "create table t6(pk bigint, active boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t6 values (1, true), (2, false), (3, true)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local flag = true");
                try (var rs = stmt.executeQuery("select pk from t6 where active = @flag")) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(1L)
                            .hasNextRow().isRowExactly(3L)
                            .hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void variableOverwriteIsVisible() throws Exception {
        final String schemaTemplate = "create table t7(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t7 values (1, 10), (2, 20), (3, 30)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local x = 10");
                stmt.execute("set local x = 20");
                try (var rs = stmt.executeQuery("select pk from t7 where val = @x")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(2L).hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void caseSensitiveVariableNames() throws Exception {
        final String schemaTemplate = "create table t8(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/LVCS"))
                .relationalExtension(relationalExtension)
                .withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true)
                .schemaTemplate(schemaTemplate)
                .build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t8 values (1, 10), (2, 20)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                // Quoted variable name "myVar" stores the key with case preserved
                stmt.execute("set local \"myVar\" = 10");
                // @"myVar" resolves to 'myVar' — same key → found
                try (var rs = stmt.executeQuery("select pk from t8 where val = @\"myVar\"")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L).hasNoNextRow();
                }
                // @myvar is an unquoted identifier; with caseSensitive=true, it preserves
                // the literal case from the input ('myvar' != 'myVar') → undefined
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select pk from t8 where val = @myvar"))
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
            }
            conn.rollback();
        }
    }

    @Test
    void variableNotVisibleAfterAutoCommit() throws Exception {
        final String schemaTemplate = "create table t9(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var conn = ddl.setSchemaAndGetConnection();
            // autoCommit=true: the SET LOCAL statement auto-commits its transaction; the
            // variable disappears before the next statement runs in its own new transaction.
            conn.setAutoCommit(true);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local x = 99");
            }
            try (var stmt = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select pk from t9 where pk = @x"))
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
            }
        }
    }

    @Test
    void variableAndPreparedParamCoexist() throws Exception {
        final String schemaTemplate = "create table t10(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t10 values (1, 100), (2, 100), (3, 200)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local valfilter = 100");
            }
            // Mix a local variable (@valfilter) with a positional prepared parameter (?)
            try (var ps = conn.prepareStatement("select pk from t10 where val = @valfilter and pk = ?")) {
                ps.setLong(1, 1L);
                try (var rs = ps.executeQuery()) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L).hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void negativeValueVariable() throws Exception {
        final String schemaTemplate = "create table t11(pk bigint, delta bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t11 values (1, -10), (2, 5), (3, -10)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local threshold = -10");
                try (var rs = stmt.executeQuery("select pk from t11 where delta = @threshold")) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(1L)
                            .hasNextRow().isRowExactly(3L)
                            .hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void planCacheHitVerifiedByLog() throws Exception {
        final String schemaTemplate = "create table t12(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t12 values (1, 10), (2, 20), (3, 30)");
            }
            final var conn = ddl.getConnection();
            try (var logAppender = LogAppenderRule.of("LocalVariableTests", PlanGenerator.class, Level.INFO)) {
                // First execution — cache miss expected
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set local v = 10");
                    try (var rs = stmt.executeQuery("select pk from t12 where val = @v options (log query)")) {
                        ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L).hasNoNextRow();
                    }
                }
                conn.commit();
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss(), "first execution should be a cache miss");

                // Second execution with a different value — same plan structure, cache hit expected
                conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set local v = 20");
                    try (var rs = stmt.executeQuery("select pk from t12 where val = @v options (log query)")) {
                        ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(2L).hasNoNextRow();
                    }
                }
                conn.rollback();
                Assertions.assertTrue(logAppender.lastMessageIsCacheHit(), "second execution should be a cache hit");
            }
        }
    }

    @Test
    void schemaTemplateFunctionWithVariableInBody() throws Exception {
        // The TVF takes an explicit parameter and is called with @var as the argument.
        // This tests the interaction between local variables and schema-template TVFs.
        // Note: using @var directly inside a TVF body requires lazy compilation support
        // (separate work item); passing @var at the call site works today.
        final String schemaTemplate =
                "create table schfoo(pk bigint, name string, primary key(pk)) " +
                "create function find_names(in target string) as select pk, name from schfoo where name = target";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into schfoo values (1, 'alice'), (2, 'bob'), (3, 'carol')");
            }
            final var conn = ddl.getConnection();

            // Without setting @varname: call fails with UNDEFINED_PARAMETER
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select pk, name from find_names(target => @varname)"))
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
            }
            conn.rollback();

            // Set @varname = 'alice' → only alice is returned
            conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local varname = 'alice'");
                try (var rs = stmt.executeQuery("select pk, name from find_names(target => @varname)")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L, "alice").hasNoNextRow();
                }
            }
            conn.commit();

            // Set @varname = 'bob' → only bob is returned (different value, same function call site)
            conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local varname = 'bob'");
                try (var rs = stmt.executeQuery("select pk, name from find_names(target => @varname)")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(2L, "bob").hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void filteredIndexSelectedByVariableWithPlanCacheConstraints() throws Exception {
        // Schema: table with a boolean column, a filtered index covering only active=true rows,
        // and a schema-template TVF that accepts the filter as a parameter.
        // The TVF is called with @filter as the argument so the Cascades planner generates a
        // constraint-based plan bundle:
        //   - when @filter=true  → filtered-index plan variant is used
        //   - when @filter=false → full-scan plan variant is used
        // Verified via plan-cache hit/miss log messages.
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

                // --- first call: @filter = true ---
                // active_filter = true satisfies the filtered index predicate active = true
                // → filtered-index plan is selected; first time seeing this query → cache miss.
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set local filter = true");
                    try (var rs = stmt.executeQuery("select pk, val from active_items(active_filter => @filter) options (log query)")) {
                        ResultSetAssert.assertThat(rs)
                                .hasNextRow().isRowExactly(1L, 10L)
                                .hasNextRow().isRowExactly(3L, 30L)
                                .hasNoNextRow();
                    }
                }
                conn.commit();
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss(), "first call (filter=true) should be a cache miss");

                // --- second call: @filter = true again ---
                // Same constraint satisfied → same plan variant → cache hit.
                conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set local filter = true");
                    try (var rs = stmt.executeQuery("select pk, val from active_items(active_filter => @filter) options (log query)")) {
                        ResultSetAssert.assertThat(rs)
                                .hasNextRow().isRowExactly(1L, 10L)
                                .hasNextRow().isRowExactly(3L, 30L)
                                .hasNoNextRow();
                    }
                }
                conn.commit();
                Assertions.assertTrue(logAppender.lastMessageIsCacheHit(), "second call (filter=true) should be a cache hit");

                // --- third call: @filter = false ---
                // active_filter = false does NOT satisfy the filtered index predicate active = true
                // → full-scan plan variant required; constraint violated → cache miss (new plan stored).
                conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set local filter = false");
                    try (var rs = stmt.executeQuery("select pk, val from active_items(active_filter => @filter) options (log query)")) {
                        ResultSetAssert.assertThat(rs)
                                .hasNextRow().isRowExactly(2L, 20L)
                                .hasNoNextRow();
                    }
                }
                conn.commit();
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss(), "call with filter=false should be a cache miss (different plan variant)");

                // --- fourth call: @filter = false again ---
                // Same full-scan plan variant → cache hit.
                conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set local filter = false");
                    try (var rs = stmt.executeQuery("select pk, val from active_items(active_filter => @filter) options (log query)")) {
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
        // @body_var directly inside a temp TVF body is resolved at invocation time, not at CREATE
        // TEMPORARY FUNCTION time. The function can be created before the variable exists; the error
        // surfaces only if @body_var is still absent when the function is actually called.
        final String schemaTemplate = "create table tvfbody(pk bigint, name string, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into tvfbody values (1, 'alice'), (2, 'bob'), (3, 'carol')");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                // CREATE succeeds even though @body_var is not set yet
                stmt.execute("create temporary function find_by_body_var() on commit drop function " +
                        "as select pk, name from tvfbody where name = @body_var");

                // calling before @body_var is set → UNDEFINED_PARAMETER at invocation time
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select pk, name from find_by_body_var()"))
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);

                // set @body_var = 'alice' → first call returns alice
                stmt.execute("set local body_var = 'alice'");
                try (var rs = stmt.executeQuery("select pk, name from find_by_body_var()")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L, "alice").hasNoNextRow();
                }

                // overwrite @body_var to 'bob' → same function now returns bob (live reference)
                stmt.execute("set local body_var = 'bob'");
                try (var rs = stmt.executeQuery("select pk, name from find_by_body_var()")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(2L, "bob").hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }

    @Test
    void tempFunctionAndLocalVariableCoexist() throws Exception {
        // Verifies that SET LOCAL and CREATE TEMPORARY FUNCTION are independent:
        // - the temp function survives a SET LOCAL in the same transaction
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
                // Create a temp function, then set a variable — function must still work
                stmt.execute("create temporary function find_val(in threshold bigint) on commit drop function as select pk from coexist where val > threshold");
                stmt.execute("set local limit_val = 15");

                // Temp function still works after SET LOCAL
                try (var rs = stmt.executeQuery("select pk from find_val(threshold => 15)")) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(2L)
                            .hasNextRow().isRowExactly(3L)
                            .hasNoNextRow();
                }

                // Local variable still works after CREATE TEMPORARY FUNCTION
                try (var rs = stmt.executeQuery("select pk from coexist where val > @limit_val")) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(2L)
                            .hasNextRow().isRowExactly(3L)
                            .hasNoNextRow();
                }

                // Both together: variable as argument to the temp function
                try (var rs = stmt.executeQuery("select pk from find_val(threshold => @limit_val)")) {
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
    void variableValueCapturedInContinuation() throws Exception {
        // Verifies that the value bound to a local variable at the time a query is first executed
        // is captured in the continuation, just like a bound prepared-statement parameter.
        // Changing the variable after the first page is fetched must NOT affect the continuation.
        final String schemaTemplate = "create table conttbl(pk bigint, name string, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into conttbl values (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave'), (5, 'eve')");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                // Filter: pk >= @min_pk  (= 2 → rows 2,3,4,5)
                stmt.execute("set local min_pk = 2");
                stmt.setMaxRows(2);
                // First page: rows 2 and 3
                final byte[] continuationBytes;
                try (var rs = stmt.executeQuery("select pk, name from conttbl where pk >= @min_pk")) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(2L, "bob")
                            .hasNextRow().isRowExactly(3L, "carol")
                            .hasNoNextRow();
                    continuationBytes = rs.getContinuation().serialize();
                }
                // Change @min_pk to a different value — the continuation must ignore this
                stmt.execute("set local min_pk = 99");
                stmt.setMaxRows(10);
                // Resume via continuation: should still see rows 4 and 5 (original binding min_pk=2)
                final String encoded = Base64.getEncoder().encodeToString(continuationBytes);
                try (var rs = stmt.executeQuery("EXECUTE CONTINUATION B64'" + encoded + "'")) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(4L, "dave")
                            .hasNextRow().isRowExactly(5L, "eve")
                            .hasNoNextRow();
                    Assertions.assertTrue(rs.getContinuation().atEnd(), "continuation should be at end after last row");
                }
            }
            conn.rollback();
        }
    }

    @Test
    void sameNameVariableAndNamedParamAreIndependent() throws Exception {
        // @x and ?x share the same underlying name "x" but are independent namespaces:
        // @x resolves from the transaction's local-variable map, ?x from the prepared-parameter map.
        final String schemaTemplate = "create table tns(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into tns values (1), (2), (3)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set local x = 1");
            }
            // @x = 1 (from SET LOCAL), ?x = 2 (from named prepared param) — both resolve independently
            try (var ps = conn.prepareStatement("select pk from tns where pk = @x or pk = ?x")) {
                ps.setLong("x", 2L);
                try (var rs = ps.executeQuery()) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(1L)
                            .hasNextRow().isRowExactly(2L)
                            .hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }
}
