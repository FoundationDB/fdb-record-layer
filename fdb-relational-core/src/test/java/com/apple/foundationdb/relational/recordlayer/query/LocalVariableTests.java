/*
 * LocalVariableTests.java
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
 * Tests for transaction-scoped local variables (SET TRANSACTION VARIABLE / GET_VARIABLE(name)).
 * Interaction with table-valued functions is covered separately, in the stacked PR that adds it.
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
                stmt.execute("set transaction variable myname = 'bob'");
                try (var rs = stmt.executeQuery("select pk, name from t1 where name = GET_VARIABLE(myname)")) {
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
                stmt.execute("set transaction variable target = 20");
                try (var rs = stmt.executeQuery("select pk from t2 where val = GET_VARIABLE(target)")) {
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
                stmt.execute("set transaction variable x = 42");
            }
            conn.commit();

            // New transaction — variable must not be visible
            conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select GET_VARIABLE(x) from t3"))
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
                        () -> stmt.executeQuery("select GET_VARIABLE(undefined) from t4"))
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
                stmt.execute("set transaction variable v = 100");
                try (var rs = stmt.executeQuery("select pk from t5 where val = GET_VARIABLE(v)")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L).hasNoNextRow();
                }
            }
            conn.commit();

            // Second value in a new transaction — same plan should be reused
            conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable v = 200");
                try (var rs = stmt.executeQuery("select pk from t5 where val = GET_VARIABLE(v)")) {
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
                stmt.execute("set transaction variable flag = true");
                try (var rs = stmt.executeQuery("select pk from t6 where active = GET_VARIABLE(flag)")) {
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
                stmt.execute("set transaction variable x = 10");
                stmt.execute("set transaction variable x = 20");
                try (var rs = stmt.executeQuery("select pk from t7 where val = GET_VARIABLE(x)")) {
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
                stmt.execute("set transaction variable \"myVar\" = 10");
                // GET_VARIABLE("myVar") resolves to 'myVar' — same key → found
                try (var rs = stmt.executeQuery("select pk from t8 where val = GET_VARIABLE(\"myVar\")")) {
                    ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L).hasNoNextRow();
                }
                // GET_VARIABLE(myvar) is an unquoted identifier; with caseSensitive=true, it preserves
                // the literal case from the input ('myvar' != 'myVar') → undefined
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select pk from t8 where val = GET_VARIABLE(myvar)"))
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
            // autoCommit=true: the SET TRANSACTION VARIABLE statement auto-commits its transaction; the
            // variable disappears before the next statement runs in its own new transaction.
            conn.setAutoCommit(true);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable x = 99");
            }
            try (var stmt = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select pk from t9 where pk = GET_VARIABLE(x)"))
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
                stmt.execute("set transaction variable valfilter = 100");
            }
            // Mix a local variable (GET_VARIABLE(valfilter)) with a positional prepared parameter (?)
            try (var ps = conn.prepareStatement("select pk from t10 where val = GET_VARIABLE(valfilter) and pk = ?")) {
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
                stmt.execute("set transaction variable threshold = -10");
                try (var rs = stmt.executeQuery("select pk from t11 where delta = GET_VARIABLE(threshold)")) {
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
                    stmt.execute("set transaction variable v = 10");
                    try (var rs = stmt.executeQuery("select pk from t12 where val = GET_VARIABLE(v) options (log query)")) {
                        ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L).hasNoNextRow();
                    }
                }
                conn.commit();
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss(), "first execution should be a cache miss");

                // Second execution with a different value — same plan structure, cache hit expected
                conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set transaction variable v = 20");
                    try (var rs = stmt.executeQuery("select pk from t12 where val = GET_VARIABLE(v) options (log query)")) {
                        ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(2L).hasNoNextRow();
                    }
                }
                conn.rollback();
                Assertions.assertTrue(logAppender.lastMessageIsCacheHit(), "second execution should be a cache hit");
            }
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
                // Filter: pk >= GET_VARIABLE(min_pk)  (= 2 → rows 2,3,4,5)
                stmt.execute("set transaction variable min_pk = 2");
                stmt.setMaxRows(2);
                // First page: rows 2 and 3
                final byte[] continuationBytes;
                try (var rs = stmt.executeQuery("select pk, name from conttbl where pk >= GET_VARIABLE(min_pk)")) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow().isRowExactly(2L, "bob")
                            .hasNextRow().isRowExactly(3L, "carol")
                            .hasNoNextRow();
                    continuationBytes = rs.getContinuation().serialize();
                }
                // Change GET_VARIABLE(min_pk) to a different value — the continuation must ignore this
                stmt.execute("set transaction variable min_pk = 99");
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
        // GET_VARIABLE(x) and ?x share the same underlying name "x" but are independent namespaces:
        // GET_VARIABLE(x) resolves from the transaction's local-variable map, ?x from the prepared-parameter map.
        final String schemaTemplate = "create table tns(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into tns values (1), (2), (3)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable x = 1");
            }
            // GET_VARIABLE(x) = 1 (from SET TRANSACTION VARIABLE), ?x = 2 (from named prepared param) — both resolve independently
            try (var ps = conn.prepareStatement("select pk from tns where pk = GET_VARIABLE(x) or pk = ?x")) {
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

    @Test
    void namedParamDoesNotResolveToLocalVariableOnPlainStatement() throws Exception {
        // Regression: GET_VARIABLE(name) and ?name are independent namespaces. On a plain (non-prepared)
        // Statement a ?name reference cannot be bound, so it must fail with UNDEFINED_PARAMETER and
        // must NOT silently pick up a same-named local variable's value. Previously the plain-
        // statement path injected local variables into the named-prepared-parameter map, so
        // `?x` resolved to `GET_VARIABLE(x)`.
        final String schemaTemplate = "create table tleak(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into tleak values (1), (2), (3)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable x = 1");
                // ?x is an unbound named prepared parameter on a plain Statement; it must not
                // resolve to the local variable GET_VARIABLE(x) (= 1).
                RelationalAssertions.assertThrowsSqlException(
                        () -> stmt.executeQuery("select pk from tleak where pk = ?x"))
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
            }
            conn.rollback();
        }
    }

    @Test
    void variableTypeChangeForcesCacheMissNotStaleReuse() throws Exception {
        // The plan cache key must fold in a variable's CURRENT type, not just its name: reusing a
        // plan compiled while a variable was BIGINT against a later STRING-valued binding of the
        // same-named variable would silently misuse a comparand shaped for the wrong type.
        final String schemaTemplate = "create table t14(pk bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t14 values (1)");
            }
            final var conn = ddl.getConnection();
            try (var logAppender = LogAppenderRule.of("LocalVariableTests_typechange", PlanGenerator.class, Level.INFO)) {
                // First execution: x is BIGINT.
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set transaction variable x = 10");
                    try (var rs = stmt.executeQuery("select GET_VARIABLE(x) from t14 where pk = 1 options (log query)")) {
                        ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(10).hasNoNextRow();
                    }
                }
                conn.commit();
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss(), "first execution (BIGINT) should be a cache miss");

                // Second execution: identical query text, but x is now STRING. Must be a cache
                // miss (a fresh compile), not a hit against the BIGINT-shaped plan, and must
                // return the STRING value correctly.
                conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set transaction variable x = 'ten'");
                    try (var rs = stmt.executeQuery("select GET_VARIABLE(x) from t14 where pk = 1 options (log query)")) {
                        ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly("ten").hasNoNextRow();
                    }
                }
                conn.rollback();
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss(), "second execution (STRING) must be a cache miss, not a stale hit against the BIGINT-typed plan");
            }
        }
    }

    @Test
    void variableNullToTypedTransitionForcesCacheMissNotStaleReuse() throws Exception {
        // NULL has no intrinsic type. Re-SETting a variable from NULL to a concrete type (or vice
        // versa) must be treated the same as any other type change for cache purposes. The
        // variable is used in a WHERE comparison (rather than selected bare) since a NULL-typed
        // value cannot itself be the declared type of a result-set column.
        final String schemaTemplate = "create table t15(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t15 values (1, 7)");
            }
            final var conn = ddl.getConnection();
            try (var logAppender = LogAppenderRule.of("LocalVariableTests_nulltotyped", PlanGenerator.class, Level.INFO)) {
                // First execution: x is NULL. val = NULL is never true, so no rows match.
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set transaction variable x = null");
                    try (var rs = stmt.executeQuery("select pk from t15 where val = GET_VARIABLE(x) options (log query)")) {
                        ResultSetAssert.assertThat(rs).hasNoNextRow();
                    }
                }
                conn.commit();
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss(), "first execution (NULL) should be a cache miss");

                // Second execution: identical query text, x is now BIGINT and matches val. Must
                // be a cache miss, not a stale hit against the NULL-typed plan.
                conn.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
                conn.setAutoCommit(false);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("set transaction variable x = 7");
                    try (var rs = stmt.executeQuery("select pk from t15 where val = GET_VARIABLE(x) options (log query)")) {
                        ResultSetAssert.assertThat(rs).hasNextRow().isRowExactly(1L).hasNoNextRow();
                    }
                }
                conn.rollback();
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss(), "second execution (BIGINT) must be a cache miss, not a stale hit against the NULL-typed plan");
            }
        }
    }

    @Test
    void getVariableSetToNullReturnsNullWithoutError() throws Exception {
        // A variable explicitly SET to NULL is a distinct state from never having been SET:
        // reading it must succeed (not throw UNDEFINED_PARAMETER). Used in a WHERE comparison
        // (rather than selected bare) since a NULL-typed value cannot itself be the declared type
        // of a result-set column; per three-valued logic, `val = NULL` matches no rows.
        final String schemaTemplate = "create table t16(pk bigint, val bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/LV")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                stmt.executeUpdate("insert into t16 values (1, 7)");
            }
            final var conn = ddl.getConnection();
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("set transaction variable x = null");
                try (var rs = stmt.executeQuery("select pk from t16 where val = GET_VARIABLE(x)")) {
                    ResultSetAssert.assertThat(rs).hasNoNextRow();
                }
            }
            conn.rollback();
        }
    }
}
