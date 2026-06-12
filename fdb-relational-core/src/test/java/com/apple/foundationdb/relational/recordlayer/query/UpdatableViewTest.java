/*
 * UpdatableViewTest.java
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Integration tests for DML (INSERT / UPDATE / DELETE) through SQL views.
 */
public class UpdatableViewTest {

    private static final class JoinViewSchema {
    }

    // Schema with a plain table, a row-filtered view, and a star view.
    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE employee (id BIGINT, name STRING, dept STRING, salary BIGINT, PRIMARY KEY(id))\n" +
            "CREATE VIEW eng_employees AS SELECT * FROM employee WHERE dept = 'eng'\n" +
            "CREATE VIEW all_employees AS SELECT * FROM employee";

    // Schema with a join view to verify that DML on non-updatable views is rejected.
    private static final String JOIN_VIEW_SCHEMA_TEMPLATE =
            "CREATE TABLE emp (id BIGINT, name STRING, dept STRING, PRIMARY KEY(id))\n" +
            "CREATE TABLE dept_info (code STRING, budget BIGINT, PRIMARY KEY(code))\n" +
            "CREATE VIEW emp_dept AS SELECT e.id, e.name FROM emp e, dept_info d WHERE e.dept = d.code";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(
            UpdatableViewTest.class,
            SCHEMA_TEMPLATE,
            new SchemaTemplateRule.SchemaTemplateOptions(true, true));

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule joinViewDatabase = new SimpleDatabaseRule(
            JoinViewSchema.class,
            JOIN_VIEW_SCHEMA_TEMPLATE,
            new SchemaTemplateRule.SchemaTemplateOptions(true, true));

    @BeforeEach
    void setUp() throws SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());
            try (final var s = conn.createStatement()) {
                s.execute("INSERT INTO employee (id, name, dept, salary) VALUES (1, 'Alice', 'eng', 90000)");
                s.execute("INSERT INTO employee (id, name, dept, salary) VALUES (2, 'Bob', 'eng', 80000)");
                s.execute("INSERT INTO employee (id, name, dept, salary) VALUES (3, 'Carol', 'mkt', 70000)");
                s.execute("INSERT INTO employee (id, name, dept, salary) VALUES (4, 'Dave', 'mkt', 60000)");
            }
        }
    }

    // ── DELETE ────────────────────────────────────────────────────────────────

    @Test
    void deleteRowVisibleThroughFilteredView() throws SQLException {
        // DELETE through eng_employees only removes the row from the base table.
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());
            conn.createStatement().execute("DELETE FROM eng_employees WHERE id = 1");
            // Alice (id=1) deleted; others remain.
            try (final var rs = conn.prepareStatement("SELECT id FROM employee ORDER BY id").executeQuery()) {
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumn("ID", 2L)
                        .hasNextRow().hasColumn("ID", 3L)
                        .hasNextRow().hasColumn("ID", 4L)
                        .hasNoNextRow();
            }
        }
    }

    @Test
    void deleteAllRowsVisibleThroughFilteredView() throws SQLException {
        // DELETE without DML WHERE deletes only rows visible through the view's predicate.
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());
            conn.createStatement().execute("DELETE FROM eng_employees");
            // Only mkt employees (Carol, Dave) remain.
            try (final var rs = conn.prepareStatement("SELECT id, dept FROM employee ORDER BY id").executeQuery()) {
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumn("ID", 3L).hasColumn("DEPT", "mkt")
                        .hasNextRow().hasColumn("ID", 4L).hasColumn("DEPT", "mkt")
                        .hasNoNextRow();
            }
        }
    }

    @Test
    void deleteRowThroughStarView() throws SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());
            conn.createStatement().execute("DELETE FROM all_employees WHERE id = 3");
            try (final var rs = conn.prepareStatement("SELECT id FROM employee ORDER BY id").executeQuery()) {
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumn("ID", 1L)
                        .hasNextRow().hasColumn("ID", 2L)
                        .hasNextRow().hasColumn("ID", 4L)
                        .hasNoNextRow();
            }
        }
    }

    // ── UPDATE ────────────────────────────────────────────────────────────────

    @Test
    void updateColumnThroughFilteredView() throws SQLException {
        // View predicate restricts affected rows to the eng department.
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());
            conn.createStatement().execute("UPDATE eng_employees SET salary = 99000");
            try (final var rs = conn.prepareStatement("SELECT id, salary FROM employee ORDER BY id").executeQuery()) {
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumn("ID", 1L).hasColumn("SALARY", 99000L) // Alice updated
                        .hasNextRow().hasColumn("ID", 2L).hasColumn("SALARY", 99000L) // Bob updated
                        .hasNextRow().hasColumn("ID", 3L).hasColumn("SALARY", 70000L) // Carol unchanged
                        .hasNextRow().hasColumn("ID", 4L).hasColumn("SALARY", 60000L) // Dave unchanged
                        .hasNoNextRow();
            }
        }
    }

    @Test
    void updateColumnThroughFilteredViewWithAdditionalWhere() throws SQLException {
        // View predicate AND DML WHERE are both applied.
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());
            conn.createStatement().execute("UPDATE eng_employees SET salary = 55000 WHERE id = 2");
            try (final var rs = conn.prepareStatement("SELECT id, salary FROM employee ORDER BY id").executeQuery()) {
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumn("ID", 1L).hasColumn("SALARY", 90000L) // Alice unchanged
                        .hasNextRow().hasColumn("ID", 2L).hasColumn("SALARY", 55000L) // Bob updated
                        .hasNextRow().hasColumn("ID", 3L).hasColumn("SALARY", 70000L) // Carol unchanged
                        .hasNextRow().hasColumn("ID", 4L).hasColumn("SALARY", 60000L) // Dave unchanged
                        .hasNoNextRow();
            }
        }
    }

    @Test
    void updateColumnThroughStarView() throws SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());
            conn.createStatement().execute("UPDATE all_employees SET name = 'Updated' WHERE id = 4");
            try (final var rs = conn.prepareStatement("SELECT id, name FROM employee WHERE id = 4").executeQuery()) {
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumn("ID", 4L).hasColumn("NAME", "Updated")
                        .hasNoNextRow();
            }
        }
    }

    // ── INSERT ────────────────────────────────────────────────────────────────

    @Test
    void insertThroughStarView() throws SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());
            conn.createStatement().execute(
                    "INSERT INTO all_employees (id, name, dept, salary) VALUES (5, 'Eve', 'eng', 85000)");
            try (final var rs = conn.prepareStatement("SELECT id, name FROM employee WHERE id = 5").executeQuery()) {
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumn("ID", 5L).hasColumn("NAME", "Eve")
                        .hasNoNextRow();
            }
        }
    }

    @Test
    void insertThroughFilteredView() throws SQLException {
        // INSERT through a filtered view: the row goes into the base table regardless of
        // whether it satisfies the view's predicate (WITH CHECK OPTION is not yet supported).
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());
            conn.createStatement().execute(
                    "INSERT INTO eng_employees (id, name, dept, salary) VALUES (10, 'Frank', 'sales', 50000)");
            try (final var rs = conn.prepareStatement("SELECT id, dept FROM employee WHERE id = 10").executeQuery()) {
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumn("ID", 10L).hasColumn("DEPT", "sales")
                        .hasNoNextRow();
            }
        }
    }

    // ── Non-updatable view rejection ──────────────────────────────────────────

    @Test
    void deleteFromJoinViewIsRejected() throws SQLException {
        try (final var conn = DriverManager.getConnection(joinViewDatabase.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(joinViewDatabase.getSchemaName());
            RelationalAssertions.assertThrowsSqlException(
                    () -> conn.createStatement().execute("DELETE FROM emp_dept WHERE id = 1"))
                    .hasErrorCode(ErrorCode.VIEW_NOT_UPDATABLE);
        }
    }

    @Test
    void updateJoinViewIsRejected() throws SQLException {
        try (final var conn = DriverManager.getConnection(joinViewDatabase.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(joinViewDatabase.getSchemaName());
            RelationalAssertions.assertThrowsSqlException(
                    () -> conn.createStatement().execute("UPDATE emp_dept SET name = 'x' WHERE id = 1"))
                    .hasErrorCode(ErrorCode.VIEW_NOT_UPDATABLE);
        }
    }

    @Test
    void insertIntoJoinViewIsRejected() throws SQLException {
        try (final var conn = DriverManager.getConnection(joinViewDatabase.getConnectionUri().toString())
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(joinViewDatabase.getSchemaName());
            RelationalAssertions.assertThrowsSqlException(
                    () -> conn.createStatement().execute("INSERT INTO emp_dept (id, name) VALUES (1, 'x')"))
                    .hasErrorCode(ErrorCode.VIEW_NOT_UPDATABLE);
        }
    }
}
