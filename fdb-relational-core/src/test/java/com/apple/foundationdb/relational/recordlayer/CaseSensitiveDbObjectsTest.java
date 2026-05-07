/*
 * CaseSensitiveDbObjectsTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import org.apache.logging.log4j.Level;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Test case-sensitive db object connection option.
 */
public class CaseSensitiveDbObjectsTest {
    @Nonnull
    private static final String SCHEMA_TEMPLATE =
            """
            CREATE TABLE "t1" ("group" bigint, "id" string, "val" bigint, PRIMARY KEY("group", "id"))
            """;

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(CaseSensitiveDbObjectsTest.class,
            SCHEMA_TEMPLATE, Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build(),
            new SchemaTemplateRule.SchemaTemplateOptions(true, true));

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build())
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @RegisterExtension
    @Order(4)
    public final LogAppenderRule logAppender = new LogAppenderRule("QueryLoggingTestLogAppender", PlanGenerator.class, Level.INFO);

    public CaseSensitiveDbObjectsTest() throws SQLException {
    }

    @Test
    void lowerCaseSelectWorks() throws SQLException {
        statement.executeUpdate("insert into t1 values (1, 'abc', 1)");
        try (RelationalResultSet resultSet = statement.executeQuery("select * from t1")) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .isRowExactly(1L, "abc", 1L)
                    .hasNoNextRow();
        }

        try (RelationalResultSet resultSet = statement.executeQuery("select id from t1 where group = 1")) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .isRowExactly("abc")
                    .hasNoNextRow();
        }
    }

    @Test
    void upperCaseNonQuotedDoesNotWork() {
        RelationalAssertions.assertThrowsSqlException(() -> statement.executeQuery("select * from T1"))
                .hasErrorCode(ErrorCode.UNDEFINED_TABLE)
                .hasMessageContaining("T1");

        RelationalAssertions.assertThrowsSqlException(() -> statement.executeQuery("select id from t1 where Group = 1"))
                .hasErrorCode(ErrorCode.UNDEFINED_COLUMN)
                .hasMessageContaining("Group");
    }

    @Test
    void quotedSelectWorks() throws SQLException {
        statement.executeUpdate("insert into \"t1\" values (1, 'abc', 1)");
        try (RelationalResultSet resultSet = statement.executeQuery("select * from \"t1\"")) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .isRowExactly(1L, "abc", 1L)
                    .hasNoNextRow();
        }

        try (RelationalResultSet resultSet = statement.executeQuery("select \"id\" from \"t1\" where \"group\" = 1")) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .isRowExactly("abc")
                    .hasNoNextRow();
        }
    }

    @Test
    void planIsProperlyCached() throws SQLException {
        Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
        statement.executeUpdate("insert into \"t1\" values (1, 'abc', 1)");
        try (RelationalResultSet resultSet = statement.executeQuery("select id from t1 where group = 1 options (log query)")) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .isRowExactly("abc")
                    .hasNoNextRow();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("select 'id' from 't1' where 'group' = ?");
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"miss\"");

        try (RelationalResultSet resultSet = statement.executeQuery("select id from t1 where group = 1 options (log query)")) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .isRowExactly("abc")
                    .hasNoNextRow();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("select 'id' from 't1' where 'group' = ?");
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");
    }

    @Test
    void planIsProperlyCachedAndReusedAcrossCaseOptionVariation() throws SQLException {
        Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
        statement.executeUpdate("insert into \"t1\" values (1, 'abc', 1)");
        try (RelationalResultSet resultSet = statement.executeQuery("select id from t1 where group = 1 options (log query)")) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .isRowExactly("abc")
                    .hasNoNextRow();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("select 'id' from 't1' where 'group' = ?");
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"miss\"");

        try (RelationalConnection caseInsensitiveConn = DriverManager.getConnection(database.getConnectionUri()
                .toString()).unwrap(RelationalConnection.class)) {
            caseInsensitiveConn.setSchema("TEST_SCHEMA");
            try (RelationalStatement caseInsensitiveStatement = caseInsensitiveConn.createStatement()) {
                try (RelationalResultSet resultSet = caseInsensitiveStatement.executeQuery(
                        "select \"id\" from \"t1\" where \"group\" = 1 options (log query)")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly("abc")
                            .hasNoNextRow();
                }
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("select 'id' from 't1' where 'group' = ?");
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");
            }
        }
    }
}
