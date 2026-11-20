/*
 * InsertAutoQATest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class InsertAutoQATest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(InsertAutoQATest.class, TestSchemas.restaurant());

    @Nonnull
    private RelationalConnection connect() throws SQLException {
        final RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class);
        conn.setSchema("TEST_SCHEMA");
        return conn;
    }

    @Nonnull
    private static List<Long> restaurantKeys(@Nonnull RelationalStatement s) throws SQLException {
        final List<Long> keys = new ArrayList<>();
        try (RelationalResultSet rs = s.executeQuery("SELECT rest_no FROM restaurant ORDER BY rest_no")) {
            while (rs.next()) {
                keys.add(rs.getLong(1));
            }
        }
        return keys;
    }

    @Test
    void sqlMultiRowInsertWithDuplicateKeyInExplicitTransactionLeavesNoRows() throws SQLException {
        // BUG AQA-27: with autoCommit=false a failed multi-row INSERT keeps its earlier rows in the transaction (rollback
        // only happens when canCommit()). Spec: ISO/IEC 9075-2 SQL-statement atomicity.
        // a failed multi-row INSERT inside an explicit transaction must not leave its earlier rows for the commit
        try (RelationalConnection conn = connect(); RelationalStatement s = conn.createStatement()) {
            conn.setAutoCommit(false);
            RelationalAssertions.assertThrowsSqlException(
                    () -> s.executeUpdate("INSERT INTO restaurant(rest_no, name) VALUES (60, 'a'), (60, 'b')"))
                    .hasErrorCode(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
            conn.commit();
            conn.setAutoCommit(true);
            assertThat(restaurantKeys(s)).isEmpty();
        }
    }
}
