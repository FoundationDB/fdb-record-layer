/*
 * SnapshotIsolationConcurrencyTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Concurrency tests for the {@code OPTIONS (ISOLATION LEVEL SNAPSHOT)} query option. These verify the
 * defining property of FDB snapshot isolation as exposed at the SQL layer: a {@code SELECT} run with
 * the option does not add read-conflict ranges to its transaction, so a concurrent write into the
 * range it read does not cause the reading transaction's commit to fail. The control case (the same
 * scenario without the option) demonstrates that a serializable read does conflict.
 * <p>
 * The two transactions are driven sequentially on a single thread: FDB conflict detection is based on
 * read/commit versions, so interleaving the operations in program order (A reads, B writes+commits,
 * A writes+commits) is sufficient and deterministic.
 */
@Tag(Tags.RequiresFDB)
public class SnapshotIsolationConcurrencyTest {

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t(id BIGINT, val BIGINT, PRIMARY KEY(id))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database =
            new SimpleDatabaseRule(SnapshotIsolationConcurrencyTest.class, SCHEMA_TEMPLATE);

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connectionA =
            new RelationalConnectionRule(database::getConnectionUri).withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connectionB =
            new RelationalConnectionRule(database::getConnectionUri).withSchema("TEST_SCHEMA");

    private void insertInitialData() throws SQLException {
        try (RelationalStatement statement = connectionA.createStatement()) {
            statement.executeUpdate("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
        }
    }

    /**
     * Reads every row visible to {@code connectionA} with {@code id < 100} (a wide range that a
     * concurrent insert of {@code id = 50} falls into), optionally at snapshot isolation, fully
     * consuming the result so the scan actually happens within transaction A.
     */
    private void readRangeOnA(boolean snapshot) throws SQLException {
        final var query = "SELECT id, val FROM t WHERE id < 100"
                + (snapshot ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");
        try (RelationalStatement statement = connectionA.createStatement();
                var resultSet = statement.executeQuery(query)) {
            int rows = 0;
            while (resultSet.next()) {
                rows++;
            }
            // Guard: the read must have observed the initial data, otherwise the test is not exercising
            // a read over the range the concurrent write targets.
            assertThat(rows).isGreaterThanOrEqualTo(5);
        }
    }

    @ParameterizedTest
    @BooleanSource("useSnapshot")
    void snapshotReadDoesNotCreateConflictRange(boolean useSnapshot) throws SQLException {
        insertInitialData();

        connectionA.setAutoCommit(false);
        connectionB.setAutoCommit(false);

        // Transaction A: snapshot read over the range [-inf, 100). Being a snapshot read, it adds no
        // read-conflict range.
        readRangeOnA(useSnapshot);

        // Transaction B: write into the range A just read, and commit.
        try (RelationalStatement statement = connectionB.createStatement()) {
            statement.executeUpdate("INSERT INTO t VALUES (50, 500)");
        }
        connectionB.commit();

        // Transaction A: perform a write (to a disjoint key so it does not itself conflict with B),
        // then commit. This should succeed because A's read did not add a conflict range.
        try (RelationalStatement statement = connectionA.createStatement()) {
            statement.executeUpdate("INSERT INTO t VALUES (200, 2000)");
        }
        if (useSnapshot) {
            assertThatCode(connectionA::commit).doesNotThrowAnyException();
        } else {
            assertThatThrownBy(connectionA::commit).isInstanceOf(SQLException.class);
        }
    }
}
