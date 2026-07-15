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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
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

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
 * <p>
 * Records are partitioned by {@code id % 3} so that the concurrent writes are always <em>interleaved
 * between</em> records transaction A actually reads. This guarantees the conflicting writes fall
 * within A's read range rather. Specifically: {@code id % 3 == 0} records are
 * the initial data A reads, {@code id % 3 == 1} records are written concurrently with the first query,
 * and {@code id % 3 == 2} records are written concurrently with the continuation.
 */
@Tag(Tags.RequiresFDB)
public class SnapshotIsolationConcurrencyTest {

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t(id BIGINT, val BIGINT, PRIMARY KEY(id))";

    /** Number of id slots (0 ... RECORD_COUNT-1) partitioned across the three {@code id % 3} buckets. */
    private static final int RECORD_COUNT = 30;

    /** Upper bound of the scan; every data id is below it, so the scan covers the whole data range. */
    private static final long SCAN_UPPER_BOUND = 1000L;

    /** Query A runs; a full range scan over the data. */
    private static final String SCAN_QUERY = "SELECT id FROM t WHERE id < " + SCAN_UPPER_BOUND;

    /**
     * A dedicated key transaction A writes (so it is a read-write transaction that can conflict). It is
     * outside the scanned range and is never written by B, so it can never cause a write-write conflict
     * that would mask the read-conflict behavior under test.
     */
    private static final long A_OWN_KEY = SCAN_UPPER_BOUND;

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

    /**
     * Inserts every id in {@code [0, RECORD_COUNT)} whose {@code id % 3 == remainder} on the given
     * connection, with {@code val = id * 10}. The three buckets interleave in the key space.
     */
    private void insertBucket(@Nonnull final RelationalConnectionRule connection, final int remainder) throws SQLException {
        String values = IntStream.range(0, RECORD_COUNT / 3)
                .map(i -> (i * 3) + remainder)
                .mapToObj(id -> "(" + id + ", " + (id * 10) + ")")
                .collect(Collectors.joining(", "));
        try (RelationalStatement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO t VALUES " + values);
        }
    }

    /** Number of records in a single {@code id % 3} bucket over {@code [0, RECORD_COUNT)}. */
    private static int bucketSize() {
        return RECORD_COUNT / 3;
    }

    /** Resumes a single page of the continuation on connection A, draining and returning the next continuation. */
    private Continuation resumeContinuationPage(@Nonnull final String resumeSql,
                                                @Nonnull final Continuation from) throws SQLException {
        try (RelationalPreparedStatement statement = connectionA.prepareStatement(resumeSql)) {
            statement.setMaxRows(1);
            statement.setBytes("continuation", from.serialize());
            try (RelationalResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    // drain the page
                }
                return resultSet.getContinuation();
            }
        }
    }

    @ParameterizedTest
    @BooleanSource("useSnapshot")
    void snapshotReadDoesNotCreateConflictRange(boolean useSnapshot) throws SQLException {
        // Initial data: the id % 3 == 0 records.
        insertBucket(connectionA, 0);

        connectionA.setAutoCommit(false);
        connectionB.setAutoCommit(false);

        // Transaction A: read the whole range, optionally at snapshot isolation, fully draining it.
        final var query = SCAN_QUERY + (useSnapshot ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");
        int rows = 0;
        try (RelationalStatement statement = connectionA.createStatement();
                RelationalResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                rows++;
            }
        }
        // Transaction A must have actually read the initial records, so that B's interleaved writes below fall
        // between records A read (and hence within A's read range).
        assertThat(rows).isEqualTo(bucketSize());

        // Transaction B: insert the id % 3 == 1 records (interleaved between the records A read) and commit.
        insertBucket(connectionB, 1);
        connectionB.commit();

        // Transaction A: perform its own write (a dedicated key B never touches) to become a read-write
        // transaction, then commit.
        try (RelationalStatement statement = connectionA.createStatement()) {
            statement.executeUpdate("INSERT INTO t VALUES (" + A_OWN_KEY + ", 0)");
        }
        if (useSnapshot) {
            // A's read added no conflict range, so B's interleaved writes do not conflict.
            assertThatCode(connectionA::commit).doesNotThrowAnyException();
        } else {
            // A's serializable read range spans B's interleaved writes, so A conflicts.
            assertThatThrownBy(connectionA::commit).isInstanceOf(SQLException.class);
        }
    }

    /**
     * Demonstrates that {@code ISOLATION LEVEL SNAPSHOT} is a per-execution option that is <em>not</em>
     * carried in the continuation: when a paginated snapshot query is resumed via
     * {@code EXECUTE CONTINUATION}, the resumed pages run at snapshot isolation only if the option is
     * re-specified on the resume statement. A bare {@code EXECUTE CONTINUATION} silently reverts to the
     * connection default (serializable), which then adds a read-conflict range.
     * <p>
     * The concurrent writes use the {@code id % 3} partitioning so that the write made concurrently with
     * the continuation ({@code id % 3 == 2}) is interleaved between records the continuation reads,
     * guaranteeing it overlaps the continuation's read range regardless of exactly where the first page
     * ended.
     */
    @ParameterizedTest
    @BooleanSource("repeatOptionOnResume")
    void snapshotIsolationIsNotCarriedAcrossContinuations(boolean repeatOptionOnResume) throws SQLException {
        // Initial data: the id % 3 == 0 records.
        insertBucket(connectionA, 0);

        connectionA.setAutoCommit(false);
        connectionB.setAutoCommit(false);

        // Transaction A: read the first page (one row) of a snapshot scan, obtaining a continuation.
        final Continuation firstPage;
        try (RelationalStatement statement = connectionA.createStatement()) {
            statement.setMaxRows(1);
            try (RelationalResultSet resultSet =
                    statement.executeQuery(SCAN_QUERY + " OPTIONS (ISOLATION LEVEL SNAPSHOT)")) {
                assertThat(resultSet.next()).isTrue();
                firstPage = resultSet.getContinuation();
            }
        }
        assertThat(firstPage.atEnd()).isFalse();

        // Transaction B: concurrent with the first query, insert the id % 3 == 1 records and commit.
        insertBucket(connectionB, 1);
        connectionB.commit();

        final var resumeSql = "EXECUTE CONTINUATION ?continuation"
                + (repeatOptionOnResume ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");

        // Resume the first page of the continuation (guaranteed to exist by the assertion above).
        Continuation current = resumeContinuationPage(resumeSql, firstPage);

        // Transaction B: concurrent with the continuation, insert the id % 3 == 2 records and commit.
        // These are interleaved between the id % 3 == 0 records the continuation is still reading, so
        // they fall within the continuation's read range no matter where the first page ended.
        insertBucket(connectionB, 2);
        connectionB.commit();

        // Drain the remaining continuation pages within transaction A.
        while (!current.atEnd()) {
            current = resumeContinuationPage(resumeSql, current);
        }

        // Transaction A: perform its own write (a dedicated key B never touches), then commit.
        try (RelationalStatement statement = connectionA.createStatement()) {
            statement.executeUpdate("INSERT INTO t VALUES (" + A_OWN_KEY + ", 0)");
        }
        if (repeatOptionOnResume) {
            // Every page ran at snapshot isolation, so no read-conflict range was added and A commits.
            assertThatCode(connectionA::commit).doesNotThrowAnyException();
        } else {
            // The resumed pages ran serializable; their read range spans B's interleaved writes, so A conflicts.
            assertThatThrownBy(connectionA::commit).isInstanceOf(SQLException.class);
        }
    }
}
