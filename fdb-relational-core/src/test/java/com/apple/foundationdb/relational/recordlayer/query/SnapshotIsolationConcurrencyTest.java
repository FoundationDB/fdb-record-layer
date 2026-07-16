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
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.RelationalResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

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
 * between</em> records transaction A actually reads. This guarantees the conflicting writes fall within
 * A's read range. Specifically: {@code id % 3 == 0} records are the initial data A reads,
 * {@code id % 3 == 1} records are written concurrently with the first query, and {@code id % 3 == 2}
 * records are written concurrently with the continuation. Tables {@code t} and {@code u} carry the same
 * ids so they can be joined and unioned.
 */
@Tag(Tags.RequiresFDB)
public class SnapshotIsolationConcurrencyTest {

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t(id BIGINT, val BIGINT, PRIMARY KEY(id))"
            + " CREATE TABLE u(id BIGINT, val BIGINT, PRIMARY KEY(id))";

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

    @ParameterizedTest
    @BooleanSource("useSnapshot")
    void snapshotReadDoesNotCreateConflictRange(boolean useSnapshot) throws SQLException {
        // Initial data: the id % 3 == 0 records.
        insertBucket(connectionA, "t", 0);

        connectionA.setAutoCommit(false);
        connectionB.setAutoCommit(false);

        // Transaction A: read the whole range, optionally at snapshot isolation, fully draining it. A must
        // have actually read the initial records, so that B's interleaved writes below fall between records
        // A read (and hence within A's read range).
        final var query = SCAN_QUERY + (useSnapshot ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");
        assertQueryOnAReturns(query, bucketSize());

        // Transaction B: insert the id % 3 == 1 records (interleaved between the records A read) and commit.
        insertBucket(connectionB, "t", 1);
        connectionB.commit();

        // A snapshot read added no conflict range, so B's interleaved writes do not conflict; a serializable
        // read spans them, so A conflicts.
        writeOwnKeyAndAssertCommit(!useSnapshot);
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
        insertBucket(connectionA, "t", 0);

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
        insertBucket(connectionB, "t", 1);
        connectionB.commit();

        final var resumeSql = "EXECUTE CONTINUATION ?continuation"
                + (repeatOptionOnResume ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");

        // Resume the first page of the continuation (guaranteed to exist by the assertion above).
        Continuation current = resumeContinuationPage(resumeSql, firstPage);

        // Transaction B: concurrent with the continuation, insert the id % 3 == 2 records and commit.
        // These are interleaved between the id % 3 == 0 records the continuation is still reading, so
        // they fall within the continuation's read range no matter where the first page ended.
        insertBucket(connectionB, "t", 2);
        connectionB.commit();

        // Drain the remaining continuation pages within transaction A.
        while (!current.atEnd()) {
            current = resumeContinuationPage(resumeSql, current);
        }

        // With the option repeated, every page ran at snapshot isolation and A commits cleanly; with a bare
        // resume the continuation ran serializable and its read range spans B's writes, so A conflicts.
        writeOwnKeyAndAssertCommit(!repeatOptionOnResume);
    }

    /**
     * Snapshot isolation changes only conflict detection, not read-your-writes: a snapshot read still
     * sees writes made earlier in the same transaction. This is verified for a point read, a range scan,
     * and after a subsequent update to the same row. (The same assertions hold at serializable isolation,
     * which the {@code useSnapshot=false} case confirms as a control.)
     */
    @ParameterizedTest
    @BooleanSource("useSnapshot")
    void snapshotReadSeesOwnWrites(boolean useSnapshot) throws SQLException {
        final var option = useSnapshot ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "";
        connectionA.setAutoCommit(false);

        // Write a new row within transaction A; a point read in the same transaction must see it.
        try (RelationalStatement statement = connectionA.createStatement()) {
            statement.executeUpdate("INSERT INTO t VALUES (777, 42)");
        }
        assertQueryOnAReturnsVal("SELECT val FROM t WHERE id = 777" + option, 42L);

        // A range scan (not a point lookup) in the same transaction also sees the freshly-written row;
        // 777 is the only row, so the scan returns exactly it.
        assertQueryOnAReturnsVal("SELECT val FROM t WHERE id > 100" + option, 42L);

        // Update the row within the same transaction; a subsequent (snapshot) read sees the new value.
        try (RelationalStatement statement = connectionA.createStatement()) {
            statement.executeUpdate("UPDATE t SET val = 99 WHERE id = 777");
        }
        assertQueryOnAReturnsVal("SELECT val FROM t WHERE id = 777" + option, 99L);

        connectionA.commit();
    }

    /**
     * A join read at snapshot isolation adds no conflict ranges for <em>either</em> joined table, so a
     * concurrent insert into either input does not cause A to conflict; the serializable control does
     * conflict. The join is on the non-indexed {@code val} column so that both inputs are fully scanned,
     * guaranteeing B's interleaved insert into whichever table falls within A's read range.
     */
    @ParameterizedTest
    @BooleanSource({"useSnapshot", "writeToU"})
    void snapshotJoinDoesNotConflictWithConcurrentWrites(boolean useSnapshot, boolean writeToU) throws SQLException {
        insertBucket(connectionA, "t", 0);
        insertBucket(connectionA, "u", 0);

        connectionA.setAutoCommit(false);
        connectionB.setAutoCommit(false);

        // Transaction A: join t and u on the non-indexed val column, forcing both tables to be scanned.
        final var query = "SELECT t.id FROM t, u WHERE t.val = u.val"
                + (useSnapshot ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");
        assertQueryOnAReturns(query, bucketSize());

        // Transaction B: insert interleaved rows into whichever joined table, and commit.
        insertBucket(connectionB, writeToU ? "u" : "t", 1);
        connectionB.commit();

        // A snapshot join added no conflict range for either input; a serializable join spans B's writes.
        writeOwnKeyAndAssertCommit(!useSnapshot);
    }

    /**
     * A {@code UNION ALL} read at snapshot isolation adds no conflict ranges for either input, so a
     * concurrent insert into either unioned table does not cause A to conflict; the serializable control
     * does conflict. {@code UNION ALL} scans both inputs fully, so B's interleaved insert into whichever
     * table falls within A's read range.
     */
    @ParameterizedTest
    @BooleanSource({"useSnapshot", "writeToU"})
    void snapshotUnionDoesNotConflictWithConcurrentWrites(boolean useSnapshot, boolean writeToU) throws SQLException {
        insertBucket(connectionA, "t", 0);
        insertBucket(connectionA, "u", 0);

        connectionA.setAutoCommit(false);
        connectionB.setAutoCommit(false);

        // Transaction A: union of t and u, which scans both tables fully.
        final var query = "SELECT id FROM t WHERE id < " + SCAN_UPPER_BOUND
                + " UNION ALL SELECT id FROM u WHERE id < " + SCAN_UPPER_BOUND
                + (useSnapshot ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");
        assertQueryOnAReturns(query, 2 * bucketSize());

        // Transaction B: insert interleaved rows into whichever unioned table, and commit.
        insertBucket(connectionB, writeToU ? "u" : "t", 1);
        connectionB.commit();

        // A snapshot union added no conflict range for either input; a serializable union spans B's writes.
        writeOwnKeyAndAssertCommit(!useSnapshot);
    }

    /**
     * Inserts every id in {@code [0, RECORD_COUNT)} whose {@code id % 3 == remainder} into the given
     * table on the given connection, with {@code val = id * 10}. The three buckets interleave in the
     * key space, and {@code t} and {@code u} share the same ids (and hence the same {@code val}s), so
     * a join on {@code val} matches row-for-row.
     */
    private void insertBucket(@Nonnull final RelationalConnectionRule connection, @Nonnull final String table,
                              final int remainder) throws SQLException {
        String values = IntStream.range(0, RECORD_COUNT / 3)
                .map(i -> (i * 3) + remainder)
                .mapToObj(id -> "(" + id + ", " + (id * 10) + ")")
                .collect(Collectors.joining(", "));
        try (RelationalStatement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO " + table + " VALUES " + values);
        }
    }

    /** Number of records in a single {@code id % 3} bucket over {@code [0, RECORD_COUNT)}. */
    private static int bucketSize() {
        return RECORD_COUNT / 3;
    }

    /** Runs a query on transaction A, fully draining it, and asserts it returned {@code expectedRows} rows. */
    private void assertQueryOnAReturns(@Nonnull final String query, final int expectedRows) throws SQLException {
        try (RelationalStatement statement = connectionA.createStatement();
                 RelationalResultSet resultSet = statement.executeQuery(query)) {
            RelationalResultSetAssert.assertThat(resultSet).hasRowCount(expectedRows);
        }
    }

    /** Runs a query on transaction A and asserts it returns exactly one row whose {@code VAL} column equals {@code expectedVal}. */
    private void assertQueryOnAReturnsVal(@Nonnull final String query, final long expectedVal) throws SQLException {
        try (RelationalStatement statement = connectionA.createStatement();
                 RelationalResultSet resultSet = statement.executeQuery(query)) {
            RelationalResultSetAssert.assertThat(resultSet).hasExactly(Map.of("VAL", expectedVal));
        }
    }

    /**
     * Performs transaction A's own write (a dedicated key B never touches, so it cannot cause a
     * write-write conflict) to make A a read-write transaction, then commits, asserting whether the
     * commit conflicts.
     */
    private void writeOwnKeyAndAssertCommit(final boolean expectConflict) throws SQLException {
        try (RelationalStatement statement = connectionA.createStatement()) {
            statement.executeUpdate("INSERT INTO t VALUES (" + A_OWN_KEY + ", 0)");
        }
        if (expectConflict) {
            RelationalAssertions.assertThrowsSqlException(connectionA::commit);
        } else {
            connectionA.commit();
        }
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
}
