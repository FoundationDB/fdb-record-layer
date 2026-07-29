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
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.RelationalResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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
 * read/commit versions, so interleaving the operations in program order (the query transaction reads,
 * the update transaction writes+commits, the query transaction writes+commits) is sufficient and
 * deterministic. {@link #queryConnection} runs the {@code SELECT} under test; {@link #updateConnection}
 * performs the concurrent write.
 * <p>
 * Records are partitioned by {@code id % 3} so that the concurrent writes are always <em>interleaved
 * between</em> records the query transaction actually reads. This guarantees the conflicting writes fall
 * within the query's read range. Specifically: {@code id % 3 == 0} records are the initial data the
 * query reads, {@code id % 3 == 1} records are written concurrently with the first query, and
 * {@code id % 3 == 2} records are written concurrently with the continuation. Tables {@code t} and
 * {@code u} carry the same ids so they can be joined and unioned.
 */
@Tag(Tags.RequiresFDB)
public class SnapshotIsolationConcurrencyTest {

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t(id BIGINT, val BIGINT, PRIMARY KEY(id))"
            + " CREATE TABLE u(id BIGINT, val BIGINT, PRIMARY KEY(id))";

    private static final int RECORD_COUNT = 30;

    /** Upper bound of the scan; every data id is below it, so the scan covers the whole data range. */
    private static final long SCAN_UPPER_BOUND = 1000L;

    /** The query transaction runs; a full range scan over the data. */
    private static final String SCAN_QUERY = "SELECT id FROM t WHERE id < " + SCAN_UPPER_BOUND;

    /**
     * A dedicated key that the query transaction writes (so it is a read-write transaction that can conflict).
     * It is outside the scanned range and is never written by the update transaction, so it can never
     * cause a conflict that would mask the read-conflict behavior under test.
     */
    private static final long QUERY_OWN_KEY = SCAN_UPPER_BOUND;
    private static final int BUCKET_COUNT = 3;

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database =
            new SimpleDatabaseRule(SnapshotIsolationConcurrencyTest.class, SCHEMA_TEMPLATE);

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule queryConnection =
            new RelationalConnectionRule(database::getConnectionUri).withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(BUCKET_COUNT)
    public final RelationalConnectionRule updateConnection =
            new RelationalConnectionRule(database::getConnectionUri).withSchema("TEST_SCHEMA");

    @ParameterizedTest
    @BooleanSource({"useSnapshot", "updateRecords"})
    void snapshotReadDoesNotCreateConflictRange(boolean useSnapshot, boolean updateRecords) throws SQLException {
        // Initial data: the id % 3 == 0 records.
        insertBucket(queryConnection, "t", 0);

        queryConnection.setAutoCommit(false);
        updateConnection.setAutoCommit(false);

        // Query transaction: read the whole range, optionally at snapshot isolation, fully draining it. It
        // must have actually read the initial records, so that the update transaction's interleaved writes
        // below fall between records it read (and hence within its read range).
        final var query = SCAN_QUERY + (useSnapshot ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");
        assertQueryReturns(queryConnection, query, bucketSize());

        // Update transaction:
        if (updateRecords) {
            // update the records we read
            try (RelationalStatement statement = updateConnection.createStatement()) {
                statement.executeUpdate("UPDATE t SET val = val + 10");
            }
        } else {
            // insert the id % 3 == 1 records (interleaved between the records the query read)
            // and commit.
            insertBucket(updateConnection, "t", 1);
        }
        updateConnection.commit();

        // A snapshot read added no conflict range, so the interleaved writes do not conflict; a serializable
        // read spans them, so the query transaction conflicts.
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
        insertBucket(queryConnection, "t", 0);

        queryConnection.setAutoCommit(false);
        updateConnection.setAutoCommit(false);

        // Query transaction: read the first page (one row) of a snapshot scan, obtaining a continuation.
        final Continuation firstPage;
        try (RelationalStatement statement = queryConnection.createStatement()) {
            statement.setMaxRows(1);
            try (RelationalResultSet resultSet =
                    statement.executeQuery(SCAN_QUERY + " OPTIONS (ISOLATION LEVEL SNAPSHOT)")) {
                assertThat(resultSet.next()).isTrue();
                firstPage = resultSet.getContinuation();
            }
        }
        assertThat(firstPage.atEnd()).isFalse();

        // Update transaction: concurrent with the first query, insert the id % 3 == 1 records and commit.
        insertBucket(updateConnection, "t", 1);
        updateConnection.commit();

        final var resumeSql = "EXECUTE CONTINUATION ?continuation"
                + (repeatOptionOnResume ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");

        // Resume the first page of the continuation (guaranteed to exist by the assertion above).
        Continuation current = resumeContinuationPage(queryConnection, resumeSql, firstPage);

        // Update transaction: concurrent with the continuation, insert the id % 3 == 2 records and commit.
        // These are interleaved between the id % 3 == 0 records the continuation is still reading, so
        // they fall within the continuation's read range no matter where the first page ended.
        insertBucket(updateConnection, "t", 2);
        updateConnection.commit();

        // Drain the remaining continuation pages within the query transaction.
        while (!current.atEnd()) {
            current = resumeContinuationPage(queryConnection, resumeSql, current);
        }

        // With the option repeated, every page ran at snapshot isolation and the query transaction commits
        // cleanly; with a bare resume the continuation ran serializable and its read range spans the update
        // transaction's writes, so the query transaction conflicts.
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
        queryConnection.setAutoCommit(false);

        // Write a new row within the query transaction; a point read in the same transaction must see it.
        try (RelationalStatement statement = queryConnection.createStatement()) {
            statement.executeUpdate("INSERT INTO t VALUES (777, 42)");
        }
        assertQueryReturnsVal(queryConnection, "SELECT val FROM t WHERE id = 777" + option, 42L);

        // A range scan (not a point lookup) in the same transaction also sees the freshly-written row;
        // 777 is the only row, so the scan returns exactly it.
        assertQueryReturnsVal(queryConnection, "SELECT val FROM t WHERE id > 100" + option, 42L);

        // Update the row within the same transaction; a subsequent (snapshot) read sees the new value.
        try (RelationalStatement statement = queryConnection.createStatement()) {
            statement.executeUpdate("UPDATE t SET val = 99 WHERE id = 777");
        }
        assertQueryReturnsVal(queryConnection, "SELECT val FROM t WHERE id = 777" + option, 99L);

        queryConnection.commit();
    }

    /**
     * Snapshot isolation affects only conflict detection, not visibility: like a serializable read, a
     * snapshot read observes the database as of the transaction's read version and does <em>not</em> see
     * writes committed by other transactions after that read version. This is what distinguishes it from
     * {@code READ_COMMITTED}, under which the second read would observe the update transaction's committed
     * insert.
     */
    @Test
    void snapshotReadDoesNotSeeOtherCommittedWrites() throws SQLException {
        // Initial committed data: the id % 3 == 0 records.
        insertBucket(updateConnection, "t", 0);

        queryConnection.setAutoCommit(false);
        updateConnection.setAutoCommit(false);

        // First snapshot read: pins the query transaction's read version and observes the initial data.
        // We do a different query so that we have not queried all of the data that we will query in the second
        // transaction.
        assertQueryReturns(queryConnection, "SELECT id FROM t WHERE id < " + BUCKET_COUNT * 2 +
                " OPTIONS (ISOLATION LEVEL SNAPSHOT)", 2);

        // Update transaction commits new rows (id % 3 == 1) after the query transaction's read version was
        // established.
        insertBucket(updateConnection, "t", 1);
        updateConnection.commit();

        // The query transaction reads again at snapshot isolation: it still sees only the original rows, not
        // the newly committed insert. Under READ_COMMITTED this second read would return 2 * bucketSize() rows.
        assertQueryReturns(queryConnection, SCAN_QUERY + " OPTIONS (ISOLATION LEVEL SNAPSHOT)", bucketSize());

        queryConnection.commit();
    }

    /**
     * A join read at snapshot isolation adds no conflict ranges for <em>either</em> joined table, so a
     * concurrent insert into either input does not cause the query transaction to conflict; the serializable
     * control does conflict. The join is on the non-indexed {@code val} column so that both inputs are fully
     * scanned, guaranteeing the update transaction's interleaved insert into whichever table falls within the
     * query's read range.
     */
    @ParameterizedTest
    @BooleanSource({"useSnapshot", "writeToU"})
    void snapshotJoinDoesNotConflictWithConcurrentWrites(boolean useSnapshot, boolean writeToU) throws SQLException {
        insertBucket(queryConnection, "t", 0);
        insertBucket(queryConnection, "u", 0);

        queryConnection.setAutoCommit(false);
        updateConnection.setAutoCommit(false);

        // Query transaction: join t and u on the non-indexed val column, forcing both tables to be scanned.
        final var query = "SELECT t.id FROM t, u WHERE t.val = u.val"
                + (useSnapshot ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");
        assertQueryReturns(queryConnection, query, bucketSize());

        // Update transaction: insert interleaved rows into whichever joined table, and commit.
        insertBucket(updateConnection, writeToU ? "u" : "t", 1);
        updateConnection.commit();

        // A snapshot join added no conflict range for either input; a serializable join spans the writes.
        writeOwnKeyAndAssertCommit(!useSnapshot);
    }

    /**
     * A {@code UNION ALL} read at snapshot isolation adds no conflict ranges for either input, so a
     * concurrent insert into either unioned table does not cause the query transaction to conflict; the
     * serializable control does conflict. {@code UNION ALL} scans both inputs fully, so the update
     * transaction's interleaved insert into whichever table falls within the query's read range.
     */
    @ParameterizedTest
    @BooleanSource({"useSnapshot", "writeToU"})
    void snapshotUnionDoesNotConflictWithConcurrentWrites(boolean useSnapshot, boolean writeToU) throws SQLException {
        insertBucket(queryConnection, "t", 0);
        insertBucket(queryConnection, "u", 0);

        queryConnection.setAutoCommit(false);
        updateConnection.setAutoCommit(false);

        // Query transaction: union of t and u, which scans both tables fully.
        final var query = "SELECT id FROM t WHERE id < " + SCAN_UPPER_BOUND
                + " UNION ALL SELECT id FROM u WHERE id < " + SCAN_UPPER_BOUND
                + (useSnapshot ? " OPTIONS (ISOLATION LEVEL SNAPSHOT)" : "");
        assertQueryReturns(queryConnection, query, 2 * bucketSize());

        // Update transaction: insert interleaved rows into whichever unioned table, and commit.
        insertBucket(updateConnection, writeToU ? "u" : "t", 1);
        updateConnection.commit();

        // A snapshot union added no conflict range for either input; a serializable union spans the writes.
        writeOwnKeyAndAssertCommit(!useSnapshot);
    }

    /**
     * Inserts every id in {@code [0, RECORD_COUNT)} whose {@code id % BUCKET_COUNT == remainder} into the given
     * table on the given connection, with {@code val = id * 10}. The three buckets interleave in the
     * key space, and {@code t} and {@code u} share the same ids (and hence the same {@code val}s), so
     * a join on {@code val} matches row-for-row.
     */
    private void insertBucket(@Nonnull final RelationalConnectionRule connection, @Nonnull final String table,
                              final int remainder) throws SQLException {
        String values = IntStream.range(0, RECORD_COUNT / BUCKET_COUNT)
                .map(i -> (i * BUCKET_COUNT) + remainder)
                .mapToObj(id -> "(" + id + ", " + (id * 10) + ")")
                .collect(Collectors.joining(", "));
        try (RelationalStatement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO " + table + " VALUES " + values);
        }
    }

    /** Number of records in a single {@code id % 3} bucket over {@code [0, RECORD_COUNT)}. */
    private static int bucketSize() {
        return RECORD_COUNT / BUCKET_COUNT;
    }

    /** Runs a query on the connection, fully draining it, and asserts it returned {@code expectedRows} rows. */
    private void assertQueryReturns(@Nonnull RelationalConnectionRule connection, @Nonnull final String query,
                                    final int expectedRows) throws SQLException {
        try (RelationalStatement statement = connection.createStatement();
                 RelationalResultSet resultSet = statement.executeQuery(query)) {
            RelationalResultSetAssert.assertThat(resultSet).hasRowCount(expectedRows);
        }
    }

    /** Runs a query on the connection and asserts it returns exactly one row whose {@code VAL} column equals {@code expectedVal}. */
    private void assertQueryReturnsVal(@Nonnull RelationalConnectionRule connection, @Nonnull final String query,
                                       final long expectedVal) throws SQLException {
        try (RelationalStatement statement = connection.createStatement();
                 RelationalResultSet resultSet = statement.executeQuery(query)) {
            RelationalResultSetAssert.assertThat(resultSet).hasExactly(Map.of("VAL", expectedVal));
        }
    }

    /**
     * Performs the query transaction's own write (a dedicated key the update transaction never touches, so
     * it cannot cause a write-write conflict) to make it a read-write transaction, then commits, asserting
     * whether the commit conflicts. A conflict surfaces specifically as a {@code SERIALIZATION_FAILURE}
     * (rather than any {@code SQLException}), so an unrelated failure cannot make the control case pass.
     */
    private void writeOwnKeyAndAssertCommit(final boolean expectConflict) throws SQLException {
        try (RelationalStatement statement = queryConnection.createStatement()) {
            statement.executeUpdate("INSERT INTO t VALUES (" + QUERY_OWN_KEY + ", 0)");
        }
        if (expectConflict) {
            RelationalAssertions.assertThrowsSqlException(queryConnection::commit)
                    .hasErrorCode(ErrorCode.SERIALIZATION_FAILURE);
        } else {
            queryConnection.commit();
        }
    }

    /** Resumes a single page of the continuation on the query transaction, draining and returning the next continuation. */
    private Continuation resumeContinuationPage(@Nonnull RelationalConnectionRule connection,
                                                @Nonnull final String resumeSql,
                                                @Nonnull final Continuation from) throws SQLException {
        try (RelationalPreparedStatement statement = connection.prepareStatement(resumeSql)) {
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
