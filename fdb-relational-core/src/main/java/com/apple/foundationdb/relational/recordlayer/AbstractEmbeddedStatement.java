/*
 * AbstractEmbeddedStatement.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Abstract class to provide a common implementation for Statement.execute.
 * Should be used by classes that derive {@link com.apple.foundationdb.relational.api.RelationalPreparedStatement} and {@link com.apple.foundationdb.relational.api.RelationalStatement}
 */
public abstract class AbstractEmbeddedStatement implements java.sql.Statement {
    @Nonnull
    final EmbeddedRelationalConnection conn;

    @Nullable
    RelationalResultSet currentResultSet;
    boolean closed;

    int currentRowCount;

    public AbstractEmbeddedStatement(@Nonnull final EmbeddedRelationalConnection conn) {
        this.conn = conn;
    }

    abstract PlanContext buildPlanContext(FDBRecordStoreBase<Message> store) throws RelationalException;

    @SuppressWarnings("PMD.PreserveStackTrace")
    public boolean executeInternal(String sql) throws SQLException, RelationalException {
        checkOpen();
        if (currentResultSet != null) {
            currentResultSet.close();
        }
        Assert.notNull(sql);
        conn.ensureTransactionActive();
        return conn.metricCollector.clock(RelationalMetric.RelationalEvent.TOTAL_PROCESS_QUERY, () -> {
            try {
                conn.ensureTransactionActive();
                Options options = conn.getOptions();
                if (conn.getSchema() == null) {
                    throw new RelationalException("No Schema specified", ErrorCode.UNDEFINED_SCHEMA);
                }
                try (var schema = conn.getRecordLayerDatabase().loadSchema(conn.getSchema())) {
                    final var store = schema.loadStore().unwrap(FDBRecordStoreBase.class);
                    final var planGenerator = PlanGenerator.of(conn.frl.getPlanCache() == null ? Optional.empty() : Optional.of(conn.frl.getPlanCache()),
                            store.getRecordMetaData(), store.getRecordStoreState(), options);
                    final Plan<?> plan = planGenerator.getPlan(sql, buildPlanContext(store));
                    options = Options.combine(planGenerator.getOptions(), options);
                    final var executionContext = Plan.ExecutionContext.of(conn.transaction, options, conn, conn.metricCollector);
                    if (plan instanceof QueryPlan) {
                        currentResultSet = new ErrorCapturingResultSet(((QueryPlan) plan).execute(executionContext));
                        if (plan.isUpdatePlan()) {
                            //this is an update statement, so generate the row count and set the update clause
                            try (ResultSet updateResultSet = currentResultSet) {
                                currentResultSet = null;
                                currentRowCount = countUpdates(updateResultSet);
                                return false;
                            }
                        } else {
                            //result set statements get a -1 for update count
                            currentRowCount = -1;
                            return true;
                        }
                    } else {
                        plan.execute(executionContext);
                        currentResultSet = null;
                        //ddl statements are updates that don't return results, so they get 0 for row count
                        currentRowCount = 0;
                        if (conn.getAutoCommit()) {
                            conn.commit();
                        }
                        return false;
                    }
                }
            } catch (RelationalException | SQLException | RuntimeException ex) {
                if (conn.getAutoCommit()) {
                    try {
                        conn.rollback();
                    } catch (SQLException e) {
                        e.addSuppressed(ex);
                        throw ExceptionUtil.toRelationalException(e);
                    }
                }
                throw ExceptionUtil.toRelationalException(ex);
            }
        });
    }

    @Override
    public RelationalResultSet getResultSet() throws SQLException {
        checkOpen();
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            var resultSet = currentResultSet;
            currentResultSet = null;
            return resultSet;
        }
        throw new SQLException("no open result set available");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkOpen();
        if (currentResultSet != null) {
            return -1;
        } else {
            return currentRowCount; // current spec.
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkOpen();
        return conn;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void close() throws SQLException {
        try {
            if (currentResultSet != null) {
                currentResultSet.close();
            }
            closed = true;
        } catch (RuntimeException ex) {
            if (conn.getAutoCommit()) {
                conn.rollback();
            }
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
        }
    }

    void checkOpen() throws SQLException {
        if (closed) {
            throw new RelationalException("Statement closed", ErrorCode.STATEMENT_CLOSED).toSqlException();
        }
    }

    private int countUpdates(@Nonnull ResultSet resultSet) throws SQLException {
        /*
         * This is a bit of a temporary hack both to address a bug(TODO), and also to get around the
         * way that RecordLayer DML plans are executed.
         *
         * The return of a record layer plan is _always_ a ResultSet, even when it's a straight insert operation.
         * For DML operations the result set contains the rows that were written, which makes sense from a planning
         * and execution perspective but is nearly useless to us at this stage, where all we want to know
         * is the number of records mutated. To get that result, we have to quickly process the returned result
         * set and count the rows returned. It's a tad expensive, but (typically) should be easy enough since
         * they'll be held in memory.
         *
         * The returned value is an int because that's what JDBC expects, but also because getting more than
         * Integer.MAX_VALUE results into FDB is currently(as of Spring 2023) impossible.
         *
         */
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            if (conn.getAutoCommit()) {
                conn.commit();
            }
            return count;
        } catch (SQLException | RuntimeException ex) {
            if (conn.getAutoCommit()) {
                conn.rollback();
            }
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
        }
    }
}
