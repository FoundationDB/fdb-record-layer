/*
 * EmbeddedRelationalPreparedStatement.java
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
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.PreparedStatementParameters;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class EmbeddedRelationalPreparedStatement implements RelationalPreparedStatement {
    @Nonnull
    private final String sql;

    private final Map<Integer, Object> parameters = new TreeMap<>();
    private final Map<String, Object> namedParameters = new TreeMap<>();

    private boolean closed;

    private RelationalResultSet resultSet;

    @Nonnull
    private final EmbeddedRelationalConnection conn;

    public EmbeddedRelationalPreparedStatement(@Nonnull String sql, @Nonnull EmbeddedRelationalConnection conn) {
        this.sql = sql;
        this.conn = conn;
    }

    @Override
    public RelationalResultSet executeQuery() throws SQLException {
        try {
            checkOpen();
            Assert.notNull(sql);
            conn.ensureTransactionActive();
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            final var optionalResultSet = conn.metricCollector.clock(RelationalMetric.RelationalEvent.TOTAL_PROCESS_QUERY, () -> executeQueryInternal(sql));
            if (optionalResultSet.isPresent()) {
                resultSet = new ErrorCapturingResultSet(optionalResultSet.get());
                return resultSet;
            } else {
                throw new RelationalException("PreparedStatement.executeQuery must return a result set but was executed on a query that doesn't: " + sql,
                        ErrorCode.NO_RESULT_SET);
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setArray(String parameterName, Array x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    private Optional<RelationalResultSet> executeQueryInternal(@Nonnull String query) throws RelationalException {
        Options options = conn.getOptions();
        if (conn.getSchema() == null) {
            throw new RelationalException("No Schema specified", ErrorCode.UNDEFINED_SCHEMA);
        }
        try (var schema = conn.getRecordLayerDatabase().loadSchema(conn.getSchema())) {
            final FDBRecordStoreBase<Message> store = schema.loadStore().unwrap(FDBRecordStoreBase.class);
            final var preparedStatementParameters = PreparedStatementParameters.of(parameters, namedParameters);
            final var planGenerator = PlanGenerator.of(conn.frl.getPlanCache() == null ? Optional.empty() : Optional.of(conn.frl.getPlanCache()),
                    store.getRecordMetaData(),
                    store.getRecordStoreState(),
                    options);
            final var planContext = PlanContext.Builder.create()
                    .fromRecordStore(store)
                    .fromDatabase(conn.getRecordLayerDatabase())
                    .withMetricsCollector(conn.metricCollector)
                    .withPreparedParameters(preparedStatementParameters)
                    .withSchemaTemplate(conn.getSchemaTemplate())
                    .build();
            final Plan<?> plan = planGenerator.getPlan(query, planContext);
            final var executionContext = Plan.ExecutionContext.of(conn.transaction, options, conn, conn.metricCollector);
            if (plan instanceof QueryPlan) {
                return Optional.of(((QueryPlan) plan).execute(executionContext));
            } else {
                plan.execute(executionContext);
                return Optional.empty();
            }
        }
    }

    @Override
    public void close() throws SQLException {
        if (resultSet != null) {
            resultSet.close();
        }
        closed = true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (resultSet != null && !resultSet.isClosed()) {
            var ret = resultSet;
            resultSet = null;
            return ret;
        }
        throw new SQLException("no open result set available");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new RelationalException("Prepared Statement closed", ErrorCode.STATEMENT_CLOSED).toSqlException();
        }
    }
}
