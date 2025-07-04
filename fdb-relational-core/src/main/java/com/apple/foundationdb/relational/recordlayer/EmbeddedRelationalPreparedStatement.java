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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;

import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.sql.Array;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@API(API.Status.EXPERIMENTAL)
public class EmbeddedRelationalPreparedStatement extends AbstractEmbeddedStatement implements RelationalPreparedStatement {
    @Nonnull
    private final String sql;
    @Nonnull
    private final Map<Integer, Object> parameters = new TreeMap<>();
    @Nonnull
    private final Map<String, Object> namedParameters = new TreeMap<>();

    public EmbeddedRelationalPreparedStatement(@Nonnull String sql, @Nonnull EmbeddedRelationalConnection conn) throws SQLException {
        super(conn);
        this.sql = sql;
    }

    @Override
    public RelationalResultSet executeQuery() throws SQLException {
        checkOpen();
        if (execute()) {
            return currentResultSet;
        } else {
            throw new SQLException(String.format(Locale.ROOT, "query '%s' does not return result set, use JDBC executeUpdate method instead", sql), ErrorCode.NO_RESULT_SET.getErrorCode());
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkOpen();
        if (execute()) {
            throw new SQLException(String.format(Locale.ROOT, "query '%s' returns a result set, use JDBC executeQuery method instead", sql), ErrorCode.EXECUTE_UPDATE_RETURNED_RESULT_SET.getErrorCode());
        }
        return currentRowCount;
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            return executeInternal(sql);
        } catch (RelationalException e) {
            throw e.toSqlException();
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
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setArray(String parameterName, Array x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setUUID(final int parameterIndex, final UUID x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setUUID(final String parameterName, final UUID x) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, null);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        checkOpen();
        namedParameters.put(parameterName, null);
    }

    @Override
    @Nonnull
    PlanContext createPlanContext(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final Options options) throws RelationalException {
        return PlanContext.builder()
                .fromRecordStore(store, options)
                .fromDatabase(conn.getRecordLayerDatabase())
                .withMetricsCollector(Assert.notNullUnchecked(conn.getMetricCollector()))
                .withPreparedParameters(PreparedParams.of(parameters, namedParameters))
                .withSchemaTemplate(conn.getTransaction().getBoundSchemaTemplateMaybe().orElse(conn.getSchemaTemplate()))
                .build();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
