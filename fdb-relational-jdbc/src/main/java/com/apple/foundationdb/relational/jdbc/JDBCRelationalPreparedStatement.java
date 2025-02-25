/*
 * JDBCRelationalPreparedStatement.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.jdbc.grpc.v1.Parameter;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;


class JDBCRelationalPreparedStatement implements RelationalPreparedStatement {
    /**
     * The engine this class uses to do its work.
     * {@link JDBCRelationalStatement} has extension for handling PreparedStatements.
     * We do it this way to save on duplication.
     */
    private final JDBCRelationalStatement statement;

    /**
     * We implement positional parameters only, at least for now.
     * Use tree map so the integer keys sort in order. Value is Column because it embodies type.
     * We don't handle nulls yet.
     */
    private final Map<Integer, Parameter> parameters = new TreeMap<>();

    private final String sql;

    JDBCRelationalPreparedStatement(@Nonnull String sql, @Nonnull final JDBCRelationalConnection connection) throws SQLException {
        this.statement = new JDBCRelationalStatement(connection);
        this.sql = sql;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return this.statement.getUpdateCount();
    }

    @Override
    public RelationalResultSet executeQuery() throws SQLException {
        return this.statement.executeQuery(this.sql, this.parameters.values());
    }

    @Override
    public boolean execute() throws SQLException {
        return this.statement.execute(this.sql, this.parameters.values());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return this.statement.executeUpdate(this.sql, this.parameters.values());
    }

    @Override
    public void setBoolean(int parameterIndex, boolean b) throws SQLException {
        parameters.put(parameterIndex, ParameterHelper.ofBoolean(b));
    }

    @Override
    public void setInt(int parameterIndex, int i) throws SQLException {
        parameters.put(parameterIndex, ParameterHelper.ofInt(i));
    }

    @Override
    public void setLong(int parameterIndex, long l) throws SQLException {
        parameters.put(parameterIndex, ParameterHelper.ofLong(l));
    }

    @Override
    public void setFloat(int parameterIndex, float f) throws SQLException {
        parameters.put(parameterIndex, ParameterHelper.ofFloat(f));
    }

    @Override
    public void setDouble(int parameterIndex, double d) throws SQLException {
        parameters.put(parameterIndex, ParameterHelper.ofDouble(d));
    }

    @Override
    public void setString(int parameterIndex, String s) throws SQLException {
        parameters.put(parameterIndex, ParameterHelper.ofString(s));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] bytes) throws SQLException {
        parameters.put(parameterIndex, ParameterHelper.ofBytes(bytes));
    }

    @Override
    public void setUUID(int parameterIndex, final UUID x) throws SQLException {
        parameters.put(parameterIndex,
                Parameter.newBuilder().setJavaSqlTypesCode(Types.OTHER).setParameter(Column.newBuilder()
                        .setString(x.toString()).build()).build());
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        parameters.put(parameterIndex, ParameterHelper.ofObject(x));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        parameters.put(parameterIndex, ParameterHelper.ofNull(sqlType));
    }

    @Override
    public void setArray(int parameterIndex, Array a) throws SQLException {
        parameters.put(parameterIndex, ParameterHelper.ofArray(a));
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setBoolean(String parameterName, boolean b) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setInt(String parameterName, int i) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setLong(String parameterName, long l) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setFloat(String parameterName, float f) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setDouble(String parameterName, double d) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setString(String parameterName, String str) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setBytes(String parameterName, byte[] bytes) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public void setArray(String parameterName, Array x) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public void setUUID(final String parameterName, final UUID x) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public void close() throws SQLException {
        this.statement.close();
    }

    @Override
    public int getMaxRows() throws SQLException {
        return statement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        statement.setMaxRows(max);
    }

    @Override
    public void cancel() throws SQLException {
        this.statement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.statement.clearWarnings();
    }

    @Override
    public RelationalResultSet getResultSet() throws SQLException {
        return this.statement.getResultSet();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.statement.getConnection();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.statement.isClosed();
    }
}
