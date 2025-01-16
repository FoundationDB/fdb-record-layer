/*
 * RelationalConnection.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.fluentsql.expression.ExpressionFactory;
import com.apple.foundationdb.relational.api.fluentsql.statement.StatementBuilderFactory;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * A connection to a Relational database.
 *
 * <p>
 *  This functions very much like a {@link java.sql.Connection}, but contains some overloaded methods
 *  to support {@link RelationalStatement}.
 * </p>
 */
public interface RelationalConnection extends java.sql.Connection {
    /**
     * Create a RelationalStatement which can be executed using this DatabaseConnection.
     *
     * If this connection instance is closed, this RelationalStatement becomes invalid and should be discarded. Using a
     * RelationalStatement object after it's backing Connection is closed is a programmer error.
     *
     * Note that this override specializes the createStatement return to make it a RelationalStatement rather than a
     * plain JDBC Statement. This change is non-additive as are the other methods in this extension to the
     * JDBC Connection Interface; it is a change made to make usage of the API go down easier. The supposition is that
     * the bulk of usage will be concerned with RelationalStatement rather than plain Statement and so we skirt the
     * need for a Statement#unwrap to get access to the Relational facility. We may come back to redo this convenience
     * later if this API 'anomaly' on the JDBC API proves a thorn.
     *
     * @return A new statement entity to manipulate the database with.
     * @throws SQLException if something goes wrong while creating a statement.
     */
    @Override
    RelationalStatement createStatement() throws SQLException;

    /**
     * Creates a {@link RelationalPreparedStatement} object for sending
     * parameterized SQL statements to the database.
            * <P>
     * A SQL statement with or without IN parameters can be
     * pre-compiled and stored in a <code>PreparedStatement</code> object. This
     * object can then be used to efficiently execute this statement
     * multiple times.
     * @param sql an SQL statement that may contain one or more '?' IN
     * parameter placeholders
     * @return a new default <code>RelationalPreparedStatement</code> object containing the
     * pre-compiled SQL statement
     * @exception SQLException if a database access error occurs
     * or this method is called on a closed connection
     */
    @Override
    RelationalPreparedStatement prepareStatement(String sql) throws SQLException;

    @Nonnull
    Options getOptions();

    void setOption(Options.Name name, Object value) throws SQLException;

    URI getPath();

    /* Unsupported SQL features*/
    @Override
    @ExcludeFromJacocoGeneratedReport
    default Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isReadOnly() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void setCatalog(String catalog) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getCatalog() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isValid(int timeout) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode(), null);
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode(), null);
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getClientInfo(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Nonnull
    default StatementBuilderFactory createStatementBuilderFactory() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Nonnull
    default ExpressionFactory createExpressionBuilderFactory() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    /**
     * Whether the connection supports multiple servers.
     * There are some changes to the behavior that are to be expected when running the tests in multi-server mode,
     * this method allows the system to make that decision.
     * @return TRUE if this connection can support multiple servers, false otherwise.
     */
    default boolean isMultiServer() {
        return false;
    }
}
