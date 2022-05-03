/*
 * RelationalResultSet.java
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
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.protobuf.Message;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;

/**
 * Represents the results of a query against the system.
 */
public interface RelationalResultSet extends java.sql.ResultSet {

    /**
     * Get the entity at the specified position as an instance of {@link Iterable}.
     * <p>
     *     For columns which are not repeated, the returned Iterable will have only a single record, whose
     *     type is the Object form of the type of the data (i.e. a {@link Long}, not {@code long}).
     * </p>
     * @param oneBasedPosition the column position to get a repeated element for, indexed from 1.
     * @return the value of the column with the specified position as an Iterable.
     * @throws SQLException if something goes wrong.
     */
    Collection<?> getRepeated(int oneBasedPosition) throws SQLException;

    /**
     * Get the entity at the specified position as an instance of {@link Iterable}.
     * <p>
     *     This is equivalent to <pre>{@code
     *     RelationalResultSetMetaData vrsmd = getResultSetMetaData();
     *     for( int i=1;i<=vrsmd.getNumColumns(); i++){
     *          if(vrsmd.getColumnName(i).equalsIgnoreCase(fieldName)){
     *              return getRepeated(i);
     *          }
     *     }
     }</pre>
     * @param fieldName the name of the column to return.
     * @return the value of the column with the specified position as an Iterable.
     * @throws SQLException if something goes wrong. If the field does not exist,
     * the error code will be {@link ErrorCode#INVALID_PARAMETER}, otherwise the error code will tell you what happened.
     */
    Collection<?> getRepeated(String fieldName) throws SQLException;

    /**
     * Determine if the result set's current row support directly returning protobuf objects.
     *
     * @return {@code true} if current row can be parsed directly to protobuf message objects, {@code false} otherwise.
     */
    boolean supportsMessageParsing();

    /**
     * Return this row as a protobuf message (if possible).
     * <p>
     *     This method exists primarily as a performance and user-experience improvement. It is possible
     *     for the caller to reconstruct any Message type that they would like based on using the other methods
     *     supported in this interface (the `getX(String)` methods) and manually constructing a Protocol buffer
     *     on the other side. However, when the underlying query result is already a protocol buffer, and
     *     the caller wants a protocol buffer, then doing so is a needless copy operation.
     * </p>
     * <p>
     *     This normally wouldn't matter, but there are existing use-cases where this type of operation is currently
     *     going on, and so it is convenient to provide some simplicity in those use-cases. However, its use outside
     *     of existing scenarios is highly discouraged; don't use this unless you <em>really</em> know what you're
     *     doing and why.
     * </p>
     * <p>
     *     To use this properly, you must <em>always</em> first call {@link #supportsMessageParsing()}. It is possible
     *     (although somewhat unlikely) that a query could be able to parse some rows in a result set but not others.
     *     If this returns {@code false}, you must be prepared to extract information using the standard JDBC
     *     functions.
     * </p>
     *
     * @param <M> the expected type of the message.
     * @return the object at the specified position, as a protobuf Message
     * @throws SQLException if something goes wrong
     * @throws ArrayIndexOutOfBoundsException if the specified position is invalid
     */
    <M extends Message> M parseMessage() throws SQLException;

    /**
     * A {@code Continuation} that can be used for retrieving the rest of the rows.
     *
     * @return  A {@code Continuation} that can be used for retrieving the rest of the rows.
     * @throws RelationalException if the continuation cannot be retrieved.
     */
    Continuation getContinuation() throws RelationalException;

    /*Unsupported Operations*/
    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean wasNull() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default byte getByte(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default byte getByte(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default short getShort(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default short getShort(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default int getInt(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default int getInt(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    default BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Date getDate(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Date getDate(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Time getTime(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Time getTime(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    default InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default int findColumn(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default int getRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default int getFetchDirection() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default int getFetchSize() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default int getType() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default int getConcurrency() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Statement getStatement() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default boolean isClosed() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    default void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

}
