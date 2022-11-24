/*
 * JDBCRelationalResultSet.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.spanner.v1.ResultSet;

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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

class JDBCRelationalResultSet implements RelationalResultSet {
    private final ResultSet delegate;
    private final int rows;
    private int rowIndex = -1;

    JDBCRelationalResultSet(ResultSet delegate) {
        this.delegate = delegate;
        this.rows = delegate.getRowsCount();
    }

    @Override
    public boolean next() throws SQLException {
        return ++rowIndex < rows;
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        // Presume column is JDBC 1-based index.
        return this.delegate.getRows(rowIndex).getValues(columnIndex - 1).getStringValue();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        // Presume column is JDBC 1-based index.
        return this.delegate.getRows(rowIndex).getValues(columnIndex - 1).getBoolValue();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public byte getByte(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public short getShort(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        // Presume column is JDBC 1-based index.
        // TODO: This needs work.
        return (int) this.delegate.getRows(rowIndex).getValues(columnIndex - 1).getNumberValue();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        // Presume column is JDBC 1-based index.
        // TODO: This needs work.
        return (long) this.delegate.getRows(rowIndex).getValues(columnIndex - 1).getNumberValue();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public float getFloat(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public double getDouble(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public byte[] getBytes(int columnIndex) throws SQLException {
        return new byte[0];
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Date getDate(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Time getTime(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getString(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean getBoolean(String columnLabel) throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public byte getByte(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public short getShort(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getInt(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public long getLong(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public float getFloat(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public double getDouble(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public byte[] getBytes(String columnLabel) throws SQLException {
        return new byte[0];
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Date getDate(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Time getTime(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void clearWarnings() throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getCursorName() throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSetMetaData getMetaData() throws SQLException {
        return new JDBCRelationalResultSetMetaData(this.delegate.getMetadata());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Continuation getContinuation() throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalStruct getStruct(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalStruct getStruct(int oneBasedColumn) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Object getObject(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Object getObject(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int findColumn(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isBeforeFirst() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isFirst() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void beforeFirst() throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void afterLast() throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean first() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean last() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getRow() throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean absolute(int row) throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean relative(int rows) throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean previous() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getType() throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getConcurrency() throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNull(int columnIndex) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateByte(int columnIndex, byte x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateString(int columnIndex, String x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateString(String columnLabel, String x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void insertRow() throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateRow() throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void deleteRow() throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void refreshRow() throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalArray getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalArray getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getNString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getNString(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
