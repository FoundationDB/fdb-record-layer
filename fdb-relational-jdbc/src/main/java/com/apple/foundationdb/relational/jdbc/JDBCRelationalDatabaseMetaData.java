/*
 * JDBCRelationalDatabaseMetaData.java
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.jdbc.grpc.v1.DatabaseMetaDataResponse;
import com.apple.foundationdb.relational.util.BuildVersion;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

class JDBCRelationalDatabaseMetaData implements RelationalDatabaseMetaData {
    private final DatabaseMetaDataResponse pbDatabaseMetaDataResponse;
    private final RelationalConnection connection;

    JDBCRelationalDatabaseMetaData(RelationalConnection connection,
                                 DatabaseMetaDataResponse pbDatabaseMetaDataResponse) {
        this.pbDatabaseMetaDataResponse = pbDatabaseMetaDataResponse;
        this.connection = connection;
    }

    @Override
    public String getURL() throws SQLException {
        return this.pbDatabaseMetaDataResponse.getUrl();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        // Hardcoded.
        return unwrap(RelationalDatabaseMetaData.class).DATABASE_PRODUCT_NAME;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return this.pbDatabaseMetaDataResponse.getDatabaseProductVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        // Hardecoded.
        return JDBCRelationalDriver.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        // Return the build version which written at build time.
        return BuildVersion.getInstance().getVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        try {
            return BuildVersion.getInstance().getMajorVersion();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    @Override
    public int getDriverMinorVersion() {
        try {
            return BuildVersion.getInstance().getMinorVersion();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        // TODO: Pick this for now.
        return this.connection.getTransactionIsolation();
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        // TODO: Do this only for now.
        return level == getDefaultTransactionIsolation();
    }

    @Nonnull
    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getSchemas() throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(), ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(), ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public RelationalResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        // TODO: We are returning databases here, not Tables (per @alacurie). For dbeaver. FIX.
        try (var statement = this.connection.createStatement()) {
            return statement.executeQuery("select * from databases");
        }
    }

    @Nonnull
    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getColumns(String catalog, String schema, String table, String column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(), ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(), ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented in the relational layer " +
                Thread.currentThread().getStackTrace()[1].getMethodName(), ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        try {
            return BuildVersion.getInstance().getMajorVersion(getDatabaseProductVersion());
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        try {
            return BuildVersion.getInstance().getMinorVersion(getDatabaseProductVersion());
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        Driver driver = DriverManager.getDriver(JDBCURI.JDBC_BASE_URL);
        return driver.getMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        Driver driver = DriverManager.getDriver(JDBCURI.JDBC_BASE_URL);
        return driver.getMinorVersion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "CATALOG";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    /**
     * What is the string used to quote SQL identifiers? This returns a space if identifier quoting
     * isn't supported. A JDBC Compliant driver will always use a double quote character.
     * 
     *
     * @return the quoting string
     * @throws SQLException if a database access error occurs
     */
    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    /**
     * Does this driver support the ANSI-92 entry level SQL grammar? All JDBC Compliant drivers must
     * return true. We currently don't I'm sure.
     *
     * @return true which is a lie for now at least.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return false
     */
    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    /**
     * {@inheritDoc}
     *
     * @return false
     */
    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }
}
