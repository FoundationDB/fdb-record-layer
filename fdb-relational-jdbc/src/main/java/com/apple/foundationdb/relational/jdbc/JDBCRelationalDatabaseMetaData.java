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

import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataResponse;
import com.apple.foundationdb.relational.util.BuildVersion;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

class JDBCRelationalDatabaseMetaData implements RelationalDatabaseMetaData {
    private final DatabaseMetaDataResponse pbDatabaseMetaDataResponse;
    private final Connection connection;

    JDBCRelationalDatabaseMetaData(Connection connection,
                                 DatabaseMetaDataResponse pbDatabaseMetaDataResponse) {
        this.pbDatabaseMetaDataResponse = pbDatabaseMetaDataResponse;
        this.connection = connection;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean allProceduresAreCallable() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean allTablesAreSelectable() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getURL() throws SQLException {
        return this.pbDatabaseMetaDataResponse.getUrl();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getUserName() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isReadOnly() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean nullsAreSortedHigh() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean nullsAreSortedLow() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean nullsAreSortedAtStart() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw new SQLException("Not implemented");
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
        return BuildVersion.getInstance().getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return BuildVersion.getInstance().getMinorVersion();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean usesLocalFiles() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean usesLocalFilePerTable() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getIdentifierQuoteString() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getSQLKeywords() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getNumericFunctions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getStringFunctions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getSystemFunctions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getTimeDateFunctions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getSearchStringEscape() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getExtraNameCharacters() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsColumnAliasing() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsConvert() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsTableCorrelationNames() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsOrderByUnrelated() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsGroupBy() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsGroupByUnrelated() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsLikeEscapeClause() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsMultipleResultSets() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsMultipleTransactions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsNonNullableColumns() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsANSI92FullSQL() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsOuterJoins() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsFullOuterJoins() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getSchemaTerm() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getProcedureTerm() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getCatalogTerm() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isCatalogAtStart() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getCatalogSeparator() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsPositionedDelete() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsPositionedUpdate() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSelectForUpdate() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsStoredProcedures() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSubqueriesInExists() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSubqueriesInIns() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsUnion() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsUnionAll() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxBinaryLiteralLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxCharLiteralLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxColumnNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxColumnsInGroupBy() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxColumnsInIndex() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxColumnsInOrderBy() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxColumnsInSelect() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxColumnsInTable() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxConnections() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxCursorNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxIndexLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxSchemaNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxProcedureNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxCatalogNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxRowSize() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxStatementLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxStatements() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxTableNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxTablesInSelect() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getDefaultTransactionIsolation() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsTransactions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getSchemas() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getCatalogs() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getTableTypes() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getTypeInfo() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsResultSetType(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean updatesAreDetected(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean deletesAreDetected(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean insertsAreDetected(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsBatchUpdates() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsSavepoints() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsNamedParameters() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsMultipleOpenResults() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return BuildVersion.getInstance().getMajorVersion(getDatabaseProductVersion());
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return BuildVersion.getInstance().getMinorVersion(getDatabaseProductVersion());
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        Driver driver = DriverManager.getDriver(JDBCRelationalDriver.JDBC_BASE_URL);
        return driver.getMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        Driver driver = DriverManager.getDriver(JDBCRelationalDriver.JDBC_BASE_URL);
        return driver.getMajorVersion();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getSQLStateType() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsStatementPooling() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLException("Not implemented");
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
