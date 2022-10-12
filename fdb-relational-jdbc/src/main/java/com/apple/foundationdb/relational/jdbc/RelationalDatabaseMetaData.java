/*
 * RelationalDatabaseMetaData.java
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

import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataResponse;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

class RelationalDatabaseMetaData implements DatabaseMetaData {
    private final DatabaseMetaDataResponse databaseMetaDataResponse;

    RelationalDatabaseMetaData(DatabaseMetaDataResponse databaseMetaDataResponse) {
        this.databaseMetaDataResponse = databaseMetaDataResponse;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getURL() throws SQLException {
        return this.databaseMetaDataResponse.getUrl();
    }

    @Override
    public String getUserName() throws SQLException {
        return this.databaseMetaDataResponse.getUserName();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.databaseMetaDataResponse.getReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return this.databaseMetaDataResponse.getDatabaseProductName();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return this.databaseMetaDataResponse.getDatabaseProductVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        return this.databaseMetaDataResponse.getDriverName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return this.databaseMetaDataResponse.getDriverVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return this.databaseMetaDataResponse.getDriverMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return this.databaseMetaDataResponse.getDriverMinorVersion();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getStringFunctions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxConnections() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxStatements() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getSQLStateType() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Not implemented");
    }
}
