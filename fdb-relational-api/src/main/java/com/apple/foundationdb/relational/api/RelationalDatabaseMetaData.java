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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.BuildVersion;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * Represents the MetaData about a database (schemas, tables, indexes, and so on).
 */
public interface RelationalDatabaseMetaData extends java.sql.DatabaseMetaData {
    /**
     * Name of the Relational product.
     * What we return when you call {@link #getDatabaseProductName()}
     */
    String DATABASE_PRODUCT_NAME = "Relational Database";

    /**
     * Get the Schemas for the specified schema pattern within this database, as a ResultSet.
     *
     * Equivalent to calling {@code getSchemas(currentDatabase(),null)}.
     *
     * The returned columns are:
     * 1. <em>TABLE_SCHEM</em> String -- schema name
     * 2. <em>TABLE_CATALOG</em> String -- The path of the database.
     *
     * Unlike in the superinterface definition, the returned results for this method are not required to be ordered.
     *
     * @return a list of schemas contained in the currently connected database.
     * @throws SQLException if something goes wrong.
     */
    @Nonnull
    @Override
    RelationalResultSet getSchemas() throws SQLException;

    /**
     * Get the Schemas for the specified schema pattern within the specified database, as a ResultSet.
     *
     * For the moment, the only valid schemaPattern is {@code null} (TODO). Any other patterns will be
     * ignored.
     *
     * The returned columns are:
     * 1. <em>TABLE_SCHEM</em> String -- schema name
     * 2. <em>TABLE_CATALOG</em> String -- The path of the database.
     *
     * Unlike in the superinterface definition, the returned results for this method are not required to be ordered.
     *
     * @param catalog the database path to get schemas for.
     * @param schemaPattern the schema pattern to use in searching. Currently ignored
     * @return a list of schemas contained in the specified database
     * @throws SQLException if something goes wrong.
     */
    @Override
    RelationalResultSet getSchemas(String catalog, String schemaPattern) throws SQLException;

    /**
     * Get the tables for the specified schema within this database, as a ResultSet.
     *
     * The returned resultset has the following fields:
     * <ol>
     *   <li> TABLE_CAT = the full path of the database</li>
     *   <li> TABLE_SCHEM = the name of the schema</li>
     *   <li> TABLE_NAME = the name of the table</li>
     *   <li> TABLE_VERSION = the version of the Schema Template where this table was created </li>
     * </ol>
     *
     * Note that the {@code TABLE_VERSION} field is not always known (due to data pre-existing the creation of Relational);
     * in these cases NULL will be returned
     *
     * @param catalog the name of the database to search.
     * @param schema the name of the schema to search.
     * @param tableName the name of the table to search.
     * @param types the type of tables to filter.
     * @return a resultset with a schema listing.
     * @throws SQLException with ErrorCode {@link ErrorCode#UNDEFINED_SCHEMA} if the schema
     * does not exist within this database; a different error code if something systemic goes wrong.
     */
    @Override
    @Nonnull
    RelationalResultSet getTables(
            String catalog,
            String schema,
            String tableName,
            String[] types) throws SQLException;

    /**
     * Get the columns for the specified table within the specified schema, as a ResultSet.
     *
     * The returned resultset has the following fields:
     * <ol>
     *  <li> TABLE_CAT = the name of the database</li>
     *  <li> TABLE_SCHEM = the name of the schema</li>
     *  <li> TABLE_NAME = the name of the table</li>
     *  <li> COLUMN_NAME = the name of the column</li>
     *  <li> TYPE_NAME = a string-formatted representation of the type of the column</li>
     *  <li> BL_INDEX_TYPE = the index of the column within the type</li>
     *  <li> BL_OPTIONS = a json-style string holding all the options set on that field by the descriptor.</li>
     * </ol>
     *
     *
     * @param catalog the database of interest.
     * @param schema the schema of interest.
     * @param table the table of interest. There must be a table with this name in the specified schema, or a
     *              {@link ErrorCode#UNDEFINED_TABLE} will be thrown.
     * @param column the column of interest.
     * @return a ResultSet with a column listing for the table.
     * @throws SQLException if something goes wrong.
     */
    @Override
    @Nonnull
    RelationalResultSet getColumns(
            String catalog,
            String schema,
            String table,
            String column) throws SQLException;

    @Override
    RelationalResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException;

    /**
     * Retrieves a description of the given table's primary key columns.
     *
     * <P>Each primary key column description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *  <LI><B>KEY_SEQ</B> short {@code =>} sequence number within primary key( a value
     *  of 1 represents the first column of the primary key, a value of 2 would
     *  represent the second column within the primary key).
     *  </OL>
     *
     *
     * <P>Unlike in the superinterface definition, the rows are not guaranteed to return in any order.
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in the database
     * @return <code>ResultSet</code> - each row is a primary key column description
     * @exception SQLException if a database access error occurs
     */
    @Override
    RelationalResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException;

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getCatalogs() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getTableTypes() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean allProceduresAreCallable() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean allTablesAreSelectable() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getURL() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getUserName() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isReadOnly() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean nullsAreSortedHigh() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean nullsAreSortedLow() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean nullsAreSortedAtStart() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean nullsAreSortedAtEnd() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getDatabaseProductName() throws SQLException {
        return DATABASE_PRODUCT_NAME;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getDatabaseProductVersion() throws SQLException {
        return BuildVersion.getInstance().getVersion();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getDriverName() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getDriverVersion() throws SQLException {
        return BuildVersion.getInstance().getVersion();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport //nothing to test
    default int getDriverMajorVersion() {
        return BuildVersion.getInstance().getMajorVersion();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport //nothing to test
    default int getDriverMinorVersion() {
        return BuildVersion.getInstance().getMinorVersion();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean usesLocalFiles() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean usesLocalFilePerTable() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean storesUpperCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean storesLowerCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean storesMixedCaseIdentifiers() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getIdentifierQuoteString() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getSQLKeywords() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getNumericFunctions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getStringFunctions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getSystemFunctions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getTimeDateFunctions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getSearchStringEscape() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    default String getExtraNameCharacters() throws SQLException {
        // For now, no extra name characters.
        return "";
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsColumnAliasing() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean nullPlusNonNullIsNull() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsConvert() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsConvert(int fromType, int toType) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsTableCorrelationNames() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsDifferentTableCorrelationNames() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsExpressionsInOrderBy() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsOrderByUnrelated() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsGroupBy() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsGroupByUnrelated() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsGroupByBeyondSelect() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsLikeEscapeClause() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsMultipleResultSets() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsMultipleTransactions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsNonNullableColumns() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsCoreSQLGrammar() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsANSI92FullSQL() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsOuterJoins() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsFullOuterJoins() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsLimitedOuterJoins() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getSchemaTerm() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getProcedureTerm() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getCatalogTerm() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isCatalogAtStart() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getCatalogSeparator() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSchemasInDataManipulation() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsPositionedDelete() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsPositionedUpdate() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSelectForUpdate() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsStoredProcedures() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSubqueriesInComparisons() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSubqueriesInExists() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSubqueriesInIns() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsCorrelatedSubqueries() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsUnion() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsUnionAll() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxBinaryLiteralLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxCharLiteralLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxColumnNameLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxColumnsInGroupBy() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxColumnsInIndex() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxColumnsInOrderBy() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxColumnsInSelect() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxColumnsInTable() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxConnections() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxCursorNameLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxIndexLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxSchemaNameLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxProcedureNameLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxCatalogNameLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxRowSize() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxStatementLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxStatements() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxTableNameLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxTablesInSelect() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getMaxUserNameLength() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getDefaultTransactionIsolation() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsTransactions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getTypeInfo() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsResultSetType(int type) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean ownDeletesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean ownInsertsAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean othersDeletesAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean othersInsertsAreVisible(int type) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean updatesAreDetected(int type) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean deletesAreDetected(int type) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean insertsAreDetected(int type) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsBatchUpdates() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default Connection getConnection() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsSavepoints() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsNamedParameters() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsMultipleOpenResults() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsGetGeneratedKeys() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsResultSetHoldability(int holdability) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getResultSetHoldability() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getDatabaseMajorVersion() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getDatabaseMinorVersion() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getJDBCMajorVersion() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getJDBCMinorVersion() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getSQLStateType() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsStatementPooling() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    default <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
