/*
 * BasicMetadataTest.java
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

import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for our basic ability to get Table and Database Metadata from the Connection.
 */
public class BasicMetadataTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension,
            URI.create("/basic_metadata_test"), TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule dbConn = new RelationalConnectionRule(database::getConnectionUri);

    @Test
    void canGetPrimaryKeysForTable() throws SQLException {
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");

        try (final RelationalResultSet pks = metaData.getPrimaryKeys(database.getDatabasePath().getPath(),
                "testSchema", "RestaurantRecord")) {
            ResultSetAssert.assertThat(pks).hasNextRow()
                    .hasRowExactly(
                            database.getDatabasePath().getPath(),
                            "testSchema",
                            "RestaurantRecord",
                            "rest_no",
                            1,
                            null);
        }
    }

    @Test
    void canGetSchemasForDatabase() throws SQLException {
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");

        try (final RelationalResultSet schemas = metaData.getSchemas(database.getDatabasePath().getPath(), null)) {
            Set<String> retData = new HashSet<>();
            ResultSetAssert.assertThat(schemas)
                    .meetsForAllRows(ResultSetAssert.perRowCondition(rs ->
                            database.getDatabasePath().getPath().equals(schemas.getString("TABLE_CATALOG")) &&
                                    retData.add(schemas.getString("TABLE_SCHEM")), "Should not see the same schema twice"));
            org.assertj.core.api.Assertions.assertThat(retData).contains("testSchema");
        }
    }

    @Test
    void getSchemasForNullDatabaseThrowsException() throws SQLException {
        //TODO(bfines) remove this test when the catalog pattern is allowed to be null(TODO)
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");

        SQLException ve = Assertions.assertThrows(SQLException.class, () -> metaData.getSchemas(null, null));
        Assertions.assertEquals(ErrorCode.UNSUPPORTED_OPERATION.getErrorCode(), ve.getSQLState(), "Incorrect SQL state!");
    }

    @Test
    void canGetTablesForSchema() throws SQLException {
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");

        try (final RelationalResultSet tables = metaData.getTables(null, "testSchema", null, null)) {
            Assertions.assertNotNull(tables, "Null tables returned");
            List<String> retTableNames = new ArrayList<>();
            while (tables.next()) {
                retTableNames.add(tables.getString("TABLE_NAME"));
            }
            assertThat(retTableNames).containsExactlyInAnyOrder("RestaurantRecord", "RestaurantReviewer");
        }
    }

    @Test
    void getTablesForMissingSchemaThrowsException() throws SQLException {
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");

        RelationalAssertions.assertThrowsSqlException(() -> metaData.getTables(null, "missingSchema", null, null))
                .hasErrorCode(ErrorCode.SCHEMA_NOT_FOUND);
    }

    @Test
    void canGetTableColumns() throws SQLException {
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");

        try (final RelationalResultSet tableData = metaData.getColumns(null, "testSchema", "RestaurantRecord", null)) {
            List<Tuple> rows = new ArrayList<>();
            while (tableData.next()) {
                rows.add(new Tuple()
                        .add(tableData.getString("TABLE_CAT"))
                        .add(tableData.getString("TABLE_SCHEM"))
                        .add(tableData.getString("TABLE_NAME"))
                        .add(tableData.getString("COLUMN_NAME"))
                        .add(tableData.getString("TYPE_NAME"))
                        .add(tableData.getLong("ORDINAL_POSITION")));
            }

            assertThat(rows).flatExtracting((Tuple t) -> t.getString(0)).containsOnly(database.getDatabasePath().getPath());
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(1)).containsOnly("testSchema");
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(2)).containsOnly("RestaurantRecord");
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(3)).isEqualTo(List.of("rest_no", "name", "location", "reviews", "tags", "customer", "encoded_bytes"));
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(4)).isEqualTo(List.of(
                    "INT64", "STRING", "LOCATION", "RESTAURANTREVIEW ARRAY", "RESTAURANTTAG ARRAY", "STRING ARRAY", "BYTES"));
            //the JDBC spec says this should be 1-indexed :( what a bummer
            assertThat(rows).flatExtracting((Tuple t) -> t.getLong(5)).isEqualTo(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L));
        }
    }

    @Test
    void canGetTableIndexes() throws SQLException {
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");
        try (final RelationalResultSet tableData = metaData.getIndexInfo(null, "testSchema", "RestaurantReviewer", false, false)) {
            ResultSetAssert.assertThat(tableData).hasNextRow()
                    .hasRowExactly(
                            URI.create("/basic_metadata_test"), //table_cat
                            "testSchema", //table_schem
                            "RestaurantReviewer", //table_name
                            false, //non_unique
                            "value", //index_qualifier
                            "reviewer_name_idx", //index_name
                            (short) 3, //index_type
                            -1, // ordinal_position
                            null, //column_name
                            null, //asc_or_desc
                            -1, //cardinality
                            -1, //pages
                            null); //filter_condition
        }
    }

    @Test
    @Disabled("Disabled until StoreCatalog#listSchemas() is supported")
    void getSchemasNoActiveTransaction() throws SQLException {
        RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        dbConn.setAutoCommit(false);
        dbConn.commit();
        try (RelationalResultSet schemas = metaData.getSchemas(dbConn.getCatalog(), null)) {
            schemas.next();
            assertThat(schemas.getString("TABLE_CATALOG")).isEqualTo(database.getDatabasePath().getPath());
            assertThat(schemas.getString("TABLE_SCHEM")).satisfiesAnyOf(
                    schema -> assertThat(schema).isEqualTo("test"),
                    schema -> assertThat(schema).isEqualTo("anotherSchema")
            );
        }
    }

    @Test
    void notSupportedGetTables() throws SQLException {
        RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getTables("foo", "testSchema", null, null))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getTables(null, null, null, null))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getTables(null, "testSchema", "foo", null))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getTables(null, "testSchema", null, new String[]{"foo"}))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
    }

    @Test
    void notSupportedGetColumns() throws SQLException {
        RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns("foo", "testSchema", "RestaurantRecord", null))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns(null, null, "RestaurantRecord", null))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns(null, "", "RestaurantRecord", null))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns(null, "testSchema", null, null))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns(null, "testSchema", "", null))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns(null, "testSchema", "RestaurantRecord", "foo"))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
    }
}
