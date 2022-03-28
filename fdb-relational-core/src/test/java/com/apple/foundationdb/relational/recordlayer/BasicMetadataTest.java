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

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.TableMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for our basic ability to get Table and Database Metadata from the Connection.
 */
public class BasicMetadataTest {
    @RegisterExtension
    @Order(0)
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @RegisterExtension
    @Order(1)
    public final RecordLayerTemplateRule template = new RecordLayerTemplateRule("RestaurantRecord", catalog)
            .setRecordFile(Restaurant.getDescriptor())
            .configureTable("RestaurantRecord", table -> table.setPrimaryKey(Key.Expressions.field("rest_no")));

    @RegisterExtension
    @Order(2)
    public final DatabaseRule database = new DatabaseRule("basic_metadata_test", catalog)
            .withSchema("test", template)
            .withSchema("anotherSchema", template);

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule dbConn = new RelationalConnectionRule(database);

    @Test
    void canGetSchemasForDatabase() throws SQLException {
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");

        try (final RelationalResultSet schemas = metaData.getSchemas()) {
            Assertions.assertNotNull(schemas, "Null schemas returned");
            List<String> retData = new ArrayList<>(2);
            while (schemas.next()) {
                Assertions.assertEquals(database.getPathString(), schemas.getString("TABLE_CATALOG"), "Incorrect db path returned!");
                Assertions.assertTrue(retData.add(schemas.getString("TABLE_SCHEM")), "Saw the same schema twice");
            }
            assertThat(retData).containsExactlyInAnyOrder("anotherSchema", "test");
        }
    }

    @Test
    void canGetTablesForSchema() throws SQLException {
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");

        try (final RelationalResultSet tables = metaData.getTables(null, "test", null, null)) {
            Assertions.assertNotNull(tables, "Null tables returned");
            List<String> retTableNames = new ArrayList<>();
            while (tables.next()) {
                retTableNames.add(tables.getString("TABLE_NAME"));
            }
            assertThat(retTableNames).containsExactly("RestaurantRecord", "RestaurantReviewer");
        }
    }

    @Test
    void canGetTableColumns() throws SQLException {
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");

        try (final RelationalResultSet tableData = metaData.getColumns(null, "test", "RestaurantRecord", null)) {
            List<Tuple> rows = new ArrayList<>();
            while (tableData.next()) {
                rows.add(new Tuple()
                        .add(tableData.getString("TABLE_CAT"))
                        .add(tableData.getString("TABLE_SCHEM"))
                        .add(tableData.getString("TABLE_NAME"))
                        .add(tableData.getString("COLUMN_NAME"))
                        .add(tableData.getString("TYPE_NAME"))
                        .add(tableData.getLong("ORDINAL_POSITION"))
                        .add(tableData.getString("BL_OPTIONS")));
            }

            assertThat(rows).flatExtracting((Tuple t) -> t.getString(0)).containsOnly(database.getPathString());
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(1)).containsOnly("test");
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(2)).containsOnly("RestaurantRecord");
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(3)).isEqualTo(List.of("rest_no", "name", "location", "reviews", "tags", "customer"));
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(4)).isEqualTo(List.of(
                    "INT64", "STRING", "Message(COM.APPLE.FOUNDATIONDB.RECORD.TEST4.LOCATION)", "Message(COM.APPLE.FOUNDATIONDB.RECORD.TEST4.RESTAURANTREVIEW)", "Message(COM.APPLE.FOUNDATIONDB.RECORD.TEST4.RESTAURANTTAG)", "STRING"));
            assertThat(rows).flatExtracting((Tuple t) -> t.getLong(5)).isEqualTo(List.of(0L, 1L, 2L, 3L, 4L, 5L));
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(6)).isEqualTo(List.of("{primary_key:true}", "{index:value}", "{}", "{}", "{}", "{}"));
        }
    }

    @Test
    void canDescribeTable() throws RelationalException, SQLException {
        final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        Assertions.assertNotNull(metaData, "Null metadata returned");

        final TableMetaData tableMetaData = metaData.describeTable("test", "RestaurantRecord");
        Assertions.assertNotNull(tableMetaData, "Null table metadata found!");
        final Descriptors.Descriptor tableType = tableMetaData.getTableTypeDescriptor();
        Assertions.assertEquals(Restaurant.RestaurantRecord.getDescriptor(), tableType, "Incorrect table type description!");
    }

    @Test
    void getSchemasNoActiveTransaction() throws SQLException {
        RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        dbConn.setAutoCommit(false);
        dbConn.commit();
        try (RelationalResultSet schemas = metaData.getSchemas()) {
            schemas.next();
            assertThat(schemas.getString("TABLE_CATALOG")).isEqualTo(database.getPathString());
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
                () -> metaData.getTables("foo", "test", null, null),
                ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getTables(null, null, null, null),
                ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getTables(null, "", null, null),
                ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getTables(null, "test", "foo", null),
                ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getTables(null, "test", null, new String[]{"foo"}),
                ErrorCode.UNSUPPORTED_OPERATION);
    }

    @Test
    void notSupportedGetColumns() throws SQLException {
        RelationalDatabaseMetaData metaData = dbConn.getMetaData();
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns("foo", "test", "RestaurantRecord", null),
                ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns(null, null, "RestaurantRecord", null),
                ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns(null, "", "RestaurantRecord", null),
                ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns(null, "test", null, null),
                ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns(null, "test", "", null),
                ErrorCode.UNSUPPORTED_OPERATION);
        RelationalAssertions.assertThrowsSqlException(
                () -> metaData.getColumns(null, "test", "RestaurantRecord", "foo"),
                ErrorCode.UNSUPPORTED_OPERATION);
    }
}
