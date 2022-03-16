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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.TableMetaData;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for our basic ability to get Table and Database Metadata from the Connection.
 */
public class BasicMetadataTest {
    @RegisterExtension
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @BeforeEach
    public final void setupCatalog() throws RelationalException {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        builder.getRecordType("RestaurantRecord").setPrimaryKey(Key.Expressions.field("rest_no"));
        catalog.createSchemaTemplate(new RecordLayerTemplate("RestaurantRecord", builder.build()));

        catalog.createDatabase(URI.create("/basic_metadata_test"),
                DatabaseTemplate.newBuilder()
                        .withSchema("test", "RestaurantRecord")
                        .withSchema("anotherSchema", "RestaurantRecord")
                        .build());
    }

    @AfterEach
    public final void tearDown() throws RelationalException {
        catalog.deleteDatabase(URI.create("/basic_metadata_test"));
    }

    @Test
    void canGetSchemasForDatabase() throws RelationalException, SQLException {
        try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:/basic_metadata_test"), Options.create())) {
            final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
            Assertions.assertNotNull(metaData, "Null metadata returned");

            final RelationalResultSet schemas = metaData.getSchemas();
            Assertions.assertNotNull(schemas, "Null schemas returned");
            List<String> retData = new ArrayList<>(2);
            while (schemas.next()) {
                Assertions.assertEquals("/basic_metadata_test", schemas.getString("TABLE_CATALOG"), "Incorrect db path returned!");
                Assertions.assertTrue(retData.add(schemas.getString("TABLE_SCHEM")), "Saw the same schema twice");
            }
            assertThat(retData).containsExactlyInAnyOrder(new String[]{"anotherSchema", "test"});
        }
    }

    @Test
    void canGetTablesForSchema() throws RelationalException, SQLException {
        try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:/basic_metadata_test"), Options.create())) {
            final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
            Assertions.assertNotNull(metaData, "Null metadata returned");

            final RelationalResultSet tables = metaData.getTables(null, "test", null, null);
            Assertions.assertNotNull(tables, "Null tables returned");
            List<String> retTableNames = new ArrayList<>();
            while (tables.next()) {
                retTableNames.add(tables.getString("TABLE_NAME"));
            }
            assertThat(retTableNames).containsExactly(new String[]{"RestaurantRecord", "RestaurantReviewer"});
        }
    }

    @Test
    void canGetTableColumns() throws RelationalException, SQLException {
        try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:/basic_metadata_test"), Options.create())) {
            final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
            Assertions.assertNotNull(metaData, "Null metadata returned");

            final RelationalResultSet tableData = metaData.getColumns(null, "test", "RestaurantRecord", null);
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
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(0)).containsOnly("/basic_metadata_test");
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(1)).containsOnly("test");
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(2)).containsOnly("RestaurantRecord");
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(3)).isEqualTo(List.of("rest_no", "name", "location", "reviews", "tags", "customer"));
            assertThat(rows).flatExtracting((Tuple t) -> t.getString(4)).isEqualTo(List.of(
                    "INT64", "STRING", "Message(COM.APPLE.FOUNDATIONDB.RECORD.TEST4.LOCATION)", "Message(COM.APPLE.FOUNDATIONDB.RECORD.TEST4.RESTAURANTREVIEW)", "Message(COM.APPLE.FOUNDATIONDB.RECORD.TEST4.RESTAURANTTAG)", "STRING"));
            assertThat(rows).flatExtracting((Tuple t) -> t.getLong(5)).isEqualTo(List.of(0L, 1L, 2L, 3L, 4L, 5L));
        }
    }

    @Test
    void canDescribeTable() throws RelationalException, SQLException {
        try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:/basic_metadata_test"), Options.create())) {
            final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
            Assertions.assertNotNull(metaData, "Null metadata returned");

            final TableMetaData tableMetaData = metaData.describeTable("test", "RestaurantRecord");
            Assertions.assertNotNull(tableMetaData, "Null table metadata found!");
            final Descriptors.Descriptor tableType = tableMetaData.getTableTypeDescriptor();
            Assertions.assertEquals(Restaurant.RestaurantRecord.getDescriptor(), tableType, "Incorrect table type description!");
        }
    }
}
