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
import com.apple.foundationdb.relational.api.DatabaseConnection;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.TableMetaData;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        try (DatabaseConnection dbConn = Relational.connect(URI.create("rlsc:embed:/basic_metadata_test"), Options.create())) {
            final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
            Assertions.assertNotNull(metaData, "Null metadata returned");

            final RelationalResultSet schemas = metaData.getSchemas();
            Assertions.assertNotNull(schemas, "Null schemas returned");
            Set<String> retData = new HashSet<>(2);
            while (schemas.next()) {
                Assertions.assertEquals("/basic_metadata_test", schemas.getString("db_path"), "Incorrect db path returned!");
                Assertions.assertTrue(retData.add(schemas.getString("schema_name")), "Saw the same schema twice");
            }
            Set<String> expected = Sets.newHashSet("test", "anotherSchema");
            Assertions.assertEquals(expected.size(), retData.size(), "Incorrect number of schemas returned");
            for (String expectedSchema :expected) {
                Assertions.assertTrue(retData.contains(expectedSchema), "Missing schema <" + expectedSchema + ">");
            }
            for (String retSchema :retData) {
                Assertions.assertTrue(expected.contains(retSchema), "Unexpected schema <" + retSchema + ">");
            }
        }
    }

    @Test
    void canGetTablesForSchema() throws RelationalException, SQLException {
        try (DatabaseConnection dbConn = Relational.connect(URI.create("rlsc:embed:/basic_metadata_test"), Options.create())) {
            final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
            Assertions.assertNotNull(metaData, "Null metadata returned");

            final RelationalResultSet tables = metaData.getTables("test");
            Assertions.assertNotNull(tables, "Null tables returned");
            List<String> retTableNames = new ArrayList<>();
            while (tables.next()) {
                retTableNames.add(tables.getString("NAME"));
            }
            List<String> expected = Arrays.asList("RestaurantRecord", "RestaurantReviewer");
            Assertions.assertEquals(expected.size(), retTableNames.size(), "Did not return the expected number of tables!");
            for (String table :expected) {
                Assertions.assertTrue(retTableNames.contains(table), "Did not contain table <" + table + "> in returned results!");
            }
            for (String retTable :retTableNames) {
                Assertions.assertTrue(expected.contains(retTable), "table <" + retTable + "> does not exist in expected list!");
            }
        }
    }

    @Test
    void canGetTableColumns() throws RelationalException, SQLException {
        try (DatabaseConnection dbConn = Relational.connect(URI.create("rlsc:embed:/basic_metadata_test"), Options.create())) {
            final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
            Assertions.assertNotNull(metaData, "Null metadata returned");

            final RelationalResultSet tableData = metaData.getColumns("test", "RestaurantRecord");
            final Descriptors.Descriptor descriptor = Restaurant.RestaurantRecord.getDescriptor();
            final List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
            final Set<String> visitedFields = new HashSet<>();
            while (tableData.next()) {
                String fieldName = tableData.getString("NAME");
                Assertions.assertNotNull(fieldName, "Missing field name!");
                Assertions.assertTrue(visitedFields.add(fieldName), "Field <" + fieldName + "> has already been visited!");

                Descriptors.FieldDescriptor fd = null;
                for (Descriptors.FieldDescriptor f : fields) {
                    if (f.getName().equalsIgnoreCase(fieldName)) {
                        fd = f;
                        break;
                    }
                }
                Assertions.assertNotNull(fd, "Field with name <" + fieldName + "> not found in descriptor");

                Assertions.assertEquals(fd.getIndex(), tableData.getLong("INDEX"), "Incorrect index");
            }
        }
    }

    @Test
    void canDescribeTable() throws RelationalException {
        try (DatabaseConnection dbConn = Relational.connect(URI.create("rlsc:embed:/basic_metadata_test"), Options.create())) {
            final RelationalDatabaseMetaData metaData = dbConn.getMetaData();
            Assertions.assertNotNull(metaData, "Null metadata returned");

            final TableMetaData tableMetaData = metaData.describeTable("test", "RestaurantRecord");
            Assertions.assertNotNull(tableMetaData, "Null table metadata found!");
            final Descriptors.Descriptor tableType = tableMetaData.getTableTypeDescriptor();
            Assertions.assertEquals(Restaurant.RestaurantRecord.getDescriptor(), tableType, "Incorrect table type description!");
        }
    }
}
