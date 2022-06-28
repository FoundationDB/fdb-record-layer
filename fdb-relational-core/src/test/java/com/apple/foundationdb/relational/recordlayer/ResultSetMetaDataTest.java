/*
 * ResultSetMetaDataTest.java
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

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.ResultSetMetaDataAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests of the validity of ResultSetMetaData queries.
 */
public abstract class ResultSetMetaDataTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension,
            ResultSetMetaDataTest.class,
            TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("testSchema");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @BeforeEach
    void setUp() throws Exception {
        List<Message> data = new ArrayList<>();
        final DynamicMessageBuilder messageBuilder = statement.getDataBuilder("RestaurantRecord");
        data.add(messageBuilder
                .setField("name", "testRestaurant0")
                .setField("rest_no", System.currentTimeMillis())
                .build());

        int insertCount = statement.executeInsert("RestaurantRecord", data.iterator());
        Assertions.assertEquals(1, insertCount, "Did not count insertions correctly!");
    }

    @Test
    void canGetColumnNamesCorrectly() throws SQLException, RelationalException {
        String[] expectedColums = new String[]{
                "rest_no", "name", "location", "reviews", "tags", "customer", "encoded_bytes"
        };
        try (RelationalResultSet r = selectAll(statement, "RestaurantRecord")) {
            ResultSetAssert.assertThat(r).metaData().hasColumnsExactlyInOrder(expectedColums);
        }
    }

    @Test
    void canGetColumnTypesCorrectly() throws SQLException, RelationalException {
        Map<String, Integer> columnTypes = Map.of(
                "rest_no", Types.BIGINT,
                "name", Types.VARCHAR,
                "location", Types.STRUCT,
                "reviews", Types.ARRAY,
                "tags", Types.ARRAY,
                "customer", Types.ARRAY,
                "encoded_bytes", Types.BINARY
        );
        try (RelationalResultSet r = selectAll(statement, "RestaurantRecord")) {
            ResultSetAssert.assertThat(r).metaData().hasColumnTypes(columnTypes);
        }
    }

    @Test
    void canGetArrayMetaDataCorrectly() throws Exception {
        try (RelationalResultSet r = selectAll(statement, "RestaurantRecord")) {
            final ResultSetMetaDataAssert mdAssert = ResultSetAssert.assertThat(r).metaData();
            mdAssert.hasArrayMetaData("customer")
                    .hasColumnTypes(Map.of("customer", Types.VARCHAR));
            mdAssert.hasArrayMetaData("tags")
                    .hasColumnTypes(Map.of("tag", Types.VARCHAR, "weight", Types.BIGINT));
            mdAssert.hasArrayMetaData("reviews")
                    .hasColumnTypes(Map.of("reviewer", Types.BIGINT, "rating", Types.BIGINT));
        }
    }

    @Test
    void canGetStructMetaDataCorrectly() throws Exception {
        try (RelationalResultSet r = selectAll(statement, "RestaurantRecord")) {
            final ResultSetMetaDataAssert mdAssert = ResultSetAssert.assertThat(r).metaData();
            mdAssert.hasStructMetaData("location")
                    .hasColumnCount(3)
                    .hasColumnTypes(Map.of("address", Types.VARCHAR,
                            "latitude", Types.VARCHAR,
                            "longitude", Types.VARCHAR));
        }
    }

    protected abstract RelationalResultSet selectAll(RelationalStatement statement, String tableName) throws RelationalException, SQLException;

    public static class DirectAccessTest extends ResultSetMetaDataTest {

        @Override
        protected RelationalResultSet selectAll(RelationalStatement statement, String tableName) throws RelationalException {
            TableScan ts = TableScan.newBuilder()
                    .withTableName(tableName)
                    .build();

            return statement.executeScan(ts, Options.NONE);
        }
    }

    public static class QueryTest extends ResultSetMetaDataTest {

        @Override
        protected RelationalResultSet selectAll(RelationalStatement statement, String tableName) throws SQLException {
            return statement.executeQuery("select * from " + tableName);
        }
    }
}
