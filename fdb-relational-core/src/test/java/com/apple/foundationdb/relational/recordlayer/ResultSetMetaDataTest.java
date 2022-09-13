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
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
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
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @BeforeEach
    void setUp() throws Exception {
        List<Message> data = new ArrayList<>();
        final DynamicMessageBuilder messageBuilder = statement.getDataBuilder("RESTAURANT");
        data.add(messageBuilder
                .setField("NAME", "testRestaurant0")
                .setField("REST_NO", System.currentTimeMillis())
                .build());

        int insertCount = statement.executeInsert("RESTAURANT", data.iterator());
        Assertions.assertEquals(1, insertCount, "Did not count insertions correctly!");
    }

    @Test
    void canGetColumnNamesCorrectly() throws SQLException, RelationalException {
        String[] expectedColums = new String[]{
                "REST_NO", "NAME", "LOCATION", "REVIEWS", "TAGS", "CUSTOMER", "ENCODED_BYTES"
        };
        try (RelationalResultSet r = selectAll(statement, "RESTAURANT")) {
            ResultSetAssert.assertThat(r).metaData().hasColumnsExactlyInOrder(expectedColums);
        }
    }

    @Test
    void canGetColumnTypesCorrectly() throws SQLException, RelationalException {
        Map<String, Integer> columnTypes = Map.of(
                "REST_NO", Types.BIGINT,
                "NAME", Types.VARCHAR,
                "LOCATION", Types.STRUCT,
                "REVIEWS", Types.ARRAY,
                "TAGS", Types.ARRAY,
                "CUSTOMER", Types.ARRAY,
                "ENCODED_BYTES", Types.BINARY
        );
        try (RelationalResultSet r = selectAll(statement, "RESTAURANT")) {
            ResultSetAssert.assertThat(r).metaData().hasColumnTypes(columnTypes);
        }
    }

    @Test
    void canGetArrayMetaDataCorrectly() throws Exception {
        try (RelationalResultSet r = selectAll(statement, "RESTAURANT")) {
            final ResultSetMetaDataAssert mdAssert = ResultSetAssert.assertThat(r).metaData();
            mdAssert.hasArrayMetaData("CUSTOMER")
                    .hasColumnTypes(Map.of("CUSTOMER", Types.VARCHAR));
            mdAssert.hasArrayMetaData("TAGS")
                    .hasColumnTypes(Map.of("TAG", Types.VARCHAR, "WEIGHT", Types.BIGINT));
            mdAssert.hasArrayMetaData("REVIEWS")
                    .hasColumnTypes(Map.of("REVIEWER", Types.BIGINT, "RATING", Types.BIGINT));
        }
    }

    @Test
    void canGetStructMetaDataCorrectly() throws Exception {
        try (RelationalResultSet r = selectAll(statement, "RESTAURANT")) {
            final ResultSetMetaDataAssert mdAssert = ResultSetAssert.assertThat(r).metaData();
            mdAssert.hasStructMetaData("LOCATION")
                    .hasColumnCount(3)
                    .hasColumnTypes(Map.of("ADDRESS", Types.VARCHAR,
                            "LATITUDE", Types.VARCHAR,
                            "LONGITUDE", Types.VARCHAR));
        }
    }

    protected abstract RelationalResultSet selectAll(RelationalStatement statement, String tableName) throws RelationalException, SQLException;

    public static class DirectAccessTest extends ResultSetMetaDataTest {

        @Override
        protected RelationalResultSet selectAll(RelationalStatement statement, String tableName) throws RelationalException {
            return statement.executeScan(tableName, new KeySet(), Options.NONE);
        }
    }

    public static class QueryTest extends ResultSetMetaDataTest {

        @Override
        protected RelationalResultSet selectAll(RelationalStatement statement, String tableName) throws SQLException {
            return statement.executeQuery("select * from " + tableName);
        }
    }
}
