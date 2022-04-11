/*
 * QueryTest.java
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
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Queryable;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.WhereClause;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.AndClause;
import com.apple.foundationdb.relational.recordlayer.query.OrClause;
import com.apple.foundationdb.relational.recordlayer.query.RelationalQuery;
import com.apple.foundationdb.relational.recordlayer.query.ValueComparisonClause;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class QueryTest {
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
    public final DatabaseRule database = new DatabaseRule("query_test", catalog)
            .withSchema("test", template);

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database)
            .withOptions(Options.create().withOption(OperationOption.forceVerifyDdl()))
            .withSchema("test");

    @RegisterExtension
    @Order(4)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    private Restaurant.RestaurantRecord insertedRecord;

    @BeforeEach
    public final void setup() throws RelationalException {
        insertedRecord = insertRestaurantRecord(statement);
    }

    @Test
    void canExecuteABasicQuery() throws RelationalException, SQLException {
        Queryable query = new RelationalQuery("RestaurantRecord", "test", null, null, false, QueryProperties.DEFAULT);
        try (final RelationalResultSet resultSet = statement.executeQuery(query, Options.create())) {
            assertMatches(resultSet, insertedRecord);
        }
    }

    @Test
    void basicQueryNoSchema() throws RelationalException, SQLException {
        Queryable query = new RelationalQuery("RestaurantRecord", null, null, null, false, QueryProperties.DEFAULT);
        try (final RelationalResultSet resultSet = statement.executeQuery(query, Options.create())) {
            assertMatches(resultSet, insertedRecord);
        }
    }

    @Test
    void getSchemaFromTableName() throws SQLException, RelationalException {
        connection.setSchema(null);
        Queryable query = new RelationalQuery("test.RestaurantRecord", null, null, null, false, QueryProperties.DEFAULT);
        try (final RelationalResultSet resultSet = statement.executeQuery(query, Options.create())) {
            assertMatches(resultSet, insertedRecord);
        }
    }

    @Test
    void incorrectSchemaPrefix() throws SQLException, RelationalException {
        connection.setSchema(null);
        Queryable query = new RelationalQuery("test.test2.RestaurantRecord", null, null, null, false, QueryProperties.DEFAULT);
        assertThatThrownBy(() -> statement.executeQuery(query, Options.create()))
                .isInstanceOf(RelationalException.class)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.CANNOT_CONVERT_TYPE);
    }

    @Test
    void transactionNotBegun() throws SQLException, RelationalException {
        connection.setAutoCommit(false);
        Queryable query = new RelationalQuery("test.RestaurantRecord", null, null, null, false, QueryProperties.DEFAULT);
        assertThatThrownBy(() -> statement.executeQuery(query, Options.create()))
                .isInstanceOf(RelationalException.class)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.TRANSACTION_INACTIVE);
    }

    @Test
    void invalidColumn() {
        Queryable query = new RelationalQuery("RestaurantRecord", null, List.of("rest_nooooo"), null, false, QueryProperties.DEFAULT);
        assertThatThrownBy(() -> statement.executeQuery(query, Options.create()))
                .isInstanceOf(RelationalException.class)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void canQuerySpecificColumns() throws RelationalException, SQLException {
        Queryable query = new RelationalQuery("RestaurantRecord", "test", Arrays.asList("rest_no", "location"), null, false, QueryProperties.DEFAULT);
        try (final RelationalResultSet resultSet = statement.executeQuery(query, Options.create())) {
            Assertions.assertNotNull(resultSet, "Did not return a result set!");
            Assertions.assertTrue(resultSet.next(), "Did not return a record!");
            Assertions.assertTrue(resultSet.supportsMessageParsing(), "Does not support message parsing!");
            Assertions.assertEquals(insertedRecord, resultSet.parseMessage(), "Incorrect returned record!");

            //now check the specific fields
            Assertions.assertEquals(insertedRecord.getRestNo(), resultSet.getLong("rest_no"), "Incorrect rest_no");
            Assertions.assertEquals(insertedRecord.getLocation(), resultSet.getObject("location"), "Incorrect location");
        }
    }

    @Test
    void canQuerySpecificColumnsWithSimpleWhereClause() throws RelationalException, SQLException {
        WhereClause clause = new ValueComparisonClause("name", ValueComparisonClause.ComparisonType.EQUALS, insertedRecord.getName());
        Queryable query = new RelationalQuery("RestaurantRecord", "test", Arrays.asList("name", "name"), clause, false, QueryProperties.DEFAULT);
        try (final RelationalResultSet resultSet = statement.executeQuery(query, Options.create())) {
            assertMatches(resultSet, insertedRecord);
        }
    }

    @Test
    void canQuerySpecificColumnsWithOrClause() throws RelationalException, SQLException {
        WhereClause c1 = new ValueComparisonClause("name", ValueComparisonClause.ComparisonType.EQUALS, insertedRecord.getName());
        WhereClause c2 = new ValueComparisonClause("name", ValueComparisonClause.ComparisonType.EQUALS, insertedRecord.getName() + "1");
        WhereClause clause = new OrClause(c1, c2);
        Queryable query = new RelationalQuery("RestaurantRecord", "test", Arrays.asList("name", "name"), clause, false, QueryProperties.DEFAULT);
        try (final RelationalResultSet resultSet = statement.executeQuery(query, Options.create())) {
            assertMatches(resultSet, insertedRecord);
        }
    }

    @Test
    void canQuerySpecificColumnsWithAndClause() throws RelationalException, SQLException {
        WhereClause c1 = new ValueComparisonClause("rest_no", ValueComparisonClause.ComparisonType.GREATER_OR_EQUALS, insertedRecord.getRestNo());
        WhereClause c2 = new ValueComparisonClause("rest_no", ValueComparisonClause.ComparisonType.LT, insertedRecord.getRestNo() + 1);
        WhereClause andClause = new AndClause(c1, c2);

        Queryable query = new RelationalQuery("RestaurantRecord", "test",
                Collections.singletonList("rest_no"), andClause, false, QueryProperties.DEFAULT);
        try (final RelationalResultSet resultSet = statement.executeQuery(query, Options.create())) {
            assertMatches(resultSet, insertedRecord);
        }
    }

    private Restaurant.RestaurantRecord insertRestaurantRecord(RelationalStatement s) throws RelationalException {
        Restaurant.RestaurantRecord rec = Restaurant.RestaurantRecord.newBuilder().setRestNo(System.currentTimeMillis()).setName("testName").build();
        int cnt = s.executeInsert("RestaurantRecord", Collections.singleton(rec), Options.create());
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return rec;
    }

    private void assertMatches(RelationalResultSet resultSet, Restaurant.RestaurantRecord rec) throws SQLException {
        Assertions.assertNotNull(resultSet, "Did not return a result set!");
        Assertions.assertTrue(resultSet.next(), "Did not return a record!");
        Assertions.assertTrue(resultSet.supportsMessageParsing(), "Does not support message parsing!");
        Assertions.assertEquals(rec, resultSet.parseMessage(), "Incorrect returned record!");
        Assertions.assertFalse(resultSet.next(), "Has more than one row!");
    }
}
