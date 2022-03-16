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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Queryable;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.WhereClause;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.AndClause;
import com.apple.foundationdb.relational.recordlayer.query.OrClause;
import com.apple.foundationdb.relational.recordlayer.query.RelationalQuery;
import com.apple.foundationdb.relational.recordlayer.query.ValueComparisonClause;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

public class QueryTest {
    @RegisterExtension
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @BeforeEach
    public final void setupCatalog() throws RelationalException {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        builder.getRecordType("RestaurantRecord").setPrimaryKey(Key.Expressions.field("rest_no"));
        catalog.createSchemaTemplate(new RecordLayerTemplate("RestaurantRecord", builder.build()));

        catalog.createDatabase(URI.create("/query_test"),
                DatabaseTemplate.newBuilder()
                        .withSchema("test", "RestaurantRecord")
                        .build());
    }

    @AfterEach
    public final void tearDown() throws RelationalException {
        catalog.deleteDatabase(URI.create("/query_test"));
    }

    @Test
    void canExecuteABasicQuery() throws RelationalException, SQLException {
        try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:/query_test"), Options.create())) {
            dbConn.setSchema("test");
            Restaurant.RestaurantRecord rec = Restaurant.RestaurantRecord.newBuilder().setRestNo(System.currentTimeMillis()).setName("testName").build();
            try (RelationalStatement s = dbConn.createStatement()) {
                int cnt = s.executeInsert("RestaurantRecord", Collections.singleton(rec), Options.create());
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                Queryable query = new RelationalQuery("RestaurantRecord", "test", null, null, false, QueryProperties.DEFAULT);
                final RelationalResultSet resultSet = s.executeQuery(query, Options.create());
                Assertions.assertNotNull(resultSet, "Did not return a result set!");
                Assertions.assertTrue(resultSet.next(), "Did not return a record!");
                Assertions.assertTrue(resultSet.supportsMessageParsing(), "Does not support message parsing!");
                Assertions.assertEquals(rec, resultSet.parseMessage(), "Incorrect returned record!");
            }
        }
    }

    @Test
    void canQuerySpecificColumns() throws RelationalException, SQLException {
        try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:/query_test"), Options.create())) {
            dbConn.setSchema("test");
            Restaurant.RestaurantRecord rec = Restaurant.RestaurantRecord.newBuilder().setRestNo(System.currentTimeMillis()).setName("testName").build();
            try (RelationalStatement s = dbConn.createStatement()) {
                int cnt = s.executeInsert("RestaurantRecord", Collections.singleton(rec), Options.create());
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                Queryable query = new RelationalQuery("RestaurantRecord", "test", Arrays.asList("rest_no", "location"), null, false, QueryProperties.DEFAULT);
                final RelationalResultSet resultSet = s.executeQuery(query, Options.create());
                Assertions.assertNotNull(resultSet, "Did not return a result set!");
                Assertions.assertTrue(resultSet.next(), "Did not return a record!");
                Assertions.assertTrue(resultSet.supportsMessageParsing(), "Does not support message parsing!");
                Assertions.assertEquals(rec, resultSet.parseMessage(), "Incorrect returned record!");

                //now check the specific fields
                Assertions.assertEquals(rec.getRestNo(), resultSet.getLong("rest_no"), "Incorrect rest_no");
                Assertions.assertEquals(rec.getLocation(), resultSet.getMessage("location"), "Incorrect location");
            }
        }
    }

    @Test
    void canQuerySpecificColumnsWithSimpleWhereClause() throws RelationalException, SQLException {
        try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:/query_test"), Options.create())) {
            dbConn.setSchema("test");
            Restaurant.RestaurantRecord rec = Restaurant.RestaurantRecord.newBuilder().setRestNo(System.currentTimeMillis()).setName("testName").build();
            try (RelationalStatement s = dbConn.createStatement()) {
                int cnt = s.executeInsert("RestaurantRecord", Collections.singleton(rec), Options.create());
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                WhereClause clause = new ValueComparisonClause("name", ValueComparisonClause.ComparisonType.EQUALS, rec.getName());
                Queryable query = new RelationalQuery("RestaurantRecord", "test", Arrays.asList("name", "name"), clause, false, QueryProperties.DEFAULT);
                final RelationalResultSet resultSet = s.executeQuery(query, Options.create());
                Assertions.assertNotNull(resultSet, "Did not return a result set!");
                Assertions.assertTrue(resultSet.next(), "Did not return a record!");
                Assertions.assertTrue(resultSet.supportsMessageParsing(), "Does not support message parsing!");
                Assertions.assertEquals(rec, resultSet.parseMessage(), "Incorrect returned record!");
            }
        }
    }

    @Test
    void canQuerySpecificColumnsWithOrClause() throws RelationalException, SQLException {
        try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:/query_test"), Options.create())) {
            dbConn.setSchema("test");
            Restaurant.RestaurantRecord rec = Restaurant.RestaurantRecord.newBuilder().setRestNo(System.currentTimeMillis()).setName("testName").build();
            try (RelationalStatement s = dbConn.createStatement()) {
                int cnt = s.executeInsert("RestaurantRecord", Collections.singleton(rec), Options.create());
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                WhereClause c1 = new ValueComparisonClause("name", ValueComparisonClause.ComparisonType.EQUALS, rec.getName());
                WhereClause c2 = new ValueComparisonClause("name", ValueComparisonClause.ComparisonType.EQUALS, rec.getName() + "1");
                WhereClause clause = new OrClause(c1, c2);
                Queryable query = new RelationalQuery("RestaurantRecord", "test", Arrays.asList("name", "name"), clause, false, QueryProperties.DEFAULT);
                final RelationalResultSet resultSet = s.executeQuery(query, Options.create());
                Assertions.assertNotNull(resultSet, "Did not return a result set!");
                Assertions.assertTrue(resultSet.next(), "Did not return a record!");
                Assertions.assertTrue(resultSet.supportsMessageParsing(), "Does not support message parsing!");
                Assertions.assertEquals(rec, resultSet.parseMessage(), "Incorrect returned record!");
            }
        }
    }

    @Test
    void canQuerySpecificColumnsWithAndClause() throws RelationalException, SQLException {
        try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:/query_test"), Options.create())) {
            dbConn.setSchema("test");
            Restaurant.RestaurantRecord rec = Restaurant.RestaurantRecord.newBuilder().setRestNo(System.currentTimeMillis()).setName("testName").build();
            try (RelationalStatement s = dbConn.createStatement()) {
                int cnt = s.executeInsert("RestaurantRecord", Collections.singleton(rec), Options.create());
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                WhereClause c1 = new ValueComparisonClause("rest_no", ValueComparisonClause.ComparisonType.GREATER_OR_EQUALS, rec.getRestNo());
                WhereClause c2 = new ValueComparisonClause("rest_no", ValueComparisonClause.ComparisonType.LT, rec.getRestNo() + 1);
                WhereClause andClause = new AndClause(c1, c2);

                Queryable query = new RelationalQuery("RestaurantRecord", "test",
                        Collections.singletonList("rest_no"), andClause, false, QueryProperties.DEFAULT);
                final RelationalResultSet resultSet = s.executeQuery(query, Options.create());
                Assertions.assertNotNull(resultSet, "Did not return a result set!");
                Assertions.assertTrue(resultSet.next(), "Did not return a record!");
                Assertions.assertTrue(resultSet.supportsMessageParsing(), "Does not support message parsing!");
                Assertions.assertEquals(rec, resultSet.parseMessage(), "Incorrect returned record!");
            }
        }
    }
}
