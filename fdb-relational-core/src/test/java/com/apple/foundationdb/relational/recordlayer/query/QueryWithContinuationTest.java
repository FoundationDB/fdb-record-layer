/*
 * QueryWithContinuationTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.continuation.grpc.ContinuationProto;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.Base64;

public class QueryWithContinuationTest {

    private static final String schemaTemplate =
            "CREATE TYPE AS STRUCT Location (address string, latitude string, longitude string)" +
                    " CREATE TYPE AS STRUCT \"ReviewerEndorsements\" (\"endorsementId\" bigint, \"endorsementText\" string)" +
                    " CREATE TYPE AS STRUCT RestaurantComplexReview (reviewer bigint, rating bigint, endorsements \"ReviewerEndorsements\" array)" +
                    " CREATE TYPE AS STRUCT RestaurantTag (tag string, weight bigint)" +
                    " CREATE TYPE AS STRUCT ReviewerStats (start_date bigint, school_name string, hometown string)" +
                    " CREATE TABLE RestaurantComplexRecord (rest_no bigint, name string, location Location, reviews RestaurantComplexReview ARRAY, tags RestaurantTag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no))" +
                    " CREATE TABLE RestaurantReviewer (id bigint, name string, email string, stats ReviewerStats, PRIMARY KEY(id))" +
                    " CREATE INDEX record_name_idx as select name from RestaurantComplexRecord" +
                    " CREATE INDEX reviewer_name_idx as select name from RestaurantReviewer" +
                    " CREATE INDEX mv1 AS SELECT R.rating from RestaurantComplexRecord AS Rec, (select rating from Rec.reviews) R" +
                    " CREATE INDEX mv2 AS SELECT endo.\"endorsementText\" FROM RestaurantComplexRecord rec, (SELECT X.\"endorsementText\" FROM rec.reviews rev, (SELECT \"endorsementText\" from rev.endorsements) X) endo";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public QueryWithContinuationTest() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void preparedStatement() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (RelationalStatement statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14)");
            }
            Continuation continuation;
            try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT 2")) {
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 11L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                    assertContinuation(continuation, false, false);
                }
            }
            try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT 2 WITH CONTINUATION ?continuation")) {
                ps.setBytes("continuation", continuation.serialize());
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 12L)
                            .hasNextRow().hasColumn("REST_NO", 13L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                    assertContinuation(continuation, false, false);
                }
                ps.setBytes("continuation", continuation.serialize());
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 14L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                    assertContinuation(continuation, false, true);
                }
            }
        }
    }

    @Test
    void standardStatement() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (RelationalStatement statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14)");
            }
            Continuation continuation;
            try (RelationalStatement statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord LIMIT 2")) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 11L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                    assertContinuation(continuation, false, false);
                }
            }
            try (RelationalStatement statement = ddl.setSchemaAndGetConnection().createStatement()) {
                String continuationString = Base64.getEncoder().encodeToString(continuation.serialize());
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord LIMIT 2 WITH CONTINUATION '" + continuationString + "'")) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 12L)
                            .hasNextRow().hasColumn("REST_NO", 13L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                    assertContinuation(continuation, false, false);
                }
                continuationString = Base64.getEncoder().encodeToString(continuation.serialize());
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord LIMIT 2 WITH CONTINUATION '" + continuationString + "'")) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 14L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                    assertContinuation(continuation, false, true);
                }
            }
        }
    }

    private void assertContinuation(Continuation continuation, boolean atBegin, boolean atEnd) throws Exception {
        ContinuationImpl impl = (ContinuationImpl) continuation;
        Assertions.assertThat(impl.atBeginning()).isEqualTo(atBegin);
        Assertions.assertThat(impl.atEnd()).isEqualTo(atEnd);
        Assertions.assertThat(impl.getVersion()).isEqualTo(1);
        ContinuationProto proto = ContinuationProto.parseFrom(continuation.serialize());
        Assertions.assertThat(proto.getBindingHash()).isNotNull();
        Assertions.assertThat(proto.getBindingHash()).isNotZero();
    }
}
