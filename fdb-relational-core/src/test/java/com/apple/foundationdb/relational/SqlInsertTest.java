/*
 * SqlInsertTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational;

import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementInsertRule;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.BasicMetadataTest;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.DriverManager;
import java.util.Map;

public class SqlInsertTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(BasicMetadataTest.class,
            """
            CREATE TABLE simple (rest_no bigint, name string, primary key(rest_no))
            CREATE TYPE AS STRUCT location (address string, latitude string, longitude string)
            CREATE TABLE with_loc (rest_no bigint, name string, loc location, primary key(rest_no))
            """
    );

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void canInsertUsingSqlSyntaxAndSingleQuotes(boolean useNamed) throws Exception {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());

            try (RelationalStatement stmt = conn.createStatement()) {
                String insert = useNamed ? "insert into simple (REST_NO,NAME) " : "insert into simple ";
                insert += "values (1,'testRecord1')";
                int inserted = stmt.executeUpdate(insert);
                Assertions.assertThat(inserted).isEqualTo(1);

                try (RelationalResultSet rrs = stmt.executeQuery("select * from simple")) {
                    ResultSetAssert.assertThat(rrs).hasNextRow()
                            .hasColumns(Map.of("NAME", "testRecord1", "REST_NO", 1L))
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void canInsertStructUnnamed() throws Exception {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());

            try (RelationalStatement stmt = conn.createStatement()) {
                int inserted = stmt.executeUpdate("insert into with_loc values (1,'testRecord1',('1234','5678','9101112'))");
                Assertions.assertThat(inserted).isEqualTo(1);

                try (RelationalResultSet rrs = stmt.executeQuery("select * from with_loc")) {
                    ResultSetAssert.assertThat(rrs).hasNextRow()
                            .hasColumns(Map.of("NAME", "testRecord1",
                                    "REST_NO", 1L,
                                    "LOC", Map.of("ADDRESS", "1234", "LATITUDE", "5678", "LONGITUDE", "9101112")))
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void canInsertStructNamed() throws Exception {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());

            try (RelationalStatement stmt = conn.createStatement()) {
                int inserted = stmt.executeUpdate("insert into with_loc (rest_no,name,loc) values (1,'testRecord1',('1234','5678','9101112'))");
                Assertions.assertThat(inserted).isEqualTo(1);

                try (RelationalResultSet rrs = stmt.executeQuery("select * from with_loc")) {
                    ResultSetAssert.assertThat(rrs).hasNextRow()
                            .hasColumns(Map.of("NAME", "testRecord1",
                                    "REST_NO", 1L,
                                    "LOC", Map.of("ADDRESS", "1234", "LATITUDE", "5678", "LONGITUDE", "9101112")))
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void cannotInsertWithInsertRuleDisabled() throws Exception {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            // Turn off the planner rule that allows for inserts to be planned
            conn.setOption(Options.Name.DISABLED_PLANNER_RULES, ImmutableSet.of(ImplementInsertRule.class.getSimpleName()));
            conn.setSchema(database.getSchemaName());

            try (RelationalStatement stmt = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> stmt.executeUpdate("insert into with_loc (rest_no,name,loc) values (1,'testRecord1',('1234','5678','9101112'))"))
                        .hasErrorCode(ErrorCode.UNSUPPORTED_QUERY);

                try (RelationalResultSet rrs = stmt.executeQuery("select * from with_loc")) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    @Disabled()
    void canInsertStructWithStructFieldsNamed() throws Exception {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());

            try (RelationalStatement stmt = conn.createStatement()) {
                int inserted = stmt.executeUpdate("insert into with_loc (rest_no,name,(address,latitude,longitude)) values (1,'testRecord1',('1234','5678','9101112'))");
                Assertions.assertThat(inserted).isEqualTo(1);

                try (RelationalResultSet rrs = stmt.executeQuery("select * from with_loc")) {
                    ResultSetAssert.assertThat(rrs).hasNextRow()
                            .hasColumns(Map.of("NAME", "testRecord1",
                                    "REST_NO", 1L,
                                    "LOC", Map.of("ADDRESS", "1234", "LATITUDE", "5678", "LONGITUDE", "9101112")))
                            .hasNoNextRow();
                }
            }
        }
    }
}
