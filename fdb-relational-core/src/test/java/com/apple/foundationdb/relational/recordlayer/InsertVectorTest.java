/*
 * InsertTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FloatRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.DriverManager;
import java.sql.SQLException;

public class InsertVectorTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(InsertVectorTest.class, "CREATE TABLE V(PK INTEGER, V1 VECTOR(4, FLOAT), V2 VECTOR(3, HALF), V3 VECTOR(2, DOUBLE), PRIMARY KEY(PK))");

    @Test
    void insertNulls() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                RelationalStruct rec = EmbeddedRelationalStruct.newBuilder().addInt("PK", 0).build();
                s.executeInsert("V", rec);
                try (RelationalResultSet rs = s.executeGet("V", new KeySet().setKeyColumn("PK", 0), Options.NONE)) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow()
                            .hasColumn("PK", 0)
                            .hasColumn("V1", null)
                            .hasColumn("V2", null)
                            .hasColumn("V3", null)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void partialInsert() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                RelationalStruct rec = EmbeddedRelationalStruct.newBuilder().addInt("PK", 0).addObject("V1", new FloatRealVector(new float[]{1f, 2f, 3f, 4f})).build();
                s.executeInsert("V", rec);
                try (RelationalResultSet rs = s.executeGet("V", new KeySet().setKeyColumn("PK", 0), Options.NONE)) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow()
                            .hasColumn("PK", 0)
                            .hasColumn("V1", new FloatRealVector(new float[]{1f, 2f, 3f, 4f}))
                            .hasColumn("V2", null)
                            .hasColumn("V3", null)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void fullInsert() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                RelationalStruct rec = EmbeddedRelationalStruct.newBuilder().addInt("PK", 0)
                        .addObject("V1", new FloatRealVector(new float[]{1f, 2f, 3f, 4f}))
                        .addObject("V2", new HalfRealVector(new int[]{1, 2, 3}))
                        .addObject("V3", new DoubleRealVector(new double[]{1d, 2d}))
                        .build();
                s.executeInsert("V", rec);
                try (RelationalResultSet rs = s.executeGet("V", new KeySet().setKeyColumn("PK", 0), Options.NONE)) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow()
                            .hasColumn("PK", 0)
                            .hasColumn("V1", new FloatRealVector(new float[]{1f, 2f, 3f, 4f}))
                            .hasColumn("V2", new HalfRealVector(new int[]{1, 2, 3}))
                            .hasColumn("V3", new DoubleRealVector(new double[]{1d, 2d}))
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void insertWrongDimensionFails() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                RelationalStruct rec = EmbeddedRelationalStruct.newBuilder().addInt("PK", 0).addObject("V1", new FloatRealVector(new float[] {1f, 2f, 3f, 4f, 5f})).build();
                RelationalAssertions.assertThrowsSqlException(
                                () -> s.executeInsert("V", rec))
                        .hasErrorCode(ErrorCode.CANNOT_CONVERT_TYPE);
            }
        }
    }

    @Test
    void insertWrongPrecisionFails() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                RelationalStruct rec = EmbeddedRelationalStruct.newBuilder().addInt("PK", 0).addObject("V1", new DoubleRealVector(new double[] {1d, 2d, 3d, 4d})).build();
                RelationalAssertions.assertThrowsSqlException(
                                () -> s.executeInsert("V", rec))
                        .hasErrorCode(ErrorCode.CANNOT_CONVERT_TYPE);
            }
        }
    }

    @Test
    void insertWrongTypeFails() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                RelationalStruct rec = EmbeddedRelationalStruct.newBuilder().addInt("PK", 0).addInt("V1", 42).build();
                RelationalAssertions.assertThrowsSqlException(
                                () -> s.executeInsert("V", rec))
                        .hasErrorCode(ErrorCode.CANNOT_CONVERT_TYPE);
            }
        }
    }
}
