/*
 * NullsInResultSetTest.java
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.utils.Ddl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.nio.charset.StandardCharsets;

public class NullsInResultSetTest {

    private static final String schemaTemplate =
            " CREATE TYPE AS STRUCT S (S1 string, S2 string)" +
                    " CREATE TABLE T(PK bigint, T1 bigint, T2 string, T3 double, T4 bytes, T5 S, T6 string array, PRIMARY KEY(PK))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public NullsInResultSetTest() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void nullValues() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (RelationalConnection conn = ddl.setSchemaAndGetConnection()) {
                try (RelationalStatement s = conn.createStatement()) {
                    s.execute("INSERT INTO T(PK) VALUES(100)");
                    try (RelationalResultSet rs = s.executeQuery("SELECT * FROM T")) {
                        rs.next();
                        Assertions.assertThat(rs.getLong("PK")).isEqualTo(100L);
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getLong("T1")).isEqualTo(0L);
                        Assertions.assertThat(rs.wasNull()).isTrue();

                        Assertions.assertThat(rs.getLong("PK")).isEqualTo(100L);
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getString("T2")).isNull();
                        Assertions.assertThat(rs.wasNull()).isTrue();

                        Assertions.assertThat(rs.getLong("PK")).isEqualTo(100L);
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getDouble("T3")).isEqualTo(0.0d);
                        Assertions.assertThat(rs.wasNull()).isTrue();

                        Assertions.assertThat(rs.getLong("PK")).isEqualTo(100L);
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getBytes("T4")).isNull();
                        Assertions.assertThat(rs.wasNull()).isTrue();

                        Assertions.assertThat(rs.getLong("PK")).isEqualTo(100L);
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getStruct("T5")).isNull();
                        Assertions.assertThat(rs.wasNull()).isTrue();

                        Assertions.assertThat(rs.getLong("PK")).isEqualTo(100L);
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getArray("T6")).isNull();
                        Assertions.assertThat(rs.wasNull()).isTrue();
                    }
                }
            }
        }
    }

    @Test
    void notNullValues() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (RelationalConnection conn = ddl.setSchemaAndGetConnection()) {
                try (RelationalStatement s = conn.createStatement()) {
                    final var struct = EmbeddedRelationalStruct.newBuilder()
                            .addLong("PK", 100)
                            .addLong("T1", 1)
                            .addString("T2", "1")
                            .addDouble("T3", 1.0)
                            .addBytes("T4", "1".getBytes(StandardCharsets.UTF_8))
                            .addStruct("T5", EmbeddedRelationalStruct.newBuilder()
                                    .addString("S1", "2")
                                    .addString("S2", "3")
                                    .build())
                            .build();
                    s.executeInsert("T", struct);
                    try (RelationalResultSet rs = s.executeQuery("SELECT * FROM T")) {
                        rs.next();
                        Assertions.assertThat(rs.getLong("PK")).isEqualTo(100L);
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getLong("T1")).isEqualTo(1L);
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getString("T2")).isEqualTo("1");
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getDouble("T3")).isEqualTo(1.0);
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getBytes("T4")).isEqualTo("1".getBytes(StandardCharsets.UTF_8));
                        Assertions.assertThat(rs.wasNull()).isFalse();
                        Assertions.assertThat(rs.getStruct("T5")).isNotNull();
                        Assertions.assertThat(rs.wasNull()).isFalse();
                    }
                }
            }
        }
    }
}
