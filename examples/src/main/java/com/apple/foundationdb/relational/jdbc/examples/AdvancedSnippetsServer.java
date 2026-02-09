/*
 * AdvancedSnippetsServer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.jdbc.examples;

import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.jdbc.JDBCRelationalArray;
import com.apple.foundationdb.relational.jdbc.JDBCRelationalStruct;

import java.sql.*;

/**
 * Code snippets for JDBC Guide advanced documentation using the server driver.
 * This class is not meant to be run, but contains tagged sections referenced by the documentation.
 */
public class AdvancedSnippetsServer {
    private static final String url = "jdbc:relational://localhost:7243/FRL/shop?schema=SHOP";

    public static void arrayOfStructs() throws SQLException {
        // tag::array-of-structs[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            String sql = "INSERT INTO orders (id, items) VALUES (?, ?)";
            try (RelationalPreparedStatement pstmt =
                    conn.prepareStatement(sql).unwrap(RelationalPreparedStatement.class)) {

                pstmt.setLong(1, 1L);

                // Create an array of STRUCT values
                pstmt.setArray(2, JDBCRelationalArray.newBuilder()
                    .addAll(
                        JDBCRelationalStruct.newBuilder()
                            .addInt("product_id", 11)
                            .addString("product_name", "Widget A")
                            .addInt("quantity", 2)
                            .build(),
                        JDBCRelationalStruct.newBuilder()
                            .addInt("product_id", 22)
                            .addString("product_name", "Widget B")
                            .addInt("quantity", 5)
                            .build()
                    )
                    .build());

                pstmt.executeUpdate();
                conn.commit();
            }
        }
        // end::array-of-structs[]
    }

    public static void structContainingArrays() throws SQLException {
        // tag::struct-containing-arrays[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            String sql = "INSERT INTO products (id, product_info) VALUES (?, ?)";
            try (RelationalPreparedStatement pstmt =
                    conn.prepareStatement(sql).unwrap(RelationalPreparedStatement.class)) {

                // Create STRUCT with embedded ARRAY fields
                RelationalStruct product = JDBCRelationalStruct.newBuilder()
                    .addLong("ID", 1L)
                    .addString("NAME", "Multi-tool")
                    .addArray("TAGS", JDBCRelationalArray.newBuilder()
                        .addAll("versatile", "portable", "durable")
                        .build())
                    .addArray("RATINGS", JDBCRelationalArray.newBuilder()
                        .addAll(4.5, 4.8, 4.2, 4.9)
                        .build())
                    .build();

                pstmt.setLong(1, 1L);
                pstmt.setObject(2, product);

                int inserted = pstmt.executeUpdate();
                conn.commit();
            }
        }
        // end::struct-containing-arrays[]
    }
}
