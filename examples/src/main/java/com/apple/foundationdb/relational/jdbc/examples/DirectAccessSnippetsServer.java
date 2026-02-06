/*
 * DirectAccessSnippetsServer.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.jdbc.JDBCRelationalStruct;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Code snippets for JDBC Key-Based Data Access documentation using the server driver.
 * This class contains only the methods that differ from DirectAccessSnippets (insertion methods that use JDBCRelationalStruct).
 * All other methods are identical and are included in DirectAccessSnippets.
 */
public class DirectAccessSnippetsServer {
    private static final String url = "jdbc:relational://localhost:7243/FRL/shop?schema=SHOP";

    public static void insertSingle() throws SQLException {
        // tag::insert-single[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Build a record to insert
                RelationalStruct product = JDBCRelationalStruct.newBuilder()
                    .addLong("id", 100L)
                    .addString("name", "New Widget")
                    .addString("category", "Electronics")
                    .addLong("price", 299L)
                    .addInt("stock", 50)
                    .build();

                int rowsInserted = relStmt.executeInsert("products", product);
                System.out.println("Inserted " + rowsInserted + " rows");

                conn.commit();
            }
        }
        // end::insert-single[]
    }

    public static void insertBatch() throws SQLException {
        // tag::insert-batch[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                List<RelationalStruct> products = new ArrayList<>();
                products.add(JDBCRelationalStruct.newBuilder()
                    .addLong("id", 101L)
                    .addString("name", "Laptop")
                    .addString("category", "Electronics")
                    .addLong("price", 999L)
                    .addInt("stock", 25)
                    .build());

                products.add(JDBCRelationalStruct.newBuilder()
                    .addLong("id", 102L)
                    .addString("name", "Mouse")
                    .addString("category", "Electronics")
                    .addLong("price", 29L)
                    .addInt("stock", 100)
                    .build());

                int rowsInserted = relStmt.executeInsert("products", products);
                System.out.println("Batch inserted " + rowsInserted + " rows");

                conn.commit();
            }
        }
        // end::insert-batch[]
    }

    public static void insertWithReplace() throws SQLException {
        // tag::insert-replace[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                RelationalStruct product = JDBCRelationalStruct.newBuilder()
                    .addLong("id", 100L) // This might conflict with existing record
                    .addString("name", "Updated Widget")
                    .addString("category", "Electronics")
                    .addLong("price", 349L)
                    .addInt("stock", 75)
                    .build();

                Options options = Options.builder()
                    .withOption(Options.Name.REPLACE_ON_DUPLICATE_PK, true)
                    .build();

                int rowsInserted = relStmt.executeInsert("products", product, options);
                System.out.println("Inserted/updated " + rowsInserted + " rows");

                conn.commit();
            }
        }
        // end::insert-replace[]
    }
}