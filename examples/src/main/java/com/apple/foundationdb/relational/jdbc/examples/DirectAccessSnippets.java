/*
 * DirectAccessSnippets.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Code snippets for JDBC Key-Based Data Access documentation.
 * This class is not meant to be run, but contains tagged sections referenced by the documentation.
 */
public class DirectAccessSnippets {
    private static final String url = "jdbc:embed:/FRL/shop?schema=SHOP";

    public static void unwrapStatement() throws SQLException {
        // tag::unwrap-statement[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                // Unwrap to RelationalStatement to access direct access methods
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Now you can use executeScan, executeGet, executeInsert, executeDelete
            }
        }
        // end::unwrap-statement[]
    }

    public static void scanBasic() throws SQLException {
        // tag::scan-basic[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Scan all records in the products table
                KeySet emptyKey = new KeySet();
                try (RelationalResultSet rs = relStmt.executeScan("products", emptyKey, Options.NONE)) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        String name = rs.getString("name");
                        System.out.println("Product: " + id + " - " + name);
                    }
                }
            }
        }
        // end::scan-basic[]
    }

    public static void scanWithPrefix() throws SQLException {
        // tag::scan-prefix[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Scan products with a specific category (assuming category is part of the key)
                KeySet keyPrefix = new KeySet()
                    .setKeyColumn("category", "Electronics");

                try (RelationalResultSet rs = relStmt.executeScan("products", keyPrefix, Options.NONE)) {
                    while (rs.next()) {
                        String name = rs.getString("name");
                        long price = rs.getLong("price");
                        System.out.println(name + ": $" + price);
                    }
                }
            }
        }
        // end::scan-prefix[]
    }

    public static void scanWithContinuation() throws SQLException {
        // tag::scan-continuation[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                KeySet keyPrefix = new KeySet();
                int batchSize = 10;
                Continuation continuation = null;

                // Initial scan without continuation
                Options options = Options.builder()
                    .withOption(Options.Name.MAX_ROWS, batchSize)
                    .build();

                int totalRecords = 0;
                try (RelationalResultSet rs = relStmt.executeScan("products", keyPrefix, options)) {
                    while (rs.next()) {
                        // Process first batch
                        System.out.println("Product: " + rs.getString("name"));
                        totalRecords++;
                    }
                    continuation = rs.getContinuation();
                }

                // Continue scanning with continuation
                while (!continuation.atEnd()) {
                    Options contOptions = Options.builder()
                        .withOption(Options.Name.CONTINUATION, continuation)
                        .withOption(Options.Name.MAX_ROWS, batchSize)
                        .build();

                    int rowCount = 0;
                    try (RelationalResultSet rs = relStmt.executeScan("products", keyPrefix, contOptions)) {
                        while (rs.next()) {
                            // Process record
                            System.out.println("Product: " + rs.getString("name"));
                            rowCount++;
                        }

                        // Get continuation for next batch
                        continuation = rs.getContinuation();
                    }

                    totalRecords += rowCount;
                    System.out.println("Processed " + rowCount + " records in this batch");
                }

                System.out.println("Total records processed: " + totalRecords);
            }
        }
        // end::scan-continuation[]
    }

    public static void getByKey() throws SQLException {
        // tag::get-basic[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Get a single record by primary key
                KeySet primaryKey = new KeySet()
                    .setKeyColumn("id", 1L);

                try (RelationalResultSet rs = relStmt.executeGet("products", primaryKey, Options.NONE)) {
                    if (rs.next()) {
                        String name = rs.getString("name");
                        long price = rs.getLong("price");
                        System.out.println("Found: " + name + " - $" + price);
                    } else {
                        System.out.println("Product not found");
                    }
                }
            }
        }
        // end::get-basic[]
    }

    public static void getCompositeKey() throws SQLException {
        // tag::get-composite[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Get with composite primary key
                KeySet primaryKey = new KeySet()
                    .setKeyColumn("store_id", 100L)
                    .setKeyColumn("product_id", 1L);

                try (RelationalResultSet rs = relStmt.executeGet("inventory", primaryKey, Options.NONE)) {
                    if (rs.next()) {
                        int stock = rs.getInt("quantity");
                        System.out.println("Stock level: " + stock);
                    }
                }
            }
        }
        // end::get-composite[]
    }

    public static void insertSingle() throws SQLException {
        // tag::insert-single[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Build a record to insert
                RelationalStruct product = EmbeddedRelationalStruct.newBuilder()
                    .addLong("id", 100L)
                    .addString("name", "New Widget")
                    .addString("category", "Electronics")
                    .addLong("price", 299L)
                    .addInt("stock", 50)
                    .build();

                int rowsInserted = relStmt.executeInsert("products", product);
                System.out.println("Inserted " + rowsInserted + " row(s)");

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
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

                // Build multiple records
                List<RelationalStruct> products = new ArrayList<>();

                products.add(EmbeddedRelationalStruct.newBuilder()
                    .addLong("id", 101L)
                    .addString("name", "Widget A")
                    .addString("category", "Electronics")
                    .addLong("price", 199L)
                    .addInt("stock", 25)
                    .build());

                products.add(EmbeddedRelationalStruct.newBuilder()
                    .addLong("id", 102L)
                    .addString("name", "Widget B")
                    .addString("category", "Electronics")
                    .addLong("price", 249L)
                    .addInt("stock", 30)
                    .build());

                int rowsInserted = relStmt.executeInsert("products", products);
                System.out.println("Inserted " + rowsInserted + " row(s)");

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
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

                RelationalStruct product = EmbeddedRelationalStruct.newBuilder()
                    .addLong("id", 1L)
                    .addString("name", "Updated Widget")
                    .addString("category", "Electronics")
                    .addLong("price", 199L)
                    .addInt("stock", 100)
                    .build();

                // Replace if the primary key already exists
                Options options = Options.builder()
                    .withOption(Options.Name.REPLACE_ON_DUPLICATE_PK, true)
                    .build();

                int rowsInserted = relStmt.executeInsert("products", product, options);
                System.out.println("Inserted/replaced " + rowsInserted + " row(s)");

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
        // end::insert-replace[]
    }

    public static void deleteSingle() throws SQLException {
        // tag::delete-single[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Delete by primary key
                KeySet primaryKey = new KeySet()
                    .setKeyColumn("id", 100L);

                List<KeySet> keysToDelete = List.of(primaryKey);
                int rowsDeleted = relStmt.executeDelete("products", keysToDelete);
                System.out.println("Deleted " + rowsDeleted + " row(s)");

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
        // end::delete-single[]
    }

    public static void deleteBatch() throws SQLException {
        // tag::delete-batch[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Delete multiple records by their keys
                List<KeySet> keysToDelete = List.of(
                    new KeySet().setKeyColumn("id", 101L),
                    new KeySet().setKeyColumn("id", 102L),
                    new KeySet().setKeyColumn("id", 103L)
                );

                int rowsDeleted = relStmt.executeDelete("products", keysToDelete);
                System.out.println("Deleted " + rowsDeleted + " row(s)");

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
        // end::delete-batch[]
    }

    public static void deleteRange() throws SQLException {
        // tag::delete-range[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Delete all products in a specific category (assuming category is part of key)
                KeySet keyPrefix = new KeySet()
                    .setKeyColumn("category", "Discontinued");

                relStmt.executeDeleteRange("products", keyPrefix, Options.NONE);
                System.out.println("Deleted all discontinued products");

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
        // end::delete-range[]
    }

    public static void withIndexHint() throws SQLException {
        // tag::index-hint[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                RelationalStatement relStmt = stmt.unwrap(RelationalStatement.class);

                // Use a specific index for the scan
                Options options = Options.builder()
                    .withOption(Options.Name.INDEX_HINT, "products_by_category")
                    .build();

                KeySet keyPrefix = new KeySet()
                    .setKeyColumn("category", "Electronics");

                try (RelationalResultSet rs = relStmt.executeScan("products", keyPrefix, options)) {
                    while (rs.next()) {
                        System.out.println("Product: " + rs.getString("name"));
                    }
                }
            }
        }
        // end::index-hint[]
    }
}
