/*
 * ComplexTypesEmbedded.java
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;

import java.sql.*;

/**
 * Example demonstrating STRUCTs and ARRAYs with the embedded driver.
 * This example is from the Advanced JDBC Features documentation.
 */
public class ComplexTypesEmbedded {
    private static final String CATALOG_URL = "jdbc:embed:/__SYS?schema=CATALOG";
    private static final String APP_URL = "jdbc:embed:/FRL/shop?schema=SHOP";

    public static void main(String[] args) {
        try {
            setupDatabase();
            insertData();
            queryData();
            System.out.println("\nAll operations completed successfully!");
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    // tag::setup[]
    private static void setupDatabase() throws SQLException {
        try (Connection conn = DriverManager.getConnection(CATALOG_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP DATABASE IF EXISTS \"/FRL/shop\"");
                stmt.executeUpdate("DROP SCHEMA TEMPLATE IF EXISTS shop_template");

                stmt.executeUpdate(
                    "CREATE SCHEMA TEMPLATE shop_template " +
                    "CREATE TABLE orders (" +
                    "  id BIGINT PRIMARY KEY, " +
                    "  customer STRUCT<name STRING, email STRING, address STRUCT<street STRING, city STRING>>, " +
                    "  items ARRAY<STRUCT<product_id INTEGER, name STRING, quantity INTEGER>>, " +
                    "  tags ARRAY<STRING>" +
                    ")"
                );

                stmt.executeUpdate("CREATE DATABASE \"/FRL/shop\"");
                stmt.executeUpdate("CREATE SCHEMA \"/FRL/shop/SHOP\" WITH TEMPLATE shop_template");

                System.out.println("Database and schema created successfully");
            }
        }
    }
    // end::setup[]

    // tag::insert-struct[]
    private static void insertData() throws SQLException {
        try (Connection conn = DriverManager.getConnection(APP_URL)) {
            conn.setAutoCommit(false);

            String sql = "INSERT INTO orders (id, customer, items, tags) " +
                         "VALUES (?, ?, ?, ?)";
            try (RelationalPreparedStatement pstmt =
                    conn.prepareStatement(sql).unwrap(RelationalPreparedStatement.class)) {

                pstmt.setLong(1, 1L);

                // Create a STRUCT with nested STRUCT for customer
                RelationalStruct customer = EmbeddedRelationalStruct.newBuilder()
                    .addString("name", "Alice Johnson")
                    .addString("email", "alice@example.com")
                    .addStruct("address", EmbeddedRelationalStruct.newBuilder()
                        .addString("street", "123 Main St")
                        .addString("city", "Springfield")
                        .build())
                    .build();

                pstmt.setObject(2, customer);

                // Create array of STRUCT values
                pstmt.setArray(3, EmbeddedRelationalArray.newBuilder()
                    .addAll(
                        EmbeddedRelationalStruct.newBuilder()
                            .addInt("product_id", 101)
                            .addString("name", "Laptop")
                            .addInt("quantity", 1)
                            .build(),
                        EmbeddedRelationalStruct.newBuilder()
                            .addInt("product_id", 202)
                            .addString("name", "Mouse")
                            .addInt("quantity", 2)
                            .build()
                    )
                    .build());

                // Create simple string array
                pstmt.setArray(4, EmbeddedRelationalArray.newBuilder()
                    .addAll("electronics", "urgent", "gift")
                    .build());

                pstmt.executeUpdate();
                conn.commit();
                System.out.println("Data inserted successfully");
            }
        }
    }
    // end::insert-struct[]

    // tag::query-struct[]
    private static void queryData() throws SQLException {
        try (Connection conn = DriverManager.getConnection(APP_URL)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM orders WHERE id = 1")) {
                    if (rs.next()) {
                        long id = rs.getLong("id");
                        System.out.printf("Order %d:%n", id);

                        // Read customer STRUCT
                        RelationalResultSet relationalRs = rs.unwrap(RelationalResultSet.class);
                        RelationalStruct customer = relationalRs.getStruct("customer");
                        if (customer != null) {
                            String name = customer.getString("name");
                            String email = customer.getString("email");

                            System.out.printf("  Customer: %s (%s)%n", name, email);

                            // Access nested address STRUCT
                            RelationalStruct address = customer.getStruct("address");
                            if (address != null) {
                                String street = address.getString("street");
                                String city = address.getString("city");
                                System.out.printf("  Address: %s, %s%n", street, city);
                            }
                        }

                        // Process items array
                        Array itemsArray = rs.getArray("items");
                        if (itemsArray != null) {
                            System.out.println("  Items:");
                            try (ResultSet arrayRs = itemsArray.getResultSet()) {
                                // Unwrap to RelationalResultSet to access getStruct()
                                RelationalResultSet itemsRelRs = arrayRs.unwrap(RelationalResultSet.class);

                                while (itemsRelRs.next()) {
                                    int index = itemsRelRs.getInt(1);
                                    RelationalStruct item = itemsRelRs.getStruct(2);

                                    int productId = item.getInt("product_id");
                                    String itemName = item.getString("name");
                                    int quantity = item.getInt("quantity");

                                    System.out.printf("    - %s (ID: %d) x %d%n",
                                        itemName, productId, quantity);
                                }
                            }
                        }

                        // Process tags array
                        Array tagsArray = rs.getArray("tags");
                        if (tagsArray != null) {
                            System.out.println("  Tags:");
                            Object[] tags = (Object[]) tagsArray.getArray();
                            for (Object tag : tags) {
                                System.out.println("    - " + tag);
                            }
                        }
                    }
                }
            }
        }
    }
    // end::query-struct[]
}
