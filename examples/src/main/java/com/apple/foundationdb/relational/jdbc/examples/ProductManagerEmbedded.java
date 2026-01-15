/*
 * ProductManagerEmbedded.java
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

import java.sql.*;

// tag::complete[]
/**
 * Example demonstrating basic JDBC operations with the embedded driver.
 * This example is from the JDBC Guide documentation.
 */
public class ProductManagerEmbedded {
    private static final String CATALOG_URL = "jdbc:embed:/__SYS?schema=CATALOG";
    private static final String APP_URL = "jdbc:embed:/FRL/shop?schema=SHOP";

    public static void main(String[] args) {
        try {
            setupDatabase();
            insertProducts();
            queryProducts();
            updateProduct();
            deleteProduct();
            System.out.println("All operations completed successfully!");
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void setupDatabase() throws SQLException {
        // Connect to CATALOG to create database objects
        try (Connection conn = DriverManager.getConnection(CATALOG_URL)) {
            try (Statement stmt = conn.createStatement()) {
                // Drop existing objects if they exist
                stmt.executeUpdate("DROP DATABASE IF EXISTS \"/FRL/shop\"");
                stmt.executeUpdate("DROP SCHEMA TEMPLATE IF EXISTS shop_template");

                // Create schema template with table definition
                stmt.executeUpdate(
                    "CREATE SCHEMA TEMPLATE shop_template " +
                    "CREATE TABLE products (" +
                    "  id BIGINT PRIMARY KEY, " +
                    "  name STRING, " +
                    "  category STRING, " +
                    "  price BIGINT, " +
                    "  stock INTEGER" +
                    ")"
                );

                // Create database and schema
                stmt.executeUpdate("CREATE DATABASE \"/FRL/shop\"");
                stmt.executeUpdate("CREATE SCHEMA \"/FRL/shop/SHOP\" WITH TEMPLATE shop_template");

                System.out.println("Database and schema created successfully");
            }
        }
    }

    private static void insertProducts() throws SQLException {
        String sql = "INSERT INTO products (id, name, category, price, stock) VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(APP_URL)) {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                conn.setAutoCommit(false);

                Object[][] products = {
                    {1L, "Widget A", "Electronics", 100L, 50},
                    {2L, "Widget B", "Electronics", 150L, 30},
                    {3L, "Gadget X", "Electronics", 200L, 20}
                };

                for (Object[] product : products) {
                    pstmt.setLong(1, (Long) product[0]);
                    pstmt.setString(2, (String) product[1]);
                    pstmt.setString(3, (String) product[2]);
                    pstmt.setLong(4, (Long) product[3]);
                    pstmt.setInt(5, (Integer) product[4]);
                    pstmt.executeUpdate();
                }

                conn.commit();
                System.out.println("Products inserted successfully");
            }
        }
    }

    private static void queryProducts() throws SQLException {
        String sql = "SELECT * FROM products WHERE category = ? ORDER BY price";

        try (Connection conn = DriverManager.getConnection(APP_URL)) {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, "Electronics");

                try (ResultSet rs = pstmt.executeQuery()) {
                    System.out.println("\nProducts in Electronics category:");
                    while (rs.next()) {
                        System.out.printf("  %d: %s - $%d (Stock: %d)%n",
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getLong("price"),
                            rs.getInt("stock")
                        );
                    }
                }
            }
        }
    }

    private static void updateProduct() throws SQLException {
        String sql = "UPDATE products SET price = ?, stock = ? WHERE id = ?";

        try (Connection conn = DriverManager.getConnection(APP_URL)) {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setLong(1, 120L);
                pstmt.setInt(2, 45);
                pstmt.setLong(3, 1L);

                int rowsAffected = pstmt.executeUpdate();
                System.out.println("\nUpdated " + rowsAffected + " product(s)");
            }
        }
    }

    private static void deleteProduct() throws SQLException {
        String sql = "DELETE FROM products WHERE id = ?";

        try (Connection conn = DriverManager.getConnection(APP_URL)) {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setLong(1, 3L);

                int rowsAffected = pstmt.executeUpdate();
                System.out.println("Deleted " + rowsAffected + " product(s)");
            }
        }
    }
}
// end::complete[]
