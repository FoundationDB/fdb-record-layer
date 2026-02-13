/*
 * BasicSnippets.java
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

/**
 * Code snippets for JDBC Guide basic documentation.
 * This class is not meant to be run, but contains tagged sections referenced by the documentation.
 */
public class BasicSnippets {
    private static final String url = "jdbc:embed:/FRL/shop?schema=SHOP";

    public static void simpleQuery() throws SQLException {
        // tag::simple-query[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM products WHERE price > 100")) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        String name = rs.getString("name");
                        long price = rs.getLong("price");
                        System.out.println(id + ": " + name + " - $" + price);
                    }
                }
            }
        }
        // end::simple-query[]
    }

    public static void simpleUpdate() throws SQLException {
        // tag::simple-update[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                int rowsAffected = stmt.executeUpdate(
                    "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 100)"
                );
                System.out.println("Rows affected: " + rowsAffected);
            }
        }
        // end::simple-update[]
    }

    public static void preparedInsert() throws SQLException {
        // tag::prepared-insert[]
        String sql = "INSERT INTO products (id, name, price, stock) VALUES (?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(url)) {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setLong(1, 1);
                pstmt.setString(2, "Widget A");
                pstmt.setLong(3, 100);
                pstmt.setInt(4, 50);

                int rowsAffected = pstmt.executeUpdate();
                System.out.println("Rows inserted: " + rowsAffected);
            }
        }
        // end::prepared-insert[]
    }

    public static void preparedQuery() throws SQLException {
        // tag::prepared-query[]
        String sql = "SELECT * FROM products WHERE category = ? AND price >= ?";

        try (Connection conn = DriverManager.getConnection(url)) {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, "Electronics");
                pstmt.setLong(2, 100);

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        System.out.println(rs.getString("name") + ": $" + rs.getLong("price"));
                    }
                }
            }
        }
        // end::prepared-query[]
    }

    public static void autocommit() throws SQLException {
        // tag::autocommit[]
        try (Connection conn = DriverManager.getConnection(url)) {
            System.out.println("Auto-commit: " + conn.getAutoCommit());

            // Disable auto-commit for manual transaction control
            conn.setAutoCommit(false);
        }
        // end::autocommit[]
    }

    public static void transaction() throws SQLException {
        // tag::transaction[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("INSERT INTO accounts (id, balance) VALUES (1, 1000)");
                stmt.executeUpdate("INSERT INTO accounts (id, balance) VALUES (2, 500)");
                stmt.executeUpdate("UPDATE accounts SET balance = balance - 100 WHERE id = 1");
                stmt.executeUpdate("UPDATE accounts SET balance = balance + 100 WHERE id = 2");

                conn.commit();
                System.out.println("Transaction committed successfully");
            } catch (SQLException e) {
                conn.rollback();
                System.out.println("Transaction rolled back due to error: " + e.getMessage());
                throw e;
            }
        }
        // end::transaction[]
    }

    public static void resultsetBasic() throws SQLException {
        // tag::resultset-basic[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM products ORDER BY price")) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        String name = rs.getString("name");
                        long price = rs.getLong("price");

                        System.out.printf("ID: %d, Name: %s, Price: %d%n", id, name, price);
                    }
                }
            }
        }
        // end::resultset-basic[]
    }

    public static void nullHandling() throws SQLException {
        // tag::null-handling[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM products")) {
                    while (rs.next()) {
                        long price = rs.getLong("price");
                        if (rs.wasNull()) {
                            System.out.println("Price is NULL");
                        } else {
                            System.out.println("Price: " + price);
                        }
                    }
                }
            }
        }
        // end::null-handling[]
    }

    public static void metadata() throws SQLException {
        // tag::metadata[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM products")) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        String columnType = metaData.getColumnTypeName(i);
                        System.out.println(columnName + " (" + columnType + ")");
                    }
                }
            }
        }
        // end::metadata[]
    }

    public static void errorHandling() throws SQLException {
        // tag::error-handling[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("INSERT INTO products (id, name) VALUES (1, 'Widget')");
            }
        } catch (SQLException e) {
            System.err.println("SQL Error: " + e.getMessage());
            System.err.println("SQL State: " + e.getSQLState());
            System.err.println("Error Code: " + e.getErrorCode());

            // Handle specific error conditions
            if (e.getSQLState().startsWith("23")) {
                System.err.println("Integrity constraint violation");
            }
        }
        // end::error-handling[]
    }

    public static void databaseMetadata() throws SQLException {
        // tag::database-metadata[]
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData metaData = conn.getMetaData();

            System.out.println("Database: " + metaData.getDatabaseProductName());
            System.out.println("Driver: " + metaData.getDriverName());
            System.out.println("Driver Version: " + metaData.getDriverVersion());

            // List tables
            try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    System.out.println("Table: " + tables.getString("TABLE_NAME"));
                }
            }
        }
        // end::database-metadata[]
    }
}
