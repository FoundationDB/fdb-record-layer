/*
 * AdvancedSnippets.java
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
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;

import java.sql.*;

/**
 * Code snippets for JDBC Guide advanced documentation.
 * This class is not meant to be run, but contains tagged sections referenced by the documentation.
 */
public class AdvancedSnippets {
    private static final String url = "jdbc:embed:/FRL/shop?schema=SHOP";

    public static void readStruct() throws SQLException {
        // tag::read-struct[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT reviewer FROM reviewers WHERE id = 1")) {
                    // Unwrap to RelationalResultSet to access getStruct()
                    RelationalResultSet relationalRs = rs.unwrap(RelationalResultSet.class);

                    if (relationalRs.next()) {
                        RelationalStruct reviewer = relationalRs.getStruct("reviewer");

                        // Access STRUCT fields by name
                        long id = reviewer.getLong("ID");
                        String name = reviewer.getString("NAME");
                        String email = reviewer.getString("EMAIL");

                        System.out.printf("ID: %d, Name: %s, Email: %s%n", id, name, email);
                    }
                }
            }
        }
        // end::read-struct[]
    }

    public static void createStructSimple() throws SQLException {
        // tag::create-struct-simple[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            String sql = "INSERT INTO reviewers (id, reviewer) VALUES (?, ?)";
            try (RelationalPreparedStatement pstmt =
                    conn.prepareStatement(sql).unwrap(RelationalPreparedStatement.class)) {

                // Build a STRUCT to insert into the reviewer column
                RelationalStruct reviewer = EmbeddedRelationalStruct.newBuilder()
                    .addLong("ID", 1L)
                    .addString("NAME", "Anthony Bourdain")
                    .addString("EMAIL", "abourdain@example.com")
                    .build();

                pstmt.setLong(1, 1L);
                pstmt.setObject(2, reviewer);

                int inserted = pstmt.executeUpdate();
                System.out.println("Rows inserted: " + inserted);

                conn.commit();
            }
        }
        // end::create-struct-simple[]
    }

    public static void createStructWithNull() throws SQLException {
        // tag::create-struct-null[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            String sql = "INSERT INTO reviewers (id, reviewer) VALUES (?, ?)";
            try (RelationalPreparedStatement pstmt =
                    conn.prepareStatement(sql).unwrap(RelationalPreparedStatement.class)) {

                // Create STRUCT with NULL email field
                RelationalStruct reviewer = EmbeddedRelationalStruct.newBuilder()
                    .addLong("ID", 1L)
                    .addString("NAME", "Anthony Bourdain")
                    .addObject("EMAIL", null)  // Explicitly set NULL
                    .build();

                pstmt.setLong(1, 1L);
                pstmt.setObject(2, reviewer);

                int inserted = pstmt.executeUpdate();
                conn.commit();
            }
        }
        // end::create-struct-null[]
    }

    public static void readArrayBasic() throws SQLException {
        // tag::read-array-basic[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT tags FROM products WHERE id = 1")) {
                    if (rs.next()) {
                        Array tagsArray = rs.getArray("tags");

                        if (tagsArray != null) {
                            // Method 1: Get array as Object[]
                            Object[] tags = (Object[]) tagsArray.getArray();
                            for (Object tag : tags) {
                                System.out.println("Tag: " + tag);
                            }
                        }
                    }
                }
            }
        }
        // end::read-array-basic[]
    }

    public static void readArrayResultSet() throws SQLException {
        // tag::read-array-resultset[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT tags FROM products WHERE id = 1")) {
                    if (rs.next()) {
                        Array tagsArray = rs.getArray("tags");

                        if (tagsArray != null) {
                            // Method 2: Get array as ResultSet
                            try (ResultSet arrayRs = tagsArray.getResultSet()) {
                                // ResultSet has two columns:
                                // Column 1: index (1-based)
                                // Column 2: value
                                while (arrayRs.next()) {
                                    int index = arrayRs.getInt(1);
                                    String value = arrayRs.getString(2);
                                    System.out.printf("tags[%d] = %s%n", index, value);
                                }
                            }
                        }
                    }
                }
            }
        }
        // end::read-array-resultset[]
    }

    public static void createArrayBasic() throws SQLException {
        // tag::create-array-basic[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            String sql = "INSERT INTO products (id, name, tags) VALUES (?, ?, ?)";
            try (RelationalPreparedStatement pstmt =
                    conn.prepareStatement(sql).unwrap(RelationalPreparedStatement.class)) {

                pstmt.setLong(1, 1L);
                pstmt.setString(2, "Widget");
                pstmt.setArray(3, EmbeddedRelationalArray.newBuilder()
                    .addAll("electronics", "gadget", "popular")
                    .build());

                int rowsAffected = pstmt.executeUpdate();
                System.out.println("Rows inserted: " + rowsAffected);

                conn.commit();
            }
        }
        // end::create-array-basic[]
    }

    public static void createArrayTypes() throws SQLException {
        // tag::create-array-types[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            String sql = "INSERT INTO data_table (id, integers, floats, booleans) " +
                         "VALUES (?, ?, ?, ?)";
            try (RelationalPreparedStatement pstmt =
                    conn.prepareStatement(sql).unwrap(RelationalPreparedStatement.class)) {

                pstmt.setLong(1, 1L);

                // Array of integers
                pstmt.setArray(2, EmbeddedRelationalArray.newBuilder()
                    .addAll(10, 20, 30, 40)
                    .build());

                // Array of floats
                pstmt.setArray(3, EmbeddedRelationalArray.newBuilder()
                    .addAll(1.5f, 2.5f, 3.5f)
                    .build());

                // Array of booleans
                pstmt.setArray(4, EmbeddedRelationalArray.newBuilder()
                    .addAll(true, false, true)
                    .build());

                pstmt.executeUpdate();
                conn.commit();
            }
        }
        // end::create-array-types[]
    }

    public static void arrayNull() throws SQLException {
        // tag::array-null[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            String sql = "INSERT INTO products (id, name, tags) VALUES (?, ?, ?)";
            try (RelationalPreparedStatement pstmt =
                    conn.prepareStatement(sql).unwrap(RelationalPreparedStatement.class)) {

                pstmt.setLong(1, 1L);
                pstmt.setString(2, "Widget");
                pstmt.setNull(3, Types.ARRAY);  // Set NULL array

                pstmt.executeUpdate();
                conn.commit();
            }
        }
        // end::array-null[]
    }

    public static void arrayMetadata() throws SQLException {
        // tag::array-metadata[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT tags FROM products WHERE id = 1")) {
                    if (rs.next()) {
                        Array tagsArray = rs.getArray("tags");

                        if (tagsArray != null) {
                            // Get array metadata
                            int baseType = tagsArray.getBaseType();
                            String baseTypeName = tagsArray.getBaseTypeName();

                            System.out.println("Array base type: " + baseType);
                            System.out.println("Array base type name: " + baseTypeName);

                            // Get array elements
                            Object[] elements = (Object[]) tagsArray.getArray();
                            System.out.println("Array length: " + elements.length);
                        }
                    }
                }
            }
        }
        // end::array-metadata[]
    }

    public static void structMetadata() throws SQLException {
        // tag::struct-metadata[]
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT reviewer FROM reviewers WHERE id = 1")) {
                    // Unwrap to RelationalResultSet to access getStruct()
                    RelationalResultSet relationalRs = rs.unwrap(RelationalResultSet.class);

                    if (relationalRs.next()) {
                        RelationalStruct reviewer = relationalRs.getStruct("reviewer");

                        if (reviewer != null) {
                            // Get STRUCT metadata
                            var metaData = reviewer.getMetaData();
                            int columnCount = metaData.getColumnCount();

                            System.out.println("STRUCT has " + columnCount + " fields:");

                            // Iterate through all fields
                            for (int i = 1; i <= columnCount; i++) {
                                String columnName = metaData.getColumnName(i);
                                String columnTypeName = metaData.getColumnTypeName(i);
                                int columnType = metaData.getColumnType(i);

                                System.out.printf("  Field %d: %s (%s, type=%d)%n",
                                    i, columnName, columnTypeName, columnType);
                            }
                        }
                    }
                }
            }
        }
        // end::struct-metadata[]
    }

    public static void arrayOfStructs() throws SQLException {
        // tag::array-of-structs[]
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            String sql = "INSERT INTO orders (id, items) VALUES (?, ?)";
            try (RelationalPreparedStatement pstmt =
                    conn.prepareStatement(sql).unwrap(RelationalPreparedStatement.class)) {

                pstmt.setLong(1, 1L);

                // Create an array of STRUCT values
                pstmt.setArray(2, EmbeddedRelationalArray.newBuilder()
                    .addAll(
                        EmbeddedRelationalStruct.newBuilder()
                            .addInt("product_id", 11)
                            .addString("product_name", "Widget A")
                            .addInt("quantity", 2)
                            .build(),
                        EmbeddedRelationalStruct.newBuilder()
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
                RelationalStruct product = EmbeddedRelationalStruct.newBuilder()
                    .addLong("ID", 1L)
                    .addString("NAME", "Multi-tool")
                    .addArray("TAGS", EmbeddedRelationalArray.newBuilder()
                        .addAll("versatile", "portable", "durable")
                        .build())
                    .addArray("RATINGS", EmbeddedRelationalArray.newBuilder()
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
