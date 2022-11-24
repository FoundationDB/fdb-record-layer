/*
 * RelationalDirectAccessStatement.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api;

import com.google.protobuf.Message;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;

import javax.annotation.Nonnull;

public interface RelationalDirectAccessStatement {
    /**
     * Execute a multi-row scan against the database, returning a {@link RelationalResultSet} containing
     * the results of the scan.
     *
     * This can be used to scan contiguous rows on a table based on a certain PK range.
     * The range should be provided via a key prefix.
     *
     * <pre>
     * Examples:
     *
     * CREATE TABLE FOO(a int64, b int64, c int64, primary key(a, b))
     * INSERT INTO FOO VALUES [
     *     {"A": 1, "B": 1, "C": 2},
     *     {"A": 1, "B": 2, "C": 3},
     *     {"A": 2, "B": 3, "C": 4}
     * ]
     *
     * // Scans every row
     * executeScan("FOO", new KeySet(), Options.NONE)
     *
     * // Scans the first two rows
     * executeScan("FOO", new KeySet().setKeyColumn("A", 1), Options.NONE)
     *
     * // Scans only the first row
     * executeScan("FOO", new KeySet().setKeyColumn("A", 1).setKeyColumn("B", 1), Options.NONE)
     *
     * // Fails because C is not part of the Primary Key
     * executeScan("FOO", new KeySet().setKeyColumn("C", 2), Options.NONE)
     *
     * // Fails because B is not a prefix of the Primary Key
     * executeScan("FOO", new KeySet().setKeyColumn("B", 1), Options.NONE)
     * </pre>
     *
     * <p>
     * The caller can specify some execution-level options, which can be used to control the execution path
     * that the execution. Specifically, the following options are honored:
     * <ul>
     *     <li>use.index: the name of a specific index to use during scan</li>
     *     <li>continuation: The value of the continuation to use for the scan</li>
     * </ul>
     *
     * @param tableName the name of the table.
     * @param keyPrefix the key prefix to use when scanning entries.
     * @param options      options that can be used to configure the scan.
     * @return a ResultSet containing the entire record in the underlying scan.
     * @throws SQLException if something goes wrong. Use the Error code to determine exactly what.
     */
    @Nonnull
    RelationalResultSet executeScan(@Nonnull String tableName, @Nonnull KeySet keyPrefix, @Nonnull Options options) throws SQLException;

    /**
     * Get a single record from the system by key.
     * <p>
     * This constructs a primary key from the specified KeySet according to the table definition, and then performs
     * a single-row lookup. This is equivalent to executing a scan on the range bounds, but can potentially be
     * more efficiently executed.
     *
     * @param tableName the name of the table to get data from
     * @param key       The constructor for the key to fetch by
     * @param options   the options for the GET operation.
     * @return a ResultSet containing the entire record from the resulting GET, either 1 row or 0. If the row does
     * not exist the ResultSet will be empty
     * @throws SQLException If something geos wrong. Use the error code to determine exactly what.
     */
    @Nonnull
    RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options) throws SQLException;

    // syntactic sugar to make it easier to insert a single record
    default int executeInsert(@Nonnull String tableName, Message message) throws SQLException {
        return executeInsert(tableName, Collections.singleton(message).iterator(), Options.NONE);
    }

    default int executeInsert(@Nonnull String tableName, Message message, Options options) throws SQLException {
        return executeInsert(tableName, Collections.singleton(message).iterator(), options);
    }

    /**
     * Insert one or more records into the specified table, updating any indexes as necessary to maintain consistency.
     * <p>
     * Equivalent to {@link #executeInsert(String, Iterator)}, but avoids the extra {@code .iterator()} call
     * for a slightly nicer user experience.
     *
     * @param tableName the name of the table to insert into.
     * @param data      the data to insert.
     * @return the number of records inserted.
     * @throws SQLException If something geos wrong. Use the error code to determine exactly what.
     */
    default int executeInsert(@Nonnull String tableName, @Nonnull Iterable<? extends Message> data) throws SQLException {
        return executeInsert(tableName, data.iterator());
    }

    /**
     * Insert one or more records into the specified table, updating any indexes as necessary to maintain consistency.
     *
     * @param tableName the name of the table to insert into.
     * @param data      the data to insert.
     * @return the number of records inserted.
     * @throws SQLException If something geos wrong. Use the error code to determine exactly what.
     */
    default int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data) throws SQLException {
        return executeInsert(tableName, data, Options.NONE);
    }

    int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws SQLException;

    DynamicMessageBuilder getDataBuilder(@Nonnull String typeName) throws SQLException;

    /**
     * Delete one or more records from the specified table, specified by key, if such records exist.
     * <p>
     * equivalent to {@link #executeDelete(String, Iterator)}, but with a marginally nicer user experience
     * of not needing to call {@code .iterator()}
     * </p>
     *
     * @param tableName the name of the table to delete from
     * @param keys      the keys to delete
     * @return the number of records deleted
     * @throws SQLException if something goes wrong. Use the error code to determine exactly what.
     */
    default int executeDelete(@Nonnull String tableName, @Nonnull Iterable<KeySet> keys) throws SQLException {
        return executeDelete(tableName, keys.iterator(), Options.NONE);
    }

    default int executeDelete(@Nonnull String tableName, @Nonnull Iterable<KeySet> keys, @Nonnull Options options) throws SQLException {
        return executeDelete(tableName, keys.iterator(), options);
    }

    /**
     * Delete one or more records from the specified table, specified by key, if such records exist.
     *
     * @param tableName the name of the table to delete from
     * @param keys      the keys to delete
     * @return the number of records deleted
     * @throws SQLException if something goes wrong. Use the error code to determine exactly what.
     */
    default int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys) throws SQLException {
        return executeDelete(tableName, keys, Options.NONE);
    }

    int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws SQLException;

    /**
     * This can be used to delete contiguous rows on a table based on a certain PK range.
     * The range should be provided via a key prefix.
     *
     * <pre>
     * Examples:
     *
     * CREATE TABLE FOO(a int64, b int64, c int64, primary key(a, b))
     * INSERT INTO FOO VALUES [
     *     {"A": 1, "B": 1, "C": 2},
     *     {"A": 1, "B": 2, "C": 3},
     *     {"A": 2, "B": 3, "C": 4}
     * ]
     *
     * // deletes every row
     * executeDeleteRange("FOO", new KeySet(), Options.NONE)
     *
     * // deletes the first two rows
     * executeDeleteRange("FOO", new KeySet().setKeyColumn("A", 1), Options.NONE)
     *
     * // deletes only the first row
     * executeDeleteRange("FOO", new KeySet().setKeyColumn("A", 1).setKeyColumn("B", 1), Options.NONE)
     *
     * // Fails because C is not part of the Primary Key
     * executeDeleteRange("FOO", new KeySet().setKeyColumn("C", 2), Options.NONE)
     *
     * // Fails because B is not a prefix of the Primary Key
     * executeDeleteRange("FOO", new KeySet().setKeyColumn("B", 1), Options.NONE)
     * </pre>
     *
     * @param tableName the name of the table to delete from
     * @param keyPrefix the key prefix to use when deleting entries
     * @param options options that can be used to configure the delete
     * @throws SQLException if something goes wrong. Use the error code to determine exactly what.
     */
    void executeDeleteRange(@Nonnull String tableName, @Nonnull KeySet keyPrefix, @Nonnull Options options) throws SQLException;
}
