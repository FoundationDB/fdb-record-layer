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

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public interface RelationalDirectAccessStatement extends AutoCloseable {
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
     * CREATE TABLE FOO(a bigint, b bigint, c bigint, primary key(a, b))
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

    /**
     * Insert.
     * @param tableName Table to insert into.
     * @param message Protobuf to insert.
     * @return How many rows inserted.
     * @throws SQLException On failed insert.
     * @deprecated since 01/19/2023 to avoid protobufs in our API; use {@link #executeInsert(String, List)} instead.
     */
    // syntactic sugar to make it easier to insert a single record
    @Deprecated
    default int executeInsert(@Nonnull String tableName, Message message) throws SQLException {
        return executeInsert(tableName, Collections.singleton(message).iterator(), Options.NONE);
    }

    /**
     * Insert.
     * @param tableName Table to insert into.
     * @param message Protobuf to insert.
     * @param options Options to color the insert.
     * @return How many rows inserted.
     * @throws SQLException On failed insert.
     * @deprecated since 01/19/2023 to avoid protobufs in our API; use {@link #executeInsert(String, List)} instead.
     */
    @Deprecated
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
     * @deprecated since 01/19/2023 because we would void having protobufs in our API; use
     * {@link #executeInsert(String, List)} instead.
     */
    @Deprecated
    default int executeInsert(@Nonnull String tableName, @Nonnull Iterable<? extends Message> data) throws SQLException {
        return executeInsert(tableName, data.iterator());
    }

    /**
     * Insert one or more records into the specified table, updating any indexes as necessary to maintain consistency.
     *
     * @param tableName the name of the table to insert into.
     * @param data      the data to insert.
     * @return the number of records inserted.
     * @throws SQLException If something goes wrong. Use the error code to determine exactly what.
     * @deprecated since 01/19/2023 because we would void having protobufs in our API; use
     * {@link #executeInsert(String, List)} instead.
     */
    @Deprecated
    default int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data) throws SQLException {
        return executeInsert(tableName, data, Options.NONE);
    }

    /**
     * Insert one or more records into the specified table, updating any indexes as necessary to maintain consistency.
     *
     * @param tableName the name of the table to insert into.
     * @param data      the data to insert.
     * @return the number of records inserted.
     * @throws SQLException If something goes wrong. Use the error code to determine exactly what.
     */
    default int executeInsert(@Nonnull String tableName, @Nonnull List<RelationalStruct> data) throws SQLException {
        return executeInsert(tableName, data, Options.NONE);
    }

    /**
     * Insert one or more records into the specified table, updating any indexes as necessary to maintain consistency.
     *
     * @param tableName the name of the table to insert into.
     * @param data      the data to insert.
     * @param options    options to apply to the insert.
     * @return the number of records inserted.
     * @throws SQLException If something goes wrong. Use the error code to determine exactly what.
     * @deprecated since 01/19/2023 because we would void having protobufs in our API; use
     * {@link #executeInsert(String, List, Options)} instead.
     */
    @Deprecated
    int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws SQLException;

    /**
     * Insert one or more records into the specified table, updating any indexes as necessary to maintain consistency.
     *
     * @param tableName the name of the table to insert into.
     * @param data      the data to insert.
     * @param options    options to apply to the insert.
     * @return the number of records inserted.
     * @throws SQLException If something goes wrong. Use the error code to determine exactly what.
     */
    int executeInsert(@Nonnull String tableName, @Nonnull List<RelationalStruct> data, @Nonnull Options options)
            throws SQLException;

    /**
     * Get a DynamicMessageBuilder instance.
     * @param tableName Table to build the message for.
     * @return Message Builder.
     * @throws SQLException On fail to get builder.
     * @deprecated since 01/19/2023 to avoid protobuf in API; use {@link #executeInsert(String, List, Options)} instead.
     */
    @Deprecated
    DynamicMessageBuilder getDataBuilder(@Nonnull String tableName) throws SQLException;

    /**
     * Creates a {@link DynamicMessageBuilder} of a specific {@code Column} in a {@code Table}. For example, say we have
     * the following DDL of a table:
     *
     * <code>
     * CREATE TYPE AS STRUCT DESK_INFO(FLOOR STRING, ROW INT, COL INT);
     * CREATE TYPE AS STRUCT EMPLOYEE(NAME STRING, DESK DESK_INFO);
     * CREATE TABLE REPORTS( ID INT, MANAGER EMPLOYEE NOT NULL, MANAGED EMPLOYEE NULL, PRIMARY KEY(ID) );
     * </code>
     * We can retrieve a {@link DynamicMessageBuilder} of "DESK" field in {@code MANAGED} field of the table "REPORTS":
     * <pre>
     * <code>getDataBuilder("REPORTS", List.of("MANAGED", "DESK"))</code>
     * </pre>
     *
     * Or, by prefixing the table name with the schema name, as in, e.g.:
     * <pre>
     * <code>getDataBuilder("SCHEMA.REPORTS", List.of("MANAGED", "DESK"))</code>
     * </pre>
     *
     * @param maybeQualifiedTableName Table name, optionally with a qualifying schema name.
     * @param nestedFields the (nested) field we want to retrieve the {@link DynamicMessageBuilder} of.
     * @return A {@link DynamicMessageBuilder} of the field.
     * @throws SQLException In case of error (e.g. failure to open a transaction).
     */
    @Nonnull
    DynamicMessageBuilder getDataBuilder(@Nonnull final String maybeQualifiedTableName, @Nonnull final List<String> nestedFields) throws SQLException;

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
     * CREATE TABLE FOO(a bigint, b bigint, c bigint, primary key(a, b))
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
