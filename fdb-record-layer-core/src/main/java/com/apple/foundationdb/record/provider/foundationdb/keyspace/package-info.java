/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

/**
 * A directory-like symbolic layout of keys in FoundationDB.
 *
 * <p>
 * All keys in FoundationDB used by the Record Layer are based on {@link com.apple.foundationdb.tuple.Tuple}s.
 * The components of the tuple that represents the location (that is, tuple prefix) of a particular {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore} can depend on several
 * separate pieces of identifying information about the record store.
 * A {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace} is a symbolic representation of this layout, giving names -- or constant values -- to each component.
 * A {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath} is a concrete instance of a {@code KeySpace}, somewhat like a directory pathname in a file system.
 * A {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory} is a template, or definition, for a type and (potentially value) that can be assigned to a node in the tree.
 * </p>
 * <p>
 * The purpose of the {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace} model is to uniquely
 * define the structure and format of the data layout for record layer. As a consumer of record layer may find out, data
 * layout complexity may grow over time and various parts of the data model may become tangled and hard to keep consistent
 * and separate. The {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace} (and through it, the
 * {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory}) are there to define and enforce
 * type safety and consistency allowing data to be uniquely interpreted.
 * Each {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory} has a {@code name}, a {@code type}
 * and potentially a {@code value}.
 * The invariants enforced by the system are:
 * <ul>
 *  <li>
 *      For each {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory}, there can only be
 *      a single sub-directory value of a certain type. For example, for a directory node named "database" there can be one
 *      child of type {@code long}, one child of type {@code String}, one child of type {@code NULL}, etc
 *  </li>
 *  <li>
 *      All children names within a parent directory are unique
 *  </li>
 * </ul>
 * The benefits provided by the KeySpace structures and these invariants are:
 * <ul>
 *     <li>
 *         A well-defined and type-safe structure for the raw {@link com.apple.foundationdb.tuple.Tuple}s that define the
 *         way in which a key is structured. The {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace}
 *         API can be used to generate Tuples for the keys that are guaranteed to follow the defined structure.
 *     </li>
 *     <li>
 *         A way to parse the raw keys from the database and an API to assign semantics to them. For example, the Key Tuple
 *         {@code ["sys", 8, 15, "user data"]} can be unambiguously parsed and assigns {@code "sys"} to "database name",
 *         {@code 8} to "user ID" etc.
 *     </li>
 *     <li>
 *         An extensible structure that can add functionality to the keyspace structure. One such example is the directory layer
 *         encoding, where (potentially long) string constituents of the key are replaced by {@code long} values and thus reduce the
 *         storage required for representation. This is done through subclassing {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory}
 *         (see {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory}) where the encoding and decoding is managed.
 *     </li>
 * </ul>
 * </p>
 * <p>
 * To illustrate the issues that this is meant to solve, imagine a structure of
 * <pre>
 *           permissions
 *                |
 *      ---------------------
 *      |                   |
 *   users(id)           groups(id)
 * </pre>
 * One way to represent this structure is by defining the method:
 * <pre>
 *     Subspace userSubspace(long id) { return permissionsTuple.add(id); }
 * </pre>
 * and then, in some other part of the code, define this method:
 * <pre>
 *     Subspace groupSubspace(long id) { return permissionsTuple.add(id); }
 * </pre>
 * but then the key tuple {@code ["permissions", 8]} is ambiguous and cannot be determined whether it refers to userId 8 or groupId 8.
 * This can cause data corruption where data meant to be written to one area of the model overwrites other pieces of data from
 * another part of the model.
 * The {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace} will prevent this from happening. A properly
 * formed (and allowed) structure would look like
 * <pre>
 *           "permissions"
 *                |
 *      ---------------------
 *      |                   |
 *   "users"           "groups"
 *      |                  |
 *    userId(long)     groupId(long)
 * </pre>
 * and the tuple {@code ["permissions", "users", 8]} is unambiguous.
 * </p>
 * <p>
 * In reality, the implementation is slightly more nuanced: Each {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory}
 * can have many children of a certain type as long as they are all defined as {@code Constants} - that is, they have a predefined value,
 * or a single {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory#ANY_VALUE} value of that type.
 * For example, the node for "database" can have children (with {@code String} type)
 * for "schema", "permissions" and "records" - where these values are known ahead of time. One cannot then add a value of type {@code String}
 * as a child with the value "user-442233".
 * </p>
 * <p>
 * For example: The following structure lists a hypothetical database data layout definition and example key/value entries that
 * conform to these values: Each entry is an instance of a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory}
 * with the format of name/type/value
 *
 * <pre>
 *                                          root/String/"root"
 *                                                 |
 *                               ---------------------------------------
 *                               |                                     |
 *                    database/String/"database"               users/String/"users"
 *                               |                                     |
 *                   --------------------------                 userId/long/AnyValue
 *                   |                        |
 *          dbId/long/AnyValue         schema/String/"schema"
 *                   |
 *        ------------------------------
 *        |                            |
 * dbName/String/AnyValue    dbSchema/String/AnyValue
 * </pre>
 * In the above example, the FDB key of {@code ["root", "database", 8, "my-db"]} would be interpreted as follows:
 * <ul>
 *     <li>"root" - root</li>
 *     <li>"database" - database</li>
 *     <li>8 - dbId</li>
 *     <li>"my-db" - dbName</li>
 * </ul>
 * and {@code ["root", "database", "my-db"]} would be considered illegal, as there is no way to match the "my-db" to a Directory node.
 * </p>
 */
package com.apple.foundationdb.record.provider.foundationdb.keyspace;
