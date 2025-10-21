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
 * and a {@code value}. The invariant enforced by the system is that for each {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory},
 * there can only be a single value of a certain type. For example, for a directory node named
 * "database" there can be one child of type {@code long}, one child of type {@code String}, one child of type {@code NULL},
 * etc. The guarantee that this provides is that when retrieving a {@link com.apple.foundationdb.KeyValue} from the
 * database and observing the prefix of the {@code Key} - for example, {@code ["root", 8, 15, "user data"]} - we can unambiguously
 * parse the key and assign the elements of the {@code Tuple} to individual named {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory}
 * instances - for example, the first element is of type {@code String} therefore it has to be the "database name", the second
 * element is a {@code long}, therefore it has to be the "owner ID", etc.
 * In fact, the implementation is slightly more nuanced: Each {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory}
 * can have many children of a certain type as long as they are all defined as {@code Constants} - that is, they have a predefined value,
 * or a single {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.AnyValue} value of that type.
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
 *     <li>"root" -> root</li>
 *     <li>"database" -> database</li>
 *     <li>8 -> dbId</li>
 *     <li>"my-db" -> dbName</li>
 * </ul>
 * and {@code ["root", "database", "my-db"]} would be considered illegal, as there is no way to match the "my-db" to a Directory node.
 * </p>
 */
package com.apple.foundationdb.record.provider.foundationdb.keyspace;
