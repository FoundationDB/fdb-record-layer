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
 * A layer for mapping potentially long strings to and from more compact tuple elements.
 *
 * <p>
 * A string identifier that is part of a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace} will be part of the key for every record and every index entry is a record store following that key space.
 * Since FoundationDB does not do prefix compression, this can add up to a lot of space when the string is long enough to really express anything.
 * Space is saved by replacing the string in the key {@link com.apple.foundationdb.tuple.Tuple} with an integer gotten by interning it.
 * </p>
 *
 * <p>
 * This interning operation is very similar to what the {@link com.apple.foundationdb.directory.DirectoryLayer} does, except that whereas the directory layer actually identifies space between the root of the key-value store,
 * an interned path element might be anywhere in the path.
 * </p>
 */
package com.apple.foundationdb.record.provider.foundationdb.layers.interning;
