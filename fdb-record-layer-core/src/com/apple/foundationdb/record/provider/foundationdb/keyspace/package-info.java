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
 * </p>
 */
package com.apple.foundationdb.record.provider.foundationdb.keyspace;
