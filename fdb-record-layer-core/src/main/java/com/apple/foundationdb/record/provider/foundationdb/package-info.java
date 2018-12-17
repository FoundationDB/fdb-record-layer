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
 * Classes for FoundationDB record storage.
 *
 * Note that at present FoundationDB is the <em>only</em> supported backend.
 *
 * <h2>Databases</h2>
 *
 * <p>
 * An FDB cluster is represented by an {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase}, which is gotten from an {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory}.
 * All operations are transactional: a transaction is represented by an {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext}, which is opened from a database.
 * </p>
 *
 * <h2>Record Stores</h2>
 *
 * <p>
 * Records are read and written by an {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore}, which is opened within a record context using a {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore.Builder}.
 * Storing a Protobuf {@link com.google.protobuf.Message} as a record in a record store produces an {@link com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord}, which associates it with a record type and primary key.
 * Retrieving a record by primary key from the record store also returns a stored record.
 * A query returns a {@link com.apple.foundationdb.record.RecordCursor} of {@link com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord FDBQueriedRecord}s, which extend {@code FDBStoredRecord}.
 * </p>
 */
package com.apple.foundationdb.record.provider.foundationdb;
