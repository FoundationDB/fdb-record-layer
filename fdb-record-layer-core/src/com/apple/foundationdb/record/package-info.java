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
 * A record layer for FoundationDB based on Protocol Buffers.
 *
 * <p>
 * This layer is a record-oriented database based on <a href="https://developers.google.com/protocol-buffers/">Protocol Buffers</a>.
 * Record instances are Protobuf {@link com.google.protobuf.Message}s stored in the FoundationDB key-value store.
 * </p>
 *
 * <h2>Highlights</h2>
 *
 * <p>
 * Record data is mediated by a {@link com.apple.foundationdb.record.RecordMetaData}, which defines {@link com.apple.foundationdb.record.metadata.RecordType}s and their transactionally-consistent secondary {@link com.apple.foundationdb.record.metadata.Index}es.
 * </p>
 *
 * <p>
 * All interaction with the database is performed inside a transaction, represented by an {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext}.
 * </p>
 *
 * <p>
 * An {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore} represents a specific contiguous {@link com.apple.foundationdb.subspace.Subspace} of the key-value store used to store records for a specific meta-data.
 * </p>
 *
 * <p>
 * Records can be retrieved directly from the record store by primary key. Or they can be gotten as the result of queries.
 * A {@link com.apple.foundationdb.record.query.RecordQuery} represents a logical query.
 * A {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner} transforms a logical query into an executable {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}, which can be run against a record store
 * to give a {@link com.apple.foundationdb.record.RecordCursor} of records.
 * </p>
 *
 * <p>
 * The same meta-data can be used to describe multiple record stores.
 * To organize the placement of these record stores in separate key-value ranges according to hierarchical parameters, a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace} is used.
 * </p>
 *
 * <p>
 * All database operations are instrumented by a {@link com.apple.foundationdb.record.provider.common.StoreTimer}, which can be hooked up to whatever logging and monitoring system a client uses.
 * </p>
 */
package com.apple.foundationdb.record;
