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
 * Support classes that do not depend on FoundationDB.
 *
 * Although the Record Layer only works with FoundationDB, a few of its pieces do not actually need the FDB API and might
 * conceivably be used with a different backend, if there ever were one.
 *
 * <h2>Serialization</h2>
 *
 * <p>
 * The underlying database is a key-value store, mapping a byte-array key to a byte-array value.
 * Serializing a record is the process of converting it to and from the byte-array value.
 * The heavy lifting of record serialization is done by Protocol Buffers, using {@link com.google.protobuf.Message#toByteArray()} and
 * {@link com.google.protobuf.Message.Builder#mergeFrom(byte[])}.
 * This is done by the {@link com.apple.foundationdb.record.provider.common.RecordSerializer}.
 * To make records self-identifying, the record is wrapped in the <code>RecordTypeUnion</code> message, which has one field for
 * each record type.
 * </p>
 *
 * <p>
 * A serializer can also do other transformations between the raw message and the on-disk format. For example, {@link com.apple.foundationdb.record.provider.common.TransformedRecordSerializer} does
 * compression and encryption.
 * </p>
 *
 * <h2>Instrumentation</h2>
 *
 * <p>
 * A {@link com.apple.foundationdb.record.provider.common.StoreTimer} is optionally associated with each open {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext}.
 * This makes it available to an open {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore}.
 * It accumulates timing information for all the operations performed by the context on the store, classified by {@link com.apple.foundationdb.record.provider.common.StoreTimer.Event} type.
 * </p>
 */
package com.apple.foundationdb.record.provider.common;
