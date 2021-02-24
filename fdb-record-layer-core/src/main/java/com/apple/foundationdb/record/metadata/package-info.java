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
 * Record meta-data structures.
 *
 * <h2>Record Layer Meta-Data</h2>
 *
 * <p>
 * All operations on records are mediated by a {@link com.apple.foundationdb.record.RecordMetaData}.
 * It describes the format of persistent records, their fields and secondary indexes.
 * There is a rough correspondence between the terminology used within the Record Layer
 * and those used by relational databases:
 * </p>
 *
 * <table border="1">
 * <caption>relational correspondences</caption>
 * <tr><th>Record Layer</th><th>Relational</th></tr>
 * <tr><td>meta-data</td><td>schema</td></tr>
 * <tr><td>record type</td><td>table</td></tr>
 * <tr><td>field</td><td>column</td></tr>
 * </table>
 *
 * <p>
 * Note that this correspondence is not exact. In particular, note that a record type and a
 * table differ in that records of various types are included within the same extent. There are
 * more details about the differences between tables and record types within the
 * <a href="https://github.com/FoundationDB/fdb-record-layer/blob/main/docs/FAQ.md#are-record-types-tables">Record Layer FAQ</a>.
 * </p>
 *
 * <p>
 * The core of the meta-data is a Protobuf {@link com.google.protobuf.Descriptors.FileDescriptor}.
 * Every {@link com.apple.foundationdb.record.metadata.RecordType} corresponds to a message {@link com.google.protobuf.Descriptors.Descriptor} in the file descriptor.
 * The possible fields of the record are the fields of the message.
 * </p>
 *
 * <p>
 * A {@link com.apple.foundationdb.record.RecordMetaDataBuilder} is used to construct meta-data from a file descriptor or to restore serialized meta-data from the database.
 * A {@link com.apple.foundationdb.record.metadata.RecordTypeBuilder} is used to specify options for a record type. In particular, a record type has a primary key and any number of secondary {@link com.apple.foundationdb.record.metadata.Index}es.
 * The fields of the primary key and of an index are specified by a {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression}.
 * </p>
 */
package com.apple.foundationdb.record.metadata;
