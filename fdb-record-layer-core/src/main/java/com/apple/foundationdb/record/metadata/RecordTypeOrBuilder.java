/*
 * RecordTypeOrBuilder.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Common interface implemented by the {@link RecordType} and {@link RecordTypeBuilder}
 * classes that contains accessors that they are both expected to have. This includes
 * the data necessary to know what is in a record and what indexes are associated with it.
 */
@API(API.Status.UNSTABLE)
public interface RecordTypeOrBuilder {
    /**
     * Get the name of the record type. This is the same as the name of the underlying
     * message type.
     * @return the message type name
     */
    @Nonnull
    String getName();

    /**
     * Get the descriptor of the underlying message type. This specifies what fields
     * the record will have and what their types should be.
     * @return the message type descriptor
     */
    @Nonnull
    Descriptors.Descriptor getDescriptor();

    /**
     * Get the list of indexes that are on this record and only this record. This does not include
     * indexes that are on multiple types even if one of them is on this type.
     * @return the list of indexes only on this type
     */
    @Nonnull
    List<Index> getIndexes();

    /**
     * Gets the list of indexes that are on multiple record types one of which is this type. This
     * will not include {@link Index}es that are on all record types (universal indexes) within a given
     * metadata configuration.
     * @return the list of indexes on multiple types including this type
     */
    @Nonnull
    List<Index> getMultiTypeIndexes();

    /**
     * Gets the primary key expression for this record type. Records of this type are inserted based
     * on the value of this key expression when evaluated on the record.
     * @return the primary key expression for this record type
     */
    @Nullable
    KeyExpression getPrimaryKey();

    /**
     * Gets a metadata version, which shows when this record type got introduced in the metadata. This information is
     * used to skip index rebuild for indices on new record types.
     * @return  metadata version when this type is introduced (<code>null</code> if this information is not available)
     */
    @Nullable
    Integer getSinceVersion();

    /**
     * Gets the {@link com.apple.foundationdb.tuple.Tuple} element value that will be used by {@link com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression} for this record type.
     * The value should be unique among record types and stable in the face of meta-data changes.
     * @return stable and unique key for record type
     */
    @Nullable
    Object getRecordTypeKey();
}
