/*
 * RecordType.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Record type meta-data.
 *
 * A record type corresponds to a Protobuf {@link com.google.protobuf.Descriptors.Descriptor} and specifies a primary key expression and any number of secondary {@link Index}es.
 */
public class RecordType implements RecordTypeOrBuilder, RecordMetaDataProvider {
    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final String name;
    @Nonnull
    private final Descriptors.Descriptor descriptor;
    @Nonnull
    private final KeyExpression primaryKey;
    @Nonnull
    private final List<Index> indexes;
    @Nonnull
    private final List<Index> multiTypeIndexes;
    @Nullable
    private Integer sinceVersion;

    public RecordType(@Nonnull RecordMetaData metaData, @Nonnull Descriptors.Descriptor descriptor, @Nonnull KeyExpression primaryKey,
                      @Nonnull List<Index> indexes, @Nonnull List<Index> multiTypeIndexes, @Nullable Integer sinceVersion) {
        this.metaData = metaData;
        this.descriptor = descriptor;
        this.primaryKey = primaryKey;
        this.name = descriptor.getName();
        this.indexes = indexes;
        this.multiTypeIndexes = multiTypeIndexes;
        this.sinceVersion = sinceVersion;
    }

    @Override
    @Nonnull
    public String getName() {
        return name;
    }

    @Override
    @Nonnull
    public Descriptors.Descriptor getDescriptor() {
        return descriptor;
    }

    @Override
    @Nonnull
    public List<Index> getIndexes() {
        return indexes;
    }

    /**
     * The Indexes that this record type is on that also contain other record types.
     * This does not include indexes that cover all record types
     * @return a list of all indexes that include this record type along with other types.
     */
    @Override
    @Nonnull
    public List<Index> getMultiTypeIndexes() {
        return multiTypeIndexes;
    }

    @Override
    @Nonnull
    public KeyExpression getPrimaryKey() {
        return primaryKey;
    }

    @Nullable
    @Override
    public Integer getSinceVersion() {
        return sinceVersion;
    }

    /**
     * Get the meta-data of which this record type is a part.
     * @return owning meta-data
     */
    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return metaData;
    }
}
