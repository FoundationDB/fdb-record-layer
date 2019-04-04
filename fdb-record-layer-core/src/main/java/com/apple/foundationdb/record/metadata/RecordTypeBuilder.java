/*
 * RecordTypeBuilder.java
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A builder for {@link RecordType}.
 *
 * A record type consists of:
 * <ul>
 * <li>name.</li>
 * <li>Protobuf {@link com.google.protobuf.Descriptors.Descriptor}.</li>
 * <li>primary key expression.</li>
 * <li>optional secondary indexes.</li>
 * </ul>
 */
@API(API.Status.MAINTAINED)
public class RecordTypeBuilder implements RecordTypeOrBuilder {
    @Nonnull
    private final String name;
    @Nonnull
    private final Descriptors.Descriptor descriptor;
    @Nullable
    private KeyExpression primaryKey;
    @Nonnull
    private final List<Index> indexes;
    @Nonnull
    private final List<Index> multiTypeIndexes;
    @Nullable
    private Integer sinceVersion;
    @Nullable
    private Object recordTypeKey;

    public RecordTypeBuilder(@Nonnull Descriptors.Descriptor descriptor) {
        this.descriptor = descriptor;
        this.name = descriptor.getName();
        this.indexes = new ArrayList<>();
        this.multiTypeIndexes = new ArrayList<>();
    }

    /**
     * Copy constructor for {@code RecordTypeBuilder} that copies all fields except the descriptor.
     * @param descriptor the descriptor of the new record type
     * @param other the record type builder to copy from
     */
    public RecordTypeBuilder(@Nonnull Descriptors.Descriptor descriptor, @Nonnull RecordTypeBuilder other) {
        this.descriptor = descriptor;
        this.name = other.name;
        this.indexes = other.indexes;
        this.multiTypeIndexes = other.multiTypeIndexes;
        this.primaryKey = other.primaryKey;
        this.recordTypeKey = other.recordTypeKey;
        this.sinceVersion = other.sinceVersion;
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
     * @return a list of all indexes that include this record type along with other types.
     */
    @Override
    @Nonnull
    public List<Index> getMultiTypeIndexes() {
        return multiTypeIndexes;
    }

    @Override
    @Nullable
    public KeyExpression getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(@Nonnull KeyExpression primaryKey) {
        if (primaryKey.versionColumns() != 0) {
            throw new MetaDataException("Version in primary key not supported");
        }
        this.primaryKey = primaryKey;
    }

    @Nullable
    @Override
    public Integer getSinceVersion() {
        return sinceVersion;
    }

    public void setSinceVersion(@Nullable Integer sinceVersion) {
        this.sinceVersion = sinceVersion;
    }

    @Nullable
    @Override
    public Object getRecordTypeKey() {
        return recordTypeKey;
    }

    public RecordTypeBuilder setRecordTypeKey(@Nullable Object recordTypeKey) {
        if (!(recordTypeKey == null || recordTypeKey instanceof Number || recordTypeKey instanceof Boolean ||
                recordTypeKey instanceof String || recordTypeKey instanceof byte[])) {
            throw new MetaDataException("Only primitive types are allowed as record type key");
        }
        this.recordTypeKey = TupleTypeUtil.toTupleEquivalentValue(recordTypeKey);
        return this;
    }

    public RecordType build(@Nonnull RecordMetaData metaData) {
        if (primaryKey == null) {
            throw new NonbuildableException("Missing primary key");
        }
        return new RecordType(metaData, descriptor, primaryKey, indexes, multiTypeIndexes, sinceVersion, recordTypeKey);
    }

    /**
     * Exception thrown when a {@link RecordTypeBuilder} is not yet in a state where it can be built.
     */
    @SuppressWarnings("serial")
    public static class NonbuildableException extends IllegalStateException {
        public NonbuildableException(String s) {
            super(s);
        }
    }
}
