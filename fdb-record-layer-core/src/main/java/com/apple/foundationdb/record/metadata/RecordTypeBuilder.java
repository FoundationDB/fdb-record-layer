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
@API(API.Status.UNSTABLE)
public class RecordTypeBuilder extends RecordTypeIndexesBuilder implements RecordTypeOrBuilder {
    @Nonnull
    private final Descriptors.Descriptor descriptor;
    @Nullable
    private KeyExpression primaryKey;
    @Nullable
    private Integer sinceVersion;

    public RecordTypeBuilder(@Nonnull Descriptors.Descriptor descriptor) {
        super(descriptor.getName());
        this.descriptor = descriptor;
    }

    /**
     * Copy constructor for {@code RecordTypeBuilder} that copies all fields except the descriptor.
     * @param descriptor the descriptor of the new record type
     * @param other the record type builder to copy from
     */
    public RecordTypeBuilder(@Nonnull Descriptors.Descriptor descriptor, @Nonnull RecordTypeBuilder other) {
        super(descriptor.getName(), other);
        this.descriptor = descriptor;
        this.primaryKey = other.primaryKey;
        this.sinceVersion = other.sinceVersion;
    }

    @Override
    public RecordTypeBuilder setRecordTypeKey(@Nullable Object recordTypeKey) {
        super.setRecordTypeKey(recordTypeKey);
        return this;
    }

    @Override
    @Nonnull
    public Descriptors.Descriptor getDescriptor() {
        return descriptor;
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
