/*
 * SyntheticRecordTypeBuilder.java
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
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A builder for {@link SyntheticRecordType}.
 * @param <C> type of constituent record types
 */
@API(API.Status.EXPERIMENTAL)
public abstract class SyntheticRecordTypeBuilder<C extends SyntheticRecordTypeBuilder.Constituent> extends RecordTypeIndexesBuilder {
    @Nonnull
    private final RecordMetaDataBuilder metaDataBuilder;
    @Nonnull
    private final List<C> constituents = new ArrayList<>();

    /**
     * A constituent type within a joined record type.
     *
     * A constituent is a record type with an associated correlation name. The correlation name must be unique.
     * The same record type can appear more than once with different correlation names (for implementing self-joins).
     */
    public static class Constituent {
        @Nonnull
        private final String name;
        @Nonnull
        private final RecordTypeBuilder recordType;

        protected Constituent(@Nonnull String name, @Nonnull RecordTypeBuilder recordType) {
            this.name = name;
            this.recordType = recordType;
        }

        @Nonnull
        public String getName() {
            return name;
        }

        @Nonnull
        public RecordTypeBuilder getRecordType() {
            return recordType;
        }
    }

    protected SyntheticRecordTypeBuilder(@Nonnull String name, @Nonnull Object recordTypeKey, @Nonnull RecordMetaDataBuilder metaDataBuilder) {
        super(name);
        this.recordTypeKey = recordTypeKey;
        this.metaDataBuilder = metaDataBuilder;
    }

    @Override
    public SyntheticRecordTypeBuilder<C> setRecordTypeKey(@Nullable Object recordTypeKey) {
        super.setRecordTypeKey(recordTypeKey);
        return this;
    }

    /**
     * Get the constitutents of this synthetic record type.
     * @return list of constituents
     */
    @Nonnull
    public List<C> getConstituents() {
        return constituents;
    }

    @Nonnull
    protected abstract C newConstituent(@Nonnull String name, @Nonnull RecordTypeBuilder recordType);

    /**
     * Add a new constituent.
     * @param constituent the new constituent
     * @return the newly added constituent
     */
    @Nonnull
    protected C addConstituent(@Nonnull C constituent) {
        constituents.add(constituent);
        return constituent;
    }

    /**
     * Add a new constituent by name.
     * @param name the correlation name for the new constituent
     * @param recordType the record type for the new constituent
     * @return the newly added constituent
     */
    @Nonnull
    public C addConstituent(@Nonnull String name, @Nonnull RecordTypeBuilder recordType) {
        return addConstituent(newConstituent(name, recordType));
    }

    /**
     * Add a new constituent by name.
     * @param name the correlation name for the new constituent
     * @param recordType the name of the record type for the new constituent
     * @return the newly added constituent
     */
    @Nonnull
    public C addConstituent(@Nonnull String name, @Nonnull String recordType) {
        return addConstituent(name, metaDataBuilder.getRecordType(recordType));
    }

    /**
     * Add a new constituent with a correlation name that is the same as the record type name.
     * @param recordType the record type for the new constituent
     * @return the newly added constituent
     */
    @Nonnull
    public C addConstituent(@Nonnull RecordTypeBuilder recordType) {
        return addConstituent(recordType.getName(), recordType);
    }

    /**
     * Add a new constituent with a correlation name that is the same as the record type name.
     * @param constituent name of the record type for the new constituent
     * @return the newly added constituent
     */
    @Nonnull
    protected C addConstituent(@Nonnull String constituent) {
        return addConstituent(constituent, constituent);
    }

    @Nonnull
    @SuppressWarnings("squid:S1452")
    public abstract SyntheticRecordType<?> build(@Nonnull RecordMetaData metaData, @Nonnull Descriptors.FileDescriptor fileDescriptor);

    @API(API.Status.INTERNAL)
    public void buildDescriptor(@Nonnull DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProto, @Nonnull Set<Descriptors.FileDescriptor> sources) {
        final DescriptorProtos.DescriptorProto.Builder descriptorProto = fileDescriptorProto.addMessageTypeBuilder();
        descriptorProto.setName(name);
        addConstituentFields(descriptorProto, sources);
    }

    protected void addConstituentFields(@Nonnull DescriptorProtos.DescriptorProto.Builder descriptorProto, @Nonnull Set<Descriptors.FileDescriptor> sources) {
        int fieldNumber = 0;
        for (Constituent constituent : constituents) {
            descriptorProto.addFieldBuilder()
                    .setName(constituent.getName())
                    .setJsonName(constituent.getName())
                    .setNumber(++fieldNumber)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName("." + constituent.getRecordType().getDescriptor().getFullName());
            sources.add(constituent.getRecordType().getDescriptor().getFile());
        }
    }

    @Nonnull
    protected KeyExpression buildPrimaryKey() {
        // The 0th component is the synthetic record type's key, since multiple synthetic records might involve the same children.
        // The nth component is the nth constituent record's primary key, with no flattening.
        return Key.Expressions.concat(
                Key.Expressions.recordType(),
                Key.Expressions.list(constituents.stream()
                        .map(c -> Key.Expressions.field(c.getName()).nest(c.getRecordType().getPrimaryKey()))
                        .collect(Collectors.toList())));
    }

}
