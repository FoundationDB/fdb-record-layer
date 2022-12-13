/*
 * FileDescriptorSerializer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.metadata.serde;

import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metadata.SkeletonVisitor;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.google.protobuf.DescriptorProtos;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.Set;

public class FileDescriptorSerializer extends SkeletonVisitor {

    @Nonnull
    private final DescriptorProtos.FileDescriptorProto.Builder fileBuilder;

    @Nonnull
    private final DescriptorProtos.DescriptorProto.Builder unionDescriptorBuilder;

    @Nonnull
    private final Set<String> descriptorNames;

    private int tableCounter;

    public FileDescriptorSerializer() {
        this(DescriptorProtos.FileDescriptorProto.newBuilder());
    }

    public FileDescriptorSerializer(@Nonnull DescriptorProtos.FileDescriptorProto.Builder fileBuilder) {
        this.fileBuilder = fileBuilder;
        this.unionDescriptorBuilder = DescriptorProtos.DescriptorProto.newBuilder().setName("RecordTypeUnion");
        final RecordMetaDataOptionsProto.RecordTypeOptions options = RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder().setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION).build();
        unionDescriptorBuilder.getOptionsBuilder().setExtension(RecordMetaDataOptionsProto.record, options);
        this.descriptorNames = new LinkedHashSet<>();
        this.tableCounter = 0;
    }

    @Override
    public void visit(@Nonnull Metadata metadata) {
        Assert.failUnchecked(String.format("unexpected call on %s", metadata.getClass().getName()));
    }

    @Override
    public void visit(@Nonnull final Table table) {
        Assert.thatUnchecked(table instanceof RecordLayerTable);
        final var recordLayerTable = (RecordLayerTable)table;
        final var type = recordLayerTable.getType();
        final var typeDescriptor = registerTypeDescriptors(type);

        // add the table as an entry in the final 'RecordTypeUnion' entry of the record store metadata.
        final var tableEntryInUnionDescriptor = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setNumber(++tableCounter)
                .setName(recordLayerTable.getName())
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(typeDescriptor)
                .build();
        unionDescriptorBuilder.addField(tableEntryInUnionDescriptor);
    }

    // (yhatem) this is temporary, we use rec layer typing also as a bridge to PB serialization for now.
    @Nonnull
    private String registerTypeDescriptors(@Nonnull final Type.Record type) {
        final var builder = TypeRepository.newBuilder();
        type.defineProtoType(builder);
        final var typeDescriptors = builder.build();
        final var typeDescriptor = typeDescriptors.getMessageDescriptor(type).getName();
        for (final var descriptorName : typeDescriptors.getMessageTypes()) {
            if (descriptorNames.contains(descriptorName)) {
                continue;
            }
            final var descriptor = typeDescriptors.getMessageDescriptor(descriptorName);
            fileBuilder.addMessageType(descriptor.toProto());
            descriptorNames.add(descriptorName);
        }
        for (final var enumName : typeDescriptors.getEnumTypes()) {
            fileBuilder.addEnumType(typeDescriptors.getEnumDescriptor(enumName).toProto());
        }
        return typeDescriptor;
    }

    @Override
    public void startVisit(@Nonnull SchemaTemplate schemaTemplate) {
        fileBuilder.setName(schemaTemplate.getName());
    }

    @Override
    public void finishVisit(@Nonnull SchemaTemplate schemaTemplate) {
        finish();
    }

    private void finish() {
        final var unionDescriptor = unionDescriptorBuilder.build();
        fileBuilder.addMessageType(unionDescriptor);
    }

    @Nonnull
    public DescriptorProtos.FileDescriptorProto.Builder getFileBuilder() {
        return fileBuilder;
    }
}
