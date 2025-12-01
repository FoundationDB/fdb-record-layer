/*
 * FileDescriptorSerializer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metadata.SkeletonVisitor;
import com.apple.foundationdb.relational.util.Assert;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public class FileDescriptorSerializer extends SkeletonVisitor {

    @Nonnull
    private final DescriptorProtos.FileDescriptorProto.Builder fileBuilder;

    @Nonnull
    private final DescriptorProtos.DescriptorProto.Builder unionDescriptorBuilder;

    @Nonnull
    private final Set<String> descriptorNames;

    @Nonnull
    private final Set<String> enumNames;

    // FileDescriptorSerializer operates in 2 modes. With `assignGenerations`=true, the serializer assumes that the
    // tables already have one (or more generations) and hence do not assign them generations by itself. Consequently,
    // for `assignGenerations`=false, the FileDescriptorSerializer expects that the tables have generations already
    // assigned for them and hence, do assign any generations.
    //
    // Dual-mode operation is temporary and should be removed once Relational has native support for some form of `ALTER`
    // commands that can `evolve` a table to new `generation`. In essence, we want generation assignment to happen at
    // a higher level, before the SchemaTemplate is made to serialize.
    private Boolean assignGenerations;

    private int tableCounter;

    public FileDescriptorSerializer() {
        this(DescriptorProtos.FileDescriptorProto.newBuilder());
    }

    public FileDescriptorSerializer(@Nonnull DescriptorProtos.FileDescriptorProto.Builder fileBuilder) {
        this.fileBuilder = fileBuilder;
        this.fileBuilder.addAllDependency(TypeRepository.DEPENDENCIES.stream().map(Descriptors.FileDescriptor::getFullName).collect(Collectors.toList()));
        this.unionDescriptorBuilder = DescriptorProtos.DescriptorProto.newBuilder().setName("RecordTypeUnion");
        final RecordMetaDataOptionsProto.RecordTypeOptions options = RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder().setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION).build();
        unionDescriptorBuilder.getOptionsBuilder().setExtension(RecordMetaDataOptionsProto.record, options);
        this.descriptorNames = new LinkedHashSet<>();
        this.enumNames = new LinkedHashSet<>();
        // Starts with 1 to maintain compatibility with the protobuf field number.
        this.tableCounter = 1;
    }

    @Override
    public void visit(@Nonnull Metadata metadata) {
        Assert.failUnchecked(String.format(Locale.ROOT, "unexpected call on %s", metadata.getClass().getName()));
    }

    @Override
    public void visit(@Nonnull final Table table) {
        Assert.thatUnchecked(table instanceof RecordLayerTable);
        final RecordLayerTable recordLayerTable = (RecordLayerTable) table;
        final Type.Record type = recordLayerTable.getType();
        final String typeDescriptor = registerTypeDescriptors(type);
        final Map<Integer, DescriptorProtos.FieldOptions> generations = recordLayerTable.getGenerations();

        checkTableGenerations(generations);

        int fieldCounter = 0;
        // add the table as an entry in the final 'RecordTypeUnion' entry of the record store metadata. There is one
        // field for each generation of the RecordLayerTable.
        for (Map.Entry<Integer, DescriptorProtos.FieldOptions> version : generations.entrySet()) {
            final var tableEntryInUnionDescriptor = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setNumber(version.getKey())
                    .setName(typeDescriptor + "_" + (fieldCounter++))
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(typeDescriptor)
                    .setOptions(version.getValue())
                    .build();
            unionDescriptorBuilder.addField(tableEntryInUnionDescriptor);
        }
    }

    // (yhatem) this is temporary, we use rec layer typing also as a bridge to PB serialization for now.
    @Nonnull
    private String registerTypeDescriptors(@Nonnull final Type.Record type) {
        final var builder = TypeRepository.newBuilder();
        type.defineProtoType(builder);
        final var typeDescriptors = builder.build();
        final var typeDescriptor = typeDescriptors.getMessageDescriptor(type).getName();
        for (final Descriptors.Descriptor descriptor : typeDescriptors.getMessageDescriptors()) {
            if (descriptorNames.contains(descriptor.getName())) {
                continue;
            }
            fileBuilder.addMessageType(descriptor.toProto());
            descriptorNames.add(descriptor.getName());
        }
        for (final var enumDescriptor : typeDescriptors.getEnumDescriptors()) {
            if (enumNames.contains(enumDescriptor.getName())) {
                continue;
            }
            fileBuilder.addEnumType(enumDescriptor.toProto());
            enumNames.add(enumDescriptor.getName());
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

    private void checkTableGenerations(@Nonnull Map<Integer, DescriptorProtos.FieldOptions> generations) {
        // Determine the mode by generations map of the first table.
        if (assignGenerations == null) {
            assignGenerations = generations.isEmpty();
        }
        if (assignGenerations) {
            Assert.thatUnchecked(generations.isEmpty(), "Table already has generations when serializing in assignGenerations mode.");
            generations.put(tableCounter++, DescriptorProtos.FieldOptions.newBuilder().build());
        } else {
            Assert.thatUnchecked(!generations.isEmpty(), "Table do not have generations when serializing in non-assignGenerations mode.");
        }
    }
}
