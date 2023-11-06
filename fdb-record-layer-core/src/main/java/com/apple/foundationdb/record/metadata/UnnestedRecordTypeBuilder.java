/*
 * UnnestedRecordTypeBuilder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Builder for creating {@link UnnestedRecordType}s.
 */
@API(API.Status.EXPERIMENTAL)
public class UnnestedRecordTypeBuilder extends SyntheticRecordTypeBuilder<UnnestedRecordTypeBuilder.NestedConstituent> {
    @Nonnull
    private final RecordTypeBuilder parentTypeBuilder;

    /**
     * Builder variant of {@link UnnestedRecordTypeBuilder.NestedConstituent}.
     */
    public static class NestedConstituent extends SyntheticRecordTypeBuilder.Constituent {
        @Nullable
        private final String parentName;
        @Nonnull
        private final KeyExpression nestingExpression;

        protected NestedConstituent(@Nonnull final String name, @Nonnull final RecordTypeBuilder recordType,
                                    @Nullable final String parentName, @Nonnull KeyExpression nestingExpression) {
            super(name, recordType);
            this.parentName = parentName;
            this.nestingExpression = nestingExpression;
        }

        @Nullable
        public String getParentName() {
            return parentName;
        }

        @Nonnull
        public KeyExpression getNestingExpression() {
            return nestingExpression;
        }

        public boolean isParent() {
            return parentName == null;
        }

        @Nonnull
        UnnestedRecordType.NestedConstituent build(@Nonnull final RecordMetaData metaData,
                                                   @Nonnull final Map<String, UnnestedRecordType.NestedConstituent> soFar) {
            final String name = getName();
            if (isParent()) {
                if (!UnnestedRecordType.PARENT_CONSTITUENT.equals(name)) {
                    throw new MetaDataException("unnexpected name for parent constituent")
                            .addLogInfo(LogMessageKeys.CONSTITUENT, name);
                }
                RecordType recordType = metaData.getRecordType(getRecordType().getName());
                return new UnnestedRecordType.NestedConstituent(name, recordType, null, nestingExpression);
            } else {
                if (UnnestedRecordType.PARENT_CONSTITUENT.equals(name) || UnnestedRecordType.POSITIONS_FIELD.equals(name)) {
                    throw new MetaDataException("reserved name cannot be used for constituent")
                            .addLogInfo(LogMessageKeys.CONSTITUENT, name);
                }
                if (soFar.containsKey(name)) {
                    throw new MetaDataException("duplicate constituent name")
                            .addLogInfo(LogMessageKeys.CONSTITUENT, name);
                }
                UnnestedRecordType.NestedConstituent parent = soFar.get(parentName);
                if (parent == null) {
                    throw new MetaDataException("missing parent constituent for nested constituent")
                            .addLogInfo(LogMessageKeys.CONSTITUENT, name)
                            .addLogInfo(LogMessageKeys.EXPECTED, parentName);
                }
                final NestedRecordType recordType = new NestedRecordType(
                        metaData,
                        getRecordType().getDescriptor(),
                        parent.getRecordType(),
                        Key.Expressions.field(UnnestedRecordType.POSITIONS_FIELD).nest(name));
                return new UnnestedRecordType.NestedConstituent(name, recordType, parent, nestingExpression);
            }
        }
    }

    /**
     * Internal constructor for creating {@link UnnestedRecordTypeBuilder}s. To create a type and add it to a
     * meta-data object, use {@link RecordMetaDataBuilder#addUnnestedRecordType(String, String)}.
     *
     * @param name name of the type
     * @param recordTypeKey record type key for the type
     * @param metaDataBuilder meta-data object this will be defined on
     * @param parentTypeBuilder the stored record type that other constituents will be extracted from
     * @see RecordMetaDataBuilder#addUnnestedRecordType(String, String)
     */
    @API(API.Status.INTERNAL)
    public UnnestedRecordTypeBuilder(@Nonnull final String name, @Nonnull Object recordTypeKey, @Nonnull final RecordMetaDataBuilder metaDataBuilder, @Nonnull RecordTypeBuilder parentTypeBuilder) {
        super(name, recordTypeKey, metaDataBuilder);
        this.parentTypeBuilder = parentTypeBuilder;
        // Parent constituent is always present
        addConstituent(new NestedConstituent(UnnestedRecordType.PARENT_CONSTITUENT, parentTypeBuilder, null, EmptyKeyExpression.EMPTY));
    }

    @API(API.Status.INTERNAL)
    public UnnestedRecordTypeBuilder(@Nonnull RecordMetaDataProto.UnnestedRecordType typeProto, @Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        super(typeProto.getName(), LiteralKeyExpression.fromProtoValue(typeProto.getRecordTypeKey()), metaDataBuilder);

        String parentTypeName = typeProto.getParentTypeName();
        this.parentTypeBuilder = metaDataBuilder.getRecordType(parentTypeName);
        addConstituent(new NestedConstituent(UnnestedRecordType.PARENT_CONSTITUENT, parentTypeBuilder, null, EmptyKeyExpression.EMPTY));

        // Now that we've found the parent, go through again and look for the non-parents. This does involve searching
        // through the file to find descriptors by name
        final Descriptors.FileDescriptor fileDescriptor = parentTypeBuilder.getDescriptor().getFile();
        for (RecordMetaDataProto.UnnestedRecordType.NestedConstituent constituentProto : typeProto.getNestedConstituentsList()) {
            @Nullable Descriptors.Descriptor nestedDescriptor = findDescriptorByName(fileDescriptor, constituentProto.getTypeName());
            if (nestedDescriptor == null) {
                throw new MetaDataException("missing descriptor for nested constituent")
                        .addLogInfo(LogMessageKeys.EXPECTED, constituentProto.getTypeName())
                        .addLogInfo(LogMessageKeys.DESCRIPTION, constituentProto.getName());
            }
            NestedRecordTypeBuilder nestedTypeBuilder = new NestedRecordTypeBuilder(nestedDescriptor);
            KeyExpression nestingExpression = KeyExpression.fromProto(constituentProto.getNestingExpression());
            addConstituent(new NestedConstituent(constituentProto.getName(), nestedTypeBuilder, constituentProto.getParent(), nestingExpression));
        }
    }

    @Nullable
    private static Descriptors.Descriptor findDescriptorByName(@Nonnull Descriptors.FileDescriptor fileDescriptor, @Nonnull String fullName) {
        // Use the seen set to protect against circular dependencies. Files should be added to the set right before they are searched through, so
        // each file will be visited at most once
        Set<Descriptors.FileDescriptor> seen = new HashSet<>();
        seen.add(fileDescriptor);
        return findDescriptorByName(fileDescriptor, fullName, seen);
    }

    @Nullable
    private static Descriptors.Descriptor findDescriptorByName(@Nonnull Descriptors.FileDescriptor fileDescriptor, @Nonnull String fullName, @Nonnull Set<Descriptors.FileDescriptor> seen) {
        if (fullName.startsWith(fileDescriptor.getPackage())) {
            for (Descriptors.Descriptor desc : fileDescriptor.getMessageTypes()) {
                Descriptors.Descriptor found = findDescriptorByName(desc, fullName);
                if (found != null) {
                    return found;
                }
            }
        }
        for (Descriptors.FileDescriptor dependency : fileDescriptor.getDependencies()) {
            if (seen.add(dependency)) {
                Descriptors.Descriptor found = findDescriptorByName(dependency, fullName, seen);
                if (found != null) {
                    return found;
                }
            }
        }
        for (Descriptors.FileDescriptor publicDependency : fileDescriptor.getPublicDependencies()) {
            if (seen.add(publicDependency)) {
                Descriptors.Descriptor found = findDescriptorByName(publicDependency, fullName, seen);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    @Nullable
    private static Descriptors.Descriptor findDescriptorByName(@Nonnull Descriptors.Descriptor descriptor, @Nonnull String fullName) {
        if (fullName.startsWith(descriptor.getFullName())) {
            if (descriptor.getFullName().equals(fullName)) {
                return descriptor;
            }
            for (Descriptors.Descriptor nestedType : descriptor.getNestedTypes()) {
                Descriptors.Descriptor foundNested = findDescriptorByName(nestedType, fullName);
                if (foundNested != null) {
                    return foundNested;
                }
            }
        }
        return null;
    }

    @Nonnull
    @Override
    protected NestedConstituent newConstituent(@Nonnull final String name, @Nonnull final RecordTypeBuilder recordType) {
        throw new RecordCoreException("unimplemented");
    }

    @Nonnull
    public NestedConstituent addNestedConstituent(@Nonnull final String name, @Nonnull Descriptors.Descriptor descriptor, @Nonnull String parent,
                                                  @Nonnull KeyExpression fanOutExpression) {
        if (getConstituents().stream().map(Constituent::getName).noneMatch(n -> n.equals(parent))) {
            throw new MetaDataException("unknown parent constituent name")
                    .addLogInfo(LogMessageKeys.EXPECTED, parent)
                    .addLogInfo(LogMessageKeys.CONSTITUENT, name)
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, getName());
        }
        NestedRecordTypeBuilder nestedRecordType = new NestedRecordTypeBuilder(descriptor);
        NestedConstituent constituent = new NestedConstituent(name, nestedRecordType, parent, fanOutExpression);
        addConstituent(constituent);
        return constituent;
    }

    @Nonnull
    private List<UnnestedRecordType.NestedConstituent> buildConstituents(@Nonnull RecordType parentType) {
        ImmutableList.Builder<UnnestedRecordType.NestedConstituent> builder = ImmutableList.builderWithExpectedSize(getConstituents().size());
        Map<String, UnnestedRecordType.NestedConstituent> soFar = Maps.newHashMapWithExpectedSize(getConstituents().size());
        RecordMetaData metaData = parentType.getRecordMetaData();
        for (NestedConstituent constituent : getConstituents()) {
            UnnestedRecordType.NestedConstituent built = constituent.build(metaData, soFar);
            builder.add(built);
            soFar.put(built.getName(), built);
        }
        return builder.build();
    }

    @Nonnull
    @Override
    protected KeyExpression buildPrimaryKey() {
        List<KeyExpression> subPrimaryKeys = new ArrayList<>(getConstituents().size());
        for (NestedConstituent constituent : getConstituents()) {
            RecordTypeBuilder typeBuilder = constituent.getRecordType();
            if (constituent.isParent()) {
                final KeyExpression typePrimaryKey = typeBuilder.getPrimaryKey();
                if (typePrimaryKey == null) {
                    throw new MetaDataException("constituent primary key is null")
                            .addLogInfo(LogMessageKeys.RECORD_TYPE, getName())
                            .addLogInfo(LogMessageKeys.CONSTITUENT, constituent.getName());
                }
                // Non-nested types need to have their primary key nested within their constituent name
                subPrimaryKeys.add(Key.Expressions.field(constituent.getName()).nest(typePrimaryKey));
            } else {
                // The primary key for nested types encodes the index of the child constituent within the parent.
                // This data is accessible from the positions field on the synthetic type
                subPrimaryKeys.add(Key.Expressions.field(UnnestedRecordType.POSITIONS_FIELD).nest(constituent.getName()));
            }
        }
        return Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.list(subPrimaryKeys));
    }

    @Override
    public void buildDescriptor(@Nonnull final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProto, @Nonnull final Set<Descriptors.FileDescriptor> sources) {
        final DescriptorProtos.DescriptorProto.Builder descriptorProto = fileDescriptorProto.addMessageTypeBuilder();
        descriptorProto.setName(name);
        addConstituentFields(descriptorProto, sources);

        final DescriptorProtos.DescriptorProto.Builder indexesProto = descriptorProto.addNestedTypeBuilder()
                .setName("Positions");
        int indexPosition = 1;
        for (Constituent constituent : getConstituents()) {
            if (!constituent.getName().equals(UnnestedRecordType.PARENT_CONSTITUENT)) {
                indexesProto.addFieldBuilder()
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                        .setName(constituent.getName())
                        .setNumber(indexPosition++);
            }
        }
        descriptorProto.addFieldBuilder()
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(name + "." + indexesProto.getName())
                .setName(UnnestedRecordType.POSITIONS_FIELD)
                .setNumber(getConstituents().size() + 1);
    }

    @Nonnull
    @Override
    public UnnestedRecordType build(@Nonnull final RecordMetaData metaData, @Nonnull final Descriptors.FileDescriptor fileDescriptor) {
        RecordType parentType = metaData.getRecordType(parentTypeBuilder.getName());
        List<UnnestedRecordType.NestedConstituent> builtConstituents = buildConstituents(parentType);
        Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(name);
        return new UnnestedRecordType(metaData, descriptor, buildPrimaryKey(), Objects.requireNonNull(recordTypeKey),
                getIndexes(), getMultiTypeIndexes(), builtConstituents);
    }
}
