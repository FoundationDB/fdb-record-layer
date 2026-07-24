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
public final class UnnestedRecordTypeBuilder extends SyntheticRecordTypeBuilder<UnnestedRecordTypeBuilder.NestedConstituent> {
    @Nonnull
    private static final String INTERNAL_PREFIX = "__";
    @Nullable
    private RecordTypeBuilder parentTypeBuilder;

    /**
     * Builder variant of {@link UnnestedRecordTypeBuilder.NestedConstituent}.
     */
    public static class NestedConstituent extends SyntheticRecordTypeBuilder.Constituent {
        @Nullable
        private final String parentName;
        @Nonnull
        private final KeyExpression nestingExpression;

        private NestedConstituent(@Nonnull final String name, @Nonnull final RecordTypeBuilder recordType,
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
                RecordType recordType = metaData.getRecordType(getRecordType().getName());
                return new UnnestedRecordType.NestedConstituent(name, recordType, null, nestingExpression);
            } else {
                if (name.startsWith(INTERNAL_PREFIX)) {
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
     * meta-data object, use {@link RecordMetaDataBuilder#addUnnestedRecordType(String)}.
     *
     * @param name name of the type
     * @param recordTypeKey record type key for the type
     * @param metaDataBuilder meta-data object this will be defined on
     * @see RecordMetaDataBuilder#addUnnestedRecordType(String)
     */
    @API(API.Status.INTERNAL)
    public UnnestedRecordTypeBuilder(@Nonnull final String name, @Nonnull Object recordTypeKey, @Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        super(name, recordTypeKey, metaDataBuilder);
    }

    @API(API.Status.INTERNAL)
    public UnnestedRecordTypeBuilder(@Nonnull RecordMetaDataProto.UnnestedRecordType typeProto, @Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        super(typeProto.getName(), LiteralKeyExpression.fromProtoValue(typeProto.getRecordTypeKey()), metaDataBuilder);

        // Deserialize each of the constituents
        final Descriptors.FileDescriptor fileDescriptor = metaDataBuilder.getUnionDescriptor().getFile();
        for (RecordMetaDataProto.UnnestedRecordType.NestedConstituent constituentProto : typeProto.getNestedConstituentsList()) {
            final String constituentName = constituentProto.getName();
            String parentConstituent = constituentProto.getParent();
            if (parentConstituent.isEmpty()) {
                // For the parent constituent (i.e., the constituent with no parent set), there is
                if (parentTypeBuilder != null) {
                    throw new MetaDataException("duplicate parent types found in unnested record type")
                            .addLogInfo(LogMessageKeys.RECORD_TYPE, getName())
                            .addLogInfo(LogMessageKeys.CONSTITUENT, constituentName);
                }
                if (constituentProto.hasNestingExpression()) {
                    throw new MetaDataException("parent constituent should not have a nesting expression")
                            .addLogInfo(LogMessageKeys.RECORD_TYPE, getName())
                            .addLogInfo(LogMessageKeys.CONSTITUENT, constituentName);
                }
                parentTypeBuilder = metaDataBuilder.getRecordType(constituentProto.getTypeName());
                addConstituent(newConstituent(constituentName, parentTypeBuilder, null, EmptyKeyExpression.EMPTY));
            } else {
                @Nullable Descriptors.Descriptor nestedDescriptor = findDescriptorByName(fileDescriptor, constituentProto.getTypeName());
                if (nestedDescriptor == null) {
                    throw new MetaDataException("missing descriptor for nested constituent")
                            .addLogInfo(LogMessageKeys.EXPECTED, constituentProto.getTypeName())
                            .addLogInfo(LogMessageKeys.CONSTITUENT, constituentName);
                }
                NestedRecordTypeBuilder nestedTypeBuilder = new NestedRecordTypeBuilder(nestedDescriptor);
                KeyExpression nestingExpression = KeyExpression.fromProto(constituentProto.getNestingExpression());
                addConstituent(newConstituent(constituentName, nestedTypeBuilder, parentConstituent, nestingExpression));
            }
        }
    }

    @API(API.Status.INTERNAL)
    @Nullable
    public static Descriptors.Descriptor findDescriptorByName(@Nonnull Descriptors.FileDescriptor fileDescriptor, @Nonnull String fullName) {
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
    private NestedConstituent newConstituent(@Nonnull final String name, @Nonnull final RecordTypeBuilder recordType, @Nullable String parentName, @Nonnull KeyExpression nestingExpression) {
        if (name.startsWith(INTERNAL_PREFIX)) {
            throw new MetaDataException("cannot create constituent with reserved prefix \"" + INTERNAL_PREFIX + "\"")
                    .addLogInfo(LogMessageKeys.CONSTITUENT, name)
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, getName());
        }
        return new NestedConstituent(name, recordType, parentName, nestingExpression);
    }

    @Nonnull
    public NestedConstituent addParentConstituent(@Nonnull final String name, @Nonnull RecordTypeBuilder recordType) {
        if (parentTypeBuilder != null) {
            throw new MetaDataException("cannot add duplicate parent type to unnested record type")
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, getName())
                    .addLogInfo(LogMessageKeys.OLD_RECORD_TYPE, parentTypeBuilder.getName())
                    .addLogInfo(LogMessageKeys.NEW_RECORD_TYPE, recordType.getName());
        }
        if (!getConstituents().isEmpty()) {
            throw new MetaDataException("parent must be added as first constituent")
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, getName());
        }
        parentTypeBuilder = recordType;
        NestedConstituent constituent = newConstituent(name, recordType, null, EmptyKeyExpression.EMPTY);
        addConstituent(constituent);
        return constituent;
    }

    @Nonnull
    public NestedConstituent addNestedConstituent(@Nonnull final String name, @Nonnull Descriptors.Descriptor descriptor, @Nonnull String parent, @Nonnull KeyExpression nestingExpression) {
        if (getConstituents().stream().map(Constituent::getName).noneMatch(n -> n.equals(parent))) {
            throw new MetaDataException("unknown parent constituent name")
                    .addLogInfo(LogMessageKeys.EXPECTED, parent)
                    .addLogInfo(LogMessageKeys.CONSTITUENT, name)
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, getName());
        }
        NestedRecordTypeBuilder nestedRecordType = new NestedRecordTypeBuilder(descriptor);
        NestedConstituent constituent = newConstituent(name, nestedRecordType, parent, nestingExpression);
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
            if (soFar.put(built.getName(), built) != null) {
                throw new MetaDataException("duplicate constituents in unnested record type share name")
                        .addLogInfo(LogMessageKeys.CONSTITUENT, constituent.getName())
                        .addLogInfo(LogMessageKeys.RECORD_TYPE, getName());
            }
            builder.add(built);
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
        for (NestedConstituent constituent : getConstituents()) {
            if (!constituent.isParent()) {
                indexesProto.addFieldBuilder()
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                        .setName(constituent.getName())
                        .setJsonName(constituent.getName())
                        .setNumber(indexPosition++);
            }
        }
        descriptorProto.addFieldBuilder()
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(name + "." + indexesProto.getName())
                .setName(UnnestedRecordType.POSITIONS_FIELD)
                .setJsonName(UnnestedRecordType.POSITIONS_FIELD)
                .setNumber(getConstituents().size() + 1);
    }

    @Nonnull
    @Override
    public UnnestedRecordType build(@Nonnull final RecordMetaData metaData, @Nonnull final Descriptors.FileDescriptor fileDescriptor) {
        if (parentTypeBuilder == null) {
            throw new MetaDataException("unnested record type missing parent type")
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, getName());
        }
        RecordType parentType = metaData.getRecordType(parentTypeBuilder.getName());
        List<UnnestedRecordType.NestedConstituent> builtConstituents = buildConstituents(parentType);
        Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(name);
        return new UnnestedRecordType(metaData, descriptor, buildPrimaryKey(), Objects.requireNonNull(recordTypeKey),
                getIndexes(), getMultiTypeIndexes(), builtConstituents);
    }
}
