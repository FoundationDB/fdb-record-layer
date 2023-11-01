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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builder for creating {@link UnnestedRecordType}s.
 */
@API(API.Status.EXPERIMENTAL)
public class UnnestedRecordTypeBuilder extends SyntheticRecordTypeBuilder<SyntheticRecordTypeBuilder.Constituent> {
    @Nonnull
    private final RecordTypeBuilder parentTypeBuilder;
    @Nonnull
    private final List<Nesting> nestings;

    /**
     * Builder version of a {@link UnnestedRecordType.Nesting}.
     */
    public class Nesting {
        @Nonnull
        private final String parent;
        @Nonnull
        private final String child;
        @Nonnull
        private final KeyExpression nestingExpression;

        public Nesting(@Nonnull String parent, @Nonnull String child, @Nonnull KeyExpression nestingExpression) {
            this.parent = parent;
            this.child = child;
            this.nestingExpression = nestingExpression;
        }

        @Nonnull
        public String getParent() {
            return parent;
        }

        @Nonnull
        public String getChild() {
            return child;
        }

        @Nonnull
        public KeyExpression getNestingExpression() {
            return nestingExpression;
        }

        public UnnestedRecordType.Nesting build(List<SyntheticRecordType.Constituent> constituents) {
            SyntheticRecordType.Constituent parentConstituent = constituents.stream()
                    .filter(c -> c.getName().equals(parent))
                    .findFirst()
                    .orElseThrow(() -> new MetaDataException("unable to find parent constituent")
                            .addLogInfo(LogMessageKeys.EXPECTED, parent)
                            .addLogInfo(LogMessageKeys.RECORD_TYPE, getName()));
            SyntheticRecordType.Constituent childConstituent = constituents.stream()
                    .filter(c -> c.getName().equals(child))
                    .findFirst()
                    .orElseThrow(() -> new MetaDataException("unable to find child constituent")
                            .addLogInfo(LogMessageKeys.EXPECTED, child)
                            .addLogInfo(LogMessageKeys.RECORD_TYPE, getName()));
            return new UnnestedRecordType.Nesting(parentConstituent, childConstituent, nestingExpression);
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
        this.nestings = new ArrayList<>();
        // Parent constituent is always present
        addConstituent(UnnestedRecordType.PARENT_CONSTITUENT, parentTypeBuilder);
    }

    @API(API.Status.INTERNAL)
    public UnnestedRecordTypeBuilder(@Nonnull RecordMetaDataProto.UnnestedRecordType typeProto, @Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        super(typeProto.getName(), LiteralKeyExpression.fromProtoValue(typeProto.getRecordTypeKey()), metaDataBuilder);

        // Parent is a bit special. Go through the constituents and attempt to find it
        String parentTypeName = null;
        for (RecordMetaDataProto.UnnestedRecordType.UnnestedConstituent constituentProto : typeProto.getUnnestedConstituentsList()) {
            if (constituentProto.getName().isEmpty()) {
                parentTypeName = constituentProto.getTypeName();
                break;
            }
        }
        if (parentTypeName == null) {
            throw new MetaDataException("no parent type found for unnested record type");
        }
        this.parentTypeBuilder = metaDataBuilder.getRecordType(parentTypeName);
        addConstituent(UnnestedRecordType.PARENT_CONSTITUENT, parentTypeBuilder);

        // Now that we've found the parent, go through again and look for the non-parents. This does involve searching
        // through the file to find descriptors by name
        final Descriptors.FileDescriptor fileDescriptor = parentTypeBuilder.getDescriptor().getFile();
        for (RecordMetaDataProto.UnnestedRecordType.UnnestedConstituent constituentProto : typeProto.getUnnestedConstituentsList()) {
            if (constituentProto.getName().isEmpty()) {
                continue;
            }
            @Nullable Descriptors.Descriptor nestedDescriptor = findDescriptorByName(fileDescriptor, constituentProto.getTypeName());
            if (nestedDescriptor == null) {
                throw new MetaDataException("missing descriptor for nested constituent")
                        .addLogInfo(LogMessageKeys.EXPECTED, constituentProto.getTypeName())
                        .addLogInfo(LogMessageKeys.DESCRIPTION, constituentProto.getName());
            }
            NestedRecordTypeBuilder nestedTypeBuilder = new NestedRecordTypeBuilder(nestedDescriptor);
            addConstituent(constituentProto.getName(), nestedTypeBuilder);
        }

        List<Nesting> foundNestings = new ArrayList<>(typeProto.getNestingsCount());
        for (RecordMetaDataProto.UnnestedRecordType.Nesting nestingProto : typeProto.getNestingsList()) {
            String parent = nestingProto.getParent().isEmpty() ? UnnestedRecordType.PARENT_CONSTITUENT : nestingProto.getParent();
            String child = nestingProto.getChild().isEmpty() ? UnnestedRecordType.PARENT_CONSTITUENT : nestingProto.getChild();
            foundNestings.add(new Nesting(parent, child, KeyExpression.fromProto(nestingProto.getNestingExpression())));
        }
        this.nestings = foundNestings;
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
    protected Constituent newConstituent(@Nonnull final String name, @Nonnull final RecordTypeBuilder recordType) {
        return new Constituent(name, recordType);
    }

    @Nonnull
    public Constituent addNestedConstituent(@Nonnull final String name, @Nonnull Descriptors.Descriptor descriptor, @Nonnull String parent,
                                            @Nonnull KeyExpression fanOutExpression) {
        if (getConstituents().stream().map(Constituent::getName).noneMatch(n -> n.equals(parent))) {
            throw new MetaDataException("unknown parent constituent name")
                    .addLogInfo(LogMessageKeys.EXPECTED, parent)
                    .addLogInfo(LogMessageKeys.CONSTITUENT, name)
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, getName());
        }
        NestedRecordTypeBuilder nestedRecordType = new NestedRecordTypeBuilder(descriptor);
        Constituent c = addConstituent(name, nestedRecordType);
        nestings.add(new Nesting(parent, name, fanOutExpression));
        return c;
    }

    private List<SyntheticRecordType.Constituent> buildConstituents(@Nonnull RecordType parentType) {
        ImmutableList.Builder<SyntheticRecordType.Constituent> builder = ImmutableList.builderWithExpectedSize(getConstituents().size());
        for (Constituent constituent : getConstituents()) {
            if (constituent.getName().equals(UnnestedRecordType.PARENT_CONSTITUENT)) {
                builder.add(new SyntheticRecordType.Constituent(UnnestedRecordType.PARENT_CONSTITUENT, parentType));
            } else {
                builder.add(new SyntheticRecordType.Constituent(constituent.getName(),
                        new NestedRecordType(parentType.getRecordMetaData(), constituent.getRecordType().getDescriptor(), parentType, Key.Expressions.field(UnnestedRecordType.POSITIONS_FIELD).nest(constituent.getName()))));
            }
        }
        return builder.build();
    }

    @Nonnull
    @Override
    protected KeyExpression buildPrimaryKey() {
        List<KeyExpression> subPrimaryKeys = new ArrayList<>(getConstituents().size());
        for (Constituent constituent : getConstituents()) {
            RecordTypeBuilder typeBuilder = constituent.getRecordType();
            if (typeBuilder instanceof NestedRecordTypeBuilder) {
                // The primary key for nested types encodes the index of the child constituent within the parent. No need to
                // do any additional nesting
                subPrimaryKeys.add(Key.Expressions.field(UnnestedRecordType.POSITIONS_FIELD).nest(constituent.getName()));
            } else {
                final KeyExpression typePrimaryKey = typeBuilder.getPrimaryKey();
                if (typePrimaryKey == null) {
                    throw new MetaDataException("constituent primary key is null")
                            .addLogInfo(LogMessageKeys.RECORD_TYPE, getName())
                            .addLogInfo(LogMessageKeys.CONSTITUENT, constituent.getName());
                }
                // Non-nested types need to have their primary key nested within their constituent name
                subPrimaryKeys.add(Key.Expressions.field(constituent.getName()).nest(typePrimaryKey));
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
        List<SyntheticRecordType.Constituent> builtConstituents = buildConstituents(parentType);
        List<UnnestedRecordType.Nesting> builtNestings = nestings.stream().map(nesting -> nesting.build(builtConstituents)).collect(Collectors.toList());
        Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(name);
        return new UnnestedRecordType(metaData, descriptor, buildPrimaryKey(), Objects.requireNonNull(recordTypeKey),
                getIndexes(), getMultiTypeIndexes(),
                builtConstituents, builtNestings);
    }
}
