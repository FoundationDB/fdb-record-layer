/*
 * TypeRepository.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.typing;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * A utility class that enables mapping of a structured {@link Type} into a dynamically-generated equivalent Protobuf message
 * descriptor.
 *
 * The generation cost is amortized, i.e. if two {@link Type}s are equal, then only a single Protobuf message descriptor will be
 * generated.
 */
public class TypeRepository {
    @Nonnull
    public static final TypeRepository EMPTY_SCHEMA = empty();

    @Nonnull
    private final FileDescriptorSet fileDescSet;

    @Nonnull
    private final Map<String, Descriptor> msgDescriptorMapFull = new LinkedHashMap<>();

    @Nonnull
    private final Map<String, Descriptor> msgDescriptorMapShort = new LinkedHashMap<>();

    @Nonnull
    private final Map<String, EnumDescriptor> enumDescriptorMapFull = new LinkedHashMap<>();

    @Nonnull
    private final Map<String, EnumDescriptor> enumDescriptorMapShort = new LinkedHashMap<>();

    @Nonnull
    private final Map<Type, String> typeToNameMap;

    @Nonnull
    public static TypeRepository empty() {
        FileDescriptorSet.Builder resultBuilder = FileDescriptorSet.newBuilder();
        try {
            return new TypeRepository(resultBuilder.build(), Maps.newHashMap());
        } catch (final DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates a new dynamic schema builder.
     *
     * @return the schema builder
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @SuppressWarnings("PMD.AssignmentInOperand")
    @Nonnull
    public static TypeRepository parseFrom(@Nonnull final InputStream schemaDescIn) throws DescriptorValidationException, IOException {
        try (schemaDescIn) {
            int len;
            byte[] buf = new byte[4096];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((len = schemaDescIn.read(buf)) > 0) {
                baos.write(buf, 0, len);
            }
            return parseFrom(baos.toByteArray());
        }
    }

    @Nonnull
    public static TypeRepository parseFrom(@Nonnull final byte[] schemaDescBuf) throws DescriptorValidationException, IOException {
        return new TypeRepository(FileDescriptorSet.parseFrom(schemaDescBuf), Maps.newHashMap());
    }

    /**
     * Creates a new dynamic message builder for the given message type.
     *
     * @param msgTypeName the message type name
     * @return the message builder (null if not found)
     */
    @Nullable
    public DynamicMessage.Builder newMessageBuilder(@Nonnull final String msgTypeName) {
        Descriptor msgType = getMessageDescriptor(msgTypeName);
        if (msgType == null) {
            return null;
        }
        return DynamicMessage.newBuilder(msgType);
    }

    /**
     * Creates a new dynamic message builder for the given type.
     *
     * @param type the type name
     * @return the message builder (null if not found)
     */
    @Nullable
    public DynamicMessage.Builder newMessageBuilder(@Nonnull final Type type) {
        final String msgTypeName = Preconditions.checkNotNull(typeToNameMap.get(type));
        Objects.requireNonNull(msgTypeName);
        return newMessageBuilder(msgTypeName);
    }

    /**
     * Returns the protobuf type name of the given {@link Type}.
     * @param type The type to get the name of.
     * @return The protobuf type name of the {@link Type}.
     */
    @Nonnull
    public String getProtoTypeName(@Nonnull final Type type) {
        final String typeName = Preconditions.checkNotNull(typeToNameMap.get(type));
        return Objects.requireNonNull(typeName);
    }

    /**
     * Gets the protobuf message descriptor for the given message type.
     *
     * @param msgTypeName the message type name
     * @return the message descriptor (null if not found)
     */
    @Nullable
    public Descriptor getMessageDescriptor(@Nonnull final String msgTypeName) {
        Descriptor msgType = msgDescriptorMapShort.get(msgTypeName);
        if (msgType == null) {
            msgType = msgDescriptorMapFull.get(msgTypeName);
        }
        return msgType;
    }

    /**
     * Gets the protobuf message descriptor for the given message type.
     *
     * @param type the type the caller wants to look up
     * @return the message descriptor (null if not found)
     */
    @Nullable
    public Descriptor getMessageDescriptor(@Nonnull final Type type) {
        String msgTypeName = getProtoTypeName(type);
        return getMessageDescriptor(msgTypeName);
    }

    /**
     * Gets the enum value for the given enum type and name.
     *
     * @param enumTypeName the enum type name
     * @param enumName the enum name
     * @return the enum value descriptor (null if not found)
     */
    @Nullable
    public EnumValueDescriptor getEnumValue(@Nonnull final String enumTypeName, String enumName) {
        EnumDescriptor enumType = getEnumDescriptor(enumTypeName);
        if (enumType == null) {
            return null;
        }
        return enumType.findValueByName(enumName);
    }

    /**
     * Gets the enum value for the given enum type and number.
     *
     * @param enumTypeName the enum type name
     * @param enumNumber the enum number
     * @return the enum value descriptor (null if not found)
     */
    @Nullable
    public EnumValueDescriptor getEnumValue(@Nonnull final String enumTypeName, int enumNumber) {
        EnumDescriptor enumType = getEnumDescriptor(enumTypeName);
        if (enumType == null) {
            return null;
        }
        return enumType.findValueByNumber(enumNumber);
    }

    /**
     * Gets the protobuf enum descriptor for the given enum type.
     *
     * @param enumTypeName the enum type name
     * @return the enum descriptor (null if not found)
     */
    @Nullable
    public EnumDescriptor getEnumDescriptor(@Nonnull final String enumTypeName) {
        EnumDescriptor enumType = enumDescriptorMapShort.get(enumTypeName);
        if (enumType == null) {
            enumType = enumDescriptorMapFull.get(enumTypeName);
        }
        return enumType;
    }

    /**
     * Gets the protobuf message descriptor for the given message type.
     *
     * @param type the type the caller wants to look up
     * @return the message descriptor (null if not found)
     */
    @Nullable
    public EnumDescriptor getEnumDescriptor(@Nonnull final Type type) {
        String msgTypeName = getProtoTypeName(type);
        return getEnumDescriptor(msgTypeName);
    }

    /**
     * Returns the message types registered with the schema.
     *
     * @return the set of message type names
     */
    @Nonnull
    public Set<String> getMessageTypes() {
        return new TreeSet<>(msgDescriptorMapFull.keySet());
    }

    /**
     * Returns the enum types registered with the schema.
     *
     * @return the set of enum type names
     */
    @Nonnull
    public Set<String> getEnumTypes() {
        return new TreeSet<>(enumDescriptorMapFull.keySet());
    }

    /**
     * Returns the internal file descriptor set of this schema.
     *
     * @return the file descriptor set
     */
    @Nonnull
    public FileDescriptorSet getFileDescriptorSet() {
        return fileDescSet;
    }

    /**
     * Serializes the schema.
     *
     * @return the serialized schema descriptor
     */
    @Nonnull
    public byte[] toByteArray() {
        return fileDescSet.toByteArray();
    }

    /**
     * Returns a string representation of the schema.
     *
     * @return the schema string
     */
    @Override
    public String toString() {
        Set<String> msgTypes = getMessageTypes();
        Set<String> enumTypes = getEnumTypes();
        return "types: " + msgTypes + "\nenums: " + enumTypes + "\n" + fileDescSet;
    }

    private TypeRepository(@Nonnull final FileDescriptorSet fileDescSet,
                           @Nonnull final Map<Type, String> typeToNameMap) throws DescriptorValidationException {
        this.fileDescSet = fileDescSet;
        Map<String, FileDescriptor> fileDescMap = init(fileDescSet);

        Set<String> msgDupes = new HashSet<>();
        Set<String> enumDupes = new HashSet<>();
        for (FileDescriptor fileDesc : fileDescMap.values()) {
            for (Descriptor msgType : fileDesc.getMessageTypes()) {
                addMessageType(msgType, null, msgDupes, enumDupes);
            }
            for (EnumDescriptor enumType : fileDesc.getEnumTypes()) {
                addEnumType(enumType, null, enumDupes);
            }
        }

        for (String msgName : msgDupes) {
            msgDescriptorMapShort.remove(msgName);
        }
        for (String enumName : enumDupes) {
            enumDescriptorMapShort.remove(enumName);
        }

        this.typeToNameMap = ImmutableMap.copyOf(typeToNameMap);
    }

    @SuppressWarnings("java:S3776")
    @Nonnull
    private static Map<String, FileDescriptor> init(@Nonnull final FileDescriptorSet fileDescSet) throws DescriptorValidationException {
        // check for dupes
        Set<String> allFdProtoNames = collectFileDescriptorNamesAndCheckForDupes(fileDescSet);

        // build FileDescriptors, resolve dependencies (imports) if any
        Map<String, FileDescriptor> resolvedFileDescMap = new HashMap<>();
        while (resolvedFileDescMap.size() < fileDescSet.getFileCount()) {
            for (FileDescriptorProto fdProto : fileDescSet.getFileList()) {
                if (resolvedFileDescMap.containsKey(fdProto.getName())) {
                    continue;
                }

                List<String> dependencyList = fdProto.getDependencyList();
                List<FileDescriptor> resolvedFdList = new ArrayList<>();
                for (String depName : dependencyList) {
                    if (!allFdProtoNames.contains(depName)) {
                        throw new IllegalArgumentException("cannot resolve import " + depName + " in " + fdProto.getName());
                    }
                    FileDescriptor fd = resolvedFileDescMap.get(depName);
                    if (fd != null) {
                        resolvedFdList.add(fd);
                    }
                }

                if (resolvedFdList.size() == dependencyList.size()) { // dependencies resolved
                    FileDescriptor[] fds = new FileDescriptor[resolvedFdList.size()];
                    FileDescriptor fd = FileDescriptor.buildFrom(fdProto, resolvedFdList.toArray(fds));
                    resolvedFileDescMap.put(fdProto.getName(), fd);
                }
            }
        }

        return resolvedFileDescMap;
    }

    @Nonnull
    private static String duplicateNameErrorMessage(@Nonnull String name) {
        return "duplicate name: " + name;
    }

    @Nonnull
    private static Set<String> collectFileDescriptorNamesAndCheckForDupes(@Nonnull final FileDescriptorSet fileDescSet) {
        Set<String> result = new HashSet<>();
        for (FileDescriptorProto fdProto : fileDescSet.getFileList()) {
            if (result.contains(fdProto.getName())) {
                throw new IllegalArgumentException(duplicateNameErrorMessage(fdProto.getName()));
            }
            result.add(fdProto.getName());
        }
        return result;
    }

    private void addMessageType(@Nonnull final Descriptor msgType,
                                @Nullable final String scope,
                                @Nonnull final Set<String> msgDupes,
                                @Nonnull final Set<String> enumDupes) {
        String msgTypeNameFull = msgType.getFullName();
        String msgTypeNameShort = (scope == null ? msgType.getName() : scope + "." + msgType.getName());

        if (msgDescriptorMapFull.containsKey(msgTypeNameFull)) {
            throw new IllegalArgumentException(duplicateNameErrorMessage(msgTypeNameFull));
        }
        if (msgDescriptorMapShort.containsKey(msgTypeNameShort)) {
            msgDupes.add(msgTypeNameShort);
        }

        msgDescriptorMapFull.put(msgTypeNameFull, msgType);
        msgDescriptorMapShort.put(msgTypeNameShort, msgType);

        for (Descriptor nestedType : msgType.getNestedTypes()) {
            addMessageType(nestedType, msgTypeNameShort, msgDupes, enumDupes);
        }
        for (EnumDescriptor enumType : msgType.getEnumTypes()) {
            addEnumType(enumType, msgTypeNameShort, enumDupes);
        }
    }

    private void addEnumType(@Nonnull final EnumDescriptor enumType,
                             @Nullable final String scope,
                             @Nonnull final Set<String> enumDupes) {
        String enumTypeNameFull = enumType.getFullName();
        String enumTypeNameShort = (scope == null ? enumType.getName() : scope + "." + enumType.getName());

        if (enumDescriptorMapFull.containsKey(enumTypeNameFull)) {
            throw new IllegalArgumentException(duplicateNameErrorMessage(enumTypeNameFull));
        }
        if (enumDescriptorMapShort.containsKey(enumTypeNameShort)) {
            enumDupes.add(enumTypeNameShort);
        }

        enumDescriptorMapFull.put(enumTypeNameFull, enumType);
        enumDescriptorMapShort.put(enumTypeNameShort, enumType);
    }

    /**
     * A builder that builds a {@link TypeRepository} object.
     */
    public static class Builder {
        private @Nonnull final FileDescriptorProto.Builder fileDescProtoBuilder;
        private @Nonnull final FileDescriptorSet.Builder fileDescSetBuilder;
        private @Nonnull final BiMap<Type, String> typeToNameMap;

        private Builder() {
            fileDescProtoBuilder = FileDescriptorProto.newBuilder();
            fileDescSetBuilder = FileDescriptorSet.newBuilder();
            typeToNameMap = HashBiMap.create();
        }

        @Nonnull
        public TypeRepository build() {
            FileDescriptorSet.Builder resultBuilder = FileDescriptorSet.newBuilder();
            resultBuilder.addFile(fileDescProtoBuilder.build());
            resultBuilder.mergeFrom(this.fileDescSetBuilder.build());
            try {
                return new TypeRepository(resultBuilder.build(), typeToNameMap);
            } catch (final DescriptorValidationException dve) {
                throw new IllegalStateException("validation should not fail", dve);
            }
        }

        @Nonnull
        public Builder setName(@Nonnull final String name) {
            fileDescProtoBuilder.setName(name);
            return this;
        }

        @Nonnull
        public Builder setPackage(@Nonnull final String name) {
            fileDescProtoBuilder.setPackage(name);
            return this;
        }

        @Nonnull
        public Builder addTypeIfNeeded(@Nonnull final Type type) {
            if (!typeToNameMap.containsKey(type)) {
                type.defineProtoType(this);
            }
            return this;
        }

        @Nonnull
        public Optional<String> getTypeName(@Nonnull final Type type) {
            return Optional.ofNullable(typeToNameMap.get(type));
        }

        @Nonnull
        public Optional<Type> getTypeByName(@Nonnull final String name) {
            return Optional.ofNullable(typeToNameMap.inverse().get(name));
        }

        @Nonnull
        public Builder addMessageType(@Nonnull final DescriptorProtos.DescriptorProto descriptorProto) {
            fileDescProtoBuilder.addMessageType(descriptorProto);
            return this;
        }

        @Nonnull
        public Builder addEnumType(@Nonnull final DescriptorProtos.EnumDescriptorProto enumDescriptorProto) {
            fileDescProtoBuilder.addEnumType(enumDescriptorProto);
            return this;
        }

        @Nonnull
        public Builder registerTypeToTypeNameMapping(@Nonnull final Type type, @Nonnull final String protoTypeName) {
            Verify.verify(!typeToNameMap.containsKey(type));
            typeToNameMap.put(type, protoTypeName);
            return this;
        }

        @Nonnull
        public Builder addAllTypes(@Nonnull final Collection<Type> types) {
            types.forEach(this::addTypeIfNeeded);
            return this;
        }

        @Nonnull
        public Optional<String> defineAndResolveType(@Nonnull final Type type) {
            addTypeIfNeeded(type);
            return Optional.ofNullable(typeToNameMap.get(type));
        }

        @Nonnull
        public Builder addDependency(@Nonnull final String dependency) {
            fileDescProtoBuilder.addDependency(dependency);
            return this;
        }

        @Nonnull
        public Builder addPublicDependency(@Nonnull final String dependency) {
            for (int i = 0; i < fileDescProtoBuilder.getDependencyCount(); i++) {
                if (fileDescProtoBuilder.getDependency(i).equals(dependency)) {
                    fileDescProtoBuilder.addPublicDependency(i);
                    return this;
                }
            }
            fileDescProtoBuilder.addDependency(dependency);
            fileDescProtoBuilder.addPublicDependency(fileDescProtoBuilder.getDependencyCount() - 1);
            return this;
        }

        @Nonnull
        public Builder addSchema(@Nonnull final TypeRepository schema) {
            fileDescSetBuilder.mergeFrom(schema.fileDescSet);
            return this;
        }
    }
}
