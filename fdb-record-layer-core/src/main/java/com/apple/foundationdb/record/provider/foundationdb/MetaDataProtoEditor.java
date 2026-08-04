/*
 * MetaDataProtoEditor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto.RecordTypeOptions;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.apple.foundationdb.record.RecordMetaDataBuilder.DEFAULT_UNION_NAME;

/**
 * A utility class for mutating the metadata proto.
 *
 * <p>This class provides utility methods for modifying a serialized metadata; for example, adding a new record type to
 * the metadata. One example of where these methods can be useful is {@link FDBMetaDataStore#mutateMetaData}. That
 * method modifies the stored metadata using a mutation callback and saves it back to the metadata store.
 */
@API(API.Status.EXPERIMENTAL)
public class MetaDataProtoEditor {
    /**
     * Add a new record type to the metadata.
     *
     * <p>Adding the record type involves three steps: the message type is added to the file descriptor's list of
     * message types, a field of the given type is added to the union, and its primary key is set. Note that adding
     * {@code UNION} record types is not allowed. To add {@code NESTED} record types, use {@link #addNestedRecordType}.
     *
     * @param metaDataBuilder the metadata builder
     * @param newRecordType the new record type
     * @param primaryKey the primary key of the new record type
     */
    public static void addRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                     @Nonnull DescriptorProtos.DescriptorProto newRecordType,
                                     @Nonnull KeyExpression primaryKey) {
        RecordTypeOptions.Usage newRecordTypeUsage = getMessageTypeUsage(newRecordType);
        if (DEFAULT_UNION_NAME.equals(newRecordType.getName()) ||
                newRecordTypeUsage == RecordTypeOptions.Usage.UNION) {
            throw new MetaDataException("Adding UNION record type not allowed");
        }
        if (newRecordTypeUsage == RecordTypeOptions.Usage.NESTED) {
            throw new MetaDataException("Use addNestedRecordType for adding NESTED record types");
        }
        if (findMessageTypeByName(metaDataBuilder.getRecordsBuilder(), newRecordType.getName()) != null) {
            throw new MetaDataException("Record type " + newRecordType.getName() + " already exists");
        }
        DescriptorProtos.FileDescriptorProto.Builder recordsBuilder = metaDataBuilder.getRecordsBuilder();
        recordsBuilder.addMessageType(newRecordType);
        metaDataBuilder.setVersion(metaDataBuilder.getVersion() + 1);
        metaDataBuilder.addRecordTypes(RecordMetaDataProto.RecordType.newBuilder()
                .setName(newRecordType.getName())
                .setPrimaryKey(primaryKey.toKeyExpression())
                .setSinceVersion(metaDataBuilder.getVersion())
                .build());
        addFieldToUnion(fetchUnionBuilder(recordsBuilder), recordsBuilder, newRecordType.getName());
    }

    /**
     * Returns the canonical union field name for a record type.
     *
     * @return {@code "_" + recordTypeName}
     */
    @Nonnull
    private static String canonicalUnionFieldName(@Nonnull String recordTypeName) {
        return "_" + recordTypeName;
    }

    /**
     * Returns whether the name of a union field is the canonical {@code _recordTypeName} form.
     */
    private static boolean isCanonicalUnionFieldName(@Nonnull String unionFieldName, @Nonnull String recordTypeName) {
        return unionFieldName.length() == recordTypeName.length() + 1
                && unionFieldName.charAt(0) == '_'
                && unionFieldName.regionMatches(1, recordTypeName, 0, recordTypeName.length());
    }

    private static void addFieldToUnion(@Nonnull DescriptorProtos.DescriptorProto.Builder unionBuilder,
                                        @Nonnull DescriptorProtos.FileDescriptorProtoOrBuilder fileBuilder,
                                        @Nonnull String typeName) {
        if (unionBuilder.getOneofDeclCount() > 0) {
            throw new MetaDataException("Adding record type to oneof is not allowed");
        }
        DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(fullyQualifiedTypeName(fileBuilder, typeName))
                .setName(canonicalUnionFieldName(typeName))
                .setNumber(assignFieldNumber(unionBuilder));
        unionBuilder.addField(fieldBuilder);
    }

    /**
     * Returns the names of the top-level record types declared in the metadata.
     */
    @Nonnull
    public static List<String> getRecordTypes(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder) {
        return metaDataBuilder.getRecordTypesList().stream().map(RecordMetaDataProto.RecordType::getName).toList();
    }

    /**
     * Returns the top-level message type descriptor with the given name, throwing if it is not found.
     */
    @Nonnull
    private static Descriptors.Descriptor getMessageTypeByName(Descriptors.FileDescriptor fileDescriptor, String name) {
        final Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(name);
        if (descriptor == null) {
            throw new MetaDataException("Could not find descriptor")
                    .addLogInfo(LogMessageKeys.NAME, name);
        }
        return descriptor;
    }

    /**
     * Returns the builder for the top-level message type with the given name, or {@code null} if none is found.
     */
    @Nullable
    private static DescriptorProtos.DescriptorProto.Builder findMessageTypeByName(
            @Nonnull DescriptorProtos.FileDescriptorProto.Builder recordsBuilder,
            @Nonnull String recordType) {
        return recordsBuilder.getMessageTypeBuilderList().stream()
                .filter(m -> m.getName().equals(recordType))
                .findAny()
                .orElse(null);
    }

    @Nonnull
    private static DescriptorProtos.DescriptorProto.Builder fetchUnionBuilder(
            @Nonnull DescriptorProtos.FileDescriptorProto.Builder fileBuilder) {
        for (DescriptorProtos.DescriptorProto.Builder messageTypeBuilder : fileBuilder.getMessageTypeBuilderList()) {
            if (isUnion(messageTypeBuilder)) {
                return messageTypeBuilder;
            }
        }
        throw new MetaDataException("Union descriptor not found");
    }

    @Nullable
    private static DescriptorProtos.FieldDescriptorProto.Builder fetchUnionFieldBuilder(
            @Nonnull DescriptorProtos.FileDescriptorProto.Builder recordsBuilder,
            @Nonnull DescriptorProtos.DescriptorProto.Builder unionBuilder,
            @Nonnull Descriptors.Descriptor unionDescriptor,
            @Nonnull String recordTypeName) {
        DescriptorProtos.FieldDescriptorProto.Builder foundField = null;
        if (recordTypeName.contains(".")) {
            throw new MetaDataException("Record Type Name cannot contain '.'");
        }
        if (unionBuilder.getNestedTypeCount() > 0) {
            throw new MetaDataException("Nested types in union type not supported");
        }
        for (DescriptorProtos.FieldDescriptorProto.Builder unionField : unionBuilder.getFieldBuilderList()) {
            final FieldTypeMatch unionFieldMatch = fieldIsType(recordsBuilder, unionDescriptor, unionField, recordTypeName);
            if (FieldTypeMatch.MATCHES.equals(unionFieldMatch)
                    && (foundField == null
                                || isCanonicalUnionFieldName(unionField.getName(), recordTypeName)
                                || unionField.getNumber() > foundField.getNumber())) {
                // Choose the matching field with either a matching name or the highest number.
                foundField = unionField;
            }
        }
        return foundField;
    }

    /**
     * Returns the declared {@code usage} of a message type, or {@code UNSET} if none is declared.
     */
    @Nonnull
    private static RecordTypeOptions.Usage getMessageTypeUsage(
            @Nonnull DescriptorProtos.DescriptorProtoOrBuilder messageType) {
        return messageType.getOptions().getExtension(RecordMetaDataOptionsProto.record).getUsage();
    }

    /**
     * Sets the usage of a message type, preserving any other options already set on the {@code record} extension.
     */
    private static void setMessageTypeUsage(@Nonnull DescriptorProtos.DescriptorProto.Builder messageTypeBuilder,
                                            @Nonnull RecordTypeOptions.Usage usage) {
        RecordTypeOptions.Builder recordOptionsBuilder =
                messageTypeBuilder.getOptions().hasExtension(RecordMetaDataOptionsProto.record)
                ? messageTypeBuilder.getOptionsBuilder().getExtension(RecordMetaDataOptionsProto.record).toBuilder()
                : RecordTypeOptions.newBuilder();
        recordOptionsBuilder.setUsage(usage);
        messageTypeBuilder.getOptionsBuilder().setExtension(
                RecordMetaDataOptionsProto.record,
                recordOptionsBuilder.build());
    }

    private static boolean isUnion(@Nonnull DescriptorProtos.DescriptorProtoOrBuilder messageType) {
        return DEFAULT_UNION_NAME.equals(messageType.getName())
                || getMessageTypeUsage(messageType) == RecordTypeOptions.Usage.UNION;
    }

    private static boolean isUnion(@Nonnull Descriptors.Descriptor messageType) {
        return DEFAULT_UNION_NAME.equals(messageType.getName())
                || getMessageTypeUsage(messageType.toProto()) == RecordTypeOptions.Usage.UNION;
    }

    @Nonnull
    private static String fullyQualifiedTypeName(@Nonnull String namespace, @Nonnull String typeName) {
        if (typeName.startsWith(".")) {
            return typeName;
        } else if (!namespace.isEmpty()) {
            return "." + namespace + "." + typeName;
        } else {
            return "." + typeName;
        }
    }

    @Nonnull
    private static String fullyQualifiedTypeName(@Nonnull DescriptorProtos.FileDescriptorProtoOrBuilder file,
                                                 @Nonnull String typeName) {
        return fullyQualifiedTypeName(file.getPackage(), typeName);
    }

    @VisibleForTesting
    enum FieldTypeMatch {
        /**
         * The field definitely does not have the type requested.
         */
        DOES_NOT_MATCH,
        /**
         * The field definitely does have the type requested.
         */
        MATCHES,
        /**
         * The field is definitely a nested type defined within the type requested. For example, the requested type
         * might be an {@code OuterMessage} and the field an {@code OuterMessage.InnerMessage}.
         */
        MATCHES_AS_NESTED
    }

    /**
     * Returns the fully-qualified name of the message type referenced by {@code field}, resolved against
     * {@code messageDescriptor} by field number rather than name or position, since the number is the only
     * identifier guaranteed to tie a mutable builder field to its resolved descriptor counterpart. Returns
     * {@code null} if the field does not reference a message type.
     */
    @Nullable
    private static String resolveFieldMessageTypeFullName(
            @Nonnull Descriptors.Descriptor messageDescriptor,
            @Nonnull DescriptorProtos.FieldDescriptorProtoOrBuilder field) {
        final Descriptors.FieldDescriptor resolvedField = Objects.requireNonNull(
                messageDescriptor.findFieldByNumber(field.getNumber()),
                "Could not find field from protobuf in descriptor");
        final Descriptors.Descriptor messageType = resolvedField.getMessageType();
        return messageType == null ? null : "." + messageType.getFullName();
    }

    /**
     * Determine if a field has a given type.
     * At the moment, this only works if (1) the field type name is fully qualified or (2) the field type is
     * fully <em>unqualified</em>. In particular, Protobuf allows the user to do things like if the
     * package name is {@code x.y.z}, to specify a record type {@code Foo} in that package as
     * {@code Foo}, {@code z.Foo}, {@code y.z.Foo}, {@code x.y.z.Foo}, or {@code .x.y.z.Foo}.
     * But that also means that if one is in package {@code x.y.z} and one sees a type specified as
     * {@code y.z.Foo}, then this could refer to: {@code .x.y.z.y.z.Foo}, {@code .x.y.y.z.Foo},
     * {@code .x.y.z.Foo}, or {@code .y.z.Foo}. Actually knowing which one is being referred to properly
     * requires knowing which types are actually defined and then traversing the namespace tree.
     *
     * <p>This can get even worse with nested types. For example, within a record {@code Foo}, if it has
     * a nested type {@code Bar}, a field with type {@code Foo} might be referring to either
     * the other {@code Foo} record or an additional type {@code Foo.Bar.Foo}.
     *
     * <p>Because getting that right is difficult and requires full knowledge of all defined types, this
     * instead takes a simpler approach where if it can be determined for sure that the type is the
     * same, it returns that the type {@link FieldTypeMatch#MATCHES}. If it can be determined that the
     * type is definitely different, then this returns that it {@link FieldTypeMatch#DOES_NOT_MATCH}.
     *
     * <p>It is also possible that the field matches (or might match) a nested type defined within the
     * given type. In that case, this can return that it matches (or might match) as a nested type.
     * This is useful for determining whether the type needs to be renamed, for example.
     *
     * @param field the field descriptor to check the type of
     * @param fullTypeName the fully-qualified type name
     *
     * @return whether the field matches or might match the given type
     */
    @Nonnull
    private static FieldTypeMatch fieldIsType(@Nonnull Descriptors.Descriptor messageDescriptor,
                                              @Nonnull DescriptorProtos.FieldDescriptorProtoOrBuilder field,
                                              @Nonnull String fullTypeName) {
        // Protobuf type name resolution is moderately complicated. Rather than trying to re-implement it on protobufs,
        // we require that the actual Descriptor be passed in so that we can work on fully qualified type names, which
        // is much, much easier, and less likely to have a bug.
        if (field.hasTypeName() && !field.getTypeName().isEmpty()) {
            final String fullyQualifiedName = resolveFieldMessageTypeFullName(messageDescriptor, field);
            if (fullyQualifiedName == null) {
                return FieldTypeMatch.DOES_NOT_MATCH;
            } else if (fullyQualifiedName.equals(fullTypeName)) {
                return FieldTypeMatch.MATCHES;
            } else if (fullyQualifiedName.startsWith(fullTypeName) && fullyQualifiedName.charAt(fullTypeName.length()) == '.') {
                return FieldTypeMatch.MATCHES_AS_NESTED;
            } else {
                return FieldTypeMatch.DOES_NOT_MATCH;
            }
        } else {
            return FieldTypeMatch.DOES_NOT_MATCH;
        }
    }

    @VisibleForTesting
    @Nonnull
    static FieldTypeMatch fieldIsType(@Nonnull DescriptorProtos.FileDescriptorProtoOrBuilder file,
                                      @Nonnull Descriptors.Descriptor descriptorForMessage,
                                      @Nonnull DescriptorProtos.FieldDescriptorProtoOrBuilder field,
                                      @Nonnull String typeName) {
        return fieldIsType(descriptorForMessage, field, fullyQualifiedTypeName(file, typeName));
    }

    private static int assignFieldNumber(@Nonnull DescriptorProtos.DescriptorProto.Builder messageType) {
        if (messageType.getFieldCount() == 0) {
            return 1;
        }
        return messageType.getFieldList().stream()
                .mapToInt(DescriptorProtos.FieldDescriptorProto::getNumber)
                .max()
                .orElseThrow()
                + 1;
    }

    /**
     * Add a new {@code NESTED} record type to the metadata. This can be used to define fields in other record types,
     * but it does not add the new record type to the union.
     *
     * @param metaDataBuilder the metadata builder
     * @param newRecordType the new record type
     */
    public static void addNestedRecordType(
            @Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
            @Nonnull DescriptorProtos.DescriptorProto newRecordType) {
        RecordTypeOptions.Usage newRecordTypeUsage = getMessageTypeUsage(newRecordType);
        if (newRecordTypeUsage != RecordTypeOptions.Usage.NESTED &&
                newRecordTypeUsage != RecordTypeOptions.Usage.UNSET) {
            throw new MetaDataException("Record type is not NESTED");
        }
        if (findMessageTypeByName(metaDataBuilder.getRecordsBuilder(), newRecordType.getName()) != null) {
            throw new MetaDataException("Record type " + newRecordType.getName() + " already exists");
        }
        metaDataBuilder.getRecordsBuilder().addMessageType(newRecordType);
    }

    /**
     * Deprecate a record type from the metadata. The record is still defined in the record definition, but any
     * occurrences
     * of the field in the union descriptor are deprecated. If there are any top-level record types that are defined
     * as nested messages within the deprecated record type, those fields in the union will also be deprecated.
     *
     * @param metaDataBuilder the metadata builder
     * @param recordType the record type to be deprecated
     */
    public static void deprecateRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                           @Nonnull String recordType,
                                           @Nonnull Descriptors.FileDescriptor[] dependencies) {
        final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = metaDataBuilder.getRecordsBuilder();
        DescriptorProtos.DescriptorProto.Builder unionBuilder = fetchUnionBuilder(fileBuilder);
        if (unionBuilder.getName().equals(recordType)) {
            throw new MetaDataException("Cannot deprecate the union");
        }
        final Descriptors.FileDescriptor fileDescriptor = RecordMetaDataBuilder.buildFileDescriptor(
                metaDataBuilder.getRecords(), dependencies);
        final Descriptors.Descriptor unionDescriptor = fileDescriptor.findMessageTypeByName(unionBuilder.getName());
        // deprecate all fields of type recordType from the union.
        boolean found = false;
        for (DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder : unionBuilder.getFieldBuilderList()) {
            final FieldTypeMatch fieldTypeMatch = fieldIsType(fileBuilder, unionDescriptor, fieldBuilder, recordType);
            if (FieldTypeMatch.MATCHES.equals(fieldTypeMatch) || FieldTypeMatch.MATCHES_AS_NESTED.equals(fieldTypeMatch)) {
                setDeprecated(fieldBuilder);
                found = true;
            }
        }
        if (!found) {
            throw new MetaDataException("Record type " + recordType + " not found");
        }
    }

    public static void renameRecordTypes(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                         @Nonnull Function<String, String> renamer,
                                         @Nonnull Descriptors.FileDescriptor[] dependencies) {
        for (final String recordType : MetaDataProtoEditor.getRecordTypes(metaDataBuilder)) {
            MetaDataProtoEditor.renameRecordType(metaDataBuilder,
                    recordType, renamer.apply(recordType), dependencies);
        }
    }

    /**
     * Renames a record type. This can be used to update any top-level record type defined within the metadata’s
     * records descriptor, including {@code NESTED} records or the union descriptor. However, it cannot be used to
     * rename nested messages (i.e., messages defined within other messages) or records defined in imported files.
     *
     * <ul>
     * <li>Message names are rewritten.
     * <li>Field types ({@code typeName}) that reference a renamed type are rewritten, whether the reference is direct
     *     or to a nested type of a renamed type.
     * <li>The union usage option is set when the union is renamed.
     * <li>The canonical {@code _typeName} union field is renamed when possible.
     * <li>If the record type has {@code RECORD} usage, the record type list, indexes, and joined record types are
     *     updated.
     * <li>Unnested record type constituents are updated.
     * </ul>
     *
     * @param metaDataBuilder the metadata builder
     * @param recordTypeName the name of the existing top-level record type
     * @param newRecordTypeName the new name to give to the record type
     */
    public static void renameRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                        @Nonnull String recordTypeName,
                                        @Nonnull String newRecordTypeName,
                                        @Nonnull Descriptors.FileDescriptor[] dependencies) {
        // Validate the rename. Rather than calling `metaDataBuilder.getRecordsBuilder()`, create a copy of the records
        // builder to avoid corrupting the one in `metaDataBuilder` before all validation has been done.
        final DescriptorProtos.FileDescriptorProto records = metaDataBuilder.getRecords();
        boolean found = false;
        for (DescriptorProtos.DescriptorProto messageType : records.getMessageTypeList()) {
            if (messageType.getName().equals(recordTypeName)) {
                found = true;
            } else if (messageType.getName().equals(newRecordTypeName)) {
                throw new MetaDataException("Cannot rename record type to " + newRecordTypeName + " as it already exists");
            }
        }
        if (!found) {
            throw new MetaDataException("No record type found with name " + recordTypeName);
        }

        // Identity transformation requires no work. From here on, we can assume `recordTypeName != newRecordTypeName`,
        // which simplifies things.
        if (recordTypeName.equals(newRecordTypeName)) {
            return;
        }
        final Descriptors.FileDescriptor fileDescriptor = RecordMetaDataBuilder.buildFileDescriptor(records, dependencies);
        final DescriptorProtos.FileDescriptorProto.Builder recordsBuilder = records.toBuilder();

        // Determine the usage of the original record type by looking through the union builder.
        // If we find a union field that matches, also update its name to be in the canonical form.
        DescriptorProtos.DescriptorProto.Builder unionBuilder = fetchUnionBuilder(recordsBuilder);
        RecordTypeOptions.Usage usage;
        if (unionBuilder.getName().equals(recordTypeName)) {
            usage = RecordTypeOptions.Usage.UNION;
        } else {
            final Descriptors.Descriptor unionDescriptor = getMessageTypeByName(fileDescriptor, unionBuilder.getName());
            DescriptorProtos.FieldDescriptorProto.Builder unionFieldBuilder = fetchUnionFieldBuilder(recordsBuilder,
                    unionBuilder, unionDescriptor, recordTypeName);
            if (unionFieldBuilder == null) {
                usage = RecordTypeOptions.Usage.NESTED;
            } else {
                usage = RecordTypeOptions.Usage.RECORD;
                // Change the name to the "canonical" form unless that would cause a field name conflict
                if (isCanonicalUnionFieldName(unionFieldBuilder.getName(), recordTypeName)) {
                    String newFieldName = canonicalUnionFieldName(newRecordTypeName);
                    if (unionBuilder.getFieldBuilderList().stream()
                            .noneMatch(otherUnionField -> otherUnionField != unionFieldBuilder
                                    && otherUnionField.getName().equals(newFieldName))) {
                        unionFieldBuilder.setName(newFieldName);
                    }
                }
            }
        }

        // Do not allow renaming to the default union name unless the record type is already the union.
        if (DEFAULT_UNION_NAME.equals(newRecordTypeName) && !RecordTypeOptions.Usage.UNION.equals(usage)) {
            throw new MetaDataException("Cannot rename record type to the default union name", LogMessageKeys.RECORD_TYPE, recordTypeName);
        }

        // Rename the record type within the file.
        renameRecordTypeUsages(recordsBuilder, recordTypeName, newRecordTypeName, fileDescriptor);

        // If the record type is a top-level record type, change its usage elsewhere in the metadata.
        if (RecordTypeOptions.Usage.RECORD.equals(usage)) {
            renameTopLevelRecordType(metaDataBuilder, recordTypeName, newRecordTypeName);
        }

        // Update unnested types (even if the record type is not a top-level type).
        renameRecordTypeUsagesInUnnested(metaDataBuilder, fileDescriptor, recordTypeName, newRecordTypeName,
                getMessageTypeByName(fileDescriptor, recordTypeName));

        // Update the file descriptor.
        metaDataBuilder.setRecords(recordsBuilder);
    }

    /**
     * Renames a record type within the records file. To this end, updates references to it in the fields of every
     * message type (recursively, including nested types). If it is the union type, also update its usage option.
     */
    private static void renameRecordTypeUsages(@Nonnull DescriptorProtos.FileDescriptorProto.Builder recordsBuilder,
                                               @Nonnull String recordTypeName, @Nonnull String newRecordTypeName,
                                               @Nonnull Descriptors.FileDescriptor fileDescriptor) {
        final String namespace = recordsBuilder.getPackage();
        final String fullRecordTypeName = fullyQualifiedTypeName(namespace, recordTypeName);
        final String fullNewRecordTypeName = fullyQualifiedTypeName(namespace, newRecordTypeName);

        // Rename the record type within the file.
        for (DescriptorProtos.DescriptorProto.Builder messageTypeBuilder : recordsBuilder.getMessageTypeBuilderList()) {
            final Descriptors.Descriptor descriptor = getMessageTypeByName(fileDescriptor, messageTypeBuilder.getName());
            // Change any fields referencing the old type so that they now reference the new type.
            renameRecordTypeUsages(namespace, messageTypeBuilder, fullRecordTypeName, fullNewRecordTypeName, descriptor);
            if (messageTypeBuilder.getName().equals(recordTypeName)) {
                // If renaming the union type, be sure that the record.usage option is set to UNION.
                if (isUnion(messageTypeBuilder) && getMessageTypeUsage(messageTypeBuilder) != RecordTypeOptions.Usage.UNION) {
                    setMessageTypeUsage(messageTypeBuilder, RecordTypeOptions.Usage.UNION);
                }
                messageTypeBuilder.setName(newRecordTypeName);
            }
        }
    }

    /**
     * Renames references to a record type among the fields of a single message type, recursing into its nested types.
     */
    private static void renameRecordTypeUsages(@Nonnull String namespace,
                                               @Nonnull DescriptorProtos.DescriptorProto.Builder messageTypeBuilder,
                                               @Nonnull String fullRecordTypeName,
                                               @Nonnull String fullNewRecordTypeName,
                                               final Descriptors.Descriptor descriptorForMessage) {
        // Rename any fields within the record type to the new type name.
        for (DescriptorProtos.FieldDescriptorProto.Builder field : messageTypeBuilder.getFieldBuilderList()) {
            final FieldTypeMatch fieldTypeMatch = fieldIsType(descriptorForMessage, field, fullRecordTypeName);
            if (FieldTypeMatch.MATCHES.equals(fieldTypeMatch)) {
                field.setTypeName(fullNewRecordTypeName);
            } else if (FieldTypeMatch.MATCHES_AS_NESTED.equals(fieldTypeMatch)) {
                final String fullOldFieldTypeName = "." + descriptorForMessage.findFieldByNumber(field.getNumber())
                        .getMessageType().getFullName();
                final String newFieldTypeName = fullNewRecordTypeName + fullOldFieldTypeName.substring(fullRecordTypeName.length());
                field.setTypeName(newFieldTypeName);
            }
        }

        // Rename the record type if used within any nested message types.
        if (messageTypeBuilder.getNestedTypeCount() > 0) {
            final String nestedNamespace =
                    namespace.isEmpty()
                    ? messageTypeBuilder.getName()
                    : (namespace + "." + messageTypeBuilder.getName());
            for (DescriptorProtos.DescriptorProto.Builder nestedTypeBuilder : messageTypeBuilder.getNestedTypeBuilderList()) {
                final Descriptors.Descriptor nestedDescriptor = Objects.requireNonNull(
                        descriptorForMessage.findNestedTypeByName(nestedTypeBuilder.getName()),
                        "FileDescriptor does not have nested type that exists in protobuf");
                renameRecordTypeUsages(nestedNamespace, nestedTypeBuilder, fullRecordTypeName, fullNewRecordTypeName,
                        nestedDescriptor);
            }
        }
    }

    /**
     * Updates the top-level record type list, any index definitions, and any joined record types that reference a
     * renamed record type. Throws if the metadata has {@code UserDefinedFunctions}, since renaming is not supported
     * in that case.
     */
    private static void renameTopLevelRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                                 @Nonnull String recordTypeName,
                                                 @Nonnull String newRecordTypeName) {
        List<RecordMetaDataProto.RecordType> recordTypes =
                new ArrayList<>(metaDataBuilder.getRecordTypesBuilderList().size());
        boolean foundRecordType = false;
        for (RecordMetaDataProto.RecordType recordType : metaDataBuilder.getRecordTypesList()) {
            if (recordType.getName().equals(newRecordTypeName)) {
                // Note: Despite the earlier check in this method, this can still be triggered if there is an imported
                // record with the given name.
                throw new MetaDataException("Cannot rename record type to " + newRecordTypeName + " as an imported record type of that name already exists");
            } else if (recordType.getName().equals(recordTypeName)) {
                recordTypes.add(recordType.toBuilder().setName(newRecordTypeName).build());
                foundRecordType = true;
            } else {
                recordTypes.add(recordType);
            }
        }
        if (!foundRecordType) {
            // This shouldn't happen, but if somehow the record type was in the union but not the record type list,
            // throw an error.
            throw new MetaDataException("Missing " + recordTypeName + " in record type list");
        }

        // Rename the record type within any indexes.
        List<RecordMetaDataProto.Index> indexes = new ArrayList<>(metaDataBuilder.getIndexesList());
        indexes.replaceAll(index -> {
            if (!index.getRecordTypeList().contains(recordTypeName)) {
                return index;
            }
            List<String> indexRecordTypes = new ArrayList<>(index.getRecordTypeList());
            indexRecordTypes.replaceAll(name -> name.equals(recordTypeName) ? newRecordTypeName : name);
            return index.toBuilder().clearRecordType().addAllRecordType(indexRecordTypes).build();
        });

        // Update the `metaDataBuilder` with all the renamed things.
        metaDataBuilder.clearRecordTypes();
        metaDataBuilder.addAllRecordTypes(recordTypes);
        metaDataBuilder.clearIndexes();
        metaDataBuilder.addAllIndexes(indexes);
        // Unnested types have to be updated even if it’s not a top-level type, so they have their own method.
        updateJoinedRecordTypes(metaDataBuilder, recordTypeName, newRecordTypeName);

        if (metaDataBuilder.getUserDefinedFunctionsCount() > 0) {
            // UserDefinedFunctions may be a string that needs parsing to figure out the types it references.
            throw new MetaDataException("Renaming record types with UserDefinedFunctions is not supported");
        }
    }

    /**
     * Updates joined record types' constituents that reference a renamed record type.
     */
    private static void updateJoinedRecordTypes(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                                @Nonnull String recordTypeName,
                                                @Nonnull String newRecordTypeName) {
        for (var joined : metaDataBuilder.getJoinedRecordTypesBuilderList()) {
            for (var constituent : joined.getJoinConstituentsBuilderList()) {
                if (constituent.getRecordType().equals(recordTypeName)) {
                    constituent.setRecordType(newRecordTypeName);
                }
            }
        }
    }

    /**
     * Updates unnested record types' constituents that reference a renamed record type as their (non-nested) parent.
     */
    private static void renameRecordTypeUsagesInUnnested(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                                         @Nonnull Descriptors.FileDescriptor fileDescriptor,
                                                         @Nonnull String recordTypeName,
                                                         @Nonnull String newRecordTypeName,
                                                         @Nonnull Descriptors.Descriptor oldTypeDescriptor) {
        for (var unnested : metaDataBuilder.getUnnestedRecordTypesBuilderList()) {
            for (var constituent : unnested.getNestedConstituentsBuilderList()) {
                // The nested constituents would most likely be nested types, not record types, and thus would not be
                // renamed.
                if (constituent.getParent().isEmpty() && constituent.getTypeName().equals(recordTypeName)) {
                    constituent.setTypeName(newRecordTypeName);
                } else {
                    final Descriptors.Descriptor constituentTypeDescriptor = UnnestedRecordTypeBuilder.findDescriptorByName(fileDescriptor,
                            constituent.getTypeName());
                    if (constituentTypeDescriptor == null) {
                        throw new MetaDataException("missing descriptor for nested constituent")
                                .addLogInfo(LogMessageKeys.EXPECTED, constituent.getTypeName())
                                .addLogInfo(LogMessageKeys.CONSTITUENT, constituent.getName());
                    }
                    if (isNested(constituentTypeDescriptor, oldTypeDescriptor)) {
                        throw new MetaDataException("Renaming types used by non-parent unnested constituents is not supported");
                    }
                }
            }
        }
    }

    /**
     * Whether the {@code targetDescriptor} is equal to or nested within {@code typeDescriptor}.
     *
     * @param typeDescriptor a type
     * @param targetDescriptor a type that may or may not be nested in {@code typeDescriptor}
     * @return {@code true} if the {@code targetDescriptor} is the same as {@code typeDescriptor} or a nested type of it
     */
    private static boolean isNested(@Nonnull Descriptors.Descriptor typeDescriptor,
                                    @Nonnull Descriptors.Descriptor targetDescriptor) {
        if (typeDescriptor.equals(targetDescriptor)) {
            return true;
        } else if (typeDescriptor.getContainingType() == null) {
            return false;
        } else {
            return isNested(typeDescriptor.getContainingType(), targetDescriptor);
        }
    }

    /**
     * Add a field to a record type.
     *
     * @param metaDataBuilder the metadata builder
     * @param recordType the record type to add the field to
     * @param field the field to be added
     */
    public static void addField(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                @Nonnull String recordType,
                                @Nonnull DescriptorProtos.FieldDescriptorProto field) {
        DescriptorProtos.DescriptorProto.Builder messageType =
                findMessageTypeByName(metaDataBuilder.getRecordsBuilder(), recordType);
        if (messageType == null) {
            throw new MetaDataException("Record type " + recordType + " does not exist");
        }
        DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = findFieldByName(messageType, field.getName());
        if (fieldBuilder != null) {
            throw new MetaDataException("Field " + field.getName() + " already exists in record type " + recordType);
        }
        messageType.addField(field);
    }

    /**
     * Deprecate a field from a record type.
     *
     * @param metaDataBuilder the metadata builder
     * @param recordType the record type to deprecate the field from
     * @param fieldName the name of the field to be deprecated
     */
    public static void deprecateField(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                      @Nonnull String recordType,
                                      @Nonnull String fieldName) {
        DescriptorProtos.DescriptorProto.Builder messageType =
                findMessageTypeByName(metaDataBuilder.getRecordsBuilder(), recordType);
        if (messageType == null) {
            throw new MetaDataException("Record type " + recordType + " does not exist");
        }
        DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = findFieldByName(messageType, fieldName);
        if (fieldBuilder == null) {
            throw new MetaDataException("Field " + fieldName + " not found in record type " + recordType);
        }
        setDeprecated(fieldBuilder);
    }

    private static void setDeprecated(DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder) {
        if (fieldBuilder.hasOptions()) {
            fieldBuilder.getOptionsBuilder().setDeprecated(true);
        } else {
            fieldBuilder.setOptions(DescriptorProtos.FieldOptions.newBuilder().setDeprecated(true).build());
        }
    }

    @Nullable
    private static DescriptorProtos.FieldDescriptorProto.Builder findFieldByName(
            @Nonnull DescriptorProtos.DescriptorProto.Builder messageType,
            @Nonnull String fieldName) {
        return messageType.getFieldBuilderList().stream()
                .filter(m -> m.getName().equals(fieldName))
                .findAny()
                .orElse(null);
    }

    /**
     * Add a default union to the given records descriptor if missing.
     *
     * <p>This method is a no-op if the union is present. Otherwise, the method will add a union to the records
     * descriptor. The union descriptor will be filled in with all the record types defined in the file except
     * {@code NESTED} record types.
     *
     * @param fileDescriptor the records descriptor of the record metadata
     * @return the resulting records descriptor
     */
    @Nonnull
    public static Descriptors.FileDescriptor addDefaultUnionIfMissing(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        if (MetaDataProtoEditor.hasUnion(fileDescriptor)) {
            return fileDescriptor;
        }
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptor.toProto();
        DescriptorProtos.FileDescriptorProto.Builder fileBuilder = fileDescriptorProto.toBuilder();
        fileBuilder.addMessageType(createDefaultUnion(fileBuilder));
        try {
            return Descriptors.FileDescriptor.buildFrom(
                    fileBuilder.build(), fileDescriptor.getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        } catch (Descriptors.DescriptorValidationException e) {
            throw new MetaDataException("Failed to add a default union", e);
        }
    }

    /**
     * Creates a default union descriptor for the given file descriptor if missing.
     *
     * <p>If the given file descriptor is missing a union message, this method will add one before updating the metadata.
     * The generated union descriptor is constructed by adding any non-{@code NESTED} types in the file descriptor to
     * the union descriptor from the currently stored metadata. A new field is not added if a field of the given type
     * already exists, and the order of any existing fields is preserved. Note that types are identified by name, so
     * renaming top-level message types may result in validation errors when trying to update the record descriptor.
     *
     * @param fileDescriptor the file descriptor to create a union for
     * @param baseUnionDescriptor the base union descriptor
     * @return the builder for the union
     */
    @Nonnull
    public static Descriptors.FileDescriptor addDefaultUnionIfMissing(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                                                      @Nonnull Descriptors.Descriptor baseUnionDescriptor) {
        if (MetaDataProtoEditor.hasUnion(fileDescriptor)) {
            return fileDescriptor;
        }
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptor.toProto();
        DescriptorProtos.FileDescriptorProto.Builder fileBuilder = fileDescriptorProto.toBuilder();
        DescriptorProtos.DescriptorProto.Builder unionDescriptorBuilder = createSyntheticUnion(fileDescriptor, baseUnionDescriptor);
        int unionTypeIndex = fileBuilder.getMessageTypeCount();
        fileBuilder.addMessageType(unionDescriptorBuilder);
        final Descriptors.FileDescriptor[] dependencies = fileDescriptor.getDependencies().toArray(new Descriptors.FileDescriptor[0]);

        try {
            fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), dependencies);
            final Descriptors.Descriptor unionDescriptor = fileDescriptor.findMessageTypeByName(unionDescriptorBuilder.getName());
            for (final Descriptors.Descriptor messageType : fileDescriptor.getMessageTypes()) {
                if (!Objects.equals(unionDescriptor, messageType)
                        && getMessageTypeUsage(messageType.toProto()) != RecordTypeOptions.Usage.NESTED) {
                    if (unionDescriptor.getFields().stream().noneMatch(field -> field.getMessageType() == messageType)) {
                        addFieldToUnion(unionDescriptorBuilder, fileBuilder, messageType.getName());
                    }
                }
            }
            fileBuilder.removeMessageType(unionTypeIndex);
            fileBuilder.addMessageType(unionDescriptorBuilder);
            return Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), dependencies);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new MetaDataException("Failed to add a default union", e);
        }
    }

    @Nonnull
    private static DescriptorProtos.DescriptorProto.Builder createDefaultUnion(@Nonnull DescriptorProtos.FileDescriptorProtoOrBuilder recordsDescriptor) {
        DescriptorProtos.DescriptorProto.Builder unionMessageType = DescriptorProtos.DescriptorProto.newBuilder();
        unionMessageType.setName(DEFAULT_UNION_NAME);
        for (DescriptorProtos.DescriptorProtoOrBuilder messageType : recordsDescriptor.getMessageTypeOrBuilderList()) {
            RecordTypeOptions.Usage messageTypeUsage = getMessageTypeUsage(messageType);
            if (messageTypeUsage != RecordTypeOptions.Usage.NESTED) {
                addFieldToUnion(unionMessageType, recordsDescriptor, messageType.getName());
            }
        }
        return unionMessageType;
    }

    /**
     * Creates a default union descriptor for the given file descriptor and a base union descriptor. It adds all the
     * non-{@code NESTED} message types that exist in the base union to the synthetic union.
     *
     * @param fileDescriptor the file descriptor to create a union for
     * @param baseUnionDescriptor the base union descriptor
     * @return the builder for the union
     */
    @Nonnull
    @API(API.Status.INTERNAL)
    public static DescriptorProtos.DescriptorProto.Builder createSyntheticUnion(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                                                                @Nonnull Descriptors.Descriptor baseUnionDescriptor) {
        DescriptorProtos.DescriptorProto.Builder unionMessageType = DescriptorProtos.DescriptorProto.newBuilder();
        unionMessageType.setName(DEFAULT_UNION_NAME);
        if (!baseUnionDescriptor.getOneofs().isEmpty()) {
            throw new MetaDataException("Adding record type to oneof is not allowed");
        }
        for (Descriptors.FieldDescriptor field : baseUnionDescriptor.getFields()) {
            Descriptors.Descriptor messageType = fileDescriptor.findMessageTypeByName(field.getMessageType().getName());
            if (messageType == null) {
                throw new MetaDataException("Record type " + field.getMessageType().getName() + " removed");
            }
            RecordTypeOptions.Usage messageTypeUsage = getMessageTypeUsage(messageType.toProto());
            if (messageTypeUsage != RecordTypeOptions.Usage.NESTED) {
                unionMessageType.addField(field.toProto().toBuilder()
                        .setTypeName(fullyQualifiedTypeName(messageType.getFile().getPackage(), messageType.getName())));
            }
        }
        return unionMessageType;
    }

    /**
     * Checks if the file descriptor has a union.
     *
     * @param fileDescriptor the file descriptor
     * @return true if the file descriptor has a union
     */
    public static boolean hasUnion(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        for (Descriptors.Descriptor messageType : fileDescriptor.getMessageTypes()) {
            if (isUnion(messageType)) {
                return true;
            }
        }
        return false;
    }

}
