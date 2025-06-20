/*
 * MetaDataProtoEditor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A helper class for mutating the meta-data proto.
 *
 * <p>
 * This class contains several helper methods for modifying a serialized meta-data, e.g., adding a new record type to the meta-data.
 * {@link FDBMetaDataStore#mutateMetaData(Consumer)} is one example of where these methods can be useful. That method
 * modifies the stored meta-data using a mutation callback and saves it back to the meta-data store.
 * </p>
 *
 */
@API(API.Status.EXPERIMENTAL)
public class MetaDataProtoEditor {
    /**
     * Add a new record type to the meta-data.
     *
     * <p>
     * Adding the record type involves three steps: the message type is added to the file descriptor's list of message types,
     * a field of the given type is added to the union, and its primary key is set.
     * Note that adding {@code UNION} record types is not allowed. To add {@code NESTED} record types, use {@link #addNestedRecordType}.
     * </p>
     *
     * @param metaDataBuilder the meta-data builder
     * @param newRecordType the new record type
     * @param primaryKey the primary key of the new record type
     */
    public static void addRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder, @Nonnull DescriptorProtos.DescriptorProto newRecordType, @Nonnull KeyExpression primaryKey) {
        RecordMetaDataOptionsProto.RecordTypeOptions.Usage newRecordTypeUsage = getMessageTypeUsage(newRecordType);
        if (RecordMetaDataBuilder.DEFAULT_UNION_NAME.equals(newRecordType.getName()) ||
                newRecordTypeUsage == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION) {
            throw new MetaDataException("Adding UNION record type not allowed");
        }
        if (newRecordTypeUsage == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED) {
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
        addFieldToUnion(fetchUnionBuilder(recordsBuilder), recordsBuilder, newRecordType);
    }

    private static void addFieldToUnion(@Nonnull DescriptorProtos.DescriptorProto.Builder unionBuilder,
                                        @Nonnull DescriptorProtos.FileDescriptorProtoOrBuilder fileBuilder,
                                        @Nonnull DescriptorProtos.DescriptorProtoOrBuilder newRecordType) {
        if (unionBuilder.getOneofDeclCount() > 0) {
            throw new MetaDataException("Adding record type to oneof is not allowed");
        }
        DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(fullyQualifiedTypeName(fileBuilder, newRecordType.getName()))
                .setName("_" + newRecordType.getName())
                .setNumber(assignFieldNumber(unionBuilder));
        unionBuilder.addField(fieldBuilder);
    }

    @Nonnull
    public static List<String> getRecordTypes(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder) {
        return metaDataBuilder.getRecordTypesBuilderList().stream()
                .map(RecordMetaDataProto.RecordType.Builder::getName)
                .collect(Collectors.toList());
    }

    @Nonnull
    private static DescriptorProtos.DescriptorProto.Builder fetchUnionBuilder(@Nonnull DescriptorProtos.FileDescriptorProto.Builder fileBuilder) {
        for (DescriptorProtos.DescriptorProto.Builder messageTypeBuilder : fileBuilder.getMessageTypeBuilderList()) {
            if (isUnion(messageTypeBuilder)) {
                return messageTypeBuilder;
            }
        }
        throw new MetaDataException("Union descriptor not found");
    }

    @Nullable
    private static DescriptorProtos.FieldDescriptorProto.Builder fetchUnionFieldBuilder(@Nonnull DescriptorProtos.FileDescriptorProto.Builder recordsBuilder,
                                                                                        @Nonnull DescriptorProtos.DescriptorProto.Builder unionBuilder,
                                                                                        @Nonnull String recordTypeName) {
        DescriptorProtos.FieldDescriptorProto.Builder foundField = null;
        if (recordTypeName.contains(".")) {
            throw new MetaDataException("Record Type Name cannot contain '.'");
        }
        if (unionBuilder.getNestedTypeCount() > 0) {
            throw new MetaDataException("Nested types in union type not supported");
        }
        for (DescriptorProtos.FieldDescriptorProto.Builder field : unionBuilder.getFieldBuilderList()) {
            if (field.getTypeName().equals(recordTypeName)) {
                if (foundField == null || field.getName().equals("_" + recordTypeName) || field.getNumber() > foundField.getNumber()) {
                    foundField = field;
                }
            } else {
                final FieldTypeMatch unionFieldMatch = fieldIsType(recordsBuilder, unionBuilder, field, recordTypeName);
                if (unionFieldMatch.isAmbiguousMatch()) {
                    throw new AmbiguousTypeNameException(recordsBuilder.getPackage(), unionBuilder, field, fullyQualifiedTypeName(recordsBuilder, recordTypeName));
                } else if (FieldTypeMatch.MATCHES.equals(unionFieldMatch)
                        && (foundField == null || field.getName().equals("_" + recordTypeName) || field.getNumber() > foundField.getNumber())) {
                    // Choose the matching field with either a matching name or the highest number.
                    foundField = field;
                }
            }
        }
        return foundField;
    }

    private static boolean isUnion(@Nonnull DescriptorProtos.DescriptorProtoOrBuilder messageType) {
        if (RecordMetaDataBuilder.DEFAULT_UNION_NAME.equals(messageType.getName())) {
            return true;
        }
        RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = messageType.getOptions()
                .getExtension(RecordMetaDataOptionsProto.record);
        return recordTypeOptions != null &&
               recordTypeOptions.hasUsage() &&
               recordTypeOptions.getUsage() == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION;
    }

    private static boolean isUnion(@Nonnull Descriptors.Descriptor messageType) {
        if (RecordMetaDataBuilder.DEFAULT_UNION_NAME.equals(messageType.getName())) {
            return true;
        }
        RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = messageType.getOptions()
                .getExtension(RecordMetaDataOptionsProto.record);
        return recordTypeOptions != null &&
               recordTypeOptions.hasUsage() &&
               recordTypeOptions.getUsage() == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION;
    }

    @Nonnull
    private static String fullyQualifiedTypeName(@Nonnull String namespace,
                                                 @Nonnull String typeName) {
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
        DOES_NOT_MATCH(false),
        /**
         * The field definitely does have the type requested.
         */
        MATCHES(false),
        /**
         * The field is definitely a nested type defined within the type requested.
         * For example, the requested type might be an {@code OuterMessage} and the field an {@code OuterMessage.InnerMessage}.
         */
        MATCHES_AS_NESTED(false),
        /**
         * The field might have the type requested, but it is ambiguous. This can result from fields with type names
         * that are not fully qualified. For example, a field that has type name {@code a.b.MessageName} in package
         * {@code a.b} might match {@code .a.b.a.b.MessageName}, {@code .a.a.b.MessageName}, or {@code .a.b.MessageName}.
         */
        MIGHT_MATCH(true),
        /**
         * The field might be a nested type within the type requested, but it is ambiguous. For example, if looking
         * for an {@code .a.b.OuterMessage}, this might match on a field of type {@code b.OuterMessage.InnerMessage} within
         * package {@code a.b} as the given field might refer either to type {@code .a.b.OuterMessage.InnerMessage} or
         * {@code .a.b.b.OuterMessage.InnerMessage}.
         */
        MIGHT_MATCH_AS_NESTED(true),
        ;

        private final boolean ambiguousMatch;

        FieldTypeMatch(boolean ambiguousMatch) {
            this.ambiguousMatch = ambiguousMatch;
        }

        /**
         * Whether the match indicates that the field may or may not be of the type asked for.
         * This can be {@code true} if the field type name is not fully specified and it is
         * possible (but not guaranteed) that the field resolves to that type.
         *
         * @return whether it is ambiguous whether the field matches the type or not
         */
        public boolean isAmbiguousMatch() {
            return ambiguousMatch;
        }
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
     * <p>
     * This can get even worse with nested types. For example, within a record {@code Foo}, if it has
     * a nested type {@code Bar}, a field with type {@code Foo} might be referring to either
     * the other {@code Foo} record or an additional type {@code Foo.Bar.Foo}.
     * </p>
     *
     * <p>
     * Because getting that right is difficult and requires full knowledge of all defined types, this
     * instead takes a simpler approach where if it can be determined for sure that the type is the
     * same, it returns that the type {@link FieldTypeMatch#MATCHES}. If it can be determined that the
     * type is definitely different, then this returns that it {@link FieldTypeMatch#DOES_NOT_MATCH}.
     * And if it's ambiguous, it returns that the type {@link FieldTypeMatch#MIGHT_MATCH}.
     * </p>
     *
     * <p>
     * It is also possible that the field matches (or might match) a nested type defined within the
     * given type. In that case, this can return that it matches (or might match) as a nested type.
     * This is useful for determining whether the type needs to be renamed, for example.
     * </p>
     *
     * @param namespace the package plus any nested types in which this field is located
     * @param field the field descriptor to check the type of
     * @param fullTypeName the fully-qualified type name
     * @return whether the field matches or might match the given type
     */
    @Nonnull
    private static FieldTypeMatch fieldIsType(@Nonnull String namespace,
                                              @Nonnull DescriptorProtos.DescriptorProtoOrBuilder message,
                                              @Nonnull DescriptorProtos.FieldDescriptorProtoOrBuilder field,
                                              @Nonnull String fullTypeName) {
        final String messageNamespace;
        if (namespace.isEmpty()) {
            messageNamespace = message.getName();
        } else {
            messageNamespace = namespace + "." + message.getName();
        }
        if (field.hasTypeName() && !field.getTypeName().isEmpty()) {
            final String fieldTypeName = field.getTypeName();
            if (fieldTypeName.startsWith(".")) {
                // If the type name begins with a ".", that means that paths are resolved from the outermost scope.
                // This means if we know the namespace, we can know if matches the type by just comparing the field's
                // type and the full type name we're looking for.
                if (fieldTypeName.equals(fullTypeName)) {
                    return FieldTypeMatch.MATCHES;
                } else if (fieldTypeName.startsWith(fullTypeName) && fieldTypeName.charAt(fullTypeName.length()) == '.') {
                    return FieldTypeMatch.MATCHES_AS_NESTED;
                }
            } else {
                // As the type doesn't begin with a ".", paths are first resolved from the innermost scope.
                // This tries all of the combinations with different numbers of elements chosen from
                // the namespace and then combining them with all of the elements of the field's declared
                // type. If the type being looked for matches this (possible) full type for the field,
                // the field may match. If the type matches a prefix of the (possible) full field type,
                // then the field might match a nested record of the type being looked for.
                String[] fullTypeParts = fullTypeName.substring(1).split("\\.");
                String[] messageNamespaceParts = messageNamespace.split("\\.");
                String[] typeNameParts = fieldTypeName.split("\\.");
                if (!typeNameParts[typeNameParts.length - 1].equals(fullTypeParts[fullTypeParts.length - 1])) {
                    return FieldTypeMatch.DOES_NOT_MATCH;
                }
                // TODO perhaps the rest of this should just become FieldTypeMatch.MIGHT_MATCH, and give up
                //      It returns MIGHT_MATCH for .T1 vs T2, so I'm not super confident in it

                // Start with including all parts in the namespace to mirror how protobuf searches from the
                // innermost scope.
                for (int n = messageNamespaceParts.length; n >= 0; n--) {
                    int fullTypePos = 0;

                    // Try to match the first n parts from the namespace.
                    boolean namespaceMatches = true;
                    for (int i = 0; i < n && fullTypePos < fullTypeParts.length; i++, fullTypePos++) {
                        if (!fullTypeParts[fullTypePos].equals(messageNamespaceParts[i])) {
                            namespaceMatches = false;
                            break;
                        }
                    }
                    if (!namespaceMatches) {
                        continue;
                    }

                    // Try to match any remaining parts of the type we're looking for from the field's
                    // type name.
                    int typeNamePos = 0;
                    while (typeNamePos < typeNameParts.length && fullTypePos < fullTypeParts.length) {
                        if (!fullTypeParts[fullTypePos].equals(typeNameParts[typeNamePos])) {
                            break;
                        }
                        typeNamePos++;
                        fullTypePos++;
                    }

                    // Check if we were able to match every part of the type we're looking for.
                    if (fullTypePos == fullTypeParts.length) {
                        if (typeNamePos == typeNameParts.length) {
                            // We were able to match every part from the field's type, so this is a possible match.
                            return FieldTypeMatch.MIGHT_MATCH;
                        } else {
                            // There were extra parts in the defined field, so the field might match a nested type
                            // of the given type.
                            return FieldTypeMatch.MIGHT_MATCH_AS_NESTED;
                        }
                    }
                }
            }
            return FieldTypeMatch.DOES_NOT_MATCH;
        } else {
            return FieldTypeMatch.DOES_NOT_MATCH;
        }
    }

    @VisibleForTesting
    @Nonnull
    static FieldTypeMatch fieldIsType(@Nonnull DescriptorProtos.FileDescriptorProtoOrBuilder file,
                                      @Nonnull DescriptorProtos.DescriptorProtoOrBuilder message,
                                      @Nonnull DescriptorProtos.FieldDescriptorProtoOrBuilder field,
                                      @Nonnull String typeName) {
        return fieldIsType(file.getPackage(), message, field, fullyQualifiedTypeName(file, typeName));
    }

    private static int assignFieldNumber(@Nonnull DescriptorProtos.DescriptorProto.Builder messageType) {
        if (messageType.getFieldCount() == 0) {
            return 1;
        }
        return messageType.getFieldList().stream().mapToInt(DescriptorProtos.FieldDescriptorProto::getNumber).max().getAsInt() + 1;
    }

    /**
     * Add a new {@code NESTED} record type to the meta-data. This can be used to define fields in other record types,
     * but it does not add the new record type to the union.
     *
     * @param metaDataBuilder the meta-data builder
     * @param newRecordType the new record type
     */
    public static void addNestedRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder, @Nonnull DescriptorProtos.DescriptorProto newRecordType) {
        RecordMetaDataOptionsProto.RecordTypeOptions.Usage newRecordTypeUsage = getMessageTypeUsage(newRecordType);
        if (newRecordTypeUsage != RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED &&
                newRecordTypeUsage != RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNSET) {
            throw new MetaDataException("Record type is not NESTED");
        }
        if (findMessageTypeByName(metaDataBuilder.getRecordsBuilder(), newRecordType.getName()) != null) {
            throw new MetaDataException("Record type " + newRecordType.getName() + " already exists");
        }
        metaDataBuilder.getRecordsBuilder().addMessageType(newRecordType);
    }

    @Nonnull
    private static RecordMetaDataOptionsProto.RecordTypeOptions.Usage getMessageTypeUsage(@Nonnull DescriptorProtos.DescriptorProtoOrBuilder messageType) {
        RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = messageType.getOptions()
                .getExtension(RecordMetaDataOptionsProto.record);
        if (recordTypeOptions != null && recordTypeOptions.hasUsage()) {
            return recordTypeOptions.getUsage();
        }
        return RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNSET;
    }

    @Nullable
    private static DescriptorProtos.DescriptorProto.Builder findMessageTypeByName(@Nonnull DescriptorProtos.FileDescriptorProto.Builder recordsBuilder, @Nonnull String recordType) {
        return recordsBuilder.getMessageTypeBuilderList().stream().filter(m -> m.getName().equals(recordType)).findAny().orElse(null);
    }

    /**
     * Deprecate a record type from the meta-data. The record is still defined in the record definition, but any occurrences
     * of the field in the union descriptor are deprecated. If there are any top-level record types that are defined
     * as nested messages within the deprecated record type, those fields in the union will also be deprecated.
     *
     * @param metaDataBuilder the meta-data builder
     * @param recordType the record type to be deprecated
     */
    public static void deprecateRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder, @Nonnull String recordType) {
        final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = metaDataBuilder.getRecordsBuilder();
        DescriptorProtos.DescriptorProto.Builder unionBuilder = fetchUnionBuilder(fileBuilder);
        if (unionBuilder.getName().equals(recordType)) {
            throw new MetaDataException("Cannot deprecate the union");
        }
        // deprecate all fields of type recordType from the union.
        boolean found = false;
        for (DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder : unionBuilder.getFieldBuilderList()) {
            final FieldTypeMatch fieldTypeMatch = fieldIsType(fileBuilder, unionBuilder, fieldBuilder, recordType);
            if (fieldTypeMatch.isAmbiguousMatch()) {
                throw new AmbiguousTypeNameException(fileBuilder.getPackage(), unionBuilder, fieldBuilder, fullyQualifiedTypeName(fileBuilder, recordType));
            } else if (FieldTypeMatch.MATCHES.equals(fieldTypeMatch) || FieldTypeMatch.MATCHES_AS_NESTED.equals(fieldTypeMatch)) {
                setDeprecated(fieldBuilder);
                found = true;
            }
        }
        if (!found) {
            throw new MetaDataException("Record type " + recordType + " not found");
        }
    }

    public static void renameRecordTypes(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                         @Nonnull Function<String, String> renamer) {
        for (final String recordType : MetaDataProtoEditor.getRecordTypes(metaDataBuilder)) {
            MetaDataProtoEditor.renameRecordType(metaDataBuilder,
                    recordType, renamer.apply(recordType));
        }
    }

    /**
     * Rename a record type. This can be used to update any top-level record type defined within the
     * meta-data's records descriptor, including {@code NESTED} records or the union descriptor. However,
     * it cannot be used to rename nested messages (i.e., messages defined within other messages) or
     * records defined in imported files. In addition to updating the file descriptor, if the record type
     * is not {@code NESTED} or the union descriptor, update any other references to the record type
     * within the meta-data (such as within index definitions).
     *
     * @param metaDataBuilder the meta-data builder
     * @param recordTypeName the name of the existing top-level record type
     * @param newRecordTypeName the new name to give to the record type
     */
    public static void renameRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                        @Nonnull String recordTypeName, @Nonnull String newRecordTypeName) {
        // Create a copy of the records builder (rather than calling metaDataBuilder.getRecordsBuilder()) to avoid
        // corrupting the records builder in metaDataBuilder before all validation has been done.
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
        if (recordTypeName.equals(newRecordTypeName)) {
            // Identity transformation requires no work.
            // From here on in, we can assume that recordTypeName != newRecordTypeName, which simplifies things.
            return;
        }
        final DescriptorProtos.FileDescriptorProto.Builder recordsBuilder = records.toBuilder();

        // Determine the usage of the original record type by looking through the union builder.
        // If we find a field that matches, also update its name to be in the canonical form (i.e., "_" + recordTypeName)
        DescriptorProtos.DescriptorProto.Builder unionBuilder = fetchUnionBuilder(recordsBuilder);
        RecordMetaDataOptionsProto.RecordTypeOptions.Usage usage;
        if (unionBuilder.getName().equals(recordTypeName)) {
            usage = RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION;
        } else {
            DescriptorProtos.FieldDescriptorProto.Builder unionFieldBuilder = fetchUnionFieldBuilder(recordsBuilder, unionBuilder, recordTypeName);
            if (unionFieldBuilder == null) {
                usage = RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED;
            } else {
                usage = RecordMetaDataOptionsProto.RecordTypeOptions.Usage.RECORD;
                // Change the name to the "canonical" form unless that would cause a field name conflict
                if (unionFieldBuilder.getName().equals("_" + recordTypeName)) {
                    String newFieldName = "_" + newRecordTypeName;
                    if (unionBuilder.getFieldBuilderList().stream()
                            .noneMatch(otherUnionField -> otherUnionField != unionFieldBuilder && otherUnionField.getName().equals(newFieldName))) {
                        unionFieldBuilder.setName(newFieldName);
                    }
                }
            }
        }
        // Do not allow renaming to the default union name unless the record type is already the union
        if (RecordMetaDataBuilder.DEFAULT_UNION_NAME.equals(newRecordTypeName) &&
                !RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION.equals(usage)) {
            throw new MetaDataException("Cannot rename record type to the default union name", LogMessageKeys.RECORD_TYPE, recordTypeName);
        }
        // Rename the record type within the file
        renameRecordTypeUsages(recordsBuilder, recordTypeName, newRecordTypeName);

        // If the record type is a top level record type, change its usage elsewhere in the meta-data
        if (RecordMetaDataOptionsProto.RecordTypeOptions.Usage.RECORD.equals(usage)) {
            renameTopLevelRecordType(metaDataBuilder, recordTypeName, newRecordTypeName);
        }

        // Update the file descriptor
        metaDataBuilder.setRecords(recordsBuilder);
    }

    private static void renameRecordTypeUsages(@Nonnull DescriptorProtos.FileDescriptorProto.Builder recordsBuilder,
                                               @Nonnull String oldRecordTypeName, @Nonnull String newRecordTypeName) {
        final String namespace = recordsBuilder.getPackage();
        final String fullOldRecordTypeName = fullyQualifiedTypeName(namespace, oldRecordTypeName);
        final String fullNewRecordTypeName = fullyQualifiedTypeName(namespace, newRecordTypeName);

        // Rename the record type within the file
        for (DescriptorProtos.DescriptorProto.Builder messageTypeBuilder : recordsBuilder.getMessageTypeBuilderList()) {
            // Change any fields referencing the old type so that they now reference the new type
            renameRecordTypeUsages(namespace, messageTypeBuilder, oldRecordTypeName, fullOldRecordTypeName, fullNewRecordTypeName, false);
            if (messageTypeBuilder.getName().equals(oldRecordTypeName)) {
                // If renaming the union type, be sure that the record.usage option is set to UNION.
                if (isUnion(messageTypeBuilder)) {
                    RecordMetaDataOptionsProto.RecordTypeOptions recordOptions = null;
                    if (messageTypeBuilder.getOptions().hasExtension(RecordMetaDataOptionsProto.record)) {
                        recordOptions = messageTypeBuilder.getOptionsBuilder().getExtension(RecordMetaDataOptionsProto.record);
                    }
                    if (recordOptions == null || !recordOptions.hasUsage() ||
                            !recordOptions.getUsage().equals(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION)) {
                        RecordMetaDataOptionsProto.RecordTypeOptions.Builder recordOptionsBuilder = recordOptions == null
                                                                                                    ? RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder()
                                                                                                    : recordOptions.toBuilder();
                        recordOptionsBuilder.setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION);
                        messageTypeBuilder.getOptionsBuilder().setExtension(RecordMetaDataOptionsProto.record, recordOptionsBuilder.build());
                    }
                }
                messageTypeBuilder.setName(newRecordTypeName);
            }
        }
    }

    private static void renameRecordTypeUsages(@Nonnull String namespace,
                                               @Nonnull DescriptorProtos.DescriptorProto.Builder messageTypeBuilder,
                                               @Nonnull String oldRecordTypeName,
                                               @Nonnull String fullOldRecordTypeName,
                                               @Nonnull String fullNewRecordTypeName,
                                               boolean isHidden) {
        isHidden |= messageTypeBuilder.getNestedTypeList().stream()
                .anyMatch(nestedType -> nestedType.getName().equals(oldRecordTypeName));

        // Rename any fields within the record type to the new type name
        for (DescriptorProtos.FieldDescriptorProto.Builder field : messageTypeBuilder.getFieldBuilderList()) {
            if (!isHidden && field.getTypeName().equals(oldRecordTypeName)) {
                field.setTypeName(fullNewRecordTypeName);
            } else {
                final FieldTypeMatch fieldTypeMatch = fieldIsType(namespace, messageTypeBuilder, field, fullOldRecordTypeName);
                if (fieldTypeMatch.isAmbiguousMatch()) {
                    throw new AmbiguousTypeNameException(namespace, messageTypeBuilder, field, fullOldRecordTypeName);
                } else if (FieldTypeMatch.MATCHES.equals(fieldTypeMatch)) {
                    field.setTypeName(fullNewRecordTypeName);
                } else if (FieldTypeMatch.MATCHES_AS_NESTED.equals(fieldTypeMatch)) {
                    final String messageNamespace = (namespace.isEmpty()) ? messageTypeBuilder.getName() : (namespace + "." + messageTypeBuilder.getName());
                    final String fieldTypeName = fullyQualifiedTypeName(messageNamespace, field.getTypeName());
                    final String newFieldTypeName = fullNewRecordTypeName + fieldTypeName.substring(fullOldRecordTypeName.length());
                    field.setTypeName(newFieldTypeName);
                }
            }
        }
        // Rename the record type if used within any nested message types
        if (messageTypeBuilder.getNestedTypeCount() > 0) {
            final String nestedNamespace = namespace.isEmpty() ? messageTypeBuilder.getName() : (namespace + "." + messageTypeBuilder.getName());
            for (DescriptorProtos.DescriptorProto.Builder nestedTypeBuilder : messageTypeBuilder.getNestedTypeBuilderList()) {
                renameRecordTypeUsages(nestedNamespace, nestedTypeBuilder, oldRecordTypeName, fullOldRecordTypeName, fullNewRecordTypeName, isHidden);
            }
        }
    }

    private static void renameTopLevelRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                                 @Nonnull String recordTypeName, @Nonnull String newRecordTypeName) {
        List<RecordMetaDataProto.RecordType> recordTypes;
        boolean foundRecordType = false;
        recordTypes = new ArrayList<>(metaDataBuilder.getRecordTypesBuilderList().size());
        for (RecordMetaDataProto.RecordType recordType : metaDataBuilder.getRecordTypesList()) {
            if (recordType.getName().equals(newRecordTypeName)) {
                // Despite the earlier check in this method, this can still be triggered if there is an imported record with the given name
                throw new MetaDataException("Cannot rename record type to " + newRecordTypeName + " as an imported record type of that name already exists");
            } else if (recordType.getName().equals(recordTypeName)) {
                recordTypes.add(recordType.toBuilder().setName(newRecordTypeName).build());
                foundRecordType = true;
            } else {
                recordTypes.add(recordType);
            }
        }
        if (!foundRecordType) {
            // This shouldn't happen, but if somehow the record type was in the union but not the record type list, throw an error
            throw new MetaDataException("Missing " + recordTypeName + " in record type list");
        }
        // Rename the record type within any indexes
        List<RecordMetaDataProto.Index> indexes = new ArrayList<>(metaDataBuilder.getIndexesList());
        indexes.replaceAll(index -> {
            if (index.getRecordTypeList().contains(recordTypeName)) {
                List<String> indexRecordTypes = new ArrayList<>(index.getRecordTypeList());
                indexRecordTypes.replaceAll(indexRecordType -> indexRecordType.equals(recordTypeName) ? newRecordTypeName : indexRecordType);
                return index.toBuilder().clearRecordType().addAllRecordType(indexRecordTypes).build();
            } else {
                return index;
            }
        });
        // Update the metaDataBuilder with all of the renamed things
        metaDataBuilder.clearRecordTypes();
        metaDataBuilder.addAllRecordTypes(recordTypes);
        metaDataBuilder.clearIndexes();
        metaDataBuilder.addAllIndexes(indexes);
        updateUnnestedTypes(metaDataBuilder, recordTypeName, newRecordTypeName);
        // TODO synthetic types
        // TODO protect UDFs
    }

    private static void updateUnnestedTypes(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                            @Nonnull String oldRecordTypeName,
                                            @Nonnull String newRecordTypeName) {
        for (var unnested : metaDataBuilder.getUnnestedRecordTypesBuilderList()) {
            for (var constituent : unnested.getNestedConstituentsBuilderList()) {
                // The nested constituents would most likely be nested types, not record types, and thus would not be
                // renamed
                if (constituent.getParent().isEmpty() && constituent.getTypeName().equals(oldRecordTypeName)) {
                    constituent.setTypeName(newRecordTypeName);
                } else if (constituent.getTypeName().startsWith(metaDataBuilder.getRecords().getPackage()) &&
                        constituent.getTypeName().endsWith(oldRecordTypeName)) {
                    throw new MetaDataException("Renaming types used by non-parent unnested constituents is not supported");
                }
            }
        }
    }

    /**
     * Add a field to a record type.
     *
     * @param metaDataBuilder the meta-data builder
     * @param recordType the record type to add the field to
     * @param field the field to be added
     */
    public static void addField(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder, @Nonnull String recordType, @Nonnull DescriptorProtos.FieldDescriptorProto field) {
        DescriptorProtos.DescriptorProto.Builder messageType = findMessageTypeByName(metaDataBuilder.getRecordsBuilder(), recordType);
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
     * @param metaDataBuilder the meta-data builder
     * @param recordType the record type to deprecate the field from
     * @param fieldName the name of the field to be deprecated
     */
    public static void deprecateField(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder, @Nonnull String recordType, @Nonnull String fieldName) {
        // Find the record type
        DescriptorProtos.DescriptorProto.Builder messageType = findMessageTypeByName(metaDataBuilder.getRecordsBuilder(), recordType);
        if (messageType == null) {
            throw new MetaDataException("Record type " + recordType + " does not exist");
        }

        // Deprecate the field
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
    private static DescriptorProtos.FieldDescriptorProto.Builder findFieldByName(@Nonnull DescriptorProtos.DescriptorProto.Builder messageType, @Nonnull String fieldName) {
        return messageType.getFieldBuilderList().stream().filter(m -> m.getName().equals(fieldName)).findAny().orElse(null);
    }

    /**
     * Add a default union to the given records descriptor if missing.
     *
     * <p>
     * This method is a no-op if the union is present. Otherwise, the method will add a union to the records
     * descriptor. The union descriptor will be filled in with all of the record types defined in the file except
     * {@code NESTED} record types.
     * </p>
     *
     * @param fileDescriptor the records descriptor of the record meta-data
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
            return Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), fileDescriptor.getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        } catch (Descriptors.DescriptorValidationException e) {
            throw new MetaDataException("Failed to add a default union", e);
        }
    }

    /**
     * Creates a default union descriptor for the given file descriptor if missing.
     *
     * <p>
     * If the given file descriptor is missing a union message, this method will add one before updating the meta-data.
     * The generated union descriptor is constructed by adding any non-{@code NESTED} types in the file descriptor to the
     * union descriptor from the currently stored meta-data. A new field is not added if a field of the given type already
     * exists, and the order of any existing fields is preserved. Note that types are identified by name, so renaming
     * top-level message types may result in validation errors when trying to update the record descriptor.
     * </p>
     *
     * @param fileDescriptor the file descriptor to create a union for
     * @param baseUnionDescriptor the base union descriptor
     * @return the builder for the union
     */
    @Nonnull
    public static Descriptors.FileDescriptor addDefaultUnionIfMissing(@Nonnull Descriptors.FileDescriptor fileDescriptor, @Nonnull Descriptors.Descriptor baseUnionDescriptor) {
        if (MetaDataProtoEditor.hasUnion(fileDescriptor)) {
            return fileDescriptor;
        }
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptor.toProto();
        DescriptorProtos.FileDescriptorProto.Builder fileBuilder = fileDescriptorProto.toBuilder();
        DescriptorProtos.DescriptorProto.Builder unionDescriptorBuilder = createSyntheticUnion(fileDescriptor, baseUnionDescriptor);
        for (DescriptorProtos.DescriptorProto.Builder messageType : fileBuilder.getMessageTypeBuilderList()) {
            RecordMetaDataOptionsProto.RecordTypeOptions.Usage messageTypeUsage = getMessageTypeUsage(messageType);
            if (messageTypeUsage != RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED
                    && !hasField(fileBuilder, unionDescriptorBuilder, messageType)) {
                addFieldToUnion(unionDescriptorBuilder, fileBuilder, messageType);
            }
        }
        fileBuilder.addMessageType(unionDescriptorBuilder);
        try {
            return Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), fileDescriptor.getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        } catch (Descriptors.DescriptorValidationException e) {
            throw new MetaDataException("Failed to add a default union", e);
        }
    }

    @Nonnull
    private static DescriptorProtos.DescriptorProto.Builder createDefaultUnion(@Nonnull DescriptorProtos.FileDescriptorProtoOrBuilder recordsDescriptor) {
        DescriptorProtos.DescriptorProto.Builder unionMessageType = DescriptorProtos.DescriptorProto.newBuilder();
        unionMessageType.setName(RecordMetaDataBuilder.DEFAULT_UNION_NAME);
        for (DescriptorProtos.DescriptorProtoOrBuilder messageType : recordsDescriptor.getMessageTypeOrBuilderList()) {
            RecordMetaDataOptionsProto.RecordTypeOptions.Usage messageTypeUsage = getMessageTypeUsage(messageType);
            if (messageTypeUsage != RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED) {
                addFieldToUnion(unionMessageType, recordsDescriptor, messageType);
            }
        }
        return unionMessageType;
    }

    /**
     * Creates a default union descriptor for the given file descriptor and a base union descriptor. It adds all of the
     * non-{@code NESTED} message types that exist in the base union to the synthetic union.
     * @param fileDescriptor the file descriptor to create a union for
     * @param baseUnionDescriptor the base union descriptor
     * @return the builder for the union
     */
    @Nonnull
    @API(API.Status.INTERNAL)
    public static DescriptorProtos.DescriptorProto.Builder createSyntheticUnion(@Nonnull Descriptors.FileDescriptor fileDescriptor, @Nonnull Descriptors.Descriptor baseUnionDescriptor) {
        DescriptorProtos.DescriptorProto.Builder unionMessageType = DescriptorProtos.DescriptorProto.newBuilder();
        unionMessageType.setName(RecordMetaDataBuilder.DEFAULT_UNION_NAME);
        if (baseUnionDescriptor.getOneofs().size() > 0) {
            throw new MetaDataException("Adding record type to oneof is not allowed");
        }
        for (Descriptors.FieldDescriptor field : baseUnionDescriptor.getFields()) {
            Descriptors.Descriptor messageType = fileDescriptor.findMessageTypeByName(field.getMessageType().getName());
            if (messageType == null) {
                throw new MetaDataException("Record type " + field.getMessageType().getName() + " removed");
            }
            RecordMetaDataOptionsProto.RecordTypeOptions.Usage messageTypeUsage = getMessageTypeUsage(messageType.toProto());
            if (messageTypeUsage != RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED) {
                unionMessageType.addField(field.toProto().toBuilder()
                        .setTypeName(fullyQualifiedTypeName(messageType.getFile().getPackage(), messageType.getName())));
            }
        }
        return unionMessageType;
    }

    /**
     * Checks if the file descriptor has a union.
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

    private static boolean hasField(@Nonnull DescriptorProtos.FileDescriptorProtoOrBuilder file,
                                    @Nonnull DescriptorProtos.DescriptorProtoOrBuilder message,
                                    @Nonnull DescriptorProtos.DescriptorProtoOrBuilder messageType) {
        for (DescriptorProtos.FieldDescriptorProto field : message.getFieldList()) {
            final String fullTypeName = fullyQualifiedTypeName(file, messageType.getName());
            FieldTypeMatch fieldTypeMatch = fieldIsType(file, message, field, fullTypeName);
            if (fieldTypeMatch.isAmbiguousMatch()) {
                throw new AmbiguousTypeNameException(file.getPackage(), message, field, fullTypeName);
            } else if (FieldTypeMatch.MATCHES.equals(fieldTypeMatch)) {
                return true;
            }
            // Nested matches do not count.
        }
        return false;
    }

    /**
     * An exception that is thrown if the type of a field is ambiguous. This can happen if, for example, a field of a nested type does not
     * fully-qualify its type name and it has a type that might (or might not) resolve to a record type whose name is being changed.
     */
    @API(API.Status.EXPERIMENTAL)
    public static class AmbiguousTypeNameException extends MetaDataException {
        private static final long serialVersionUID = 1L;

        private AmbiguousTypeNameException(@Nonnull String namespace, @Nonnull DescriptorProtos.DescriptorProtoOrBuilder messageTypeBuilder,
                                           @Nonnull DescriptorProtos.FieldDescriptorProtoOrBuilder field, @Nonnull String fullRecordTypeName) {
            super("Field " + field.getName() + " in message " + fullyQualifiedTypeName(namespace, messageTypeBuilder.getName()) + " of type " + field.getTypeName() + " might be of type " + fullRecordTypeName);
        }
    }
}
