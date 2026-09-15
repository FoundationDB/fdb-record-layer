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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;

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

    /**
     * Returns the declared {@code usage} of a message type. If the message type declares no {@code record} options,
     * or no {@code usage} within them, returns {@code UNSET} (which is the Protobuf default for the field).
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
     * Returns the fully-qualified name of the message or enum type referenced by {@code field}, resolved against
     * {@code messageDescriptor} by field number rather than name or position, since the number is the only
     * identifier guaranteed to tie a mutable builder field to its resolved descriptor counterpart. Returns
     * {@code null} if the field is of a primitive type, and so references no named type at all.
     */
    @Nullable
    private static String resolveFieldTypeFullName(
            @Nonnull Descriptors.Descriptor messageDescriptor,
            @Nonnull DescriptorProtos.FieldDescriptorProtoOrBuilder field) {
        final Descriptors.FieldDescriptor resolvedField = Objects.requireNonNull(
                messageDescriptor.findFieldByNumber(field.getNumber()),
                "Could not find field from protobuf in descriptor");
        return switch (resolvedField.getJavaType()) {
            case MESSAGE -> "." + resolvedField.getMessageType().getFullName();
            case ENUM -> "." + resolvedField.getEnumType().getFullName();
            default -> null;
        };
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
            final String fullyQualifiedName = resolveFieldTypeFullName(messageDescriptor, field);
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

    /**
     * Internal representation of a record type to be renamed by {@link #renameRecordTypes}.
     */
    private static final class RecordTypeRename {
        /** The current name. */
        @Nonnull
        private final String name;
        /** The new name. */
        @Nonnull
        private final String newName;
        /** The fully qualified current name. */
        @Nonnull
        private final String fullName;
        /** The fully qualified new name. */
        @Nonnull
        private final String fullNewName;
        /**
         * The usage, as determined by looking at the union type. (Initially {@code UNSET}, to be filled in by
         * {@link #determineRecordTypeUnionFieldsAndUsages}).
         */
        @Nonnull
        private RecordTypeOptions.Usage usage = RecordTypeOptions.Usage.UNSET;
        /**
         * Builder of the referencing union field, if any. (Initially null, to be filled in by
         * {@link #determineRecordTypeUnionFieldsAndUsages}).
         */
        @Nullable
        private DescriptorProtos.FieldDescriptorProto.Builder unionField;

        RecordTypeRename(@Nonnull String namespace, @Nonnull String name, @Nonnull String newName) {
            this.name = name;
            this.newName = newName;
            this.fullName = fullyQualifiedTypeName(namespace, name);
            this.fullNewName = fullyQualifiedTypeName(namespace, newName);
        }
    }

    /**
     * A map of {@link RecordTypeRename}s, keyed by the record type’s current (simple) name, as built by
     * {@link #analyzeRecordTypeRenames}. Also provides a lookup by fully qualified name, built lazily on first use.
     */
    private static final class RecordTypeRenames {
        @Nonnull
        private final Map<String, RecordTypeRename> byName;
        @Nullable
        private Map<String, RecordTypeRename> byFullName;

        RecordTypeRenames(@Nonnull Map<String, RecordTypeRename> byName) {
            this.byName = byName;
        }

        boolean isEmpty() {
            return byName.isEmpty();
        }

        @Nonnull
        Collection<RecordTypeRename> values() {
            return byName.values();
        }

        @Nullable
        RecordTypeRename get(@Nonnull String name) {
            return byName.get(name);
        }

        /**
         * Returns the new name for {@code name}, if there is a rename of {@code name} with the given {@code usage};
         * otherwise, returns {@code null}.
         */
        @Nullable
        String get(@Nonnull String name, @Nonnull RecordTypeOptions.Usage usage) {
            final RecordTypeRename rename = byName.get(name);
            return rename != null && rename.usage == usage ? rename.newName : null;
        }

        /**
         * Returns the rename whose (original) fully qualified name is {@code fullName}, if any.
         */
        @Nullable
        RecordTypeRename getByFullName(@Nonnull String fullName) {
            if (byFullName == null) {
                byFullName = new HashMap<>();
                for (final RecordTypeRename rename : byName.values()) {
                    byFullName.put(rename.fullName, rename);
                }
            }
            return byFullName.get(fullName);
        }
    }

    /**
     * Renames the record types in the metadata, according to the name mapping defined by {@code renamer}. For each
     * renamed record type (where {@code renamer} yields a name that is not equal to the current one), this method
     * applies the same transformations that {@link #renameRecordType} would, but it operates in an efficient, batched
     * manner. The entire mapping is applied in a single walk over the given {@code metadata}, and the records
     * {@link Descriptors.FileDescriptor} is compiled exactly once.
     *
     * <p><b>Precondition:</b> The {@code renamer} must define a consistent, collision-free mapping. That is, no two
     * distinct existing top-level record types may map to the same new name, and no record type may be renamed to a
     * name that collides with another (renamed or unchanged) top-level type or an imported record type. If a collision
     * is detected, no rename is performed, and a {@link MetaDataException} is thrown.
     *
     * <p>The following is an example of a simple, collision-free renaming. It prepends a fixed string to every name:
     * <pre>
     * MetaDataProtoEditor.renameRecordTypes(builder, name -> "prefix_" + name, dependencies);
     * </pre>
     *
     * <h3>Usage notes</h3>
     *
     * <p>For a collision-free mapping of a single {@code RECORD}-usage type, this method is exactly equivalent to the
     * corresponding {@link #renameRecordType} call. For anything broader, the two diverge in a few respects, each
     * noted below: which types the mapping is applied to, how imported types are treated, and which mappings are
     * accepted rather than rejected. Where they differ, it is generally because applying the whole batch at once
     * admits mappings that no single ordering of one-by-one renames could express.
     *
     * <p>Unlike {@code renameRecordType}, {@code renamer} is only ever applied to (and can therefore only rename)
     * {@code RECORD}-usage top-level types, i.e., those in {@code MetaData.record_types}; it cannot rename
     * {@code NESTED} types or the union type itself.
     *
     * <p>Imported record types, i.e., those registered in {@code MetaData.record_types} whose message type is defined
     * in a dependency file rather than in {@code MetaData.records}, cannot be renamed by this metadata, so
     * {@code renamer} is never applied to them. (Note that {@code renameRecordType}, by contrast, rejects such a
     * rename outright, with a “No record type found” exception.) An imported record type can still be the cause of
     * a collision, however.
     *
     * <p>Validating the mapping as a whole means a batch may be accepted where the equivalent one-by-one renames
     * would fail. A batch that permutes existing names, for instance swapping {@code Foo} and {@code Bar}, is
     * collision-free and therefore accepted, whereas renaming those types one at a time would collide on whichever
     * is renamed first.
     *
     * @param metadata the metadata builder
     * @param renamer a function mapping each existing top-level record type name to its new name
     * @param dependencies the dependencies of the records file descriptor
     * @see #renameRecordType
     */
    public static void renameRecordTypes(@Nonnull RecordMetaDataProto.MetaData.Builder metadata,
                                         @Nonnull UnaryOperator<String> renamer,
                                         @Nonnull Descriptors.FileDescriptor[] dependencies) {
        // Collect the renames into a map, skipping identity renames. Throws `MetaDataException` on any conflict.
        final RecordTypeRenames renames = analyzeRecordTypeRenames(metadata, renamer);
        if (renames.isEmpty()) {
            return;
        }

        // Build the file descriptor exactly once, from the original `MetaData.records` proto. Every descriptor
        // lookup below is done by original name, so we can use this single descriptor for every rename in the mapping.
        final Descriptors.FileDescriptor fileDescriptor =
                RecordMetaDataBuilder.buildFileDescriptor(metadata.getRecords(), dependencies);

        applyRecordTypeRenames(metadata, renames, fileDescriptor);
    }

    /**
     * Renames a record type. This can be used to update any top-level record type defined within the metadata’s
     * records descriptor, including {@code NESTED} records or the union descriptor. However, it cannot be used to
     * rename nested messages (i.e., messages defined within other messages) or records defined in imported files.
     *
     * <p>Unlike {@link #renameRecordTypes}, which can only rename {@code RECORD}-usage top-level types, this method
     * can rename any of the three: {@code RECORD}, {@code NESTED}, or the union type itself.
     *
     * <ul>
     * <li>Message names are rewritten.
     * <li>Field types ({@code typeName}) that reference a renamed type are rewritten, whether the reference is direct
     *     or to a nested type of a renamed type.
     * <li>The union usage option is set when the union is renamed.
     * <li>The canonical {@code _typeName} union field is renamed; if the union already has a field under the new
     *     canonical name, the rename is rejected instead.
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

        // Likewise, check for a collision against the metadata's full record type registry, which (unlike
        // `records.getMessageTypeList()` above) also includes record types imported from a dependency file.
        for (final String name : getRecordTypes(metaDataBuilder)) {
            if (!name.equals(recordTypeName) && name.equals(newRecordTypeName)) {
                throw new MetaDataException("Cannot rename record type to " + newRecordTypeName + " as an imported record type of that name already exists");
            }
        }

        // Identity transformation requires no work. From here on, we can assume `recordTypeName != newRecordTypeName`,
        // which simplifies things.
        if (recordTypeName.equals(newRecordTypeName)) {
            return;
        }
        final Descriptors.FileDescriptor fileDescriptor = RecordMetaDataBuilder.buildFileDescriptor(records, dependencies);
        final RecordTypeRename rename = new RecordTypeRename(records.getPackage(), recordTypeName, newRecordTypeName);
        applyRecordTypeRenames(metaDataBuilder, new RecordTypeRenames(Map.of(recordTypeName, rename)), fileDescriptor);
    }

    /**
     * Applies {@code renames} to every part of the metadata. Shared by {@link #renameRecordType} and
     * {@link #renameRecordTypes}, which differ only in how they build and pre-validate the map of renames. The steps
     * are ordered so that every validation happens before the first mutation, leaving {@code metaDataBuilder}
     * untouched if any of them raises {@link MetaDataException}.
     *
     * @param metaDataBuilder the metadata builder to rewrite
     * @param renames the renames to apply, which must be non-empty and collision-free
     * @param fileDescriptor the compiled {@code MetaData.records}, as it stands before any rename has been applied
     */
    private static void applyRecordTypeRenames(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                                               @Nonnull RecordTypeRenames renames,
                                               @Nonnull Descriptors.FileDescriptor fileDescriptor) {
        // Validate that `MetaData.user_defined_functions` is empty.
        validateNoUserDefinedFunctions(metaDataBuilder);

        // Validate the `MetaData.unnested_record_types` constituents.
        validateRecordTypeUsagesInUnnestedRecordTypes(metaDataBuilder.getUnnestedRecordTypesBuilderList(),
                fileDescriptor, renames);

        // Determine the usage of each renamed type by looking at the union message type within `MetaData.records`.
        final DescriptorProtos.DescriptorProto.Builder union = fetchUnionBuilder(metaDataBuilder.getRecordsBuilder());
        if (union.getNestedTypeCount() > 0) {
            throw new MetaDataException("Nested types in union type not supported");
        }
        determineRecordTypeUnionFieldsAndUsages(renames, fileDescriptor, union);

        // A RECORD-usage type was found in the union, so it must be registered in `MetaData.record_types` too. This
        // shouldn't happen, but if somehow a record type was in the union but not the record type list, throw.
        final Set<String> recordTypeNames = new HashSet<>(getRecordTypes(metaDataBuilder));
        for (final RecordTypeRename rename : renames.values()) {
            if (rename.usage == RecordTypeOptions.Usage.RECORD && !recordTypeNames.contains(rename.name)) {
                throw new MetaDataException("Missing " + rename.name + " in record type list");
            }
        }

        // Validate that renaming the canonical union fields would not cause a collision.
        validateUnionFieldRenames(union, renames);

        // Every validation is now done, so the metadata can be mutated.

        // Rename the canonical union field, if present, for each renamed type.
        renameUnionFields(renames);

        // Rename every message type in `MetaData.records`, and all field type references.
        renameRecordTypeUsagesInMessageTypes(metaDataBuilder.getRecordsBuilder().getMessageTypeBuilderList(), renames,
                fileDescriptor);

        // Update `MetaData.record_types` for every top-level RECORD type.
        renameRecordTypeUsagesInRecordTypes(metaDataBuilder.getRecordTypesBuilderList(), renames);

        // Update `MetaData.indexes` for every top-level RECORD type.
        renameRecordTypeUsagesInIndexes(metaDataBuilder.getIndexesBuilderList(), renames);

        // Update `MetaData.joined_record_types` constituents for every renamed type.
        renameRecordTypeUsagesInJoinedRecordTypes(metaDataBuilder.getJoinedRecordTypesBuilderList(), renames);

        // Rename `MetaData.unnested_record_types` constituents for every renamed type.
        renameRecordTypeUsagesInUnnestedRecordTypes(metaDataBuilder.getUnnestedRecordTypesBuilderList(), renames);
    }

    /**
     * A helper for {@link #renameRecordTypes} that converts the record-type name mapping defined by {@code renamer}
     * into the internal {@link RecordTypeRenames} map. Also validates the mapping against the full set of top-level
     * message types, and raises {@link MetaDataException} if it is invalid.
     */
    @Nonnull
    private static RecordTypeRenames analyzeRecordTypeRenames(
            @Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
            @Nonnull UnaryOperator<String> renamer) {
        final String namespace = metaDataBuilder.getRecords().getPackage();
        // Apply `renamer` to each record type name and build the map representing the renamings. Imported record types
        // need to be skipped. They are registered in `record_types` but defined in a dependency file, so this metadata
        // cannot rename them.
        final Set<String> localMessageTypes = new HashSet<>();
        for (final DescriptorProtos.DescriptorProto messageType : metaDataBuilder.getRecords().getMessageTypeList()) {
            localMessageTypes.add(messageType.getName());
        }
        final Map<String, RecordTypeRename> renames = new LinkedHashMap<>();
        for (final String recordType : getRecordTypes(metaDataBuilder)) {
            if (!localMessageTypes.contains(recordType)) {
                continue;
            }
            final String newName = renamer.apply(recordType);
            // Skip identity renames, as they require no work.
            if (recordType.equals(newName)) {
                continue;
            }
            renames.put(recordType, new RecordTypeRename(namespace, recordType, newName));
        }

        if (renames.isEmpty()) {
            return new RecordTypeRenames(renames);
        }

        // Build the inverse new-to-old mapping and use it to perform basic validation:
        // * No two distinct existing record types may map to the same new name.
        // * No record type may be renamed to a name that collides with another (renamed or unchanged) top-level type.
        final Map<String, String> inverse = new HashMap<>();
        for (final RecordTypeRename rename : renames.values()) {
            final String previous = inverse.put(rename.newName, rename.name);
            if (previous != null) {
                throw new MetaDataException(
                        "Cannot rename record types `" + previous + "` and `" + rename.name
                        + "` to the same name `" + rename.newName + "`");
            }
        }

        // If a message type is not itself being renamed, then it must not be the target of a rename either.
        for (final DescriptorProtos.DescriptorProto messageType : metaDataBuilder.getRecords().getMessageTypeList()) {
            final String name = messageType.getName();
            if (!renames.containsKey(name) && inverse.containsKey(name)) {
                throw new MetaDataException("Cannot rename record type to " + name + " as it already exists");
            }
        }

        // Likewise for the metadata’s full record type registry, which (unlike `getMessageTypeList()` above) also
        // includes record types imported from a dependency file.
        for (final String name : getRecordTypes(metaDataBuilder)) {
            if (!renames.containsKey(name) && inverse.containsKey(name)) {
                throw new MetaDataException(
                        "Cannot rename record type to " + name +
                        " as an imported record type of that name already exists");
            }
        }

        return new RecordTypeRenames(renames);
    }

    /**
     * A helper for {@link #applyRecordTypeRenames} that determines the {@link RecordTypeOptions.Usage Usage} of each
     * renamed type by looking at the union. Fills in the {@code usage} and {@code unionField} of the entries
     * in {@code renames}. Does not mutate {@code fileDescriptor} and {@code unionBuilder}.
     */
    private static void determineRecordTypeUnionFieldsAndUsages(
            @Nonnull RecordTypeRenames renames,
            @Nonnull Descriptors.FileDescriptor fileDescriptor,
            @Nonnull DescriptorProtos.DescriptorProto.Builder unionBuilder) {
        final String unionName = unionBuilder.getName();
        final Descriptors.Descriptor unionDescriptor = getMessageTypeByName(fileDescriptor, unionName);

        // Find, for each renamed record type, the union field that references it (if any), in a single pass over the
        // union’s fields.
        for (final DescriptorProtos.FieldDescriptorProto.Builder unionField : unionBuilder.getFieldBuilderList()) {
            if (!unionField.hasTypeName() || unionField.getTypeName().isEmpty()) {
                continue;
            }

            final String fullReferencedName = resolveFieldTypeFullName(unionDescriptor, unionField);
            if (fullReferencedName == null) {
                continue;
            }

            final RecordTypeRename rename = renames.getByFullName(fullReferencedName);
            if (rename == null) {
                continue;
            }

            // If multiple fields reference this record type, prefer the canonically-named one; otherwise, keep the
            // one with the highest field number.
            if (rename.unionField == null
                    || isCanonicalUnionFieldName(unionField.getName(), rename.name)
                    || unionField.getNumber() > rename.unionField.getNumber()) {
                rename.unionField = unionField;
            }
        }

        for (final RecordTypeRename rename : renames.values()) {
            // Determine the usage of each renamed record type, based on the union field found above, if any.
            // * If the type name equals the union name, the usage is UNION.
            // * Otherwise, if the type has a corresponding union field, it is a top-level record type, i.e., RECORD.
            // * Otherwise, it can only ever be used as an embedded message type, i.e., NESTED.
            if (rename.name.equals(unionName)) {
                rename.unionField = null;
                rename.usage = RecordTypeOptions.Usage.UNION;
            } else {
                rename.usage = rename.unionField == null
                               ? RecordTypeOptions.Usage.NESTED
                               : RecordTypeOptions.Usage.RECORD;
            }

            // Prevent renaming a non-UNION type to the default union name.
            if (!rename.usage.equals(RecordTypeOptions.Usage.UNION) && rename.newName.equals(DEFAULT_UNION_NAME)) {
                throw new MetaDataException(
                        "Cannot rename record type to the default union name",
                        LogMessageKeys.RECORD_TYPE, rename.name);
            }
        }
    }

    /**
     * Validates that renaming the canonical union field for each rename (if any) to its new canonical name would not
     * collide with any other field of the union message type within {@code MetaData.records}, once every rename in
     * {@code renames} has been applied.
     */
    private static void validateUnionFieldRenames(@Nonnull DescriptorProtos.DescriptorProto.Builder unionBuilder,
                                                   @Nonnull RecordTypeRenames renames) {
        // Fields that are themselves about to be renamed to their new canonical form never count as a collision target
        // below, since they won’t keep their current name once this batch of renames is applied. (Without this
        // exclusion, a batch that e.g. swaps two type names, Foo -> Baz and Bar -> Foo, could spuriously be rejected,
        // since Bar’s rename to _Foo would appear to collide with Foo’s own, still-pristine _Foo field.)
        final Set<DescriptorProtos.FieldDescriptorProto.Builder> beingRenamed = new HashSet<>();
        for (final RecordTypeRename rename : renames.values()) {
            if (rename.unionField != null && isCanonicalUnionFieldName(rename.unionField.getName(), rename.name)) {
                beingRenamed.add(rename.unionField);
            }
        }
        for (final RecordTypeRename rename : renames.values()) {
            if (!beingRenamed.contains(rename.unionField)) {
                continue;
            }
            final String newName = canonicalUnionFieldName(rename.newName);
            final boolean hasCollision = unionBuilder.getFieldBuilderList().stream()
                    .anyMatch(fb -> fb != rename.unionField && fb.getName().equals(newName)
                            && !beingRenamed.contains(fb));
            if (hasCollision) {
                throw new MetaDataException(
                        "Cannot rename union field to " + newName + " as a field of that name already exists",
                        LogMessageKeys.RECORD_TYPE, rename.name);
            }
        }
    }

    /**
     * Renames the canonical union field, if present, for each rename in {@code renames}. The union field is a field
     * of the union message type within {@code MetaData.records}. Callers must have already validated the renames
     * via {@link #validateUnionFieldRenames}.
     */
    private static void renameUnionFields(@Nonnull RecordTypeRenames renames) {
        for (final RecordTypeRename rename : renames.values()) {
            if (rename.unionField != null && isCanonicalUnionFieldName(rename.unionField.getName(), rename.name)) {
                rename.unionField.setName(canonicalUnionFieldName(rename.newName));
            }
        }
    }

    /**
     * A helper for {@link #applyRecordTypeRenames} that applies the name mapping in a single walk over the message types
     * in {@code MetaData.records}, using the given compiled file descriptor for type resolution. Field type
     * references are resolved (via the original descriptor) to the original type they point at; if that original
     * type is, or is nested within, any renamed type, the reference is rewritten to point at the renamed type.
     */
    private static void renameRecordTypeUsagesInMessageTypes(
            @Nonnull List<DescriptorProtos.DescriptorProto.Builder> messageTypes,
            @Nonnull RecordTypeRenames renames,
            @Nonnull Descriptors.FileDescriptor fileDescriptor) {
        // Walk every message type, rewriting references to renamed types and renaming the type itself.
        for (final DescriptorProtos.DescriptorProto.Builder mtb : messageTypes) {
            final String name = mtb.getName();

            // Rewrite `typeName` field references within the message type.
            final Descriptors.Descriptor descriptor = getMessageTypeByName(fileDescriptor, name);
            renameRecordTypeUsagesInMessageType(mtb, renames, descriptor);

            final RecordTypeRename rename = renames.get(name);
            if (rename == null) {
                continue;
            }

            // If renaming the union type, be sure that the `record.usage` option is set to UNION.
            if (name.equals(DEFAULT_UNION_NAME) && getMessageTypeUsage(mtb) != RecordTypeOptions.Usage.UNION) {
                setMessageTypeUsage(mtb, RecordTypeOptions.Usage.UNION);
            }

            // Rename the message type itself.
            mtb.setName(rename.newName);
        }
    }

    /**
     * Recursively rewrites {@code typeName} field references within a message type and its nested types. For each
     * message or enum field, it resolves the original referenced type to its fully-qualified name and, if that type is
     * or is nested within any renamed type, rewrites the field’s {@code typeName} accordingly.
     */
    private static void renameRecordTypeUsagesInMessageType(
            @Nonnull DescriptorProtos.DescriptorProto.Builder messageTypeBuilder,
            @Nonnull RecordTypeRenames renames,
            @Nonnull Descriptors.Descriptor descriptorForMessage) {
        for (final DescriptorProtos.FieldDescriptorProto.Builder field : messageTypeBuilder.getFieldBuilderList()) {
            if (!field.hasTypeName() || field.getTypeName().isEmpty()) {
                continue;
            }
            final String fullReferencedName = resolveFieldTypeFullName(descriptorForMessage, field);
            if (fullReferencedName == null) {
                continue;
            }

            // Check the referenced type, then each of its containing types in turn, against the renamed types. To this
            // end we truncate `candidateName` at the last '.' to walk from the referenced type up to its outermost
            // containing type, which is equivalent to (but cheaper than) repeatedly calling `getContainingType()`.
            // For example, if `fullReferencedName` is ".pkg.Outer.Inner" and "Outer" is being renamed to "NewOuter",
            // `candidateName` first tries ".pkg.Outer.Inner" (no match), then ".pkg.Outer" (matches), yielding the
            // rewritten type name ".pkg.NewOuter" + ".Inner" = ".pkg.NewOuter.Inner".
            String candidateName = fullReferencedName;
            while (true) {
                // If `candidateName` is a renamed type, substitute its new name for the matched prefix, leaving any
                // trailing nested-type suffix (e.g., ".InnerRecord") untouched.
                final RecordTypeRename rename = renames.getByFullName(candidateName);
                if (rename != null) {
                    field.setTypeName(rename.fullNewName + fullReferencedName.substring(candidateName.length()));
                    break;
                }
                // Strip the last name segment to move up to the containing type; stop once there is no more
                // package/type prefix left (the leading '.' is always at index 0).
                final int lastDot = candidateName.lastIndexOf('.');
                if (lastDot <= 0) {
                    break;
                }
                candidateName = candidateName.substring(0, lastDot);
            }
        }

        // Recurse into nested types, since a field elsewhere in the file may reference one of them.
        for (final DescriptorProtos.DescriptorProto.Builder nestedTypeBuilder : messageTypeBuilder.getNestedTypeBuilderList()) {
            final Descriptors.Descriptor nestedDescriptor = Objects.requireNonNull(
                    descriptorForMessage.findNestedTypeByName(nestedTypeBuilder.getName()),
                    "FileDescriptor does not have nested type that exists in protobuf");
            // Recursively rewrite field type references within the nested type.
            renameRecordTypeUsagesInMessageType(nestedTypeBuilder, renames, nestedDescriptor);
        }
    }

    /**
     * Validates that {@code MetaData.user_defined_functions} is empty, since a user-defined function may be a
     * string that needs parsing to figure out the record types it references, which renaming does not support.
     * Performs no mutation, so that this can be called before any other edits are made.
     */
    private static void validateNoUserDefinedFunctions(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder) {
        if (metaDataBuilder.getUserDefinedFunctionsCount() > 0) {
            throw new MetaDataException("Renaming record types with UserDefinedFunctions is not supported");
        }
    }

    /**
     * Rewrites {@code MetaData.record_types} for every {@code RECORD}-usage rename in {@code renames}. Assumes that
     * any collision with an un-renamed type has already been ruled out upfront, by the caller.
     */
    private static void renameRecordTypeUsagesInRecordTypes(
            @Nonnull List<RecordMetaDataProto.RecordType.Builder> recordTypes,
            @Nonnull RecordTypeRenames renames) {
        for (final var recordType : recordTypes) {
            final String newName = renames.get(recordType.getName(), RecordTypeOptions.Usage.RECORD);
            if (newName != null) {
                recordType.setName(newName);
            }
        }
    }

    /**
     * Rewrites the record types referenced by any {@code MetaData.indexes} entry, for every {@code RECORD}-usage
     * rename in {@code renames}.
     */
    private static void renameRecordTypeUsagesInIndexes(@Nonnull List<RecordMetaDataProto.Index.Builder> indexes,
                                                        @Nonnull RecordTypeRenames renames) {
        for (final var index : indexes) {
            for (int i = 0; i < index.getRecordTypeCount(); i++) {
                final String newName = renames.get(index.getRecordType(i), RecordTypeOptions.Usage.RECORD);
                if (newName != null) {
                    index.setRecordType(i, newName);
                }
            }
        }
    }

    /**
     * Updates the join constituents in {@code MetaData.joined_record_types} that reference any {@code RECORD}-usage
     * rename in {@code renames}; renames of any other usage are ignored.
     */
    private static void renameRecordTypeUsagesInJoinedRecordTypes(
            @Nonnull List<RecordMetaDataProto.JoinedRecordType.Builder> joinedRecordTypes,
            @Nonnull RecordTypeRenames renames) {
        for (final var joined : joinedRecordTypes) {
            for (final var constituent : joined.getJoinConstituentsBuilderList()) {
                final RecordTypeRename rename = renames.get(constituent.getRecordType());
                if (rename != null && rename.usage == RecordTypeOptions.Usage.RECORD) {
                    constituent.setRecordType(rename.newName);
                }
            }
        }
    }

    /**
     * Validates the nested constituents of {@code MetaData.unnested_record_types} affected by any renamed type.
     * Any constituent whose type is nested within a renamed type, other than as that type’s own (non-nested)
     * constituent, causes a {@link MetaDataException}, since renaming a type used by a non-parent unnested constituent
     * is not supported.
     */
    private static void validateRecordTypeUsagesInUnnestedRecordTypes(
            @Nonnull List<RecordMetaDataProto.UnnestedRecordType.Builder> unnestedRecordTypes,
            @Nonnull Descriptors.FileDescriptor fileDescriptor,
            @Nonnull RecordTypeRenames renames) {
        for (var unnested : unnestedRecordTypes) {
            for (var constituent : unnested.getNestedConstituentsBuilderList()) {
                // The nested constituents would most likely be nested types, not record types, and thus would not be
                // renamed.
                if (constituent.getParent().isEmpty()) {
                    continue;
                }
                final String name = constituent.getTypeName();
                final Descriptors.Descriptor constituentTypeDescriptor
                        = UnnestedRecordTypeBuilder.findDescriptorByName(fileDescriptor, name);
                if (constituentTypeDescriptor == null) {
                    throw new MetaDataException("missing descriptor for nested constituent")
                            .addLogInfo(LogMessageKeys.EXPECTED, name)
                            .addLogInfo(LogMessageKeys.CONSTITUENT, constituent.getName());
                }
                // Walk up the containing-type chain. If the constituent’s type, or any type it is nested within, is
                // being renamed, this non-parent reference to it can’t be safely updated.
                for (Descriptors.Descriptor typeDescriptor = constituentTypeDescriptor;
                        typeDescriptor != null;
                        typeDescriptor = typeDescriptor.getContainingType()) {
                    if (renames.getByFullName("." + typeDescriptor.getFullName()) != null) {
                        throw new MetaDataException(
                                "Renaming types used by non-parent unnested constituents is not supported");
                    }
                }
            }
        }
    }

    /**
     * Renames the nested constituents of {@code MetaData.unnested_record_types} that directly name (as their own,
     * non-nested type) any rename in {@code renames}. Callers must have already validated the rename via
     * {@link #validateRecordTypeUsagesInUnnestedRecordTypes}.
     */
    private static void renameRecordTypeUsagesInUnnestedRecordTypes(
            @Nonnull List<RecordMetaDataProto.UnnestedRecordType.Builder> unnestedRecordTypes,
            @Nonnull RecordTypeRenames renames) {
        for (var unnested : unnestedRecordTypes) {
            for (var constituent : unnested.getNestedConstituentsBuilderList()) {
                if (constituent.getParent().isEmpty()) {
                    final RecordTypeRename rename = renames.get(constituent.getTypeName());
                    if (rename != null) {
                        constituent.setTypeName(rename.newName);
                    }
                }
            }
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
