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
import com.google.common.base.Verify;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
     * Returns the top-level message type that contains {@code type}, or {@code type} itself if it isn’t a nested type.
     */
    @Nonnull
    private static Descriptors.Descriptor getOutermostType(@Nonnull Descriptors.Descriptor type) {
        while (type.getContainingType() != null) {
            type = type.getContainingType();
        }
        return type;
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

    /**
     * Returns the fully qualified name of the top-level type {@code name} in package {@code namespace}, in the form
     * returned by {@link Descriptors.GenericDescriptor#getFullName}.
     */
    @Nonnull
    private static String qualify(@Nonnull String namespace, @Nonnull String name) {
        return namespace.isEmpty() ? name : namespace + "." + name;
    }

    @Nonnull
    private static String fullyQualifiedTypeName(@Nonnull String namespace, @Nonnull String typeName) {
        return typeName.startsWith(".") ? typeName : "." + qualify(namespace, typeName);
    }

    @Nonnull
    private static String fullyQualifiedTypeName(@Nonnull DescriptorProtos.FileDescriptorProtoOrBuilder file,
                                                 @Nonnull String typeName) {
        return fullyQualifiedTypeName(file.getPackage(), typeName);
    }

    /**
     * Returns {@code name} with its leading {@code oldPrefix} replaced by {@code newPrefix}. Also verifies that
     * {@code name} indeed starts with {@code oldPrefix}.
     */
    @Nonnull
    private static String replacePrefix(@Nonnull String name, @Nonnull String oldPrefix, @Nonnull String newPrefix) {
        Verify.verify(name.startsWith(oldPrefix));
        return newPrefix + name.substring(oldPrefix.length());
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
     * Returns the message or enum type referenced by {@code field}, resolved against {@code messageDescriptor} by field
     * number rather than name or position, since the number is the only identifier guaranteed to tie a mutable builder
     * field to its resolved descriptor counterpart. Returns {@code null} if the field is of a primitive type, and so
     * references no named type at all.
     */
    @Nullable
    private static Descriptors.GenericDescriptor resolveFieldType(
            @Nonnull Descriptors.Descriptor messageDescriptor,
            @Nonnull DescriptorProtos.FieldDescriptorProtoOrBuilder field) {
        final Descriptors.FieldDescriptor resolvedField = Objects.requireNonNull(
                messageDescriptor.findFieldByNumber(field.getNumber()),
                "Could not find field from protobuf in descriptor");
        return switch (resolvedField.getJavaType()) {
            case MESSAGE -> resolvedField.getMessageType();
            case ENUM -> resolvedField.getEnumType();
            default -> null;
        };
    }

    /**
     * Returns the fully-qualified name of the message or enum type referenced by {@code field}, as resolved by
     * {@link #resolveFieldType}, or {@code null} if the field references no named type at all.
     */
    @Nullable
    private static String resolveFieldTypeFullName(
            @Nonnull Descriptors.Descriptor messageDescriptor,
            @Nonnull DescriptorProtos.FieldDescriptorProtoOrBuilder field) {
        final Descriptors.GenericDescriptor type = resolveFieldType(messageDescriptor, field);
        return type == null ? null : "." + type.getFullName();
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
     * Internal representation of a record type to be renamed by {@link #renameRecordType} or
     * {@link #renameRecordTypes}.
     */
    private static final class RecordTypeRename {
        /** The current name. */
        @Nonnull
        private final String name;
        /** The new name. */
        @Nonnull
        private final String newName;
        /**
         * The fully qualified current name, in the form returned by {@link Descriptors.GenericDescriptor#getFullName}.
         */
        @Nonnull
        private final String fullName;
        /**
         * The fully qualified new name, in the form returned by {@link Descriptors.GenericDescriptor#getFullName}.
         */
        @Nonnull
        private final String fullNewName;
        /** The canonical union field name for the current name, i.e., {@code _name}. */
        @Nonnull
        private final String canonicalFieldName;
        /** The canonical union field name for the new name, i.e., {@code _newName}. */
        @Nonnull
        private final String newCanonicalFieldName;
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
        /**
         * Whether {@link #unionField} is to be renamed to {@link #newCanonicalFieldName}, which is the case exactly
         * when it currently carries the canonical name for the old type name. (Initially {@code false}, to be filled
         * in by {@link #determineRecordTypeUnionFieldsAndUsages}).
         */
        private boolean renamesUnionField;

        RecordTypeRename(@Nonnull String namespace, @Nonnull String name, @Nonnull String newName) {
            this.name = name;
            this.newName = newName;
            this.fullName = qualify(namespace, name);
            this.fullNewName = qualify(namespace, newName);
            this.canonicalFieldName = canonicalUnionFieldName(name);
            this.newCanonicalFieldName = canonicalUnionFieldName(newName);
        }
    }

    /**
     * A map of {@link RecordTypeRename} objects, keyed by the current (simple) name of the record type, as built by
     * {@link #renameRecordType} or {@link #analyzeRecordTypeRenames}. Also provides a lookup by descriptor, matching
     * the fully qualified name, built lazily on first use.
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
         * Returns the rename of the type described by {@code type}, if any, matching it by its (original) fully
         * qualified name.
         */
        @Nullable
        RecordTypeRename get(@Nonnull Descriptors.GenericDescriptor type) {
            if (byFullName == null) {
                byFullName = new HashMap<>();
                for (final RecordTypeRename rename : byName.values()) {
                    byFullName.put(rename.fullName, rename);
                }
            }
            return byFullName.get(type.getFullName());
        }

        /**
         * Returns the new fully qualified name of the message type {@code type} if a rename affects it, i.e., if it is,
         * or is nested within, a renamed type; otherwise, returns {@code null}. The new name substitutes the new name
         * of the renamed outermost containing type for its old one, leaving any nested-type suffix (e.g., ".Inner")
         * untouched.
         */
        @Nullable
        String getNewFullName(@Nonnull Descriptors.Descriptor type) {
            return getNewFullName(type, type);
        }

        /**
         * Returns the new fully qualified name of the enum type {@code type} if a rename affects it, i.e., if it is
         * nested within a renamed type; otherwise, returns {@code null}. A top-level enum is never affected.
         */
        @Nullable
        String getNewFullName(@Nonnull Descriptors.EnumDescriptor type) {
            final Descriptors.Descriptor containingType = type.getContainingType();
            return containingType == null ? null : getNewFullName(type, containingType);
        }

        @Nullable
        private String getNewFullName(@Nonnull Descriptors.GenericDescriptor type,
                                      @Nonnull Descriptors.Descriptor messageType) {
            final RecordTypeRename rename = get(getOutermostType(messageType));
            return rename == null ? null : replacePrefix(type.getFullName(), rename.fullName, rename.fullNewName);
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
     * name that collides with another (renamed or unchanged) top-level type or a synthetic record type. If a collision
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
     * <p>Unlike {@code renameRecordType}, {@code renamer} is only ever applied to—and can therefore only rename—
     * {@code RECORD}-usage top-level types, i.e., those referenced by a field of the union message type. It cannot
     * rename {@code NESTED} types or the union type itself. Like {@code renameRecordType}, it rejects renaming a record
     * type with an index that only the {@code (field).index} extension declares.
     *
     * <p>Record types not backed by a top-level message type in {@code MetaData.records} cannot be renamed by this
     * metadata. That is the case for imported record types, whose message type is defined in a dependency file, and for
     * record types backed by a message type nested within another one. Since {@code renamer} is meant to apply to every
     * record type, any rename is rejected outright if the union references such a record type. (Note that
     * {@code renameRecordType} likewise rejects a rename of such a record type, with a “No record type found”
     * exception, but it can still rename the other record types of such a metadata.)
     *
     * <p>Any rename is rejected outright if the metadata declares {@code user_defined_functions}, {@code views} or
     * {@code stored_queries}. Each of those holds a string that would need parsing to figure out the record types it
     * references, so renaming cannot keep them consistent.
     *
     * <p>Validating the mapping as a whole means a batch may be accepted where the equivalent one-by-one renames would
     * fail. For example, a batch that permutes existing names, for instance swapping {@code Foo} and {@code Bar}, is
     * collision-free and therefore accepted, whereas renaming those types one at a time would collide on whichever is
     * renamed first.
     *
     * @param metadata the metadata builder
     * @param renamer a function mapping each existing top-level record type name to its new name
     * @param dependencies the dependencies of the records file descriptor
     * @see #renameRecordType
     */
    public static void renameRecordTypes(@Nonnull RecordMetaDataProto.MetaData.Builder metadata,
                                         @Nonnull UnaryOperator<String> renamer,
                                         @Nonnull Descriptors.FileDescriptor[] dependencies) {
        // Build the file descriptor exactly once, from the original `MetaData.records` proto. Every descriptor
        // lookup below is done by original name, so we can use this single descriptor for every rename in the mapping.
        final DescriptorProtos.FileDescriptorProto records = metadata.getRecords();
        final Descriptors.FileDescriptor fileDesc = RecordMetaDataBuilder.buildFileDescriptor(records, dependencies);

        // Fetch the union message type within `MetaData.records`. This is used to tell apart the record types this
        // metadata defines itself from the types it merely imports.
        final DescriptorProtos.DescriptorProto.Builder union = fetchUnionBuilder(metadata.getRecordsBuilder());
        if (union.getNestedTypeCount() > 0) {
            throw new MetaDataException("Nested types in union type not supported");
        }
        final Descriptors.Descriptor unionDescriptor = getMessageTypeByName(fileDesc, union.getName());
        final Set<String> recordTypes = unionRecordTypes(unionDescriptor);

        // Collect the renames into a map, skipping identity renames. Throws `MetaDataException` on any conflict.
        final RecordTypeRenames renames = analyzeRecordTypeRenames(metadata, renamer, recordTypes);
        if (renames.isEmpty()) {
            return;
        }

        applyRecordTypeRenames(metadata, renames, fileDesc, union, unionDescriptor);
    }

    /**
     * Applies {@code renames} to every part of the metadata. Shared by {@link #renameRecordType} and
     * {@link #renameRecordTypes}, which differ only in how they build and pre-validate the map of renames. The steps
     * are ordered so that every validation happens before the first mutation, leaving {@code metadata}
     * untouched if any of them raises {@link MetaDataException}.
     *
     * @param metadata the metadata builder to rewrite
     * @param renames the renames to apply, which must be non-empty and collision-free
     * @param fileDesc the compiled {@code MetaData.records}, as it stands before any rename has been applied
     * @param union the builder of the union message type within {@code MetaData.records}
     * @param unionDescriptor the descriptor of the union message type within {@code fileDesc}
     */
    private static void applyRecordTypeRenames(@Nonnull RecordMetaDataProto.MetaData.Builder metadata,
                                               @Nonnull RecordTypeRenames renames,
                                               @Nonnull Descriptors.FileDescriptor fileDesc,
                                               @Nonnull DescriptorProtos.DescriptorProto.Builder union,
                                               @Nonnull Descriptors.Descriptor unionDescriptor) {
        // Validate that `MetaData.user_defined_functions`, `MetaData.views` and `MetaData.stored_queries` are empty.
        validateNoUnrenamableDefinitions(metadata);

        // Resolve the type of every non-parent `MetaData.unnested_record_types` constituent.
        final Map<RecordMetaDataProto.UnnestedRecordType.NestedConstituent.Builder, Descriptors.Descriptor>
                nonParentConstituentTypes =
                resolveNonParentUnnestedConstituents(metadata.getUnnestedRecordTypesBuilderList(), fileDesc);

        // Determine the usage of each renamed type by looking at the union message type within `MetaData.records`.
        determineRecordTypeUnionFieldsAndUsages(renames, unionDescriptor, union);

        // Validate that no renamed record type has an index that only the `(field).index` extension declares.
        validateNoExtensionOnlyIndexes(metadata, renames, fileDesc);

        // Validate that renaming the canonical union fields would not cause a collision.
        validateUnionFieldRenames(union, renames);

        // Rename the canonical union field, if present, for each renamed type.
        renameUnionFields(renames);

        // Rename every message type in `MetaData.records`, and all field type references.
        renameRecordTypeUsagesInMessageTypes(metadata.getRecordsBuilder().getMessageTypeBuilderList(), renames, fileDesc);

        // Update `MetaData.record_types` for every top-level RECORD type.
        renameRecordTypeUsagesInRecordTypes(metadata.getRecordTypesBuilderList(), renames);

        // Update `MetaData.indexes` for every top-level RECORD type.
        renameRecordTypeUsagesInIndexes(metadata.getIndexesBuilderList(), renames);

        // Update `MetaData.joined_record_types` constituents for every renamed type.
        renameRecordTypeUsagesInJoinedRecordTypes(metadata.getJoinedRecordTypesBuilderList(), renames);

        // Rename `MetaData.unnested_record_types` constituents for every renamed type.
        renameRecordTypeUsagesInUnnestedRecordTypes(metadata.getUnnestedRecordTypesBuilderList(),
                nonParentConstituentTypes, renames);
    }

    /**
     * Renames a record type. This can be used to update any top-level record type defined within the metadata’s
     * records descriptor, including {@code NESTED} records or the union descriptor. However, it cannot be used to
     * rename nested messages (i.e., messages defined within other messages) or records defined in imported files.
     *
     * <p>Unlike {@link #renameRecordTypes}, which can only rename {@code RECORD}-usage top-level types, this method
     * can rename any of the three: {@code RECORD}, {@code NESTED}, or the union type itself.
     *
     * <p>The rename is validated upfront and rejected with a {@link MetaDataException} (leaving
     * {@code metaDataBuilder} untouched) if no top-level message type in the records descriptor has the name
     * {@code recordTypeName}. Otherwise, renaming the record type to its current name is a no-op, and any other rename
     * is likewise rejected if any of the following holds:
     * <ul>
     * <li>Another top-level message type or a synthetic record type already has the name {@code newRecordTypeName}.
     * <li>The type has {@code RECORD} usage, and a record type not backed by a top-level message type in the records
     *     descriptor (i.e., an imported record type, or one backed by a nested message type) already has the name
     *     {@code newRecordTypeName}.
     * <li>The type has {@code RECORD} usage and an index that only the {@code (field).index} extension declares,
     *     i.e., a field with that extension for which {@code MetaData.indexes} lists no index.
     * <li>A type other than the union would be renamed to the default union name, or is itself named that way.
     * <li>The union already has a field under the new canonical union field name {@code _newRecordTypeName}.
     * <li>The records descriptor has no union message type, or the union message type declares nested types.
     * <li>A non-parent unnested record type constituent names a type that cannot be resolved.
     * <li>The metadata declares {@code user_defined_functions}, {@code views} or {@code stored_queries}.
     * </ul>
     *
     * <p>Upon successful validation, the following edits are performed:
     * <ul>
     * <li>Message names are rewritten.
     * <li>Field types ({@code typeName}) that reference a renamed type are rewritten, whether the reference is direct
     *     or to a nested type of a renamed type.
     * <li>The union usage option is set when the union is renamed.
     * <li>The canonical {@code _typeName} union field is renamed.
     * <li>If the record type has {@code RECORD} usage, the record type list, indexes, joined record types, and the
     *     parent constituents of unnested record types are updated.
     * <li>The non-parent constituents of unnested record types whose type is, or is nested within, the renamed type
     *     are updated. This applies to a record type of any usage.
     * </ul>
     *
     * @param metaDataBuilder the metadata builder
     * @param recordTypeName the name of the existing top-level record type
     * @param newRecordTypeName the new name to give to the record type
     * @param dependencies the dependencies of the records file descriptor
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
                throw new MetaDataException("Cannot rename record type as a type of the new name already exists",
                        LogMessageKeys.RECORD_TYPE, recordTypeName,
                        LogMessageKeys.NEW_RECORD_TYPE, newRecordTypeName);
            }
        }
        if (!found) {
            throw new MetaDataException("No record type found", LogMessageKeys.RECORD_TYPE, recordTypeName);
        }

        // Identity transformation requires no work. Return before any further validation.
        if (recordTypeName.equals(newRecordTypeName)) {
            return;
        }

        // Reject a rename onto the name of a synthetic record type. Synthetic record types share the record type
        // namespace, but cannot be renamed through `renameRecordType()`, so such a rename is a collision.
        if (syntheticRecordTypeNames(metaDataBuilder).contains(newRecordTypeName)) {
            throw new MetaDataException(
                    "Cannot rename record type as a synthetic record type of the new name already exists",
                    LogMessageKeys.RECORD_TYPE, recordTypeName,
                    LogMessageKeys.NEW_RECORD_TYPE, newRecordTypeName);
        }

        final Descriptors.FileDescriptor fileDescriptor =
                RecordMetaDataBuilder.buildFileDescriptor(records, dependencies);
        final DescriptorProtos.DescriptorProto.Builder union = fetchUnionBuilder(metaDataBuilder.getRecordsBuilder());
        if (union.getNestedTypeCount() > 0) {
            throw new MetaDataException("Nested types in union type not supported");
        }
        final Descriptors.Descriptor unionDescriptor = getMessageTypeByName(fileDescriptor, union.getName());

        // Reject a rename of a RECORD-usage type onto the name of another record type. Each union field defines a
        // record type named after the simple name of the message type it references, so this also catches record
        // types not backed by a top-level message type in `MetaData.records`, i.e., imported record types and record
        // types backed by a nested message type. (A collision with a top-level message type was ruled out above.)
        // Types of any other usage don't share the record type namespace, so they cannot collide with a record type.
        if (isReferencedByUnion(unionDescriptor, fileDescriptor, recordTypeName)) {
            for (final Descriptors.FieldDescriptor unionField : unionDescriptor.getFields()) {
                if (unionField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                        && unionField.getMessageType().getName().equals(newRecordTypeName)) {
                    throw new MetaDataException(
                            "Cannot rename record type as a record type of the new name already exists",
                            LogMessageKeys.RECORD_TYPE, recordTypeName,
                            LogMessageKeys.NEW_RECORD_TYPE, newRecordTypeName);
                }
            }
        }

        final RecordTypeRename rename = new RecordTypeRename(records.getPackage(), recordTypeName, newRecordTypeName);
        applyRecordTypeRenames(metaDataBuilder, new RecordTypeRenames(Map.of(recordTypeName, rename)), fileDescriptor,
                union, unionDescriptor);
    }

    /**
     * Returns whether a field of the union message type references the top-level message type named
     * {@code recordTypeName} in {@code fileDescriptor}, which makes that message type a {@code RECORD}-usage record
     * type.
     */
    private static boolean isReferencedByUnion(@Nonnull Descriptors.Descriptor unionDescriptor,
                                               @Nonnull Descriptors.FileDescriptor fileDescriptor,
                                               @Nonnull String recordTypeName) {
        for (final Descriptors.FieldDescriptor unionField : unionDescriptor.getFields()) {
            if (unionField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                final Descriptors.Descriptor messageType = unionField.getMessageType();
                if (messageType.getFile().equals(fileDescriptor)
                        && messageType.getContainingType() == null
                        && messageType.getName().equals(recordTypeName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * A helper for {@link #renameRecordTypes} that determines the names of the record types of the metadata from the
     * fields of the union message type. Each union field defines a record type, named after the simple name of the
     * message type it references. This is aligned with how {@link RecordMetaDataBuilder} determines the record types.
     * Raises {@link MetaDataException} if the union references a record type that is not backed by a top-level message
     * type in {@code MetaData.records}, since only such a record type can be renamed here.
     *
     * @return the names of the record types, in union field order
     */
    @Nonnull
    private static Set<String> unionRecordTypes(@Nonnull Descriptors.Descriptor unionDescriptor) {
        final Descriptors.FileDescriptor file = unionDescriptor.getFile();
        final Set<String> result = new LinkedHashSet<>();
        for (final Descriptors.FieldDescriptor unionField : unionDescriptor.getFields()) {
            // Skip fields that reference no message type, such as a scalar field in a raw proto.
            if (unionField.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            // Reject an imported record type, i.e., one whose message type is defined in a dependency file rather than
            // in `MetaData.records`. Resolving the union field to the type it actually references tells it apart from a
            // local message type of the same name.
            final Descriptors.Descriptor descriptor = unionField.getMessageType();
            if (!descriptor.getFile().equals(file)) {
                throw new MetaDataException(
                        "Renaming record types with imported record types is not supported",
                        LogMessageKeys.RECORD_TYPE,
                        descriptor.getName());
            }
            // Likewise, reject a record type backed by a message type nested within another one.
            if (descriptor.getContainingType() != null) {
                throw new MetaDataException(
                        "Renaming record types with record types backed by nested message types is not supported",
                        LogMessageKeys.RECORD_TYPE,
                        descriptor.getName());
            }
            result.add(descriptor.getName());
        }
        return result;
    }

    /**
     * A helper for {@link #renameRecordTypes} that converts the record-type name mapping defined by {@code renamer}
     * into the internal {@link RecordTypeRenames} map. Also validates the mapping against the full set of top-level
     * message types, and raises {@link MetaDataException} if it is invalid.
     */
    @Nonnull
    private static RecordTypeRenames analyzeRecordTypeRenames(
            @Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
            @Nonnull UnaryOperator<String> renamer,
            @Nonnull Set<String> recordTypes) {
        final String namespace = metaDataBuilder.getRecords().getPackage();
        // Apply `renamer` to each record type, and build the map representing the renamings.
        final Map<String, RecordTypeRename> renames = new LinkedHashMap<>();
        for (final String name : recordTypes) {
            final String newName = renamer.apply(name);
            // Skip identity renames, as they require no work.
            if (name.equals(newName)) {
                continue;
            }
            renames.put(name, new RecordTypeRename(namespace, name, newName));
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
                throw new MetaDataException("Cannot rename two record types to the same name",
                        LogMessageKeys.OLD_RECORD_TYPE, previous,
                        LogMessageKeys.RECORD_TYPE, rename.name,
                        LogMessageKeys.NEW_RECORD_TYPE, rename.newName);
            }
        }

        // If a message type is not itself being renamed, then it must not be the target of a rename either. (This check
        // also covers every record type, since each record type is backed by a top-level message type, as verified
        // by `unionRecordTypes()`.)
        for (final DescriptorProtos.DescriptorProto messageType : metaDataBuilder.getRecords().getMessageTypeList()) {
            final String name = messageType.getName();
            if (!renames.containsKey(name) && inverse.containsKey(name)) {
                throw new MetaDataException("Cannot rename record type as a type of the new name already exists",
                        LogMessageKeys.RECORD_TYPE, inverse.get(name),
                        LogMessageKeys.NEW_RECORD_TYPE, name);
            }
        }

        // Synthetic record types share the record type namespace, but are not themselves renamable here, so any
        // rename targeting one of their names is a collision.
        for (final String name : syntheticRecordTypeNames(metaDataBuilder)) {
            if (inverse.containsKey(name)) {
                throw new MetaDataException(
                        "Cannot rename record type as a synthetic record type of the new name already exists",
                        LogMessageKeys.RECORD_TYPE, inverse.get(name),
                        LogMessageKeys.NEW_RECORD_TYPE, name);
            }
        }

        return new RecordTypeRenames(renames);
    }

    /**
     * Returns the names of the synthetic record types declared in the metadata, that is, of its joined and unnested
     * record types.
     */
    @Nonnull
    private static List<String> syntheticRecordTypeNames(
            @Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder) {
        final List<String> names = new ArrayList<>(
                metaDataBuilder.getJoinedRecordTypesCount() + metaDataBuilder.getUnnestedRecordTypesCount());
        for (final RecordMetaDataProto.JoinedRecordType joined : metaDataBuilder.getJoinedRecordTypesList()) {
            names.add(joined.getName());
        }
        for (final RecordMetaDataProto.UnnestedRecordType unnested : metaDataBuilder.getUnnestedRecordTypesList()) {
            names.add(unnested.getName());
        }
        return names;
    }

    /**
     * A helper for {@link #applyRecordTypeRenames} that determines the {@link RecordTypeOptions.Usage Usage} of each
     * renamed type by looking at the union. Fills in the {@code usage} and {@code unionField} of the entries
     * in {@code renames}. Does not mutate {@code unionBuilder}.
     */
    private static void determineRecordTypeUnionFieldsAndUsages(
            @Nonnull RecordTypeRenames renames,
            @Nonnull Descriptors.Descriptor unionDescriptor,
            @Nonnull DescriptorProtos.DescriptorProto.Builder unionBuilder) {
        final String unionName = unionBuilder.getName();

        // Find, for each renamed record type, the union field that references it (if any), in a single pass over the
        // union’s fields.
        for (final DescriptorProtos.FieldDescriptorProto.Builder unionField : unionBuilder.getFieldBuilderList()) {
            // Skip fields that name no type at all. Only message- and enum-typed fields carry a `type_name`; scalar
            // fields don’t. A union holding a scalar field would be malformed (though technically legal proto).
            // (Ignoring such fields here rather than actively rejecting them leaves room for potentially introducing
            // such non-record fields to the union later.)
            if (!unionField.hasTypeName() || unionField.getTypeName().isEmpty()) {
                continue;
            }

            final Descriptors.GenericDescriptor referencedType = resolveFieldType(unionDescriptor, unionField);
            if (referencedType == null) {
                continue;
            }

            final RecordTypeRename rename = renames.get(referencedType);
            if (rename == null) {
                continue;
            }

            // If multiple fields reference this record type, prefer the canonically named one; otherwise, keep the
            // one with the highest field number. This is the same rule which `RecordMetaDataBuilder.remapUnionField()`
            // uses to choose the union field that records of the type are written under. The two must be kept in sync.
            if (rename.unionField == null
                    || rename.canonicalFieldName.equals(unionField.getName())
                    || (!rename.canonicalFieldName.equals(rename.unionField.getName())
                            && unionField.getNumber() > rename.unionField.getNumber())) {
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

            // Record whether the union field will have to be renamed along with the type. That is the case only if it
            // currently carries the canonical name; a field named anything else keeps the name it has.
            rename.renamesUnionField =
                    rename.unionField != null && rename.canonicalFieldName.equals(rename.unionField.getName());

            // Prevent renaming a non-UNION type to the default union name.
            if (!rename.usage.equals(RecordTypeOptions.Usage.UNION) && rename.newName.equals(DEFAULT_UNION_NAME)) {
                throw new MetaDataException(
                        "Cannot rename record type to the default union name",
                        LogMessageKeys.RECORD_TYPE, rename.name);
            }

            // Likewise, prevent renaming a non-UNION type that for some reason has the default union name. Such a type
            // is indistinguishable from the union by name alone, so renaming it would flip its `record.usage` option to
            // UNION and leave the records descriptor with two types claiming to be the union. (This case can only be
            // reached with a raw proto, since `RecordMetaDataBuilder` rejects such a descriptor outright.)
            if (!rename.usage.equals(RecordTypeOptions.Usage.UNION) && rename.name.equals(DEFAULT_UNION_NAME)) {
                throw new MetaDataException(
                        "Cannot rename a non-union record type that has the default union name",
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
            if (rename.renamesUnionField) {
                beingRenamed.add(rename.unionField);
            }
        }

        // Index the names that the union’s fields will still be holding afterwards, so that each rename below can be
        // checked against them in constant time.
        final Set<String> retainedFieldNames = new HashSet<>();
        for (final DescriptorProtos.FieldDescriptorProto.Builder field : unionBuilder.getFieldBuilderList()) {
            if (!beingRenamed.contains(field)) {
                retainedFieldNames.add(field.getName());
            }
        }

        // No two renames can target the same new canonical field name, since the new type names are distinct, so a
        // collision can only be with a retained name.
        for (final RecordTypeRename rename : renames.values()) {
            if (rename.renamesUnionField && retainedFieldNames.contains(rename.newCanonicalFieldName)) {
                throw new MetaDataException(
                        "Cannot rename union field because a field of the new name already exists",
                        LogMessageKeys.RECORD_TYPE, rename.name,
                        LogMessageKeys.NEW_FIELD_NAME, rename.newCanonicalFieldName);
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
            if (rename.renamesUnionField) {
                Objects.requireNonNull(rename.unionField).setName(rename.newCanonicalFieldName);
            }
        }
    }

    /**
     * A helper for {@link #applyRecordTypeRenames} that applies the name mapping in a single walk over the message
     * types in {@code MetaData.records}, using the given compiled file descriptor for type resolution. Field type
     * references are resolved (via the original descriptor) to the original type they point at; if that original type
     * is, or is nested within, any renamed type, the reference is rewritten to point at the renamed type.
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

            // If renaming the union type, be sure that the `record.usage` option is set to UNION. Note that we detect
            // this from the `usage` rather than via `name.equals(DEFAULT_UNION_NAME)`. This is to prevent a type that is
            // named DEFAULT_UNION_NAME for some reason from being mislabelled. (Though such a type is normally
            // rejected upfront by `determineRecordTypeUnionFieldsAndUsages()`.)
            if (rename.usage.equals(RecordTypeOptions.Usage.UNION)
                    && getMessageTypeUsage(mtb) != RecordTypeOptions.Usage.UNION) {
                setMessageTypeUsage(mtb, RecordTypeOptions.Usage.UNION);
            }

            // Rename the message type itself.
            mtb.setName(rename.newName);
        }
    }

    /**
     * Recursively rewrites {@code typeName} field references within a message type and its nested types. For each
     * message or enum field, it resolves the referenced type and, if the outermost type containing it is renamed,
     * rewrites the field’s {@code typeName} accordingly.
     */
    private static void renameRecordTypeUsagesInMessageType(
            @Nonnull DescriptorProtos.DescriptorProto.Builder messageTypeBuilder,
            @Nonnull RecordTypeRenames renames,
            @Nonnull Descriptors.Descriptor descriptorForMessage) {
        for (final DescriptorProtos.FieldDescriptorProto.Builder field : messageTypeBuilder.getFieldBuilderList()) {
            final Descriptors.GenericDescriptor referencedType = resolveFieldType(descriptorForMessage, field);
            final String newFullName;
            if (referencedType instanceof Descriptors.Descriptor messageType) {
                newFullName = renames.getNewFullName(messageType);
            } else if (referencedType instanceof Descriptors.EnumDescriptor enumType) {
                newFullName = renames.getNewFullName(enumType);
            } else {
                // The field references no named type at all.
                continue;
            }
            if (newFullName != null) {
                // Note: A leading '.' indicates a fully qualified name in a `type_name`.
                field.setTypeName("." + newFullName);
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
     * Validates that no {@code RECORD}-usage rename in {@code renames} affects an index that only the
     * {@code (field).index} extension declares, i.e., one for which {@code MetaData.indexes} lists no index. When the
     * metadata is built with extension options processed, {@link RecordMetaDataBuilder} derives such an index from a
     * top-level field of the record type and names it {@code RecordType$field}. Since the subspace key of an index
     * defaults to its name, renaming the record type would turn such an index into a different index, whose existing
     * entries are no longer found. (An index that {@code MetaData.indexes} lists keeps its name across the rename.)
     */
    @SuppressWarnings("deprecation") // for `FieldOptions.hasIndexed()`, which `RecordMetaDataBuilder` still honors
    private static void validateNoExtensionOnlyIndexes(@Nonnull RecordMetaDataProto.MetaData.Builder metadata,
                                                       @Nonnull RecordTypeRenames renames,
                                                       @Nonnull Descriptors.FileDescriptor fileDesc) {
        Set<String> listedIndexNames = null;
        for (final RecordTypeRename rename : renames.values()) {
            if (rename.usage != RecordTypeOptions.Usage.RECORD) {
                continue;
            }
            for (final Descriptors.FieldDescriptor field : getMessageTypeByName(fileDesc, rename.name).getFields()) {
                final RecordMetaDataOptionsProto.FieldOptions fieldOptions =
                        field.getOptions().getExtension(RecordMetaDataOptionsProto.field);
                if (!fieldOptions.hasIndex() && !fieldOptions.hasIndexed()) {
                    continue;
                }
                if (listedIndexNames == null) {
                    listedIndexNames = new HashSet<>();
                    for (final RecordMetaDataProto.Index index : metadata.getIndexesList()) {
                        listedIndexNames.add(index.getName());
                    }
                }
                // This is the name `RecordMetaDataBuilder.protoFieldOptions()` gives an index derived from the field.
                final String indexName = rename.name + "$" + field.getName();
                if (!listedIndexNames.contains(indexName)) {
                    throw new MetaDataException(
                            "Cannot rename record type with an index that only the field index extension declares",
                            LogMessageKeys.RECORD_TYPE, rename.name,
                            LogMessageKeys.INDEX_NAME, indexName);
                }
            }
        }
    }

    /**
     * Validates that {@code MetaData.user_defined_functions}, {@code MetaData.views} and
     * {@code MetaData.stored_queries} are all empty. Each of them holds a string that would need parsing to figure out
     * the record types it references, which renaming does not support.
     */
    private static void validateNoUnrenamableDefinitions(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder) {
        if (metaDataBuilder.getUserDefinedFunctionsCount() > 0) {
            throw new MetaDataException("Renaming record types with UserDefinedFunctions is not supported");
        }
        if (metaDataBuilder.getViewsCount() > 0) {
            throw new MetaDataException("Renaming record types with views is not supported");
        }
        if (metaDataBuilder.getStoredQueriesCount() > 0) {
            throw new MetaDataException("Renaming record types with stored queries is not supported");
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
     * Resolves the type of every non-parent constituent of {@code MetaData.unnested_record_types}, so that
     * {@link #renameRecordTypeUsagesInUnnestedRecordTypes} can rewrite it if it is affected by a rename. Raises
     * {@link MetaDataException} if a type cannot be resolved.
     *
     * @return the resolved type of each non-parent constituent, keyed by the constituent builder (by identity)
     */
    @Nonnull
    private static Map<RecordMetaDataProto.UnnestedRecordType.NestedConstituent.Builder, Descriptors.Descriptor>
            resolveNonParentUnnestedConstituents(
                    @Nonnull List<RecordMetaDataProto.UnnestedRecordType.Builder> unnestedRecordTypes,
                    @Nonnull Descriptors.FileDescriptor fileDescriptor) {
        final Map<RecordMetaDataProto.UnnestedRecordType.NestedConstituent.Builder, Descriptors.Descriptor> result =
                new IdentityHashMap<>();
        for (var unnested : unnestedRecordTypes) {
            for (var constituent : unnested.getNestedConstituentsBuilderList()) {
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
                result.put(constituent, constituentTypeDescriptor);
            }
        }
        return result;
    }

    /**
     * Renames the constituents of {@code MetaData.unnested_record_types} affected by any rename in {@code renames}.
     * The parent constituent names a {@code RECORD}-usage type by its simple name; a non-parent constituent names a
     * message type by its fully qualified name, which also changes if the type is nested within a renamed type.
     * The types of the non-parent constituents are as resolved by {@link #resolveNonParentUnnestedConstituents}.
     */
    private static void renameRecordTypeUsagesInUnnestedRecordTypes(
            @Nonnull List<RecordMetaDataProto.UnnestedRecordType.Builder> unnestedRecordTypes,
            @Nonnull Map<RecordMetaDataProto.UnnestedRecordType.NestedConstituent.Builder, Descriptors.Descriptor>
                    nonParentConstituentTypes,
            @Nonnull RecordTypeRenames renames) {
        for (var unnested : unnestedRecordTypes) {
            for (var constituent : unnested.getNestedConstituentsBuilderList()) {
                if (constituent.getParent().isEmpty()) {
                    final String newName = renames.get(constituent.getTypeName(), RecordTypeOptions.Usage.RECORD);
                    if (newName != null) {
                        constituent.setTypeName(newName);
                    }
                }
            }
        }
        for (final var entry : nonParentConstituentTypes.entrySet()) {
            final String newFullName = renames.getNewFullName(entry.getValue());
            if (newFullName != null) {
                entry.getKey().setTypeName(newFullName);
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
