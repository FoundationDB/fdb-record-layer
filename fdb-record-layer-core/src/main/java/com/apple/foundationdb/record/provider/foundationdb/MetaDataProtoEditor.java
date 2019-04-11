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
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Consumer;

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
    @Nonnull
    public static void addRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder, @Nonnull DescriptorProtos.DescriptorProto newRecordType, @Nonnull KeyExpression primaryKey) {
        RecordMetaDataOptionsProto.RecordTypeOptions.Usage newRecordTypeUsage = getMessageTypeUsage(newRecordType);
        if (newRecordType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME) ||
                newRecordTypeUsage == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION) {
            throw new MetaDataException("Adding UNION record type not allowed");
        }
        if (newRecordTypeUsage == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED) {
            throw new MetaDataException("Use addNestedRecordType for adding NESTED record types");
        }
        if (findMessageTypeByName(metaDataBuilder.getRecordsBuilder(), newRecordType.getName()) != null) {
            throw new MetaDataException("Record type " + newRecordType.getName() + " already exists");
        }
        metaDataBuilder.getRecordsBuilder().addMessageType(newRecordType);
        metaDataBuilder.setVersion(metaDataBuilder.getVersion() + 1);
        metaDataBuilder.addRecordTypes(RecordMetaDataProto.RecordType.newBuilder()
                .setName(newRecordType.getName())
                .setPrimaryKey(primaryKey.toKeyExpression())
                .setSinceVersion(metaDataBuilder.getVersion())
                .build());
        addFieldToUnion(fetchUnionBuilder(metaDataBuilder.getRecordsBuilder()), newRecordType.getName(), newRecordType.getName());
    }

    private static void addFieldToUnion(@Nonnull DescriptorProtos.DescriptorProto.Builder unionBuilder, @Nonnull String name, @Nonnull String typeName) {
        if (unionBuilder.getOneofDeclCount() > 0) {
            throw new MetaDataException("Adding record type to oneof is not allowed");
        }
        DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setName("_" + name)
                .setTypeName(typeName)
                .setNumber(assignFieldNumber(unionBuilder));
        unionBuilder.addField(fieldBuilder);
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

    private static boolean isUnion(@Nonnull DescriptorProtos.DescriptorProto.Builder messageTypeBuilder) {
        if (messageTypeBuilder.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
            return true;
        }
        RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = messageTypeBuilder.getOptions()
                .getExtension(RecordMetaDataOptionsProto.record);
        return recordTypeOptions != null &&
               recordTypeOptions.hasUsage() &&
               recordTypeOptions.getUsage() == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION;
    }

    private static boolean isUnion(@Nonnull Descriptors.Descriptor messageType) {
        if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
            return true;
        }
        RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = messageType.getOptions()
                .getExtension(RecordMetaDataOptionsProto.record);
        return recordTypeOptions != null &&
               recordTypeOptions.hasUsage() &&
               recordTypeOptions.getUsage() == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION;
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
    @Nonnull
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
    private static RecordMetaDataOptionsProto.RecordTypeOptions.Usage getMessageTypeUsage(@Nonnull DescriptorProtos.DescriptorProto messageType) {
        RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = messageType.getOptions()
                .getExtension(RecordMetaDataOptionsProto.record);
        if (recordTypeOptions != null && recordTypeOptions.hasUsage()) {
            return recordTypeOptions.getUsage();
        }
        return RecordMetaDataOptionsProto.RecordTypeOptions.Usage.RECORD;
    }

    @Nullable
    private static DescriptorProtos.DescriptorProto.Builder findMessageTypeByName(@Nonnull DescriptorProtos.FileDescriptorProto.Builder recordsBuilder, @Nonnull String recordType) {
        return recordsBuilder.getMessageTypeBuilderList().stream().filter(m -> m.getName().equals(recordType)).findAny().orElse(null);
    }

    /**
     * Deprecate a record type from the meta-data.
     *
     * @param metaDataBuilder the meta-data builder
     * @param recordType the record type to be deprecated
     */
    @Nonnull
    public static void deprecateRecordType(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder, @Nonnull String recordType) {
        DescriptorProtos.DescriptorProto.Builder unionBuilder = fetchUnionBuilder(metaDataBuilder.getRecordsBuilder());
        if (unionBuilder.getName().equals(recordType)) {
            throw new MetaDataException("Cannot deprecate the union");
        }
        // deprecate all fields of type recordType from the union.
        boolean found = false;
        for (DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder : unionBuilder.getFieldBuilderList()) {
            if (fieldBuilder.getTypeName().equals(recordType)) {
                setDeprecated(fieldBuilder);
                found = true;
            }
        }
        if (!found) {
            throw new MetaDataException("Record type " + recordType + " not found");
        }
    }

    /**
     * Add a field to a record type.
     *
     * @param metaDataBuilder the meta-data builder
     * @param recordType the record type to add the field to
     * @param field the field to be added
     */
    @Nonnull
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
    @Nonnull
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
        fileBuilder.addMessageType(createDefaultUnion(fileDescriptor));
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
        for (Descriptors.Descriptor messageType : fileDescriptor.getMessageTypes()) {
            RecordMetaDataOptionsProto.RecordTypeOptions.Usage messageTypeUsage = getMessageTypeUsage(messageType.toProto());
            if (messageTypeUsage != RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED) {
                if (!hasField(unionDescriptorBuilder, messageType)) {
                    addFieldToUnion(unionDescriptorBuilder, messageType.getName(), getFullyQualifiedFullName(messageType));
                }
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
    private static DescriptorProtos.DescriptorProto.Builder createDefaultUnion(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        DescriptorProtos.DescriptorProto.Builder unionMessageType = DescriptorProtos.DescriptorProto.newBuilder();
        unionMessageType.setName(RecordMetaDataBuilder.DEFAULT_UNION_NAME);
        for (Descriptors.Descriptor messageType : fileDescriptor.getMessageTypes()) {
            RecordMetaDataOptionsProto.RecordTypeOptions.Usage messageTypeUsage = getMessageTypeUsage(messageType.toProto());
            if (messageTypeUsage != RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED) {
                addFieldToUnion(unionMessageType, messageType.getName(), getFullyQualifiedFullName(messageType));
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
                unionMessageType.addField(field.toProto().toBuilder().setTypeName(getFullyQualifiedFullName(messageType)));
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

    private static boolean hasField(@Nonnull DescriptorProtos.DescriptorProto.Builder builder, @Nonnull Descriptors.Descriptor messageType) {
        for (DescriptorProtos.FieldDescriptorProto field : builder.getFieldList()) {
            // TODO: this should call fieldHasType once issue https://github.com/FoundationDB/fdb-record-layer/issues/508 is closed.
            if (field.getTypeName().equals(getFullyQualifiedFullName(messageType))) {
                return true;
            }
        }
        return false;
    }

    private static String getFullyQualifiedFullName(@Nonnull Descriptors.Descriptor messageType) {
        return "." + messageType.getFullName();
    }
}
