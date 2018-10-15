/*
 * RecordMetaDataBuilder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.FormerIndex;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/**
 * A builder for {@link RecordMetaData}.
 *
 * Meta-data can be built in two ways.
 *
 * <p>
 * <b>From compiled .proto</b><br>
 * Simple single field indexes and single field primary keys can be specified in the .proto source using option extensions.
 * Additional indexes or more complicated primary keys need to be specified with code using this builder.
 * </p>
 *
 * <p>
 * <b>From a {@link RecordMetaDataProto.MetaData} Protobuf message</b><br>
 * The Protobuf form can store the complete meta-data.
 * @see RecordMetaData#toProto
 * </p>
 *
 */
public class RecordMetaDataBuilder implements RecordMetaDataProvider {

    private static final Logger logger = LoggerFactory.getLogger(RecordMetaDataBuilder.class);

    @Nonnull
    private final Descriptors.FileDescriptor recordsDescriptor;
    @Nonnull
    private final Descriptors.Descriptor unionDescriptor;
    @Nonnull
    private final Map<String, RecordTypeBuilder> recordTypes;
    @Nonnull
    private final Map<String, Index> indexes;
    @Nonnull
    private final Map<String, Index> universalIndexes;
    private boolean splitLongRecords;
    private boolean storeRecordVersions;
    private int version;
    @Nonnull
    private final List<FormerIndex> formerIndexes;
    @Nullable
    private KeyExpression recordCountKey;
    @Nullable
    private RecordMetaData recordMetaData;

    private static void validateRecords(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        Queue<Descriptors.Descriptor> toValidate = new ArrayDeque<>(fileDescriptor.getMessageTypes());
        Set<Descriptors.Descriptor> seen = new HashSet<>();
        while (!toValidate.isEmpty()) {
            Descriptors.Descriptor descriptor = toValidate.remove();
            if (seen.add(descriptor)) {
                for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
                    switch (field.getType()) {
                        case INT32:
                        case INT64:
                        case SFIXED32:
                        case SFIXED64:
                        case SINT32:
                        case SINT64:
                        case BOOL:
                        case STRING:
                        case BYTES:
                        case FLOAT:
                        case DOUBLE:
                        case ENUM:
                            // These types are allowed ; nothing to do.
                            break;
                        case MESSAGE:
                        case GROUP:
                            if (!seen.contains(field.getMessageType())) {
                                toValidate.add(field.getMessageType());
                            }
                            break;
                        case FIXED32:
                        case FIXED64:
                        case UINT32:
                        case UINT64:
                            throw new MetaDataException(
                                            "Field " + field.getName()
                                          + " in message " + descriptor.getFullName()
                                          + " has illegal unsigned type " + field.getType().name());
                        default:
                            throw new MetaDataException(
                                            "Field " + field.getName()
                                          + " in message " + descriptor.getFullName()
                                          + " has unknown type " + field.getType().name());
                    }
                }
            }
        }
    }

    /**
     * Creates a new builder from the provided protobuf.
     * @param fileDescriptor a file descriptor to be loaded, containing all the record types in the metadata
     */
    public RecordMetaDataBuilder(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        recordsDescriptor = fileDescriptor;
        recordTypes = new HashMap<>(fileDescriptor.getMessageTypes().size());
        indexes = new HashMap<>();
        universalIndexes = new HashMap<>();
        formerIndexes = new ArrayList<>();
        validateRecords(fileDescriptor);
        unionDescriptor = initRecordTypes(fileDescriptor);
        RecordMetaDataOptionsProto.SchemaOptions schemaOptions = fileDescriptor.getOptions()
                .getExtension(RecordMetaDataOptionsProto.schema);
        if ((schemaOptions != null) && schemaOptions.hasSplitLongRecords()) {
            splitLongRecords = schemaOptions.getSplitLongRecords();
        }
        if ((schemaOptions != null) && schemaOptions.hasStoreRecordVersions()) {
            storeRecordVersions = schemaOptions.getStoreRecordVersions();
        }
    }

    private Descriptors.Descriptor initRecordTypes(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        Descriptors.Descriptor union = null;
        for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {
            @Nullable Integer sinceVersion = null;
            RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = descriptor.getOptions()
                    .getExtension(RecordMetaDataOptionsProto.record);
            if (recordTypeOptions != null) {
                switch (recordTypeOptions.getUsage()) {
                    case UNION:
                        if (union != null) {
                            throw new MetaDataException("Only one union descriptor is allowed");
                        }
                        union = descriptor;
                        continue;
                    case NESTED:
                        continue;
                    case RECORD:
                    default:
                        break;
                }
                if (recordTypeOptions.hasSinceVersion()) {
                    sinceVersion = recordTypeOptions.getSinceVersion();
                }
            }
            if ("RecordTypeUnion".equals(descriptor.getName())) {
                if (union != null) {
                    throw new MetaDataException("Only one union descriptor is allowed");
                }
                union = descriptor;
                continue;
            }

            RecordTypeBuilder recordType = new RecordTypeBuilder(descriptor);
            recordType.setSinceVersion(sinceVersion);
            recordTypes.put(recordType.getName(), recordType);
            // Add indexes from custom options.
            for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
                RecordMetaDataOptionsProto.FieldOptions fieldOptions = fieldDescriptor.getOptions()
                        .getExtension(RecordMetaDataOptionsProto.field);
                if (fieldOptions != null) {
                    protoFieldOptions(recordType, descriptor, fieldDescriptor, fieldOptions);
                }
            }
        }
        if (union == null) {
            // Having an opinion here; if you don't have a union descriptor, the metadata is fixed at
            // having one record type
            throw new MetaDataException("Union descriptor is required");
        }
        return union;
    }

    @SuppressWarnings("deprecation")
    private void protoFieldOptions(RecordTypeBuilder recordType,
                                   Descriptors.Descriptor descriptor, Descriptors.FieldDescriptor fieldDescriptor,
                                   RecordMetaDataOptionsProto.FieldOptions fieldOptions) {
        if (fieldOptions.hasIndex() || fieldOptions.hasIndexed()) {
            String type;
            Map<String, String> options;
            if (fieldOptions.hasIndex()) {
                RecordMetaDataOptionsProto.FieldOptions.IndexOption indexOption = fieldOptions.getIndex();
                type = indexOption.getType();
                options = Index.buildOptions(indexOption.getOptionsList(), indexOption.getUnique());
            } else {
                type = Index.indexTypeToType(fieldOptions.getIndexed());
                options = Index.indexTypeToOptions(fieldOptions.getIndexed());
            }
            final FieldKeyExpression field = Key.Expressions.fromDescriptor(fieldDescriptor);
            final KeyExpression expr;
            if (type.equals(IndexTypes.RANK)) {
                expr = field.ungrouped();
            } else {
                expr = field;
            }
            final Index index = new Index(descriptor.getName() + "$" + fieldDescriptor.getName(),
                    expr, Index.EMPTY_VALUE, type, options);
            addIndex(recordType, index);
        } else if (fieldOptions.getPrimaryKey()) {
            if (recordType.getPrimaryKey() != null) {
                throw new MetaDataException("Only one primary key per record type is allowed have: " +
                                            recordType.getPrimaryKey() + "; adding on " + fieldDescriptor.getName());
            } else {
                if (fieldDescriptor.isRepeated()) {
                    // TODO maybe default to concatenate for this.
                    throw new MetaDataException("Primary key cannot be set on a repeated field");
                } else {
                    recordType.setPrimaryKey(Key.Expressions.fromDescriptor(fieldDescriptor));
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
    public RecordMetaDataBuilder(@Nonnull RecordMetaDataProto.MetaData metaDataProto,
                                 @Nonnull Descriptors.FileDescriptor[] dependencies) {
        this(buildFileDescriptor(metaDataProto.getRecords(), dependencies));
        validateRecords(recordsDescriptor);
        for (RecordMetaDataProto.Index indexProto : metaDataProto.getIndexesList()) {
            List<RecordTypeBuilder> recordTypeBuilders = new ArrayList<>(indexProto.getRecordTypeCount());
            for (String recordTypeName : indexProto.getRecordTypeList()) {
                recordTypeBuilders.add(getRecordType(recordTypeName));
            }
            try {
                addMultiTypeIndex(recordTypeBuilders, new Index(indexProto));
            } catch (KeyExpression.DeserializationException e) {
                throw new MetaDataProtoDeserializationException(e);
            }
        }
        for (RecordMetaDataProto.PrimaryKey primaryKey : metaDataProto.getPrimaryKeysList()) {
            try {
                getRecordType(primaryKey.getRecordType()).setPrimaryKey(KeyExpression.fromProto(primaryKey.getExpression()));
            } catch (KeyExpression.DeserializationException e) {
                throw new MetaDataProtoDeserializationException(e);
            }
        }
        if (metaDataProto.hasSplitLongRecords()) {
            splitLongRecords = metaDataProto.getSplitLongRecords();
        }
        if (metaDataProto.hasStoreRecordVersions()) {
            storeRecordVersions = metaDataProto.getStoreRecordVersions();
        }
        for (RecordMetaDataProto.FormerIndex formerIndex : metaDataProto.getFormerIndexesList()) {
            formerIndexes.add(new FormerIndex(formerIndex));
        }
        if (metaDataProto.hasRecordCountKey()) {
            try {
                recordCountKey = KeyExpression.fromProto(metaDataProto.getRecordCountKey());
            } catch (KeyExpression.DeserializationException e) {
                throw new MetaDataProtoDeserializationException(e);
            }
        }
        if (metaDataProto.hasVersion()) {
            version = metaDataProto.getVersion();
        }
    }

    private static Descriptors.FileDescriptor buildFileDescriptor(@Nonnull DescriptorProtos.FileDescriptorProto fileDescriptorProto,
                                                                  @Nonnull Descriptors.FileDescriptor[] dependencies) {
        try {
            return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies);
        } catch (Descriptors.DescriptorValidationException ex) {
            throw new MetaDataException("Error converting to protobuf", ex);
        }
    }

    @Nonnull
    public Descriptors.Descriptor getUnionDescriptor() {
        return unionDescriptor;
    }

    @Nonnull
    public Descriptors.FieldDescriptor getUnionFieldForRecordType(@Nonnull RecordType recordType) {
        final Descriptors.FieldDescriptor unionField = getUnionFieldForRecordType(recordType.getName());
        if (unionField == null) {
            throw new MetaDataException("RecordType is not in the union");
        }
        return unionField;
    }

    @Nullable
    private Descriptors.FieldDescriptor getUnionFieldForRecordType(@Nonnull String recordType) {
        for (Descriptors.FieldDescriptor field : unionDescriptor.getFields()) {
            if (field.getMessageType().getFullName().equals(recordType)) {
                return field;
            }
        }
        return null;
    }

    @Nonnull
    public RecordTypeBuilder getRecordType(@Nonnull String name) {
        RecordTypeBuilder recordType = recordTypes.get(name);
        if (recordType == null) {
            throw new MetaDataException("Unknown record type " + name);
        }
        return recordType;
    }

    @Nonnull
    public Index getIndex(@Nonnull String indexName) {
        Index index = indexes.get(indexName);
        if (null == index) {
            throw new MetaDataException("Index " + indexName + " not defined");
        }
        return index;
    }

    // Common code shared by all the methods that add indexes. It runs some validation
    // and bumps the version if necessary.
    private void addIndexCommon(@Nonnull Index index) {
        if (indexes.containsKey(index.getName())) {
            throw new MetaDataException("Index " + index.getName() + " already defined");
        }
        if (index.getVersion() <= 0) {
            index.setVersion(++version);
        } else if (index.getVersion() > version) {
            version = index.getVersion();
        }
        indexes.put(index.getName(), index);
    }

    /**
     * Adds a new index. This index can either be a universal index or an index for
     * a single record type.
     * @param recordType if null this index will exist for all record types
     * @param index the index to be added
     */
    public void addIndex(@Nullable RecordTypeBuilder recordType, @Nonnull Index index) {
        addIndexCommon(index);
        if (recordType != null) {
            recordType.getIndexes().add(index);
        } else {
            universalIndexes.put(index.getName(), index);
        }
    }

    /**
     * Adds a new index that contains multiple record types.
     * If the list is null or empty, the resulting index will include all record types.
     * If the list has one element it will just be a normal single record type index.
     * @param recordTypes a list of record types that the index will include
     * @param index the index to be added
     */
    public void addMultiTypeIndex(@Nullable List<RecordTypeBuilder> recordTypes, @Nonnull Index index) {
        addIndexCommon(index);
        if (recordTypes == null || recordTypes.size() == 0) {
            universalIndexes.put(index.getName(), index);
        } else if (recordTypes.size() == 1) {
            recordTypes.get(0).getIndexes().add(index);
        } else {
            for (RecordTypeBuilder recordType : recordTypes) {
                recordType.getMultiTypeIndexes().add(index);
            }
        }
    }

    public void removeIndex(@Nonnull String name) {
        Index index = indexes.remove(name);
        if (index == null) {
            throw new MetaDataException("No index named " + name + " defined");
        }
        for (RecordTypeBuilder recordType : recordTypes.values()) {
            recordType.getIndexes().remove(index);
            recordType.getMultiTypeIndexes().remove(index);
        }
        universalIndexes.remove(name);
        formerIndexes.add(new FormerIndex(index.getSubspaceKey(), ++version));
    }

    public boolean isSplitLongRecords() {
        return splitLongRecords;
    }

    public void setSplitLongRecords(boolean splitLongRecords) {
        if (this.splitLongRecords != splitLongRecords) {
            version += 1;
            this.splitLongRecords = splitLongRecords;
        }
    }

    public boolean isStoreRecordVersions() {
        return storeRecordVersions;
    }

    public void setStoreRecordVersions(boolean storeRecordVersions) {
        if (this.storeRecordVersions != storeRecordVersions) {
            version += 1;
            this.storeRecordVersions = storeRecordVersions;
        }
    }

    /**
     * Get the record count key, if any.
     * @return the record count key of {@code null}
     * @deprecated use {@code COUNT} type indexes instead
     */
    @Nullable
    @Deprecated
    public KeyExpression getRecordCountKey() {
        return recordCountKey;
    }

    /**
     * Set the key used for maintaining record counts.
     * @deprecated Use a <code>COUNT</code> type index instead.
     * @param recordCountKey grouping key for counting
     * @deprecated use {@code COUNT} type indexes instead
     */
    @Deprecated
    public void setRecordCountKey(KeyExpression recordCountKey) {
        if (!Objects.equals(this.recordCountKey, recordCountKey)) {
            version += 1;
            this.recordCountKey = recordCountKey;
        }
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * If there is only one record type, get it.
     * @return the only type defined for this store.
     */
    @Nonnull
    public RecordTypeBuilder getOnlyRecordType() {
        if (recordTypes.size() != 1) {
            throw new MetaDataException("Must have exactly one record type defined.");
        }
        return recordTypes.values().iterator().next();
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        if (recordMetaData != null && recordMetaData.getVersion() == version) {
            return recordMetaData;
        }
        Map<String, RecordType> recordTypeBuilders = new HashMap<>();
        recordMetaData = new RecordMetaData(recordsDescriptor, unionDescriptor, recordTypeBuilders,
                indexes, universalIndexes, formerIndexes,
                splitLongRecords, storeRecordVersions, version, recordCountKey);
        for (RecordTypeBuilder recordTypeBuilder : this.recordTypes.values()) {
            KeyExpression primaryKey = recordTypeBuilder.getPrimaryKey();
            if (primaryKey != null) {
                recordTypeBuilders.put(recordTypeBuilder.getName(), recordTypeBuilder.build(recordMetaData));
                for (Index index : recordTypeBuilder.getIndexes()) {
                    index.setPrimaryKeyComponentPositions(buildPrimaryKeyComponentPositions(index.getRootExpression(), primaryKey));
                }
            } else {
                for (Index index : recordTypeBuilder.getIndexes()) {
                    if (logger.isWarnEnabled()) {
                        logger.warn(KeyValueLogMessage.of("Created Index indexes a record type without a primary key",
                                "record_type", recordTypeBuilder.getName(),
                                "indexName", index.getName()));
                    }
                    indexes.remove(index.getName());
                }
            }
        }
        return recordMetaData;
    }

    // Note that there is no harm in this returning null for very complex overlaps; that just results in some duplication.
    @Nullable
    public static int[] buildPrimaryKeyComponentPositions(@Nonnull KeyExpression indexKey, @Nonnull KeyExpression primaryKey) {
        List<KeyExpression> indexKeys = indexKey.normalizeKeyForPositions();
        List<KeyExpression> primaryKeys = primaryKey.normalizeKeyForPositions();
        int[] positions = new int[primaryKeys.size()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = indexKeys.indexOf(primaryKeys.get(i));
        }
        if (Arrays.stream(positions).anyMatch(p -> p >= 0)) {
            return positions;
        } else {
            return null;
        }
    }

    /**
     * Exception thrown when meta-data cannot be loaded from serialized form.
     */
    @SuppressWarnings("serial")
    public static class MetaDataProtoDeserializationException extends MetaDataException {
        public MetaDataProtoDeserializationException(@Nullable Throwable cause) {
            super("Error converting from protobuf", cause);
        }
    }

}
