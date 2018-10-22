/*
 * RecordMetaData.java
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

import com.apple.foundationdb.record.metadata.FormerIndex;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Meta-data for Record Layer record stores.
 *
 * Records are represented using Protobuf {@link com.google.protobuf.Message}s.
 * Each message {@link com.google.protobuf.Descriptors.Descriptor} corresponds to a {@link RecordType}.
 * All message types in the database come from a single {@link com.google.protobuf.Descriptors.FileDescriptor}.
 * The Protobuf file must also define a union message type (conventionally named {@code RecordTypeUnion}) with fields for
 * each of the possible record types.
 * When serializing, the record is put in the corresponding field and the whole saved as a byte string.
 * Then when deserializing, the returned record can be of any of the allowed types.
 *
 * Meta-data can also define any number of secondary {@link Index}es for record types.
 *
 * @see RecordMetaDataBuilder
 */
public class RecordMetaData implements RecordMetaDataProvider {
    @Nonnull
    private final Descriptors.FileDescriptor recordsDescriptor;
    @Nonnull
    private final Descriptors.Descriptor unionDescriptor;
    @Nonnull
    private final Map<Descriptors.Descriptor, Descriptors.FieldDescriptor> unionFields;
    @Nonnull
    private final Map<String, RecordType> recordTypes;
    @Nonnull
    private final Map<String, Index> indexes;
    @Nonnull
    private Map<String, Index> universalIndexes;
    @Nonnull
    private final List<FormerIndex> formerIndexes;
    private final boolean splitLongRecords;
    private final boolean storeRecordVersions;
    private final int version;
    @Nullable
    private final KeyExpression recordCountKey;

    @SuppressWarnings("squid:S00107") // There is a Builder.
    protected RecordMetaData(@Nonnull Descriptors.FileDescriptor recordsDescriptor,
                             @Nonnull Descriptors.Descriptor unionDescriptor,
                             @Nonnull Map<Descriptors.Descriptor, Descriptors.FieldDescriptor> unionFields,
                             @Nonnull Map<String, RecordType> recordTypes,
                             @Nonnull Map<String, Index> indexes,
                             @Nonnull Map<String, Index> universalIndexes,
                             @Nonnull List<FormerIndex> formerIndexes,
                             boolean splitLongRecords,
                             boolean storeRecordVersions,
                             int version,
                             @Nullable KeyExpression recordCountKey) {
        this.recordsDescriptor = recordsDescriptor;
        this.unionDescriptor = unionDescriptor;
        this.unionFields = unionFields;
        this.recordTypes = recordTypes;
        this.indexes = indexes;
        this.universalIndexes = universalIndexes;
        this.formerIndexes = formerIndexes;
        this.splitLongRecords = splitLongRecords;
        this.storeRecordVersions = storeRecordVersions;
        this.version = version;
        this.recordCountKey = recordCountKey;
    }

    @Nonnull
    public Descriptors.FileDescriptor getRecordsDescriptor() {
        return recordsDescriptor;
    }

    @Nonnull
    public Descriptors.Descriptor getUnionDescriptor() {
        return unionDescriptor;
    }

    @Nonnull
    public Descriptors.FieldDescriptor getUnionFieldForRecordType(@Nonnull RecordType recordType) {
        final Descriptors.FieldDescriptor unionField = unionFields.get(recordType.getDescriptor());
        if (unionField == null) {
            throw new MetaDataException("Record type " + recordType.getName() + " is not in the union");
        }
        return unionField;
    }

    @Nonnull
    public Map<String, RecordType> getRecordTypes() {
        return recordTypes;
    }

    @Nonnull
    public RecordType getRecordType(@Nonnull String name) {
        RecordType recordType = recordTypes.get(name);
        if (recordType == null) {
            throw new MetaDataException("Unknown record type " + name);
        }
        return recordType;
    }

    @Nonnull
    public RecordType getRecordTypeForDescriptor(@Nonnull Descriptors.Descriptor descriptor) {
        RecordType recordType = getRecordType(descriptor.getName());
        if (recordType.getDescriptor() != descriptor) {
            throw new MetaDataException("descriptor did not match record type");
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

    public boolean hasIndex(@Nonnull String indexName) {
        return indexes.get(indexName) != null;
    }

    @Nonnull
    public List<Index> getAllIndexes() {
        return new ArrayList<>(indexes.values());
    }

    @Nonnull
    public Index getUniversalIndex(@Nonnull String indexName) {
        Index index = universalIndexes.get(indexName);
        if (null == index) {
            throw new MetaDataException("Index " + indexName + " not defined");
        }
        return index;
    }

    public boolean hasUniversalIndex(@Nonnull String indexName) {
        return universalIndexes.get(indexName) != null;
    }

    @Nonnull
    public List<Index> getUniversalIndexes() {
        return new ArrayList<>(universalIndexes.values());
    }

    public List<FormerIndex> getFormerIndexes() {
        return formerIndexes;
    }

    public boolean isSplitLongRecords() {
        return splitLongRecords;
    }

    public boolean isStoreRecordVersions() {
        return storeRecordVersions;
    }

    public int getVersion() {
        return version;
    }

    public List<FormerIndex> getFormerIndexesSince(int version) {
        List<FormerIndex> result = new ArrayList<>();
        for (FormerIndex formerIndex : formerIndexes) {
            if (formerIndex.getVersion() > version) {
                result.add(formerIndex);
            }
        }
        return result;
    }

    public Map<Index, List<RecordType>> getIndexesSince(int version) {
        Map<Index, List<RecordType>> result = new HashMap<>();
        for (RecordType recordType : recordTypes.values()) {
            for (Index index : recordType.getIndexes()) {
                if (index.getVersion() > version) {
                    result.put(index, Collections.singletonList(recordType));
                }
            }
            for (Index index : recordType.getMultiTypeIndexes()) {
                if (index.getVersion() > version) {
                    if (!result.containsKey(index)) {
                        result.put(index, new ArrayList<>());
                    }
                    result.get(index).add(recordType);
                }
            }
        }
        for (Index index : universalIndexes.values()) {
            if (index.getVersion() > version) {
                result.put(index, null);
            }
        }
        return result;
    }

    @Nonnull
    public Collection<RecordType> recordTypesForIndex(@Nonnull Index index) {
        if (getUniversalIndexes().contains(index)) {
            return getRecordTypes().values();
        }
        List<RecordType> result = new ArrayList<>();
        for (RecordType recordType : getRecordTypes().values()) {
            if (recordType.getIndexes().contains(index)) {
                return Collections.singletonList(recordType);
            } else if (recordType.getMultiTypeIndexes().contains(index)) {
                result.add(recordType);
            }
        }
        return result;
    }

    @Nullable
    public KeyExpression getRecordCountKey() {
        return recordCountKey;
    }

    /**
     * Determine whether every record type in this meta-data has {@link RecordType#primaryKeyHasRecordTypePrefix}.
     *
     * If so, records are strictly partitioned by record type.
     * @return {@code true} if every record type has a record type prefix on the primary key
     */
    public boolean primaryKeyHasRecordTypePrefix() {
        return recordTypes.values().stream().allMatch(RecordType::primaryKeyHasRecordTypePrefix);
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return this;
    }

    public static RecordMetaData build(Descriptors.FileDescriptor descriptor) {
        return new RecordMetaDataBuilder(descriptor).getRecordMetaData();
    }

    @Nonnull
    @SuppressWarnings("deprecation")
    public RecordMetaDataProto.MetaData toProto() throws KeyExpression.SerializationException {
        RecordMetaDataProto.MetaData.Builder builder = RecordMetaDataProto.MetaData.newBuilder();
        builder.setRecords(recordsDescriptor.toProto());

        // Create builders for each index so that we can then add associated record types (etc.).
        Map<String, RecordMetaDataProto.Index.Builder> indexBuilders = new TreeMap<>();
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            indexBuilders.put(entry.getKey(), entry.getValue().toProto().toBuilder());
        }

        for (RecordType recordType : getRecordTypes().values()) {
            // Add this record type to each appropriate index.
            for (Index index : recordType.getIndexes()) {
                indexBuilders.get(index.getName()).addRecordType(recordType.getName());
            }
            for (Index index : recordType.getMultiTypeIndexes()) {
                indexBuilders.get(index.getName()).addRecordType(recordType.getName());
            }

            RecordMetaDataProto.RecordType.Builder typeBuilder = builder.addRecordTypesBuilder()
                    .setName(recordType.getName())
                    .setPrimaryKey(recordType.getPrimaryKey().toKeyExpression());
            if (recordType.getSinceVersion() != null) {
                typeBuilder.setSinceVersion(recordType.getSinceVersion());
            }
            if (recordType.hasExplicitRecordTypeKey()) {
                typeBuilder.setExplicitKey(LiteralKeyExpression.toProtoValue(recordType.getExplicitRecordTypeKey()));
            }
        }
        indexBuilders.values().forEach(builder::addIndexes);

        // Add in the former indexes.
        for (FormerIndex formerIndex : getFormerIndexes()) {
            builder.addFormerIndexes(formerIndex.toProto());
        }

        // Add in the final options.
        builder.setSplitLongRecords(splitLongRecords);
        builder.setStoreRecordVersions(storeRecordVersions);
        builder.setVersion(version);
        if (recordCountKey != null) {
            builder.setRecordCountKey(recordCountKey.toKeyExpression());
        }

        return builder.build();
    }
}
