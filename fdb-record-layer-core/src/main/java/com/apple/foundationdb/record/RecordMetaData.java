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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.FormerIndex;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.JoinedRecordType;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;
import com.apple.foundationdb.record.util.MapUtils;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
@API(API.Status.MAINTAINED)
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
    private final Map<String, SyntheticRecordType<?>> syntheticRecordTypes;
    @Nonnull
    private final Map<Object, SyntheticRecordType<?>> recordTypeKeyToSyntheticTypeMap;
    @Nonnull
    private final Map<String, Index> indexes;
    @Nonnull
    private final Map<String, Index> universalIndexes;
    @Nonnull
    private final List<FormerIndex> formerIndexes;
    private final boolean splitLongRecords;
    private final boolean storeRecordVersions;
    private final int version;
    private final long subspaceKeyCounter;
    private final boolean usesSubspaceKeyCounter;
    @Nullable
    private final KeyExpression recordCountKey;
    private final boolean usesLocalRecordsDescriptor;
    private final Map<Index, Collection<RecordType>> recordTypesForIndex;

    private static final Descriptors.FileDescriptor[] defaultExcludedDependencies = {
            RecordMetaDataProto.getDescriptor(), RecordMetaDataOptionsProto.getDescriptor(), TupleFieldsProto.getDescriptor()
    };

    protected RecordMetaData(@Nonnull RecordMetaData orig) {
        this(orig.getRecordsDescriptor(),
                orig.getUnionDescriptor(),
                Collections.unmodifiableMap(orig.unionFields),
                Collections.unmodifiableMap(orig.recordTypes),
                Collections.unmodifiableMap(orig.syntheticRecordTypes),
                Collections.unmodifiableMap(orig.recordTypeKeyToSyntheticTypeMap),
                Collections.unmodifiableMap(orig.indexes),
                Collections.unmodifiableMap(orig.universalIndexes),
                Collections.unmodifiableList(orig.formerIndexes),
                orig.splitLongRecords,
                orig.storeRecordVersions,
                orig.version,
                orig.subspaceKeyCounter,
                orig.usesSubspaceKeyCounter,
                orig.recordCountKey,
                orig.usesLocalRecordsDescriptor);
    }

    @SuppressWarnings("squid:S00107") // There is a Builder.
    protected RecordMetaData(@Nonnull Descriptors.FileDescriptor recordsDescriptor,
                             @Nonnull Descriptors.Descriptor unionDescriptor,
                             @Nonnull Map<Descriptors.Descriptor, Descriptors.FieldDescriptor> unionFields,
                             @Nonnull Map<String, RecordType> recordTypes,
                             @Nonnull Map<String, SyntheticRecordType<?>> syntheticRecordTypes,
                             @Nonnull Map<Object, SyntheticRecordType<?>> recordTypeKeyToSyntheticTypeMap,
                             @Nonnull Map<String, Index> indexes,
                             @Nonnull Map<String, Index> universalIndexes,
                             @Nonnull List<FormerIndex> formerIndexes,
                             boolean splitLongRecords,
                             boolean storeRecordVersions,
                             int version,
                             long subspaceKeyCounter,
                             boolean usesSubspaceKeyCounter,
                             @Nullable KeyExpression recordCountKey,
                             boolean usesLocalRecordsDescriptor) {
        this.recordsDescriptor = recordsDescriptor;
        this.unionDescriptor = unionDescriptor;
        this.unionFields = unionFields;
        this.recordTypes = recordTypes;
        this.syntheticRecordTypes = syntheticRecordTypes;
        this.recordTypeKeyToSyntheticTypeMap = recordTypeKeyToSyntheticTypeMap;
        this.indexes = indexes;
        this.universalIndexes = universalIndexes;
        this.formerIndexes = formerIndexes;
        this.splitLongRecords = splitLongRecords;
        this.storeRecordVersions = storeRecordVersions;
        this.version = version;
        this.subspaceKeyCounter = subspaceKeyCounter;
        this.usesSubspaceKeyCounter = usesSubspaceKeyCounter;
        this.recordCountKey = recordCountKey;
        this.usesLocalRecordsDescriptor = usesLocalRecordsDescriptor;
        this.recordTypesForIndex = new ConcurrentHashMap<>();
    }

    /**
     * Creates an instance of {@link RecordMetaDataBuilder}.
     * @return a new builder
     */
    @Nonnull
    public static RecordMetaDataBuilder newBuilder() {
        return new RecordMetaDataBuilder();
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
            throw unknownTypeException(name);
        }
        return recordType;
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RecordType getRecordTypeForDescriptor(@Nonnull Descriptors.Descriptor descriptor) {
        RecordType recordType = getRecordType(descriptor.getName());
        if (recordType.getDescriptor() != descriptor) {
            throw new MetaDataException("descriptor did not match record type");
        }
        return recordType;
    }

    /**
     * Get the record type that uses the given record type key.
     * @param recordTypeKey the key used as a prefix for some record type
     * @return the record type
     * @throws MetaDataException if the given key does not correspond to any record type
     */
    @Nonnull
    public RecordType getRecordTypeFromRecordTypeKey(@Nonnull Object recordTypeKey) {
        for (RecordType recordType : recordTypes.values()) {
            if (recordType.getRecordTypeKey().equals(recordTypeKey)) {
                return recordType;
            }
        }
        throw new MetaDataException("Unknown record type key " + recordTypeKey);
    }

    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    @SuppressWarnings("squid:S1452")
    public Map<String, SyntheticRecordType<?>> getSyntheticRecordTypes() {
        return syntheticRecordTypes;
    }

    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    @SuppressWarnings("squid:S1452")
    public SyntheticRecordType<?> getSyntheticRecordType(@Nonnull String name) {
        SyntheticRecordType<?> recordType = syntheticRecordTypes.get(name);
        if (recordType == null) {
            throw new MetaDataException("Unknown synthetic record type " + name);
        }
        return recordType;
    }

    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    @SuppressWarnings("squid:S1452")
    public SyntheticRecordType<?> getSyntheticRecordTypeFromRecordTypeKey(@Nonnull Object recordTypeKey) {
        final SyntheticRecordType<?> recordType = recordTypeKeyToSyntheticTypeMap.get(recordTypeKey);
        if (recordType == null) {
            throw new MetaDataException("Unknown synthetic record type " + recordTypeKey);
        }
        return recordType;
    }

    /**
     * Get a record type or synthetic record type by name as used in an index.
     * @param name the name of the record type
     * @return the possibly synthetic record type
     */
    public RecordType getIndexableRecordType(@Nonnull String name) {
        RecordType recordType = recordTypes.get(name);
        if (recordType == null) {
            recordType = syntheticRecordTypes.get(name);
        }
        if (recordType == null) {
            throw unknownTypeException(name);
        }
        return recordType;
    }

    /**
     * Get a record type or synthetic record type by name as used in a query.
     * @param name the name of the record type
     * @return the possibly synthetic record type
     */
    public RecordType getQueryableRecordType(@Nonnull String name) {
        RecordType recordType = recordTypes.get(name);
        if (recordType == null) {
            recordType = syntheticRecordTypes.get(name);
        }
        if (recordType == null) {
            throw unknownTypeException(name);
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

    /**
     * Get the index that uses the given subspace key.
     * @param subspaceKey the key used as a prefix for some index
     * @return the index
     * @throws MetaDataException if the given key does not correspond to any index
     */
    @Nonnull
    public Index getIndexFromSubspaceKey(@Nonnull Object subspaceKey) {
        for (Index index : indexes.values()) {
            if (index.getSubspaceKey().equals(subspaceKey)) {
                return index;
            }
        }
        throw new MetaDataException("Unknown index subspace key " + subspaceKey);
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

    /**
     * Get value of the counter used for index subspace keys if the counter-based assignment is used.
     * @return the value of the counter
     * @see RecordMetaDataBuilder#enableCounterBasedSubspaceKeys()
     */
    public long getSubspaceKeyCounter() {
        return subspaceKeyCounter;
    }

    /**
     * Checks if counter-based subspace key assignment is used.
     * @return {@code true} if the subspace key counter is used
     * @see RecordMetaDataBuilder#enableCounterBasedSubspaceKeys()
     */
    public boolean usesSubspaceKeyCounter() {
        return usesSubspaceKeyCounter;
    }

    public List<FormerIndex> getFormerIndexesSince(int version) {
        List<FormerIndex> result = new ArrayList<>();
        for (FormerIndex formerIndex : formerIndexes) {
            // An index that was added *and* removed before this version does not require any action.
            if (formerIndex.getRemovedVersion() > version && formerIndex.getAddedVersion() <= version) {
                result.add(formerIndex);
            }
        }
        return result;
    }

    /**
     * Get all indexes in the meta-data that were modified after the given version. This is useful for determining
     * which indexes have changed when the meta-data used by a given record store is updated.
     *
     * @param version the oldest version from which to look for changed indexes
     * @return a map linking each index that has been modified since the given version to the list of record
     *    types on which that index is defined
     */
    @Nonnull
    public Map<Index, List<RecordType>> getIndexesSince(int version) {
        Map<Index, List<RecordType>> result = new HashMap<>();
        for (RecordType recordType : recordTypes.values()) {
            for (Index index : recordType.getIndexes()) {
                if (index.getLastModifiedVersion() > version) {
                    result.put(index, Collections.singletonList(recordType));
                }
            }
            for (Index index : recordType.getMultiTypeIndexes()) {
                if (index.getLastModifiedVersion() > version) {
                    if (!result.containsKey(index)) {
                        result.put(index, new ArrayList<>());
                    }
                    result.get(index).add(recordType);
                }
            }
        }
        for (Index index : universalIndexes.values()) {
            if (index.getLastModifiedVersion() > version) {
                result.put(index, null);
            }
        }
        for (SyntheticRecordType<?> recordType : syntheticRecordTypes.values()) {
            for (Index index : recordType.getIndexes()) {
                if (index.getLastModifiedVersion() > version) {
                    List<RecordType> storedTypes = List.copyOf(SyntheticRecordPlanner.storedRecordTypesForIndex(this, index, List.of(recordType)));
                    result.put(index, storedTypes);
                }
            }
            for (Index index : recordType.getMultiTypeIndexes()) {
                if (index.getLastModifiedVersion() > version) {
                    if (!result.containsKey(index)) {
                        result.put(index, new ArrayList<>());
                    }
                    result.get(index).addAll(SyntheticRecordPlanner.storedRecordTypesForIndex(this, index, List.of(recordType)));
                }
            }
        }
        return result;
    }

    /**
     * Get all indexes in the meta-data that were modified after the given version that should be built. This is
     * similar to {@link #getIndexesSince(int)}, but it filters out any indexes that stores should no longer try
     * and keep built. In practice, this currently excludes any index that has
     * {@linkplain Index#getReplacedByIndexNames() replacement indexes} defined, as record stores should no longer
     * try to build those indexes, instead relying on the new indexes that replaced them.
     *
     * @param version the oldest version from which to look for changed indexes
     * @return a map linking each index that has been modified since the given version to the list of record
     *    types on which that index is defined
     * @see #getIndexesSince(int)
     * @see Index#getReplacedByIndexNames()
     * @see com.apple.foundationdb.record.metadata.IndexOptions#REPLACED_BY_OPTION_PREFIX
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    public Map<Index, List<RecordType>> getIndexesToBuildSince(int version) {
        final Map<Index, List<RecordType>> indexesToBuild = getIndexesSince(version);
        indexesToBuild.keySet().removeIf(index -> !index.getReplacedByIndexNames().isEmpty());
        return indexesToBuild;
    }

    @Nonnull
    public Collection<RecordType> recordTypesForIndex(@Nonnull Index index) {
        return MapUtils.computeIfAbsent(recordTypesForIndex, index, idx -> {
            if (hasUniversalIndex(idx.getName())) {
                return getRecordTypes().values();
            } else {
                List<RecordType> result = new ArrayList<>();
                for (RecordType recordType : getRecordTypes().values()) {
                    if (recordType.getIndexes().contains(idx)) {
                        return Collections.singletonList(recordType);
                    } else if (recordType.getMultiTypeIndexes().contains(idx)) {
                        result.add(recordType);
                    }
                }
                for (SyntheticRecordType<?> recordType : getSyntheticRecordTypes().values()) {
                    if (recordType.getIndexes().contains(idx)) {
                        return Collections.singletonList(recordType);
                    } else if (recordType.getMultiTypeIndexes().contains(idx)) {
                        result.add(recordType);
                    }
                }
                return result;
            }
        });
    }

    @Nullable
    @API(API.Status.DEPRECATED)
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

    /**
     * Determine whether every record type in this meta-data has the same primary key.
     * @return the common primary key or {@code null}
     */
    @Nullable
    public KeyExpression commonPrimaryKey() {
        return commonPrimaryKey(recordTypes.values());
    }

    @Nullable
    public static KeyExpression commonPrimaryKey(@Nonnull Collection<RecordType> recordTypes) {
        KeyExpression common = null;
        boolean first = true;
        for (RecordType recordType : recordTypes) {
            if (first) {
                common = recordType.getPrimaryKey();
                first = false;
            } else if (!common.equals(recordType.getPrimaryKey())) {
                return null;
            }
        }
        return common;
    }

    /**
     * Calculate and return the length common to all the PKs of the given types. The common PK length is calculated by
     * ensuring ALL PKs for the given types have the same length (number of fields). If there is no common length, -1 is
     * returned.
     * @param recordTypes the (non-null) collection of record types to calculate the common PK length for
     * @return the common length for all given types, -1 if not found
     */
    public static int commonPrimaryKeyLength(@Nonnull Collection<RecordType> recordTypes) {
        int common = -1;
        boolean first = true;
        for (RecordType recordType : recordTypes) {
            if (first) {
                common = recordType.getPrimaryKey().getColumnSize();
                first = false;
            } else if (common != recordType.getPrimaryKey().getColumnSize()) {
                return -1;
            }
        }
        return common;
    }

    /**
     * Get this <code>RecordMetaData</code> instance.
     *
     * @return this <code>RecordMetaData</code> instance
     */
    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return this;
    }

    @Nonnull
    public static RecordMetaData build(@Nonnull Descriptors.FileDescriptor descriptor) {
        return RecordMetaData.newBuilder().setRecords(descriptor).getRecordMetaData();
    }

    /**
     * Factory method to deserialize a record meta-data proto. It assumes that the proto contains all of the dependencies
     * and does not process extension options.
     * @param proto the serialized proto message of the {@code RecordMetaData}
     * @return the {@code RecordMetaData} object
     */
    @Nonnull
    public static RecordMetaData build(@Nonnull RecordMetaDataProto.MetaData proto) {
        return RecordMetaData.newBuilder().setRecords(proto).getRecordMetaData();
    }

    private static void getDependencies(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                        @Nonnull Map<String, Descriptors.FileDescriptor> allDependencies,
                                        @Nullable Map<String, Descriptors.FileDescriptor> excludedDependencies) {
        for (Descriptors.FileDescriptor dependency : fileDescriptor.getDependencies()) {
            if (excludedDependencies != null && excludedDependencies.containsKey(dependency.getName())) {
                // Just pass.
                continue;
            } else if (!allDependencies.containsKey(dependency.getName())) {
                allDependencies.put(dependency.getName(), dependency);
                getDependencies(dependency, allDependencies, excludedDependencies);
            } else if (!allDependencies.get(dependency.getName()).equals(dependency)) {
                throw new MetaDataException("Dependency mismatch found for file").addLogInfo(LogMessageKeys.VALUE, dependency.getName());
            }
        }
    }

    /**
     * Serializes the record meta-data to a {@code MetaData} proto message. By default, it includes all of the
     * dependencies except {@code TupleFieldsProto}, {@code RecordMetaDataOptionsProto} and {@code RecordMetaDataProto}.
     *
     * <p>
     * Note that if this record meta-data object was created with a
     * {@linkplain RecordMetaDataBuilder#setLocalFileDescriptor(Descriptors.FileDescriptor) local file descriptor},
     * then serializing the meta-data to a proto message is disallowed. This is because setting a local file descriptor
     * can change the meta-data in ways that would change its serialization, but it also will not update the meta-data
     * version. This means that if the meta-data were then saved to disk, existing clients would not be informed that
     * the meta-data had been updated.
     * </p>
     *
     * @return the serialized <code>MetaData</code> proto message
     * @throws KeyExpression.SerializationException on any serialization failures
     * @throws MetaDataException if this {@code RecordMetaData} was initialized with a
     *      {@linkplain RecordMetaDataBuilder#setLocalFileDescriptor(Descriptors.FileDescriptor) local file descriptor}
     */
    @Nonnull
    public RecordMetaDataProto.MetaData toProto() {
        return toProto(defaultExcludedDependencies);
    }

    /**
     * Serializes the record meta-data to a <code>MetaData</code> proto message. This operates like
     * {@link #toProto()} except that any dependency in the excluded list is not included in the
     * serialized proto message. If the list is set to {@code null}, then all dependencies will be serialized
     * to the proto message including those that are execluded by default.
     *
     * @param excludedDependencies a list of dependencies not to include in the serialized proto
     * @return the serialized <code>MetaData</code> proto message
     * @throws KeyExpression.SerializationException on any serialization failures
     * @throws MetaDataException if this {@code RecordMetaData} was initialized with a
     *      {@linkplain RecordMetaDataBuilder#setLocalFileDescriptor(Descriptors.FileDescriptor) local file descriptor}
     * @see #toProto()
     */
    @Nonnull
    @SuppressWarnings("deprecation")
    public RecordMetaDataProto.MetaData toProto(@Nullable Descriptors.FileDescriptor[] excludedDependencies)
            throws KeyExpression.SerializationException {
        if (usesLocalRecordsDescriptor) {
            throw new MetaDataException("cannot serialize meta-data with a local records descriptor to proto");
        }
        RecordMetaDataProto.MetaData.Builder builder = RecordMetaDataProto.MetaData.newBuilder();

        // Set the root records.
        builder.setRecords(recordsDescriptor.toProto());

        // Convert the exclusion list to a map
        Map<String, Descriptors.FileDescriptor> excludeMap = null;
        if (excludedDependencies != null) {
            excludeMap = new HashMap<>(excludedDependencies.length);
            for (Descriptors.FileDescriptor dependency : excludedDependencies) {
                excludeMap.put(dependency.getName(), dependency);
            }
        }

        // Add in the rest of dependencies.
        Map<String, Descriptors.FileDescriptor> allDependencies = new TreeMap<>();
        getDependencies(recordsDescriptor, allDependencies, excludeMap);
        for (Descriptors.FileDescriptor dependency : allDependencies.values()) {
            builder.addDependencies(dependency.toProto());
        }

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
        for (SyntheticRecordType<?> syntheticRecordType : syntheticRecordTypes.values()) {
            if (syntheticRecordType instanceof JoinedRecordType) {
                builder.addJoinedRecordTypes(((JoinedRecordType)syntheticRecordType).toProto());
            } else if (syntheticRecordType instanceof UnnestedRecordType) {
                builder.addUnnestedRecordTypes(((UnnestedRecordType)syntheticRecordType).toProto());
            }
            for (Index syntheticIndex : syntheticRecordType.getIndexes()) {
                indexBuilders.get(syntheticIndex.getName()).addRecordType(syntheticRecordType.getName());
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
        if (usesSubspaceKeyCounter()) {
            builder.setSubspaceKeyCounter(subspaceKeyCounter);
            builder.setUsesSubspaceKeyCounter(true);
        }
        if (recordCountKey != null) {
            builder.setRecordCountKey(recordCountKey.toKeyExpression());
        }

        return builder.build();
    }

    @Nonnull
    public Map<String, Descriptors.FieldDescriptor> getFieldDescriptorMapFromNames(@Nonnull final Collection<String> recordTypeNames) {
        return getFieldDescriptorMap(recordTypeNames.stream().map(this::getRecordType));
    }

    @Nonnull
    public static Map<String, Descriptors.FieldDescriptor> getFieldDescriptorMapFromTypes(@Nonnull final Collection<RecordType> recordTypes) {
        if (recordTypes.size() == 1) {
            final var recordType = Iterables.getOnlyElement(recordTypes);
            return Type.Record.toFieldDescriptorMap(recordType.getDescriptor().getFields());
        }
        return getFieldDescriptorMap(recordTypes.stream());
    }

    @Nonnull
    private static Map<String, Descriptors.FieldDescriptor> getFieldDescriptorMap(@Nonnull final Stream<RecordType> recordTypeStream) {
        // todo: should be removed https://github.com/FoundationDB/fdb-record-layer/issues/1884
        return recordTypeStream
                .sorted(Comparator.comparing(RecordType::getName))
                .flatMap(recordType -> recordType.getDescriptor().getFields().stream())
                .collect(Collectors.groupingBy(Descriptors.FieldDescriptor::getName,
                        LinkedHashMap::new,
                        Collectors.reducing(null,
                                (fieldDescriptor, fieldDescriptor2) -> {
                                    Verify.verify(fieldDescriptor != null || fieldDescriptor2 != null);
                                    if (fieldDescriptor == null) {
                                        return fieldDescriptor2;
                                    }
                                    if (fieldDescriptor2 == null) {
                                        return fieldDescriptor;
                                    }
                                    // TODO improve
                                    if (fieldDescriptor.getType().getJavaType() ==
                                            fieldDescriptor2.getType().getJavaType()) {
                                        return fieldDescriptor;
                                    }

                                    throw new IllegalArgumentException("cannot form union type of complex fields");
                                })));
    }

    @Nonnull
    private MetaDataException unknownTypeException(final @Nonnull String name) {
        return new MetaDataException("Unknown record type " + name);
    }
}
