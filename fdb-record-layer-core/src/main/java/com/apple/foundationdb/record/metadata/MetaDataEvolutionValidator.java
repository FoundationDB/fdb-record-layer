/*
 * MetaDataEvolutionValidator.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistry;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerRegistry;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class can be used to validate that the new meta-data for a record store is responsibly evolved from its
 * existing meta-data. In particular, this validates that:
 *
 * <ul>
 *     <li>No record types are dropped.</li>
 *     <li>None of the fields of any existing record type are dropped.</li>
 *     <li>None of the fields change type except in ways that preserve their serialized form in both Protobuf fields and as {@link com.apple.foundationdb.tuple.Tuple} elements.</li>
 *     <li>None of the fields change name (which is required as key expressions reference fields by name).</li>
 *     <li>None of the fields change their label (e.g., switch from {@code optional} to {@code required}.</li>
 *     <li>The new meta-data continues to split long records if the old meta-data did.</li>
 *     <li>New record types include the version in which they were introduced.</li>
 *     <li>The added and last modified versions of {@link Index}es are consistent with the two meta-data versions.</li>
 *     <li>Any index that is removed is replaced with a {@link FormerIndex}.</li>
 *     <li>The added and removed versions of {@code FormerIndex}es are consistent with the two meta-data versions.</li>
 *     <li>Any changes to existing indexes do not change the on-disk format.</li>
 * </ul>
 *
 * <p>
 * This is a relatively strict set of requirements that aims to allow anything written by a record store built with the old meta-data
 * to be readable from record stores built with the new meta-data. Some of the validation rules, however, are stricter
 * than necessary and are instead intended to prevent the user from accidentally changing the meta-data in a way that
 * will require significant work. For example, it is assumed that any change that requires that an existing index be rebuilt
 * is a mistake, but there are legitimate use cases for which this is actually the desired behavior. The user can disable
 * those checks by creating a new validator and allowing laxer standards. For example, one can create a validator that
 * allows format-incompatible changes to existing indexes by calling:
 * </p>
 *
 * <pre>{@code
 *     MetaDataEvolutionValidator.newBuilder()
 *         .setAllowIndexRebuilds(true)
 *         .build();
 * }</pre>
 *
 * <p>
 * See the <a href="https://foundationdb.github.io/fdb-record-layer/SchemaEvolution.html">Schema Evolution Guidelines</a>
 * in our documentation for more details on what changes may be safely made to the meta-data and why.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class MetaDataEvolutionValidator {
    @Nonnull
    private static final MetaDataEvolutionValidator DEFAULT_INSTANCE = new MetaDataEvolutionValidator();

    @Nonnull
    private final IndexValidatorRegistry indexValidatorRegistry;
    private final boolean allowNoVersionChange;
    private final boolean allowNoSinceVersion;
    private final boolean allowIndexRebuilds;
    private final boolean allowMissingFormerIndexNames;
    private final boolean allowOlderFormerIndexAddedVersions;
    private final boolean allowUnsplitToSplit;
    private final boolean disallowTypeRenames;

    private MetaDataEvolutionValidator() {
        this.indexValidatorRegistry = IndexMaintainerFactoryRegistryImpl.instance();
        this.allowNoVersionChange = false;
        this.allowNoSinceVersion = false;
        this.allowIndexRebuilds = false;
        this.allowMissingFormerIndexNames = false;
        this.allowOlderFormerIndexAddedVersions = false;
        this.allowUnsplitToSplit = false;
        this.disallowTypeRenames = false;
    }

    private MetaDataEvolutionValidator(@Nonnull Builder builder) {
        this.indexValidatorRegistry = builder.indexValidatorRegistry;
        this.allowNoVersionChange = builder.allowNoVersionChange;
        this.allowNoSinceVersion = builder.allowNoSinceVersion;
        this.allowIndexRebuilds = builder.allowIndexRebuilds;
        this.allowMissingFormerIndexNames = builder.allowMissingFormerIndexNames;
        this.allowOlderFormerIndexAddedVersions = builder.allowOlderFormerIndexAddedVersions;
        this.allowUnsplitToSplit = builder.allowUnsplitToSplit;
        this.disallowTypeRenames = builder.disallowTypeRenames;
    }

    /**
     * Validate that the new meta-data has been safely evolved from an older version.
     * This makes sure that existing records saved within the record store can still be read using the
     * new meta-data and it verifies that indexes are added and removed in a safe way.
     *
     * @param oldMetaData the current meta-data for one or more record stores
     * @param newMetaData the new meta-data for the same record stores
     */
    public void validate(@Nonnull RecordMetaData oldMetaData, @Nonnull RecordMetaData newMetaData) {
        if (oldMetaData.getVersion() > newMetaData.getVersion() || !allowNoVersionChange && oldMetaData.getVersion() == newMetaData.getVersion()) {
            throw new MetaDataException("new meta-data does not have newer version than old meta-data",
                    LogMessageKeys.OLD_VERSION, oldMetaData.getVersion(),
                    LogMessageKeys.NEW_VERSION, newMetaData.getVersion());
        }
        validateSchemaOptions(oldMetaData, newMetaData);
        validateUnion(oldMetaData.getUnionDescriptor(), newMetaData.getUnionDescriptor());
        Map<String, String> typeRenames = getTypeRenames(oldMetaData.getUnionDescriptor(), newMetaData.getUnionDescriptor());
        validateRecordTypes(oldMetaData, newMetaData, typeRenames);
        validateCurrentAndFormerIndexes(oldMetaData, newMetaData, typeRenames);
    }

    private void validateSchemaOptions(@Nonnull RecordMetaData oldMetaData, @Nonnull RecordMetaData newMetaData) {
        if (!allowUnsplitToSplit && !oldMetaData.isSplitLongRecords() && newMetaData.isSplitLongRecords()) {
            // Going from unsplit to split is fine assuming the record store was created
            // after FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION. However, there's no way to check
            // that here.
            throw new MetaDataException("new meta-data splits long records");
        } else if (oldMetaData.isSplitLongRecords() && !newMetaData.isSplitLongRecords()) {
            // It is not fine to go from split to unsplit because there may be unsplit data.
            throw new MetaDataException("new meta-data no longer splits long records");
        }
    }

    /**
     * Validate that the record types have all been evolved in a legal way. In particular, this makes sure that
     * each record type defined in the union descriptor is in the new union descriptor in the correct
     * place. It will then verify that each message type has been updated in a legal way, i.e., that it only
     * includes new fields.
     *
     * @param oldUnionDescriptor the union descriptor for the existing meta-data for some record store
     * @param newUnionDescriptor the new proposed meta-data
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public void validateUnion(@Nonnull Descriptor oldUnionDescriptor, @Nonnull Descriptor newUnionDescriptor) {
        if (oldUnionDescriptor == newUnionDescriptor) {
            // Don't bother validating the record types if they are all the same.
            return;
        }
        final BiMap<Descriptor, Descriptor> updatedDescriptors = HashBiMap.create(oldUnionDescriptor.getFields().size());
        final Set<NonnullPair<Descriptor, Descriptor>> seenDescriptors = new HashSet<>();

        for (FieldDescriptor oldUnionField : oldUnionDescriptor.getFields()) {
            if (!oldUnionField.getType().equals(FieldDescriptor.Type.MESSAGE)) {
                throw new MetaDataException("field in union is not a message type", LogMessageKeys.FIELD_NAME, oldUnionField.getName());
            }
            int fieldNumber = oldUnionField.getNumber();
            FieldDescriptor newUnionField = newUnionDescriptor.findFieldByNumber(fieldNumber);
            if (newUnionField != null) {
                if (!newUnionField.getType().equals(FieldDescriptor.Type.MESSAGE)) {
                    throw new MetaDataException("field in new union is not a message type", LogMessageKeys.FIELD_NAME, newUnionField.getName());
                }
                Descriptor oldRecord = oldUnionField.getMessageType();
                Descriptor newRecord = newUnionField.getMessageType();

                // Verify that all fields of the same type in the old union are also of the same type
                // in the new union (i.e., that there are no "splits" or "merges" of record types).
                Descriptor alreadySeenNewRecord = updatedDescriptors.get(oldRecord);
                if (alreadySeenNewRecord != null) {
                    if (alreadySeenNewRecord != newRecord) {
                        // A "split" -- the same type in the old union points to two different types in the new union
                        throw new MetaDataException("record type corresponds to multiple types in new meta-data",
                                LogMessageKeys.OLD_RECORD_TYPE, oldRecord.getName(),
                                LogMessageKeys.NEW_RECORD_TYPE, newRecord.getName() + " & " + alreadySeenNewRecord.getName());
                    }
                } else {
                    if (updatedDescriptors.containsValue(newRecord)) {
                        // A "merge" -- two different types in the old union point to the same type in the new union
                        final Descriptor alreadySeenOldRecord = updatedDescriptors.inverse().get(newRecord);
                        throw new MetaDataException("record type corresponds to multiple types in old meta-data",
                                LogMessageKeys.OLD_RECORD_TYPE, oldRecord.getName() + " & " + alreadySeenOldRecord.getName(),
                                LogMessageKeys.NEW_RECORD_TYPE, newRecord.getName());
                    }
                }
                updatedDescriptors.put(oldRecord, newRecord);

                // Validate the form of the old and new record types
                validateMessage(oldRecord, newRecord, seenDescriptors);
            } else {
                throw new MetaDataException("record type removed from union", LogMessageKeys.RECORD_TYPE, oldUnionField.getMessageType());
            }
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private void validateMessage(@Nonnull Descriptor oldDescriptor, @Nonnull Descriptor newDescriptor,
                                 @Nonnull Set<NonnullPair<Descriptor, Descriptor>> seenDescriptors) {
        if (oldDescriptor == newDescriptor) {
            // Don't bother validating message types that are the same.
            return;
        }
        if (!seenDescriptors.add(NonnullPair.of(oldDescriptor, newDescriptor))) {
            // Note that because messages can contain fields that are of the same type as the containing
            // message, if this check to make sure the pair hadn't already been validated weren't present,
            // this might recurse infinitely on some inputs.
            return;
        }
        // Validate that the syntax (i.e., proto2 or proto3) of the two descriptors is the same.
        validateProtoSyntax(oldDescriptor, newDescriptor);

        // Validate the form--the schema, if you will--of the two descriptors. That means making sure that the
        // two fields are compatible. Note that the name of the descriptor isn't checked.

        // Validate that every field in the old descriptor is still in the new descriptor
        for (FieldDescriptor oldField : oldDescriptor.getFields()) {
            int fieldNumber = oldField.getNumber();
            FieldDescriptor newField = newDescriptor.findFieldByNumber(fieldNumber);
            if (newField != null) {
                validateField(oldField, newField, seenDescriptors);
            } else {
                throw new MetaDataException("field removed from message descriptor", LogMessageKeys.FIELD_NAME, oldField.getName());
            }
        }
        // Validate that none of the new fields are "required", which would make reading back any existing record impossible
        for (FieldDescriptor newField : newDescriptor.getFields()) {
            if (oldDescriptor.findFieldByNumber(newField.getNumber()) == null && newField.isRequired()) {
                throw new MetaDataException("required field added to record type", LogMessageKeys.FIELD_NAME, newField.getName());
            }
        }
    }

    private void validateProtoSyntax(@Nonnull Descriptors.Descriptor oldDescriptor, @Nonnull Descriptors.Descriptor newDescriptor) {
        if (!oldDescriptor.getFile().toProto().getSyntax().equals(newDescriptor.getFile().toProto().getSyntax())
                || !oldDescriptor.getFile().toProto().getEdition().equals(newDescriptor.getFile().toProto().getEdition())) {
            throw new MetaDataException("message descriptor proto syntax changed",
                    LogMessageKeys.RECORD_TYPE, oldDescriptor.getName());
        }
    }

    private void validateField(@Nonnull FieldDescriptor oldFieldDescriptor, @Nonnull FieldDescriptor newFieldDescriptor,
                               @Nonnull Set<NonnullPair<Descriptor, Descriptor>> seenDescriptors) {
        if (!oldFieldDescriptor.getName().equals(newFieldDescriptor.getName())) {
            // TODO: Field renaming should be allowed with some caveats about if the field is indexed or not
            throw new MetaDataException("field renamed",
                    LogMessageKeys.OLD_FIELD_NAME, oldFieldDescriptor.getName(),
                    LogMessageKeys.NEW_FIELD_NAME, newFieldDescriptor.getName());
        }
        if (!oldFieldDescriptor.getType().equals(newFieldDescriptor.getType())) {
            validateTypeChange(oldFieldDescriptor, newFieldDescriptor);
        }
        if (oldFieldDescriptor.isRequired() && !newFieldDescriptor.isRequired()) {
            throw new MetaDataException("required field is no longer required",
                    LogMessageKeys.FIELD_NAME, oldFieldDescriptor.getName());
        } else if (oldFieldDescriptor.isRepeated() && !newFieldDescriptor.isRepeated()) {
            throw new MetaDataException("repeated field is no longer repeated",
                    LogMessageKeys.FIELD_NAME, oldFieldDescriptor.getName());
        } else if (oldFieldDescriptor.hasPresence() != newFieldDescriptor.hasPresence()) {
            throw new MetaDataException("field changed whether default values are stored if set explicitly",
                    LogMessageKeys.FIELD_NAME, oldFieldDescriptor.getName());
        }
        if (oldFieldDescriptor.getType().equals(FieldDescriptor.Type.ENUM)) {
            validateEnum(newFieldDescriptor.getName(), oldFieldDescriptor.getEnumType(), newFieldDescriptor.getEnumType());
        }
        if (oldFieldDescriptor.getType().equals(FieldDescriptor.Type.GROUP) || oldFieldDescriptor.getType().equals(FieldDescriptor.Type.MESSAGE)) {
            // Message types need to be validated against each other as well.
            final Descriptor oldMessageType = oldFieldDescriptor.getMessageType();
            final Descriptor newMessageType = newFieldDescriptor.getMessageType();
            validateMessage(oldMessageType, newMessageType, seenDescriptors);
        }
    }

    private void validateTypeChange(@Nonnull FieldDescriptor oldFieldDescriptor, @Nonnull FieldDescriptor newFieldDescriptor) {
        // Allowed changes: Going from a variable length 32 bit integer to a variable length 64 bit integer
        // Other changes either change the Protobuf or the Tuple serialization of the field or can lead to a loss of precision.
        if (!(oldFieldDescriptor.getType().equals(FieldDescriptor.Type.INT32) && newFieldDescriptor.getType().equals(FieldDescriptor.Type.INT64)) &&
                !(oldFieldDescriptor.getType().equals(FieldDescriptor.Type.SINT32) && newFieldDescriptor.getType().equals(FieldDescriptor.Type.SINT64))) {
            throw new MetaDataException("field type changed",
                    LogMessageKeys.FIELD_NAME, oldFieldDescriptor.getName(),
                    LogMessageKeys.OLD_FIELD_TYPE, oldFieldDescriptor.getType(),
                    LogMessageKeys.NEW_FIELD_TYPE, newFieldDescriptor.getType());
        }
    }

    private void validateEnum(@Nonnull String fieldName, @Nonnull EnumDescriptor oldEnumDescriptor, @Nonnull EnumDescriptor newEnumDescriptor) {
        for (Descriptors.EnumValueDescriptor oldEnumValue : oldEnumDescriptor.getValues()) {
            Descriptors.EnumValueDescriptor newEnumValue = newEnumDescriptor.findValueByNumber(oldEnumValue.getNumber());
            if (newEnumValue == null) {
                throw new MetaDataException("enum removes value",
                        LogMessageKeys.FIELD_NAME, fieldName);
            }
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Nonnull
    private Map<String, String> getTypeRenames(@Nonnull Descriptor oldUnionDescriptor, @Nonnull Descriptor newUnionDescriptor) {
        if (oldUnionDescriptor == newUnionDescriptor) {
            return Collections.emptyMap();
        }

        final Map<String, String> renames = disallowTypeRenames ? Collections.emptyMap() : new HashMap<>();
        for (FieldDescriptor oldField : oldUnionDescriptor.getFields()) {
            Descriptor oldRecord = oldField.getMessageType();
            Descriptor newRecord = newUnionDescriptor.findFieldByNumber(oldField.getNumber()).getMessageType();
            if (!oldRecord.getName().equals(newRecord.getName())) {
                if (disallowTypeRenames) {
                    throw new MetaDataException("record type name changed",
                            LogMessageKeys.OLD_RECORD_TYPE, oldRecord.getName(),
                            LogMessageKeys.NEW_RECORD_TYPE, newRecord.getName());
                }
                String existingName = renames.putIfAbsent(oldRecord.getName(), newRecord.getName());
                if (existingName != null && !existingName.equals(newRecord.getName())) {
                    // This shouldn't be possible because of the validation done in validateUnion, but it's easy enough to check here.
                    throw new MetaDataException("record type corresponds to multiple types in new meta-data",
                            LogMessageKeys.OLD_RECORD_TYPE, oldRecord.getName(),
                            LogMessageKeys.NEW_RECORD_TYPE, newRecord.getName() + " & " + existingName);
                }
            }
        }
        return renames;
    }

    private void validateRecordTypes(@Nonnull RecordMetaData oldMetaData, @Nonnull RecordMetaData newMetaData,
                                     @Nonnull Map<String, String> typeRenames) {
        final Map<String, RecordType> oldRecordTypes = oldMetaData.getRecordTypes();
        final Map<String, RecordType> newRecordTypes = newMetaData.getRecordTypes();
        // Validate that all of the old record types are still in the new map
        for (Map.Entry<String, RecordType> oldRecordTypeEntry : oldRecordTypes.entrySet()) {
            final String oldRecordTypeName = oldRecordTypeEntry.getKey();
            final String newRecordTypeName = typeRenames.getOrDefault(oldRecordTypeName, oldRecordTypeName);
            if (!newRecordTypes.containsKey(newRecordTypeName)) {
                throw new MetaDataException("record type removed from meta-data",
                        LogMessageKeys.OLD_RECORD_TYPE, oldRecordTypeName,
                        LogMessageKeys.NEW_RECORD_TYPE, newRecordTypeName);
            }
            final RecordType oldRecordType = oldRecordTypeEntry.getValue();
            final RecordType newRecordType = newRecordTypes.get(newRecordTypeName);
            if (!Objects.equals(oldRecordType.getSinceVersion(), newRecordType.getSinceVersion())) {
                throw new MetaDataException("record type since version changed",
                        LogMessageKeys.RECORD_TYPE, newRecordTypeName,
                        LogMessageKeys.OLD_VERSION, oldRecordType.getSinceVersion(),
                        LogMessageKeys.NEW_VERSION, newRecordType.getSinceVersion());
            }
            if (!oldRecordType.getPrimaryKey().equals(newRecordType.getPrimaryKey())) {
                throw new MetaDataException("record type primary key changed",
                        LogMessageKeys.RECORD_TYPE, newRecordTypeName,
                        LogMessageKeys.OLD_KEY_EXPRESSION, oldRecordType.getPrimaryKey(),
                        LogMessageKeys.NEW_KEY_EXPRESSION, newRecordType.getPrimaryKey());
            }
            if (!oldRecordType.getRecordTypeKey().equals(newRecordType.getRecordTypeKey())) {
                throw new MetaDataException("record type key changed",
                        LogMessageKeys.RECORD_TYPE, newRecordTypeName);
            }
        }
        // Validate that any new type sets its since version
        Set<String> olderRecordTypeNames = Sets.newHashSetWithExpectedSize(oldRecordTypes.size());
        oldRecordTypes.forEach((typeName, ignore) -> olderRecordTypeNames.add(typeRenames.getOrDefault(typeName, typeName)));
        for (Map.Entry<String, RecordType> newRecordTypeEntry : newRecordTypes.entrySet()) {
            final String recordTypeName = newRecordTypeEntry.getKey();
            if (!olderRecordTypeNames.contains(recordTypeName)) {
                final RecordType newRecordType = newRecordTypeEntry.getValue();
                final Integer newSinceVersion = newRecordType.getSinceVersion();
                if (newSinceVersion == null) {
                    if (!allowNoSinceVersion) {
                        throw new MetaDataException("new record type is missing since version",
                                LogMessageKeys.RECORD_TYPE, recordTypeName);
                    }
                } else if (newSinceVersion <= oldMetaData.getVersion()) {
                    throw new MetaDataException("new record type has since version older than old meta-data",
                            LogMessageKeys.RECORD_TYPE, recordTypeName);
                }
            }
        }
    }

    @Nonnull
    private Map<Object, FormerIndex> getFormerIndexMap(@Nonnull RecordMetaData metaData) {
        Map<Object, FormerIndex> formerIndexMap;
        final List<FormerIndex> formerIndexes = metaData.getFormerIndexes();
        if (formerIndexes.isEmpty()) {
            formerIndexMap = Collections.emptyMap();
        } else {
            formerIndexMap = new HashMap<>(2 * formerIndexes.size());
            for (FormerIndex formerIndex : formerIndexes) {
                formerIndexMap.put(formerIndex.getSubspaceKey(), formerIndex);
            }
        }
        return formerIndexMap;
    }

    @Nonnull
    private Map<Object, Index> getIndexMap(@Nonnull RecordMetaData metaData) {
        Map<Object, Index> indexMap;
        final List<Index> allIndexes = metaData.getAllIndexes();
        if (allIndexes.isEmpty()) {
            indexMap = Collections.emptyMap();
        } else {
            indexMap = new HashMap<>(allIndexes.size() * 2);
            for (Index index : allIndexes) {
                indexMap.put(index.getSubspaceKey(), index);
            }
        }
        return indexMap;
    }

    private void validateCurrentAndFormerIndexes(@Nonnull RecordMetaData oldMetaData, @Nonnull RecordMetaData newMetaData,
                                                 @Nonnull Map<String, String> typeRenames) {
        final Map<Object, FormerIndex> oldFormerIndexMap = getFormerIndexMap(oldMetaData);
        final Map<Object, Index> oldIndexMap = getIndexMap(oldMetaData);
        final Map<Object, FormerIndex> newFormerIndexMap = getFormerIndexMap(newMetaData);
        final Map<Object, Index> newIndexMap = getIndexMap(newMetaData);

        // Validate all former indexes are still former indexes
        for (Map.Entry<Object, FormerIndex> oldFormerIndexEntry : oldFormerIndexMap.entrySet()) {
            final Object subspaceKey = oldFormerIndexEntry.getKey();
            if (newIndexMap.containsKey(subspaceKey)) {
                throw new MetaDataException("former index key used for new index in meta-data",
                        LogMessageKeys.SUBSPACE_KEY, subspaceKey,
                        LogMessageKeys.INDEX_NAME, newIndexMap.get(subspaceKey).getName());
            } else if (!newFormerIndexMap.containsKey(subspaceKey)) {
                throw new MetaDataException("former index removed from meta-data",
                        LogMessageKeys.SUBSPACE_KEY, subspaceKey);
            }
        }
        // Validate all old indexes are either still indexes or are now former indexes
        for (Map.Entry<Object, Index> oldIndexEntry : oldIndexMap.entrySet()) {
            final Object subspaceKey = oldIndexEntry.getKey();
            if (!newIndexMap.containsKey(subspaceKey) && !newFormerIndexMap.containsKey(subspaceKey)) {
                throw new MetaDataException("index missing in new meta-data",
                        LogMessageKeys.SUBSPACE_KEY, subspaceKey,
                        LogMessageKeys.INDEX_NAME, oldIndexEntry.getValue().getName());
            }
        }
        // Validate all new former indexes that are based on existing former indexes or prior indexes
        // have valid versions. Note that the new meta-data might introduce a former index that is
        // *not* based on an existing former index or index if an index was added and then dropped
        // between oldMetaData and newMetaData.
        for (Map.Entry<Object, FormerIndex> newFormerIndexEntry : newFormerIndexMap.entrySet()) {
            final Object subspaceKey = newFormerIndexEntry.getKey();
            final FormerIndex newFormerIndex = newFormerIndexEntry.getValue();
            if (oldFormerIndexMap.containsKey(subspaceKey)) {
                final FormerIndex oldFormerIndex = oldFormerIndexMap.get(subspaceKey);
                validateFormerIndex(subspaceKey, oldFormerIndex, newFormerIndex);
            } else {
                // Validate that this new former index has a version that is actually newer than
                // the old meta-data so that it will get removed during the next upgrade
                if (newFormerIndex.getRemovedVersion() <= oldMetaData.getVersion()) {
                    throw new MetaDataException("new former index has removed version that is not newer than the old meta-data version",
                            LogMessageKeys.SUBSPACE_KEY, subspaceKey,
                            LogMessageKeys.VERSION, newFormerIndex.getRemovedVersion());
                }
                // Validate that the former index's versions are compatible with the versions of the
                // index it is replacing. It might be the case that the index isn't present if the
                // index was added and dropped between the meta-data being
                final Index oldIndex = oldIndexMap.get(subspaceKey);
                if (oldIndex == null) {
                    if (!allowOlderFormerIndexAddedVersions && newFormerIndex.getAddedVersion() <= oldMetaData.getVersion()) {
                        throw new MetaDataException("former index without existing index has added version prior to old meta-data version",
                                LogMessageKeys.SUBSPACE_KEY, subspaceKey,
                                LogMessageKeys.OLD_VERSION, oldMetaData.getVersion(),
                                LogMessageKeys.NEW_VERSION, newFormerIndex.getAddedVersion());
                    }
                } else {
                    validateFormerIndexFromIndex(subspaceKey, oldIndex, newFormerIndex);
                }
            }
        }
        // Validate each index in the new meta-data matches an existing one or is new
        for (Map.Entry<Object, Index> newIndexEntry : newIndexMap.entrySet()) {
            final Object subspaceKey = newIndexEntry.getKey();
            final Index newIndex = newIndexEntry.getValue();
            if (oldIndexMap.containsKey(subspaceKey)) {
                final Index oldIndex = oldIndexMap.get(subspaceKey);
                validateIndex(oldMetaData, oldIndex, newMetaData, newIndex, typeRenames);
            } else {
                if (newIndex.getLastModifiedVersion() <= oldMetaData.getVersion()) {
                    throw new MetaDataException("new index has version that is not newer than the old meta-data version",
                            LogMessageKeys.INDEX_NAME, newIndex.getName(),
                            LogMessageKeys.VERSION, newIndex.getLastModifiedVersion());
                }
            }
        }
    }

    private void validateFormerIndexFromIndex(@Nonnull Object subspaceKey,
                                              @Nonnull Index oldIndex,
                                              @Nonnull FormerIndex newFormerIndex) {
        // Make sure the name is either dropped entirely or retained correctly
        if ((!allowMissingFormerIndexNames || newFormerIndex.getFormerName() != null) && !Objects.equals(newFormerIndex.getFormerName(), oldIndex.getName())) {
            throw new MetaDataException("former index has different name than old index",
                    LogMessageKeys.SUBSPACE_KEY, subspaceKey,
                    LogMessageKeys.OLD_INDEX_NAME, oldIndex.getName(),
                    LogMessageKeys.NEW_INDEX_NAME, newFormerIndex.getFormerName());
        }
        // Make sure the former index records an "added" version that is at least as old as the
        // added version recorded for the index. This ensures anything that upgrades from something
        // that has some version of this index knows to remove it.
        if (newFormerIndex.getAddedVersion() > oldIndex.getAddedVersion()) {
            throw new MetaDataException("former index added after old index",
                    LogMessageKeys.SUBSPACE_KEY,
                    LogMessageKeys.INDEX_NAME, oldIndex.getName(),
                    LogMessageKeys.OLD_VERSION, oldIndex.getAddedVersion(),
                    LogMessageKeys.NEW_VERSION, newFormerIndex.getAddedVersion());
        }
        if (!allowOlderFormerIndexAddedVersions && newFormerIndex.getAddedVersion() != oldIndex.getAddedVersion()) {
            throw new MetaDataException("former index reports added version older than replacing index",
                    LogMessageKeys.SUBSPACE_KEY, subspaceKey,
                    LogMessageKeys.INDEX_NAME, oldIndex.getName(),
                    LogMessageKeys.OLD_VERSION, oldIndex.getAddedVersion(),
                    LogMessageKeys.NEW_VERSION, newFormerIndex.getAddedVersion());
        }
        // Make sure the index removal happens after any versions in which the index was modified
        if (newFormerIndex.getRemovedVersion() <= oldIndex.getLastModifiedVersion()) {
            throw new MetaDataException("former index removed before old index's last modification",
                    LogMessageKeys.SUBSPACE_KEY, subspaceKey,
                    LogMessageKeys.INDEX_NAME, oldIndex.getName(),
                    LogMessageKeys.OLD_VERSION, oldIndex.getLastModifiedVersion(),
                    LogMessageKeys.NEW_VERSION, newFormerIndex.getRemovedVersion());
        }
    }

    private void validateFormerIndex(@Nonnull Object subspaceKey,
                                     @Nonnull FormerIndex oldFormerIndex,
                                     @Nonnull FormerIndex newFormerIndex) {
        // The removed version determines whether the subspace needs to be the same as the version in the old
        // meta-data. In theory, it would be acceptable if this version is newer, but if that information is
        // dropped, it can be a sign something else went wrong.
        if (oldFormerIndex.getRemovedVersion() != newFormerIndex.getRemovedVersion()) {
            throw new MetaDataException("removed version of former index differs from prior version",
                    LogMessageKeys.SUBSPACE_KEY, subspaceKey,
                    LogMessageKeys.OLD_VERSION, oldFormerIndex.getRemovedVersion(),
                    LogMessageKeys.NEW_VERSION, newFormerIndex.getRemovedVersion());
        }
        // Technically, it would okay if the added version were set to something in the new former
        // index that is less than the old former index's added version, but if that happens, then
        // it's possible something went wrong.
        if (oldFormerIndex.getAddedVersion() != newFormerIndex.getAddedVersion()) {
            throw new MetaDataException("added version of former index differs from prior version",
                    LogMessageKeys.SUBSPACE_KEY, subspaceKey,
                    LogMessageKeys.OLD_VERSION, oldFormerIndex.getAddedVersion(),
                    LogMessageKeys.NEW_VERSION, newFormerIndex.getAddedVersion());
        }
        // It would be okay if the name were lost or something or just changed, as it's only used
        // as additional book-keeping, but if it's lost, then that could be a sign that something
        // deeper is wrong.
        if (!Objects.equals(oldFormerIndex.getFormerName(), newFormerIndex.getFormerName())) {
            throw new MetaDataException("name of former index differs from prior version",
                    LogMessageKeys.SUBSPACE_KEY, subspaceKey,
                    LogMessageKeys.OLD_INDEX_NAME, oldFormerIndex.getFormerName(),
                    LogMessageKeys.NEW_INDEX_NAME, newFormerIndex.getFormerName());
        }
    }

    private void validateIndex(@Nonnull RecordMetaData oldMetaData, @Nonnull Index oldIndex,
                               @Nonnull RecordMetaData newMetaData, @Nonnull Index newIndex,
                               @Nonnull Map<String, String> typeRenames) {
        if (!oldIndex.getName().equals(newIndex.getName())) {
            // The index name is stored durably as part of the record store state.
            // If that were to use the subspace key, this check would probably be unnecessary.
            throw new MetaDataException("index name changed",
                    LogMessageKeys.OLD_INDEX_NAME, oldIndex.getName(),
                    LogMessageKeys.NEW_INDEX_NAME, newIndex.getName());
        }
        if (oldIndex.getAddedVersion() != newIndex.getAddedVersion()) {
            throw new MetaDataException("new index added version does not match old index added version",
                    LogMessageKeys.INDEX_NAME, newIndex.getName(),
                    LogMessageKeys.OLD_VERSION, oldIndex.getAddedVersion(),
                    LogMessageKeys.NEW_VERSION, newIndex.getAddedVersion());
        }
        if (oldIndex.getLastModifiedVersion() > newIndex.getLastModifiedVersion()) {
            throw new MetaDataException("old index has last-modified version newer than new index",
                    LogMessageKeys.INDEX_NAME, newIndex.getName(),
                    LogMessageKeys.OLD_VERSION, oldIndex.getLastModifiedVersion(),
                    LogMessageKeys.NEW_VERSION, newIndex.getLastModifiedVersion());
        }
        if (!allowIndexRebuilds && oldIndex.getLastModifiedVersion() != newIndex.getLastModifiedVersion()) {
            throw new MetaDataException("last modified version of index changed",
                    LogMessageKeys.INDEX_NAME, newIndex.getName(),
                    LogMessageKeys.OLD_VERSION, oldIndex.getLastModifiedVersion(),
                    LogMessageKeys.NEW_VERSION, newIndex.getLastModifiedVersion());
        }
        if (allowIndexRebuilds && oldIndex.getLastModifiedVersion() < newIndex.getLastModifiedVersion()) {
            // All subsequent checks verify that the index has not been modified in a way that
            // requires rebuilding. This is always an invalid modification if the last modified version
            // has not changed, but it is allowed if the allowIndexRebuilds option has been set to true.
            return;
        }
        if (!oldIndex.getType().equals(newIndex.getType())) {
            throw new MetaDataException("index type changed",
                    LogMessageKeys.INDEX_NAME, newIndex.getName(),
                    LogMessageKeys.OLD_INDEX_TYPE, oldIndex.getType(),
                    LogMessageKeys.NEW_INDEX_TYPE, newIndex.getType());
        }
        if (!oldIndex.getRootExpression().equals(newIndex.getRootExpression())) {
            throw new MetaDataException("index key expression changed",
                    LogMessageKeys.INDEX_NAME, newIndex.getName(),
                    LogMessageKeys.OLD_KEY_EXPRESSION, oldIndex.getRootExpression(),
                    LogMessageKeys.NEW_KEY_EXPRESSION, newIndex.getRootExpression());
        }
        // The new index must be defined on all record types that the old index was defined on. It may be defined
        // on additional types, but to avoid a rebuild, these record types must be newer than the old meta-data.
        Set<String> oldRecordTypeNames = oldMetaData.recordTypesForIndex(oldIndex).stream()
                .map(recordType -> typeRenames.getOrDefault(recordType.getName(), recordType.getName()))
                .collect(Collectors.toSet());
        Set<String> newRecordTypeNames = newMetaData.recordTypesForIndex(newIndex).stream()
                .map(RecordType::getName)
                .collect(Collectors.toSet());
        for (String oldRecordTypeName : oldRecordTypeNames) {
            if (!newRecordTypeNames.contains(oldRecordTypeName)) {
                throw new MetaDataException("new index removes record type",
                        LogMessageKeys.INDEX_NAME, newIndex.getName(),
                        LogMessageKeys.RECORD_TYPE, oldRecordTypeName);
            }
        }
        for (String newRecordTypeName : newRecordTypeNames) {
            if (!oldRecordTypeNames.contains(newRecordTypeName)) {
                RecordType newRecordType = newMetaData.getRecordType(newRecordTypeName);
                Integer sinceVersion = newRecordType.getSinceVersion();
                if (sinceVersion == null || newRecordType.getSinceVersion() <= oldMetaData.getVersion()) {
                    throw new MetaDataException("new index adds record type that is not newer than old meta-data",
                            LogMessageKeys.INDEX_NAME, newIndex.getName(),
                            LogMessageKeys.RECORD_TYPE, newRecordTypeName);
                }
            }
        }
        // Make sure the primary key component positions are the same
        if (oldIndex.hasPrimaryKeyComponentPositions()) {
            if (newIndex.hasPrimaryKeyComponentPositions()) {
                int[] oldPositions = oldIndex.getPrimaryKeyComponentPositions();
                int[] newPositions = newIndex.getPrimaryKeyComponentPositions();
                if (!Arrays.equals(oldPositions, newPositions)) {
                    throw new MetaDataException("new index changes primary key component positions",
                            LogMessageKeys.INDEX_NAME, newIndex.getName());
                }
            } else {
                throw new MetaDataException("new index drops primary key component positions",
                        LogMessageKeys.INDEX_NAME, newIndex.getName());
            }
        } else {
            if (newIndex.hasPrimaryKeyComponentPositions()) {
                throw new MetaDataException("new index adds primary key component positions",
                        LogMessageKeys.INDEX_NAME, newIndex.getName());
            }
        }
        // If there have been any changes to the index options, ask the index validator for that type
        // to validate the changed options.
        if (!oldIndex.getOptions().equals(newIndex.getOptions())) {
            IndexValidator validatorForIndex = indexValidatorRegistry.getIndexValidator(newIndex);
            validatorForIndex.validateChangedOptions(oldIndex);
        }
    }

    /**
     * Get the registry of index maintainers used to validate indexes. This registry should generally
     * be the same registry as the registry that will be used by any record stores that may use
     * the meta-data. By default, this uses the default {@link IndexMaintainerFactoryRegistryImpl} instance.
     *
     * @return the index maintainer registry used to validate indexes
     * @see com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase.BaseBuilder#setIndexMaintainerRegistry(IndexMaintainerFactoryRegistry)
     */
    @Nonnull
    public IndexValidatorRegistry getIndexValidatorRegistry() {
        return indexValidatorRegistry;
    }

    /**
     * Whether this validator allows the evolved meta-data to have the same version as the
     * old meta-data. By default, this is {@code false} as the assumption is that this validator
     * is only being called after the meta-data has been updated in some way, and it is good
     * practice to update the meta-data version in that instance.
     *
     * @return whether this validator allows the evolved meta-data to have the same version as the old meta-data
     */
    public boolean allowsNoVersionChange() {
        return allowNoVersionChange;
    }

    /**
     * Whether this validator allows new record types to not declare a {@linkplain RecordType#getSinceVersion() "since version"}.
     * It is good practice to set the since version on newer record types as that can be used to prevent unnecessary
     * index builds from occurring on indexes defined on the new types. However, failing to set this version is
     * safe insofar as none of the existing data is corrupted and the extra work, while unfortunate, is safe to perform.
     * By default, this option is set to {@code false}.
     *
     * @return whether this validator allows new record types to not declare a since version
     */
    public boolean allowsNoSinceVersion() {
        return allowNoSinceVersion;
    }

    /**
     * Whether this validator allows new meta-data that require existing indexes to be rebuilt.
     * This can happen if an index is modified and its {@linkplain Index#getLastModifiedVersion() last modified version}
     * is updated. By default, this is {@code false} as index rebuilds are expensive and the index
     * will not be queryable during the rebuild. The best practice if an index must be updated is to
     * create a new index with the change and then remove the index once the new index has been built.
     * This allows the old index to serve queries until such time as the new index is ready.
     *
     * @return whether this validator allows existing indexes to be modified in a ways that requires rebuilding
     */
    public boolean allowsIndexRebuilds() {
        return allowIndexRebuilds;
    }

    /**
     * Whether this validator allows former indexes to drop the name of the index they replace.
     * This is information is only used for book-keeping, so it is safe to create a {@link FormerIndex}
     * to replace an existing {@link Index} where {@link FormerIndex#getFormerName()} returns {@code null}.
     * However, this can be a sign that some amount of information is being lost in the upgrade process,
     * so the default value for this option is {@code false}, but users wanting more lenient validation can
     * set it to {@code true}.
     *
     * @return whether this validator allows former indexes to drop the associated name of the index they replace
     */
    public boolean allowsMissingFormerIndexNames() {
        return allowMissingFormerIndexNames;
    }

    /**
     * Whether this validator allows former indexes to set their added version to something that is older than
     * necessary. The added version of a {@link FormerIndex} is used as an optimization at meta-data upgrade
     * time to decide not to clear the data for a former index if the index was both added and dropped since
     * the meta-data version was last updated. As a result, it is safe to include older versions than the
     * actual added version of the associated {@link Index} within a {@code FormerIndex}, but it might result
     * in extra work and could also be a sign of another problem. As a result, the default value of this option
     * is {@code false}, but users wanting more lenient validation can set it to {@code true}.
     *
     * @return whether this validator allows former indexes to have older added versions than necessary
     */
    public boolean allowsOlderFormerIndexAddedVersions() {
        return allowOlderFormerIndexAddedVersions;
    }

    /**
     * Whether this validator allows the meta-data to begin to split long records. For record stores that were first
     * created with format version
     * {@link com.apple.foundationdb.record.provider.foundationdb.FormatVersion#SAVE_UNSPLIT_WITH_SUFFIX FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX}
     * or newer, this change is safe as the data layout is the same regardless of the value of
     * {@link RecordMetaData#isSplitLongRecords()}. However, older stores used a different format for if records were
     * unsplit, and accidentally upgrading the meta-data to split long records is potentially catastrophic as every
     * record in the store will now appear to be written under the wrong key. The default value of this option
     * is thus {@code false}, but as the new format version has been the default since prior to version 2.0,
     * most users should be able set this to {@code true}.
     *
     * @return whether this validator allows the meta-data to begin to split long records.
     */
    public boolean allowsUnsplitToSplit() {
        return allowUnsplitToSplit;
    }

    /**
     * Whether this validator disallows record types from being renamed. The record type name is not
     * persisted to the database with any record, so renaming a record type is generally possible
     * as long as one also updates the record type's name in the any referencing index. Note, however,
     * that renaming a record type also requires that the user modify any existing queries that are
     * restricted to that record type. As a result, the user may elect to disallow renaming record types
     * to avoid needing to audit and update any references to the changed record type name.
     *
     * @return whether this validator disallows record types from being renamed
     */
    public boolean disallowsTypeRenames() {
        return disallowTypeRenames;
    }

    /**
     * Convert this validator into a builder. This will copy over any options that have been set into the
     * builder.
     *
     * @return a new {@link Builder MetaDataEvoluationValidator.Builder} with the same options as this validator
     */
    @Nonnull
    public Builder asBuilder() {
        return new Builder(this);
    }

    /**
     * Create a new builder. This will have all of its options set to their default values.
     *
     * @return a new {@link Builder MetaDataEvoluationValidator.Builder} with all options set to their defaults
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder(DEFAULT_INSTANCE);
    }

    /**
     * Get the default validator. This has all options set to their default values.
     *
     * @return the default validator
     */
    @Nonnull
    public static MetaDataEvolutionValidator getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    /**
     * A builder class for the {@link MetaDataEvolutionValidator}. The validator has several options about how
     * strict it is when evaluating the changes that have been applied to the new meta-data. This builder
     * allows the user to set those options.
     */
    public static class Builder {
        @Nonnull
        private IndexValidatorRegistry indexValidatorRegistry;
        private boolean allowNoVersionChange;
        private boolean allowNoSinceVersion;
        private boolean allowIndexRebuilds;
        private boolean allowMissingFormerIndexNames;
        private boolean allowOlderFormerIndexAddedVersions;
        private boolean allowUnsplitToSplit;
        private boolean disallowTypeRenames;

        private Builder(@Nonnull MetaDataEvolutionValidator validator) {
            this.indexValidatorRegistry = validator.indexValidatorRegistry;
            this.allowNoVersionChange = validator.allowNoVersionChange;
            this.allowNoSinceVersion = validator.allowNoSinceVersion;
            this.allowIndexRebuilds = validator.allowIndexRebuilds;
            this.allowMissingFormerIndexNames = validator.allowMissingFormerIndexNames;
            this.allowOlderFormerIndexAddedVersions = validator.allowOlderFormerIndexAddedVersions;
            this.allowUnsplitToSplit = validator.allowUnsplitToSplit;
            this.disallowTypeRenames = validator.disallowTypeRenames;
        }

        /**
         * Set the registry of index validators used to validate indexes.
         *
         * @param indexValidatorRegistry the index validator registry used to validate indexes
         * @return this builder
         * @see com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase.BaseBuilder#setIndexMaintainerRegistry(IndexMaintainerFactoryRegistry)
         * @see MetaDataEvolutionValidator#getIndexValidatorRegistry()
         */
        @Nonnull
        public Builder setIndexValidatorRegistry(@Nonnull IndexMaintainerRegistry indexValidatorRegistry) {
            this.indexValidatorRegistry = indexValidatorRegistry;
            return this;
        }

        /**
         * Get the registry of index validators used to validate indexes.
         *
         * @return the index maintainer registry used to validate indexes
         * @see com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase.BaseBuilder#setIndexMaintainerRegistry(IndexMaintainerFactoryRegistry)
         * @see MetaDataEvolutionValidator#getIndexValidatorRegistry()
         */
        @Nonnull
        public IndexValidatorRegistry getIndexValidatorRegistry() {
            return indexValidatorRegistry;
        }

        /**
         * Set whether the validator will allow the evolved meta-data to have the same version as the old meta-data.
         * @param allowNoVersionChange whether the validator will allow the evolved meta-data to have the same version as the old meta-data
         * @return this builder
         * @see MetaDataEvolutionValidator#allowsNoVersionChange()
         */
        @Nonnull
        public Builder setAllowNoVersionChange(boolean allowNoVersionChange) {
            this.allowNoVersionChange = allowNoVersionChange;
            return this;
        }

        /**
         * Whether the validator will allow the evolved meta-data to have same version as the old meta-data.
         * @return whether the validator will allow the evolved meta-data to have the same version as the old meta-data
         * @see MetaDataEvolutionValidator#allowsNoVersionChange()
         */
        public boolean allowsNoVersionChange() {
            return allowNoVersionChange;
        }

        /**
         * Set whether the validator will allow new record types to not declare a {@linkplain RecordType#getSinceVersion() "since version"}.
         *
         * @param allowNoSinceVersion whether the validator will allow new record types to not declare a since version
         * @return this builder
         * @see MetaDataEvolutionValidator#allowsNoSinceVersion()
         */
        @Nonnull
        public Builder setAllowNoSinceVersion(boolean allowNoSinceVersion) {
            this.allowNoSinceVersion = allowNoSinceVersion;
            return this;
        }

        /**
         * Whether the validator will allow new record types to not declare a {@linkplain RecordType#getSinceVersion() "since version"}.
         *
         * @return whether the validator will allow new record types to not declare a since version
         * @see MetaDataEvolutionValidator#allowsNoSinceVersion()
         */
        public boolean allowsNoSinceVersion() {
            return allowNoSinceVersion;
        }

        /**
         * Set whether the validator will allow changes to indexes that require rebuilds.
         * @param allowIndexRebuilds whether the validator will allow changes to indexes that require rebuilds
         * @return this builder
         * @see MetaDataEvolutionValidator#allowsIndexRebuilds()
         */
        @Nonnull
        public Builder setAllowIndexRebuilds(boolean allowIndexRebuilds) {
            this.allowIndexRebuilds = allowIndexRebuilds;
            return this;
        }

        /**
         * Whether the validator will allow changes to indexes that require rebuilds.
         * @return whether the validator will allow changes to index that require rebuilds
         * @see MetaDataEvolutionValidator#allowsIndexRebuilds()
         */
        public boolean allowsIndexRebuilds() {
            return allowIndexRebuilds;
        }

        /**
         * Set whether the validator will allow former indexes to drop the name of the index they replace.
         * @param allowMissingFormerIndexNames whether the validator will allow former indexes to drop the name of the index they replace
         * @return this builder
         * @see MetaDataEvolutionValidator#allowsMissingFormerIndexNames()
         */
        @Nonnull
        public Builder setAllowMissingFormerIndexNames(boolean allowMissingFormerIndexNames) {
            this.allowMissingFormerIndexNames = allowMissingFormerIndexNames;
            return this;
        }

        /**
         * Whether the validator will allow former indexes to drop the name of the index they replace.
         * @return whether the validator will allow former indexes to drop the name of the index they replace
         * @see MetaDataEvolutionValidator#allowsMissingFormerIndexNames()
         */
        public boolean allowsMissingFormerIndexNames() {
            return allowMissingFormerIndexNames;
        }

        /**
         * Set whether the validator will allow former indexes to set their added version to something that is older than
         * necessary.
         *
         * @param allowOlderFormerIndexAddedVerions whether the validator will allow former indexes to have older added versions than necessary
         * @return this builder
         * @see MetaDataEvolutionValidator#allowsOlderFormerIndexAddedVersions()
         */
        @Nonnull
        public Builder setAllowOlderFormerIndexAddedVerions(boolean allowOlderFormerIndexAddedVerions) {
            this.allowOlderFormerIndexAddedVersions = allowOlderFormerIndexAddedVerions;
            return this;
        }

        /**
         * Whether the validator will allow former indexes to set their added version to something that is older than
         * necessary.
         *
         * @return whether the validator will allow former indexes to have older added versions than necessary
         * @see MetaDataEvolutionValidator#allowsOlderFormerIndexAddedVersions()
         */
        public boolean allowsOlderFormerIndexAddedVersions() {
            return allowOlderFormerIndexAddedVersions;
        }

        /**
         * Set whether the validator will allow the meta-data to begin to split long records.
         *
         * @param allowUnsplitToSplit whether the validator will allow the meta-data to begin to split long records.
         * @return this builder
         * @see MetaDataEvolutionValidator#allowsUnsplitToSplit()
         */
        @Nonnull
        public Builder setAllowUnsplitToSplit(boolean allowUnsplitToSplit) {
            this.allowUnsplitToSplit = allowUnsplitToSplit;
            return this;
        }

        /**
         * Get whether the validator will allow the meta-data to begin to split long records.
         *
         * @return whether this validator will allow the meta-data to begin to split long records.
         * @see MetaDataEvolutionValidator#allowsUnsplitToSplit()
         */
        public boolean allowsUnsplitToSplit() {
            return allowUnsplitToSplit;
        }

        /**
         * Set whether the validator will disallow record types from being renamed.
         *
         * @param disallowTypeRenames whether the validator will disallow record types from being renamed
         * @return this builder
         * @see MetaDataEvolutionValidator#disallowsTypeRenames()
         */
        @Nonnull
        public Builder setDisallowTypeRenames(boolean disallowTypeRenames) {
            this.disallowTypeRenames = disallowTypeRenames;
            return this;
        }

        /**
         * Get whether this validator will disallow record types from being renamed.
         *
         * @return whether this validator disallow record types from being renamed
         * @see MetaDataEvolutionValidator#disallowsTypeRenames()
         */
        public boolean disallowsTypeRenames() {
            return disallowTypeRenames;
        }

        /**
         * Create a {@link MetaDataEvolutionValidator} using the options specified through this
         * builder.
         *
         * @return a new {@link MetaDataEvolutionValidator}
         */
        @Nonnull
        public MetaDataEvolutionValidator build() {
            return new MetaDataEvolutionValidator(this);
        }
    }
}
