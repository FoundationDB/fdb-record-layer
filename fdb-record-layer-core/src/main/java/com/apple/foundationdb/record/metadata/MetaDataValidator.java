/*
 * MetaDataValidator.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Validator for {@link RecordMetaData}.
 * @see MetaDataException
 */
@API(API.Status.MAINTAINED)
public class MetaDataValidator implements RecordMetaDataProvider {
    @Nonnull
    protected final RecordMetaData metaData;
    @Nonnull
    protected final IndexValidatorRegistry indexRegistry;
    @Nonnull
    protected final Map<Object, Index> assignedPrefixes;
    @Nonnull
    protected final Map<Object, FormerIndex> assignedFormerPrefixes;
    @Nonnull
    protected final Map<Object, RecordType> recordTypeKeys;

    public MetaDataValidator(@Nonnull RecordMetaDataProvider metaData, @Nonnull IndexValidatorRegistry indexRegistry) {
        this.metaData = metaData.getRecordMetaData();
        this.indexRegistry = indexRegistry;
        this.assignedPrefixes = new HashMap<>();
        this.assignedFormerPrefixes = new HashMap<>();
        this.recordTypeKeys = new HashMap<>();
    }

    public void validate() {
        validateUnionDescriptor(metaData.getUnionDescriptor());
        if (metaData.getRecordTypes().isEmpty()) {
            throw new MetaDataException("No record types defined in meta-data");
        }
        metaData.getRecordTypes().values().forEach(this::validateRecordType);
        validateCurrentAndFormerIndexes();
    }

    protected void validateUnionDescriptor(Descriptors.Descriptor unionDescriptor) {
        final List<Descriptors.OneofDescriptor> oneofs = unionDescriptor.getOneofs();
        if (!oneofs.isEmpty()) {
            if (oneofs.size() > 1) {
                throw new MetaDataException("Union descriptor has more than one oneof");
            }
            if (oneofs.get(0).getFieldCount() != unionDescriptor.getFields().size()) {
                throw new MetaDataException("Union descriptor oneof must contain every field");
            }
        }
    }

    protected void validateRecordType(@Nonnull RecordType recordType) {
        metaData.getUnionFieldForRecordType(recordType);    // Throws if missing.
        validatePrimaryKeyForRecordType(recordType.getPrimaryKey(), recordType);
        RecordType otherRecordType = recordTypeKeys.put(recordType.getRecordTypeKey(), recordType);
        if (otherRecordType != null) {
            throw new MetaDataException("Same record type key " + recordType.getRecordTypeKey() +
                                        " used by both " + recordType.getName() + " and " + otherRecordType.getName());
        }
        if (recordType.getSinceVersion() != null && recordType.getSinceVersion() > metaData.getVersion()) {
            throw new MetaDataException("Record type " + recordType.getName() + " has since version of " +
                                        recordType.getSinceVersion() + " which is greater than the meta-data version " +
                                        metaData.getVersion());
        }
    }

    protected void validatePrimaryKeyForRecordType(@Nonnull KeyExpression primaryKey, @Nonnull RecordType recordType) {
        primaryKey.validate(recordType.getDescriptor());
        if (primaryKey.createsDuplicates()) {
            throw new MetaDataException("Primary key for " + recordType.getName() +
                                        " can generate more than one entry");
        }
    }

    protected void validateCurrentAndFormerIndexes() {
        metaData.getAllIndexes().forEach(this::validateIndex);
        metaData.getFormerIndexes().forEach(this::validateFormerIndex);
        for (Map.Entry<Object, Index> assignedPrefixEntry : assignedPrefixes.entrySet()) {
            final Object indexSubspaceKey = assignedPrefixEntry.getKey();
            final FormerIndex formerIndex = assignedFormerPrefixes.get(indexSubspaceKey);
            if (formerIndex != null) {
                throw new MetaDataException("Same subspace key " + indexSubspaceKey +
                                            " used by index " + assignedPrefixEntry.getValue().getName() +
                                            " and former index" + (formerIndex.getFormerName() == null ? "" : (" " + formerIndex.getFormerName())));
            }
        }
    }

    protected void validateIndex(@Nonnull Index index) {
        indexRegistry.getIndexValidator(index).validate(this);
        final Index otherIndex = assignedPrefixes.put(index.getSubspaceKey(), index);
        if (otherIndex != null) {
            throw new MetaDataException("Same subspace key " + index.getSubspaceKey() +
                                        " used by both " + index.getName() + " and " + otherIndex.getName());
        }
        if (index.getAddedVersion() > metaData.getVersion()) {
            throw new MetaDataException("Index " + index.getName() + " has added version " +
                                        index.getAddedVersion() + " which is greater than the meta-data version " +
                                        metaData.getVersion());
        }
        if (index.getLastModifiedVersion() > metaData.getVersion()) {
            throw new MetaDataException("Index " + index.getName() + " has last modified version " +
                                        index.getLastModifiedVersion() + " which is greater than the meta-data version " +
                                        metaData.getVersion());
        }
        final List<String> replacementIndexNames = index.getReplacedByIndexNames();
        if (!replacementIndexNames.isEmpty()) {
            // Make sure all of the indexes are in the meta-data
            for (String replacementIndexName : replacementIndexNames) {
                if (metaData.hasIndex(replacementIndexName)) {
                    // Make sure none of the replacement indexes themselves have any indexes that they are being
                    // replaced by, as that can either lead to ambiguous situations where indexes may or may not
                    // be fully replaced (if, for example, its replacement indexes aren't READABLE but the indexes
                    // replacing its replacements are). In theory, this could be fixed by more complicated replacement
                    // logic that traverses the replacement graph to make sure all leaves in the graph are built
                    final Index replacementIndex = metaData.getIndex(replacementIndexName);
                    if (!replacementIndex.getReplacedByIndexNames().isEmpty()) {
                        throw new MetaDataException("Index " + index.getName() + " has replacement index "
                                                    + replacementIndexName + " that itself has replacement indexes");
                    }
                } else {
                    throw new MetaDataException("Index " + index.getName() + " has replacement index "
                                                + replacementIndexName + " that is not in the meta-data");
                }
            }
        }
    }

    protected void validateFormerIndex(@Nonnull FormerIndex formerIndex) {
        final FormerIndex otherFormerIndex = assignedFormerPrefixes.put(formerIndex.getSubspaceKey(), formerIndex);
        if (otherFormerIndex != null) {
            final String indexNameString = (formerIndex.getFormerName() == null ? "<unknown>" : formerIndex.getFormerName()) +
                                           " and " + (otherFormerIndex.getFormerName() == null ? "<unknown>" : otherFormerIndex.getFormerName());
            throw new MetaDataException("Same subspace key " + formerIndex.getSubspaceKey() +
                                        " used by two former indexes " + indexNameString);
        }
        if (formerIndex.getAddedVersion() > formerIndex.getRemovedVersion()) {
            throw new MetaDataException("Former index" + (formerIndex.getFormerName() == null ? "" : (" " + formerIndex.getFormerName())) +
                                        " has added version " + formerIndex.getAddedVersion() +
                                        " which is greater than the removed version " + formerIndex.getRemovedVersion());
        }
        if (formerIndex.getAddedVersion() > metaData.getVersion()) {
            throw new MetaDataException("Former index" + (formerIndex.getFormerName() == null ? "" : (" " + formerIndex.getFormerName())) +
                                        " has added version " + formerIndex.getAddedVersion() +
                                        " which is greater than the meta-data version " + metaData.getVersion());
        }
        if (formerIndex.getRemovedVersion() > metaData.getVersion()) {
            throw new MetaDataException("Former index" + (formerIndex.getFormerName() == null ? "" : (" " + formerIndex.getFormerName())) +
                                        " has removed version " + formerIndex.getRemovedVersion() +
                                        " which is greater than the meta-data version " + metaData.getVersion());
        }
    }

    public void validateIndexForRecordTypes(@Nonnull Index index, @Nonnull IndexValidator indexValidator) {
        for (RecordType recordType : metaData.recordTypesForIndex(index)) {
            indexValidator.validateIndexForRecordType(recordType, this);
        }
    }

    @Nonnull
    public List<Descriptors.FieldDescriptor> validateIndexForRecordType(@Nonnull Index index, @Nonnull RecordType recordType) {
        return index.validate(recordType.getDescriptor());
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return metaData;
    }

}
