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
public class MetaDataValidator implements RecordMetaDataProvider {
    @Nonnull
    protected final RecordMetaData metaData;
    @Nonnull
    protected final IndexValidatorRegistry indexRegistry;
    protected final Map<Object, Index> assignedPrefixes;

    public MetaDataValidator(@Nonnull RecordMetaDataProvider metaData, @Nonnull IndexValidatorRegistry indexRegistry) {
        this.metaData = metaData.getRecordMetaData();
        this.indexRegistry = indexRegistry;
        this.assignedPrefixes = new HashMap<>();
    }

    public void validate() {
        metaData.getRecordTypes().values().stream().forEach(this::validateRecordType);
        metaData.getAllIndexes().stream().forEach(this::validateIndex);
    }

    protected void validateRecordType(@Nonnull RecordType recordType) {
        metaData.getUnionFieldForRecordType(recordType);    // Throws if missing.
        validatePrimaryKeyForRecordType(recordType.getPrimaryKey(), recordType);
    }

    protected void validatePrimaryKeyForRecordType(@Nonnull KeyExpression primaryKey, @Nonnull RecordType recordType) {
        if (primaryKey.createsDuplicates()) {
            throw new MetaDataException("Primary key for " + recordType.getName() +
                                        " can generate more than one entry");
        }
    }

    protected void validateIndex(@Nonnull Index index) {
        indexRegistry.getIndexValidator(index).validate(this);
        final Index otherIndex = assignedPrefixes.put(index.getSubspaceKey(), index);
        if (otherIndex != null) {
            throw new MetaDataException("Same subspace key " + index.getSubspaceKey() +
                                        " used by both " + index + " and " + otherIndex);
        }
    }

    public void validateIndexForRecordTypes(@Nonnull Index index, @Nonnull IndexValidator indexValidator) {
        for (RecordType recordType : metaData.recordTypesForIndex(index)) {
            indexValidator.validateIndexForRecordType(recordType, this);
        }
    }

    public List<Descriptors.FieldDescriptor> validateIndexForRecordType(@Nonnull Index index, @Nonnull RecordType recordType) {
        return index.validate(recordType.getDescriptor());
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return metaData;
    }

}
