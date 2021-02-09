/*
 * AvailableFields.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.query.plan.planning.TextScanPlanner;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a subset of the fields available in a stream of records, including partial records.
 *
 * If a stream includes full records, all fields are available; they are not represented individually.
 * For partial records, this class tracks a (non-strict) subset of the fields that are actually available.
 * For example, all repeated fields are currently dropped. It is extremely important that no field ever be included
 * when it is not actually available on a partial record.
 */
public class AvailableFields {
    @Nonnull
    public static final AvailableFields ALL_FIELDS = new AvailableFields(null);
    @Nonnull
    public static final AvailableFields NO_FIELDS = new AvailableFields(Collections.emptyMap());

    @Nullable
    private final Map<KeyExpression, FieldData> fields;

    private AvailableFields(@Nullable Map<KeyExpression, FieldData> fields) {
        this.fields = fields;
    }


    public boolean hasAllFields() {
        return fields == null;
    }

    public boolean containsAll(@Nonnull Collection<KeyExpression> requiredFields) {
        if (fields == null) {
            return true;
        }
        return fields.keySet().containsAll(requiredFields);
    }

    @Nullable
    public IndexKeyValueToPartialRecord.Builder buildIndexKeyValueToPartialRecord(@Nonnull RecordType recordType) {
        if (fields == null) {
            return null;
        }

        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);
        for (Map.Entry<KeyExpression, FieldData> entry : fields.entrySet()) {
            if (!addCoveringField(entry.getKey(), entry.getValue(), builder)) {
                return null;
            }
        }
        if (!builder.isValid()) {
            return null;
        }

        return builder;
    }

    @Nonnull
    public static AvailableFields fromIndex(@Nonnull RecordType recordType,
                                            @Nonnull Index index,
                                            @Nonnull PlannableIndexTypes indexTypes,
                                            @Nullable KeyExpression commonPrimaryKey) {
        final KeyExpression rootExpression = index.getRootExpression();

        final List<KeyExpression> keyFields = new ArrayList<>();
        final List<KeyExpression> valueFields = new ArrayList<>();
        if (indexTypes.getTextTypes().contains(index.getType())) {
            // Full text index entries have all of their fields except the tokenized one.
            keyFields.addAll(TextScanPlanner.getOtherFields(rootExpression));
        } else if (indexTypes.getValueTypes().contains(index.getType()) ||
                   indexTypes.getRankTypes().contains(index.getType())) {
            keyFields.addAll(KeyExpression.getKeyFields(rootExpression));
            valueFields.addAll(KeyExpression.getValueFields(rootExpression));
        } else {
            // Aggregate index
            if (rootExpression instanceof GroupingKeyExpression) {
                GroupingKeyExpression groupingKeyExpression = (GroupingKeyExpression) rootExpression;
                keyFields.addAll(groupingKeyExpression.getGroupingSubKey().normalizeKeyForPositions());
            }
        }

        // Like FDBRecordStoreBase.indexEntryKey(), but with key expressions instead of actual values.
        final List<KeyExpression> primaryKeys = new ArrayList<>(commonPrimaryKey == null
                                                                ? Collections.emptyList()
                                                                : commonPrimaryKey.normalizeKeyForPositions());
        index.trimPrimaryKey(primaryKeys);
        keyFields.addAll(primaryKeys);

        Map<KeyExpression, FieldData> fields = new HashMap<>();
        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);
        for (int i = 0; i < keyFields.size(); i++) {
            KeyExpression keyField = keyFields.get(i);
            FieldData fieldData = FieldData.of(IndexKeyValueToPartialRecord.TupleSource.KEY, i);
            if (!keyField.createsDuplicates() && addCoveringField(keyField, fieldData, builder)) {
                fields.put(keyField, fieldData);
            }
        }
        for (int i = 0; i < valueFields.size(); i++) {
            KeyExpression valueField = valueFields.get(i);
            FieldData fieldData = FieldData.of(IndexKeyValueToPartialRecord.TupleSource.VALUE, i);
            if (!valueField.createsDuplicates() && addCoveringField(valueField, fieldData, builder)) {
                fields.put(valueField, fieldData);
            }
        }

        if (!builder.isValid()) {
            return NO_FIELDS;
        }
        return new AvailableFields(fields);
    }

    public static boolean addCoveringField(@Nonnull KeyExpression requiredExpr,
                                           @Nonnull FieldData fieldData,
                                           @Nonnull IndexKeyValueToPartialRecord.Builder builder) {
        while (requiredExpr instanceof NestingKeyExpression) {
            NestingKeyExpression nesting = (NestingKeyExpression)requiredExpr;
            String fieldName = nesting.getParent().getFieldName();
            requiredExpr = nesting.getChild();
            builder = builder.getFieldBuilder(fieldName);
        }
        if (requiredExpr instanceof FieldKeyExpression) {
            FieldKeyExpression fieldKeyExpression = (FieldKeyExpression)requiredExpr;
            if (!fieldKeyExpression.getNullStandin().equals(Key.Evaluated.NullStandin.NULL) ||
                    fieldKeyExpression.getFanType().equals(KeyExpression.FanType.FanOut)) {
                return false;
            }
            builder.addField(fieldKeyExpression.getFieldName(), fieldData.source, fieldData.index);
            return true;
        } else {
            return false;
        }
    }

    @Nonnull
    public static AvailableFields intersection(@Nonnull List<AvailableFields> toIntersect) {
        if (toIntersect.isEmpty()) {
            throw new RecordCoreException("tried to find intersection of an empty list of available fields");
        }

        Map<KeyExpression, FieldData> intersection = null;
        for (AvailableFields fields : toIntersect) {
            if (!fields.hasAllFields()) {
                if (intersection == null) {
                    intersection = new HashMap<>(fields.fields);
                } else {
                    // keySet() is backed by the map itself
                    intersection.keySet().retainAll(fields.fields.keySet());
                }
            }
        }
        if (intersection == null) {
            return ALL_FIELDS;
        } else {
            return new AvailableFields(intersection);
        }
    }

    /**
     * A pair of a tuple source (key or value) and an index within that tuple source.
     */
    @API(API.Status.INTERNAL)
    public static class FieldData {
        @Nonnull
        private final IndexKeyValueToPartialRecord.TupleSource source;
        private final int index;

        private FieldData(@Nonnull final IndexKeyValueToPartialRecord.TupleSource source, final int index) {
            this.source = source;
            this.index = index;
        }

        @Nonnull
        public static FieldData of(@Nonnull IndexKeyValueToPartialRecord.TupleSource source, int index) {
            return new FieldData(source, index);
        }
    }
}
