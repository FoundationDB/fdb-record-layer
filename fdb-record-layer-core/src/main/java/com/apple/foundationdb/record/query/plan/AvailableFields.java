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
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.query.plan.planning.TextScanPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.primitives.ImmutableIntArray;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Represents a subset of the fields available in a stream of records, including partial records.
 *
 * If a stream includes full records, all fields are available; they are not represented individually.
 * For partial records, this class tracks a (non-strict) subset of the fields that are actually available.
 * For example, all repeated fields are currently dropped. It is extremely important that no field ever be included
 * when it is not actually available on a partial record.
 */
@SuppressWarnings("ALL")
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

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    /**
     * Get the fields that are available from just the index scan part of an index plan.
     */
    public static AvailableFields fromIndex(@Nonnull RecordType recordType,
                                            @Nonnull Index index,
                                            @Nonnull PlannableIndexTypes indexTypes,
                                            @Nullable KeyExpression commonPrimaryKey,
                                            @Nonnull RecordQueryPlanWithIndex indexPlan) {
        final KeyExpression rootExpression = index.getRootExpression();

        final List<KeyExpression> keyFields = new ArrayList<>();
        final List<KeyExpression> valueFields = new ArrayList<>();
        final List<KeyExpression> nonStoredFields = new ArrayList<>();
        final List<KeyExpression> otherFields = new ArrayList<>();
        if (indexTypes.getTextTypes().contains(index.getType())) {
            // Full text index entries have all of their fields except the tokenized one.
            keyFields.addAll(TextScanPlanner.getOtherFields(rootExpression));
        } else if (indexTypes.getValueTypes().contains(index.getType()) ||
                   indexTypes.getRankTypes().contains(index.getType())) {
            keyFields.addAll(KeyExpression.getKeyFields(rootExpression));
            valueFields.addAll(KeyExpression.getValueFields(rootExpression));
        } else if (indexTypes.getUnstoredNonPrimaryKeyTypes().contains(index.getType())) {
            keyFields.addAll(rootExpression.normalizeKeyForPositions());
            nonStoredFields.addAll(keyFields);
            nonStoredFields.removeAll(recordType.getPrimaryKey().normalizeKeyForPositions());
            if (rootExpression instanceof GroupingKeyExpression) {
                GroupingKeyExpression groupingKeyExpression = (GroupingKeyExpression) rootExpression;
                nonStoredFields.removeAll(groupingKeyExpression.getGroupingSubKey().normalizeKeyForPositions());
            }
            if (indexPlan instanceof PlanWithStoredFields) {
                ((PlanWithStoredFields)indexPlan).getStoredFields(keyFields, nonStoredFields, otherFields);
            }
        } else {
            // Aggregate index
            if (rootExpression instanceof GroupingKeyExpression) {
                GroupingKeyExpression groupingKeyExpression = (GroupingKeyExpression) rootExpression;
                keyFields.addAll(groupingKeyExpression.getGroupingSubKey().normalizeKeyForPositions());
            }
        }

        // Like FDBRecordStoreBase.indexEntryKey(), but with key expressions instead of actual values.
        Map<KeyExpression, FieldData> fields = new HashMap<>();

        final ListMultimap<KeyExpression, FieldData> primaryKeyPartsToTuplePathMap;
        if (commonPrimaryKey != null) {
            final BitSet coveredPrimaryKeyPositions = index.getCoveredPrimaryKeyPositions();
            final ExpressionToTuplePathVisitor expressionToTuplePathVisitor =
                    new ExpressionToTuplePathVisitor(commonPrimaryKey, IndexKeyValueToPartialRecord.TupleSource.KEY, keyFields.size(), coveredPrimaryKeyPositions);
            primaryKeyPartsToTuplePathMap = expressionToTuplePathVisitor.compute();
        } else {
            primaryKeyPartsToTuplePathMap = ImmutableListMultimap.of();
        }

        final ImmutableMap<String, ImmutableIntArray> constituentNameToPathMap;
        if (recordType.isSynthetic() && commonPrimaryKey != null) {
            //
            // We need to only copy parts of the index entry which are not currently producing a null-constituent due to
            // an outer join.
            //
            Verify.verify(recordType instanceof SyntheticRecordType);
            final SyntheticRecordType<?> syntheticRecordType = (SyntheticRecordType<?>)recordType;
            final List<? extends SyntheticRecordType.Constituent> constituents = syntheticRecordType.getConstituents();
            final int startConstituents = keyFields.size() + 1;
            final ImmutableMap.Builder<String, ImmutableIntArray> constituentNameToPathMapBuilder =
                    ImmutableMap.builder();
            for (int i = 0; i < constituents.size(); i++) {
                final SyntheticRecordType.Constituent constituent = constituents.get(i);
                constituentNameToPathMapBuilder.put(constituent.getName(), ImmutableIntArray.of(startConstituents + i));
            }
            constituentNameToPathMap = constituentNameToPathMapBuilder.build();
        } else {
            constituentNameToPathMap = ImmutableMap.of();
        }

        // KEYs -- from the index's definition
        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);
        for (int i = 0; i < keyFields.size(); i++) {
            KeyExpression keyField = keyFields.get(i);
            FieldData fieldData = constrainFieldData(recordType, constituentNameToPathMap, keyField, ImmutableIntArray.of(i));
            if (!nonStoredFields.contains(keyField) && !keyField.createsDuplicates() && addCoveringField(keyField, fieldData, builder)) {
                fields.put(keyField, fieldData);
            }
        }
        if (commonPrimaryKey != null) {
            // KEYs -- from the primary key
            for (final Map.Entry<KeyExpression, FieldData> entry : primaryKeyPartsToTuplePathMap.entries()) {
                KeyExpression keyField = entry.getKey();
                FieldData fieldData = entry.getValue();
                fieldData = constrainFieldData(recordType, constituentNameToPathMap, keyField, fieldData.getOrdinalPath());
                if (!nonStoredFields.contains(keyField) && !keyField.createsDuplicates() && addCoveringField(keyField, fieldData, builder)) {
                    fields.put(keyField, fieldData);
                }
            }
        }
        for (int i = 0; i < valueFields.size(); i++) {
            KeyExpression valueField = valueFields.get(i);
            FieldData fieldData = FieldData.ofUnconditional(IndexKeyValueToPartialRecord.TupleSource.VALUE, ImmutableIntArray.of(i));
            if (!nonStoredFields.contains(valueField) && !valueField.createsDuplicates() && addCoveringField(valueField, fieldData, builder)) {
                fields.put(valueField, fieldData);
            }
        }
        for (int i = 0; i < otherFields.size(); i++) {
            KeyExpression valueField = otherFields.get(i);
            FieldData fieldData = FieldData.ofUnconditional(IndexKeyValueToPartialRecord.TupleSource.OTHER, ImmutableIntArray.of(i));
            fields.put(valueField, fieldData);
        }

        if (!builder.isValid()) {
            return NO_FIELDS;
        }
        return new AvailableFields(fields);
    }

    @Nonnull
    private static FieldData constrainFieldData(@Nonnull final RecordType recordType,
                                                @Nonnull final ImmutableMap<String, ImmutableIntArray> constituentNameToPathMap,
                                                @Nonnull final KeyExpression keyField,
                                                @Nonnull final ImmutableIntArray path) {
        final String constituentName;
        if (recordType.isSynthetic()) {
            if (keyField instanceof FieldKeyExpression) {
                constituentName = ((FieldKeyExpression)keyField).getFieldName();
            } else if (keyField instanceof NestingKeyExpression) {
                constituentName = ((NestingKeyExpression)keyField).getParent().getFieldName();
            } else {
                constituentName = null;
            }
        } else {
            constituentName = null;
        }

        if ((constituentName != null) && constituentNameToPathMap.containsKey(constituentName)) {
            final ImmutableIntArray conditionalPath = constituentNameToPathMap.get(constituentName);
            return FieldData.of(IndexKeyValueToPartialRecord.TupleSource.KEY, path,
                    tuple -> IndexKeyValueToPartialRecord.existsSubTupleForOrdinalPath(tuple, conditionalPath));
        } else {
            return FieldData.ofUnconditional(IndexKeyValueToPartialRecord.TupleSource.KEY, path);
        }
    }

    public static boolean addCoveringField(@Nonnull KeyExpression requiredExpr,
                                           @Nonnull FieldData fieldData,
                                           @Nonnull IndexKeyValueToPartialRecord.Builder builder) {
        if (fieldData.source == IndexKeyValueToPartialRecord.TupleSource.OTHER) {
            return true;
        }
        while (requiredExpr instanceof NestingKeyExpression) {
            NestingKeyExpression nesting = (NestingKeyExpression)requiredExpr;
            String fieldName = nesting.getParent().getFieldName();
            requiredExpr = nesting.getChild();
            builder = builder.getFieldBuilder(fieldName);
        }
        if (requiredExpr instanceof FieldKeyExpression) {
            FieldKeyExpression fieldKeyExpression = (FieldKeyExpression)requiredExpr;
            // If a key part of an index entry is populated with null stand-ins (it is not NULL in the index but
            // NULL in the base record), we cannot use the key part of the index tuple in stead of the field from
            // the fetched record as we don't know if the field is in fact NULL or a non-NULL value
            // (e.g., a default value).
            if (fieldKeyExpression.getNullStandin().equals(Key.Evaluated.NullStandin.NOT_NULL) ||
                    fieldKeyExpression.getFanType().equals(KeyExpression.FanType.FanOut)) {
                return false;
            }
            if (!builder.hasField(fieldKeyExpression.getFieldName())) {
                builder.addField(fieldKeyExpression.getFieldName(), fieldData.source, fieldData.copyIfPredicate, fieldData.ordinalPath);
            }
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
                Objects.requireNonNull(fields.fields);
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
        @Nonnull
        private final ImmutableIntArray ordinalPath;
        private final Predicate<Tuple> copyIfPredicate;

        private FieldData(@Nonnull final IndexKeyValueToPartialRecord.TupleSource source, @Nonnull final ImmutableIntArray ordinalPath, final Predicate<Tuple> copyIfPredicate) {
            this.source = source;
            this.ordinalPath = ordinalPath;
            this.copyIfPredicate = copyIfPredicate;
        }

        @Nonnull
        public IndexKeyValueToPartialRecord.TupleSource getSource() {
            return source;
        }

        public ImmutableIntArray getOrdinalPath() {
            return ordinalPath;
        }

        public Predicate<Tuple> getCopyIfPredicate() {
            return copyIfPredicate;
        }

        @Nonnull
        public static FieldData ofUnconditional(@Nonnull IndexKeyValueToPartialRecord.TupleSource source, final ImmutableIntArray ordinalPath) {
            return new FieldData(source, ordinalPath, t -> true);
        }

        @Nonnull
        public static FieldData of(@Nonnull IndexKeyValueToPartialRecord.TupleSource source, @Nonnull final ImmutableIntArray ordinalPath, @Nonnull final Predicate<Tuple> copyIfPredicate) {
            return new FieldData(source, ordinalPath, copyIfPredicate);
        }
    }
}
