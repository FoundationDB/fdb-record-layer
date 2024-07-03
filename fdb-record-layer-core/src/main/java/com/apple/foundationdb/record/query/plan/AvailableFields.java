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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.InvertibleFunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord.PCopyIfPredicate;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord.PCopyIfPredicate.PConditionalUponPathPredicate;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord.PCopyIfPredicate.PTruePredicate;
import com.apple.foundationdb.record.query.plan.planning.TextScanPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.tuple.Tuple;
import com.google.auto.service.AutoService;
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
    public static AvailableFields fromIndex(@Nonnull final RecordType recordType,
                                            @Nonnull final Index index,
                                            @Nonnull final PlannableIndexTypes indexTypes,
                                            @Nullable final KeyExpression commonPrimaryKey,
                                            @Nonnull final RecordQueryPlanWithIndex indexPlan) {
        final KeyExpression rootExpression = index.getRootExpression();

        final List<KeyExpression> keyFields = new ArrayList<>();
        final List<KeyExpression> valueFields = new ArrayList<>();
        final List<KeyExpression> nonStoredFields = new ArrayList<>();
        final List<KeyExpression> otherFields = new ArrayList<>();
        partitionFields(recordType, index, indexTypes, indexPlan,
                rootExpression, keyFields, valueFields, nonStoredFields, otherFields);

        // Like FDBRecordStoreBase.indexEntryKey(), but with key expressions instead of actual values.
        Map<KeyExpression, FieldData> fields = new HashMap<>();

        final ImmutableMap<String, ImmutableIntArray> constituentNameToPathMap;
        constituentNameToPathMap = commonPrimaryKey == null
                                   ? ImmutableMap.of()
                                   : getConstituentToPathMap(recordType, keyFields.size());

        // KEYs -- from the index's definition
        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);
        int keyPosition = 0;
        for (KeyExpression keyField : keyFields) {
            KeyExpression keyFieldToAdd = keyField;
            FieldData fieldData = constrainFieldData(recordType, constituentNameToPathMap, keyFieldToAdd, ImmutableIntArray.of(keyPosition));
            if (keyFieldToAdd instanceof InvertibleFunctionKeyExpression) {
                InvertibleFunctionKeyExpression invertibleExpression = (InvertibleFunctionKeyExpression)keyFieldToAdd;
                if (invertibleExpression.isInjective()) {
                    // We could theoretically make both the function and its input available, but the former is never going
                    // to pass addCoveringField, so just try the latter.
                    keyFieldToAdd = invertibleExpression.getChild();
                    final String invertibleFunction = invertibleExpression.getName();
                    fieldData = fieldData.withInvertibleFunction(invertibleFunction);
                }
            }
            if (!nonStoredFields.contains(keyFieldToAdd) && !keyFieldToAdd.createsDuplicates() && addCoveringField(keyFieldToAdd, fieldData, builder)) {
                fields.put(keyFieldToAdd, fieldData);
            }
            keyPosition += keyFieldToAdd.getColumnSize();
        }
        if (commonPrimaryKey != null) {
            // KEYs -- from the primary key
            fields.putAll(getPrimaryKeyFieldMap(recordType, index, commonPrimaryKey, keyPosition, nonStoredFields, constituentNameToPathMap, builder));
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
    public static ImmutableMap<String, ImmutableIntArray> getConstituentToPathMap(final @Nonnull RecordType recordType, final int startPrimaryKey) {
        if (!recordType.isSynthetic()) {
            return ImmutableMap.of();
        }

        //
        // We need to only copy parts of the index entry which are not currently producing a null-constituent due to
        // an outer join.
        //
        Verify.verify(recordType instanceof SyntheticRecordType);
        final SyntheticRecordType<?> syntheticRecordType = (SyntheticRecordType<?>)recordType;
        final List<? extends SyntheticRecordType.Constituent> constituents = syntheticRecordType.getConstituents();
        final int startConstituents = startPrimaryKey + 1; // record type indicator accounts for one
        final ImmutableMap.Builder<String, ImmutableIntArray> constituentNameToPathMapBuilder =
                ImmutableMap.builder();
        for (int i = 0; i < constituents.size(); i++) {
            final SyntheticRecordType.Constituent constituent = constituents.get(i);
            constituentNameToPathMapBuilder.put(constituent.getName(), ImmutableIntArray.of(startConstituents + i));
        }
        return constituentNameToPathMapBuilder.build();
    }

    private static void partitionFields(@Nonnull final RecordType recordType,
                                        @Nonnull final Index index,
                                        @Nonnull final PlannableIndexTypes indexTypes,
                                        @Nonnull final RecordQueryPlanWithIndex indexPlan,
                                        @Nonnull final KeyExpression rootExpression,
                                        @Nonnull final List<KeyExpression> keyFields,
                                        @Nonnull final List<KeyExpression> valueFields,
                                        @Nonnull final List<KeyExpression> nonStoredFields,
                                        @Nonnull final List<KeyExpression> otherFields) {
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
                GroupingKeyExpression groupingKeyExpression = (GroupingKeyExpression)rootExpression;
                nonStoredFields.removeAll(groupingKeyExpression.getGroupingSubKey().normalizeKeyForPositions());
            }
            if (indexPlan instanceof PlanWithStoredFields) {
                ((PlanWithStoredFields)indexPlan).getStoredFields(keyFields, nonStoredFields, otherFields);
            }
        } else {
            // Aggregate index
            if (rootExpression instanceof GroupingKeyExpression) {
                GroupingKeyExpression groupingKeyExpression = (GroupingKeyExpression)rootExpression;
                keyFields.addAll(groupingKeyExpression.getGroupingSubKey().normalizeKeyForPositions());
            }
        }
    }

    public static Map<KeyExpression, FieldData> getPrimaryKeyFieldMap(@Nonnull final RecordType recordType,
                                                                      @Nonnull final Index index,
                                                                      @Nonnull final KeyExpression commonPrimaryKey,
                                                                      @Nonnull final int primaryKeyStartOrdinal,
                                                                      @Nonnull final List<KeyExpression> nonStoredFields,
                                                                      @Nonnull final ImmutableMap<String, ImmutableIntArray> constituentNameToPathMap,
                                                                      @Nonnull final IndexKeyValueToPartialRecord.Builder builder) {
        final ListMultimap<KeyExpression, FieldData> primaryKeyPartsToTuplePathMap;
        if (commonPrimaryKey != null) {
            final BitSet coveredPrimaryKeyPositions = index.getCoveredPrimaryKeyPositions();
            final ExpressionToTuplePathVisitor expressionToTuplePathVisitor =
                    new ExpressionToTuplePathVisitor(commonPrimaryKey,
                            IndexKeyValueToPartialRecord.TupleSource.KEY,
                            primaryKeyStartOrdinal, coveredPrimaryKeyPositions);
            primaryKeyPartsToTuplePathMap = expressionToTuplePathVisitor.compute();
        } else {
            primaryKeyPartsToTuplePathMap = ImmutableListMultimap.of();
        }

        final ImmutableMap.Builder<KeyExpression, FieldData> resultFieldMapBuilder = ImmutableMap.builder();
        for (final Map.Entry<KeyExpression, FieldData> entry : primaryKeyPartsToTuplePathMap.entries()) {
            KeyExpression keyField = entry.getKey();
            FieldData fieldData = entry.getValue();
            fieldData = constrainFieldData(recordType, constituentNameToPathMap, keyField, fieldData.getOrdinalPath());
            if (!nonStoredFields.contains(keyField) && !keyField.createsDuplicates() && addCoveringField(keyField, fieldData, builder)) {
                resultFieldMapBuilder.put(keyField, fieldData);
            }
        }
        return resultFieldMapBuilder.build();
    }

    @Nonnull
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
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
            final ImmutableIntArray conditionalPath = Objects.requireNonNull(constituentNameToPathMap.get(constituentName));
            return FieldData.ofConditional(IndexKeyValueToPartialRecord.TupleSource.KEY, path, conditionalPath);
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
                builder.addField(fieldKeyExpression.getFieldName(), fieldData.source,
                        fieldData.copyIfPredicate, fieldData.ordinalPath, fieldData.invertibleFunction);
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
        @Nonnull
        private final CopyIfPredicate copyIfPredicate;
        @Nullable
        private final String invertibleFunction;

        private FieldData(@Nonnull final IndexKeyValueToPartialRecord.TupleSource source,
                          @Nonnull final ImmutableIntArray ordinalPath,
                          @Nonnull final CopyIfPredicate copyIfPredicate,
                          @Nullable final String invertibleFunction) {
            this.source = source;
            this.ordinalPath = ordinalPath;
            this.copyIfPredicate = copyIfPredicate;
            this.invertibleFunction = invertibleFunction;
        }

        @Nonnull
        public IndexKeyValueToPartialRecord.TupleSource getSource() {
            return source;
        }

        public ImmutableIntArray getOrdinalPath() {
            return ordinalPath;
        }

        public CopyIfPredicate getCopyIfPredicate() {
            return copyIfPredicate;
        }

        @Nullable
        public String getInvertibleFunction() {
            return invertibleFunction;
        }

        @Nonnull
        public FieldData withInvertibleFunction(@Nullable final String invertibleFunction) {
            return new FieldData(source, ordinalPath, copyIfPredicate, invertibleFunction);
        }

        @Nonnull
        public static FieldData ofUnconditional(@Nonnull final IndexKeyValueToPartialRecord.TupleSource source,
                                                @Nonnull final ImmutableIntArray ordinalPath) {
            return new FieldData(source, ordinalPath, new TruePredicate(), null);
        }

        @Nonnull
        public static FieldData ofConditional(@Nonnull final IndexKeyValueToPartialRecord.TupleSource source,
                                              @Nonnull final ImmutableIntArray ordinalPath,
                                              @Nonnull final ImmutableIntArray conditionalPath) {
            return new FieldData(source, ordinalPath, new ConditionalUponPathPredicate(conditionalPath), null);
        }
    }

    /**
     * A predicate that tests whether a copy in
     * {@link com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.Copier} is necessary. In addition,
     * this predicate is also {@link PlanHashable} and {@link PlanSerializable}.
     */
    @API(API.Status.INTERNAL)
    public interface CopyIfPredicate extends Predicate<Tuple>, PlanHashable, PlanSerializable {
        @Nonnull
        PCopyIfPredicate toCopyIfPredicateProto(@Nonnull PlanSerializationContext serializationContext);

        @Nonnull
        static CopyIfPredicate fromCopyIfPredicateProto(@Nonnull final PlanSerializationContext serializationContext,
                                                        @Nonnull final PCopyIfPredicate copyIfPredicateProto) {
            return (CopyIfPredicate)PlanSerialization.dispatchFromProtoContainer(serializationContext, copyIfPredicateProto);
        }
    }

    /**
     * A copy-if-predicate that always returns {@code true}.
     */
    public static class TruePredicate implements CopyIfPredicate {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("True-Predicate");

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return PlanHashable.objectPlanHash(hashMode, BASE_HASH);
        }

        @Nonnull
        @Override
        public PTruePredicate toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PTruePredicate.newBuilder().build();
        }

        @Nonnull
        @Override
        public PCopyIfPredicate toCopyIfPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PCopyIfPredicate.newBuilder().setTruePredicate(toProto(serializationContext)).build();
        }

        @Override
        public boolean test(final Tuple tuple) {
            return true;
        }

        @Nonnull
        public static TruePredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PTruePredicate truePredicateProto) {
            return new TruePredicate();
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PTruePredicate, TruePredicate> {
            @Nonnull
            @Override
            public Class<PTruePredicate> getProtoMessageClass() {
                return PTruePredicate.class;
            }

            @Nonnull
            @Override
            public TruePredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final PTruePredicate truePredicateProto) {
                return TruePredicate.fromProto(serializationContext, truePredicateProto);
            }
        }
    }

    /**
     * A copy-if-predicate that returns {@code true} if a particula ordinal path already exists in the tuple.
     */
    public static class ConditionalUponPathPredicate implements CopyIfPredicate {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Conditional-Upon-Predicate");

        @Nonnull
        private final ImmutableIntArray conditionalPath;

        public ConditionalUponPathPredicate(@Nonnull final ImmutableIntArray conditionalPath) {
            this.conditionalPath = conditionalPath;
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, conditionalPath);
        }

        @Nonnull
        @Override
        public PConditionalUponPathPredicate toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PConditionalUponPathPredicate.Builder builder = PConditionalUponPathPredicate.newBuilder();
            conditionalPath.forEach(i -> builder.addOrdinalPath(i));
            return builder.build();
        }

        @Nonnull
        @Override
        public PCopyIfPredicate toCopyIfPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PCopyIfPredicate.newBuilder().setConditionalUponPathPredicate(toProto(serializationContext)).build();
        }

        @Override
        public boolean test(final Tuple tuple) {
            return IndexKeyValueToPartialRecord.existsSubTupleForOrdinalPath(tuple, conditionalPath);
        }

        @Nonnull
        public static ConditionalUponPathPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                             @Nonnull final PConditionalUponPathPredicate conditionalUponPredicateProto) {
            final ImmutableIntArray.Builder ordinalPathBuilder = ImmutableIntArray.builder();
            for (int i = 0; i < conditionalUponPredicateProto.getOrdinalPathCount(); i ++) {
                ordinalPathBuilder.add(i);
            }
            return new ConditionalUponPathPredicate(ordinalPathBuilder.build());
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PConditionalUponPathPredicate, ConditionalUponPathPredicate> {
            @Nonnull
            @Override
            public Class<PConditionalUponPathPredicate> getProtoMessageClass() {
                return PConditionalUponPathPredicate.class;
            }

            @Nonnull
            @Override
            public ConditionalUponPathPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                          @Nonnull final PConditionalUponPathPredicate conditionalUponPathPredicateProto) {
                return ConditionalUponPathPredicate.fromProto(serializationContext, conditionalUponPathPredicateProto);
            }
        }
    }
}
