/*
 * ScanWithFetchMatchCandidate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.ImmutableIntArray;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Interface to represent a candidate that replaces with an index scan.
 */
public interface ScanWithFetchMatchCandidate extends WithPrimaryKeyMatchCandidate {
    @Nonnull
    Optional<Value> pushValueThroughFetch(@Nonnull Value value,
                                          @Nonnull CorrelationIdentifier sourceAlias,
                                          @Nonnull CorrelationIdentifier targetAlias);

    @Nonnull
    static Optional<Value> pushValueThroughFetch(@Nonnull final Value toBePushedValue,
                                                 @Nonnull final CorrelationIdentifier baseAlias,
                                                 @Nonnull final CorrelationIdentifier sourceAlias,
                                                 @Nonnull final CorrelationIdentifier targetAlias,
                                                 @Nonnull final Value providedRecordValue) {
        //
        // Maximum-match the value against the record the index scan provides, so that as much of it as the entry can
        // supply gets expressed over the scan. A value computed from provided fields pushes even though it is not
        // itself a field, while one reading a field the entry lacks stays correlated to sourceAlias and fails below.
        //
        final var maxMatchMap =
                MaxMatchMap.compute(toBePushedValue, providedRecordValue,
                        ImmutableSet.of(baseAlias),
                        ValueEquivalence.fromAliasMap(AliasMap.ofAliases(sourceAlias, baseAlias)));

        // the translation was successful if the translated value is not correlated to sourceAlias anymore
        return maxMatchMap.translateQueryValueMaybe(targetAlias)
                .map(translatedValue -> translatedValue.simplify(EvaluationContext.empty(), AliasMap.emptyMap(), ImmutableSet.of()))
                .filter(translatedValue -> !translatedValue.getCorrelatedTo().contains(sourceAlias));
    }

    static boolean addCoveringField(@Nonnull final IndexKeyValueToPartialRecord.Builder builder,
                                    @Nonnull final FieldValue fieldValue,
                                    @Nonnull final Value extractFromIndexEntryValue) {
        final var parentBuilderForFieldOptional = getParentBuilderForFieldMaybe(builder, fieldValue);
        if (parentBuilderForFieldOptional.isEmpty()) {
            return false;
        }

        final var parentBuilderForField = parentBuilderForFieldOptional.get();

        // TODO not sure what to do with the null standing requirement
        final var maybeFieldName = fieldValue.getFieldPath().getLastFieldStorageName();
        if (maybeFieldName.isEmpty()) {
            return false;
        }
        final String fieldName = maybeFieldName.get();
        if (!parentBuilderForField.hasField(fieldName)) {
            parentBuilderForField.addField(fieldName, extractFromIndexEntryValue);
        }
        return true;
    }

    @Nonnull
    private static Optional<IndexKeyValueToPartialRecord.Builder> getParentBuilderForFieldMaybe(@Nonnull IndexKeyValueToPartialRecord.Builder builder,
                                                                                                @Nonnull final FieldValue fieldValue) {
        // TODO field names are for debugging purposes only, we should probably use field ordinals here instead.
        for (final var maybeFieldName : fieldValue.getFieldPrefix().getOptionalFieldStorageNames()) {
            if (maybeFieldName.isEmpty()) {
                return Optional.empty();
            }
            builder = builder.getFieldBuilder(maybeFieldName.get());
        }

        return Optional.of(builder);
    }

    @Nonnull
    static Optional<ScanWithFetchMatchCandidate.IndexEntryToLogicalRecord> computeIndexEntryToLogicalRecord(@Nonnull final Collection<RecordType> queriedRecordTypes,
                                                                                                            @Nonnull final CorrelationIdentifier baseAlias,
                                                                                                            @Nonnull final Type baseType,
                                                                                                            @Nonnull final List<Value> indexKeyValues,
                                                                                                            @Nonnull final List<Value> indexValueValues) {
        if (queriedRecordTypes.size() > 1) {
            return Optional.empty();
        }
        final var queriedRecordType = Iterables.getOnlyElement(queriedRecordTypes);
        final var builder = IndexKeyValueToPartialRecord.newBuilder(queriedRecordType);
        final var baseObjectValue = QuantifiedObjectValue.of(baseAlias, baseType);
        final var covered = new IndexEntryToRecordValueHelper();
        final var providedFieldsBuilder = ImmutableList.<FieldValue>builder();
        final var logicalKeyValuesBuilder = ImmutableList.<Value>builder();
        for (int i = 0; i < indexKeyValues.size(); i++) {
            final Value keyValue = indexKeyValues.get(i);

            final var extractFromIndexEntryPairOptional =
                    keyValue.extractFromIndexEntryMaybe(baseObjectValue, EvaluationContext.empty(), AliasMap.emptyMap(),
                            ImmutableSet.of(), IndexKeyValueToPartialRecord.TupleSource.KEY, ImmutableIntArray.of(i));
            if (extractFromIndexEntryPairOptional.isPresent()) {
                final var extractFromIndexEntryPair = extractFromIndexEntryPairOptional.get();
                if (!addCoveringField(builder, extractFromIndexEntryPair.getKey(),
                        extractFromIndexEntryPair.getValue())) {
                    return Optional.empty();
                }
                recordCoveredField(covered, extractFromIndexEntryPair);
                providedFieldsBuilder.add(extractFromIndexEntryPair.getKey());
                logicalKeyValuesBuilder.add(extractFromIndexEntryPair.getLeft());
            }
        }

        final var logicalValueValuesBuilder = ImmutableList.<Value>builder();
        for (int i = 0; i < indexValueValues.size(); i++) {
            final Value valueValue = indexValueValues.get(i);
            final var extractFromIndexEntryPairOptional =
                    valueValue.extractFromIndexEntryMaybe(baseObjectValue, EvaluationContext.empty(),
                            AliasMap.emptyMap(), ImmutableSet.of(), IndexKeyValueToPartialRecord.TupleSource.VALUE,
                            ImmutableIntArray.of(i));
            if (extractFromIndexEntryPairOptional.isPresent()) {
                final var extractFromIndexEntryPair = extractFromIndexEntryPairOptional.get();
                if (!addCoveringField(builder, extractFromIndexEntryPair.getKey(),
                        extractFromIndexEntryPair.getValue())) {
                    return Optional.empty();
                }
                recordCoveredField(covered, extractFromIndexEntryPair);
                providedFieldsBuilder.add(extractFromIndexEntryPair.getKey());
                logicalValueValuesBuilder.add(extractFromIndexEntryPair.getLeft());
            }
        }

        if (!builder.isValid()) {
            return Optional.empty();
        }

        return Optional.of(
                new ScanWithFetchMatchCandidate.IndexEntryToLogicalRecord(queriedRecordType, builder.build(),
                        logicalKeyValuesBuilder.build(), logicalValueValuesBuilder.build(),
                        indexEntryToRecordValue(baseType, covered),
                        providedRecordValue(baseType, providedFieldsBuilder.build())));
    }

    /**
     * The record a covering scan provides, as a value over the base: every field the entry carries reads itself, the
     * rest read absent. Addressed by ordinal rather than by field name, because the names an index's extracted
     * {@link FieldValue}s carry are the record descriptor's, which differ from the base type's wherever an identifier
     * had to be escaped.
     *
     * @param baseType the record an entry is read into
     * @param providedFields what the entry carries, as paths into {@code baseType}
     *
     * @return that value, or {@code null} if one cannot be built
     */
    @Nullable
    private static Value providedRecordValue(@Nonnull final Type baseType,
                                             @Nonnull final List<FieldValue> providedFields) {
        if (!(baseType instanceof Type.Record)) {
            return null;
        }
        return providedRecordValue((Type.Record)baseType, providedFields, 0);
    }

    @Nonnull
    private static Value providedRecordValue(@Nonnull final Type.Record recordType,
                                             @Nonnull final List<FieldValue> providedFields,
                                             final int depth) {
        final var fields = recordType.getFields();
        final var columns = ImmutableList.<Column<? extends Value>>builder();
        for (int ordinal = 0; ordinal < fields.size(); ordinal++) {
            final var field = fields.get(ordinal);
            final int currentOrdinal = ordinal;
            final var through = providedFields.stream()
                    .filter(fieldValue -> fieldValue.getFieldOrdinals().length() > depth
                            && fieldValue.getFieldOrdinals().get(depth) == currentOrdinal)
                    .collect(ImmutableList.toImmutableList());
            // the first one ending here wins, keeping the first of an index's repeated columns for one field
            final var endingHere = through.stream()
                    .filter(fieldValue -> fieldValue.getFieldOrdinals().length() == depth + 1)
                    .findFirst();
            final Value column;
            if (endingHere.isPresent()) {
                column = endingHere.get();
            } else if (!through.isEmpty() && field.getFieldType() instanceof Type.Record) {
                column = providedRecordValue((Type.Record)field.getFieldType(), through, depth + 1);
            } else {
                column = IndexEntryToRecordValueHelper.absent(field.getFieldType());
            }
            columns.add(Column.of(field, column));
        }
        return RecordConstructorValue.ofColumns(columns.build());
    }

    /**
     * Records what the given extraction covers, descending a node per field of the path it fills, which may run into
     * nested messages. Every name is present here, {@code addCoveringField} having already refused the extraction
     * otherwise.
     */
    private static void recordCoveredField(@Nonnull final IndexEntryToRecordValueHelper covered,
                                           @Nonnull final NonnullPair<FieldValue, Value> extraction) {
        final var fieldValue = extraction.getKey();
        var node = covered;
        for (final var prefixFieldName : fieldValue.getFieldPrefix().getOptionalFieldNames()) {
            node = node.withChild(prefixFieldName.orElseThrow());
        }
        node.withChild(fieldValue.getFieldPath().getLastFieldName().orElseThrow()).cover(extraction.getValue());
    }

    /**
     * The value reading an entry into the queried record, or {@code null} when one cannot be built and the copiers do the
     * decoding instead. Nested covered fields need a value per submessage, which is not built yet.
     */
    @Nullable
    private static Value indexEntryToRecordValue(@Nonnull final Type baseType,
                                                 @Nonnull final IndexEntryToRecordValueHelper covered) {
        if (covered.getChildrenMap() == null || !(baseType instanceof Type.Record)) {
            return null;
        }
        return covered.toRecordValue((Type.Record)baseType);
    }

    /**
     * The precomputed mapping from an index entry to the logical (partial) record, both ways of decoding it.
     *
     * @param queriedRecordType the record type an entry decodes into
     * @param indexKeyValueToPartialRecord the copiers that decode it
     * @param logicalKeyValues what each entry key column reads from the record
     * @param logicalValueValues what each entry value column reads from the record
     * @param indexEntryToRecordValue the value that decodes it, or {@code null} if one cannot be built
     * @param providedRecordValue the record a covering scan provides, each carried field reading itself, or
     *        {@code null} if one cannot be built
     */
    record IndexEntryToLogicalRecord(@Nonnull RecordType queriedRecordType,
                                     @Nonnull IndexKeyValueToPartialRecord indexKeyValueToPartialRecord,
                                     @Nonnull List<Value> logicalKeyValues, @Nonnull List<Value> logicalValueValues,
                                     @Nullable Value indexEntryToRecordValue,
                                     @Nullable Value providedRecordValue) {
    }
}
