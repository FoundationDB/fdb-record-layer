/*
 * ScanWithFetchMatchCandidate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.values.IndexEntryColumnValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
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

    /**
     * Rewrites {@code toBePushedValue}, which reads the records a fetch produces, into a value reading what the index
     * scan underneath that fetch produces, so that the fetch can be dropped. Fails, returning an empty optional, when
     * the value asks for anything the index does not carry.
     *
     * @param toBePushedValue the value to rewrite, correlated to {@code sourceAlias}
     * @param baseAlias the alias standing for a record in {@code providedValuesFromIndex} and {@code computedColumns}
     * @param sourceAlias the alias {@code toBePushedValue} reads the fetched record through
     * @param targetAlias the alias the rewritten value is to read the index scan through
     * @param providedValuesFromIndex for each element of an index entry that a partial record holds, what the index
     *        reads from a record to produce that element
     * @param computedColumns for each remaining element of an index entry, what the index computes from a record to
     *        produce that element, paired with the value that reads that element back out of an index entry
     * @return the rewritten value, or an empty optional if the value cannot be pushed
     */
    @Nonnull
    static Optional<Value> pushValueThroughFetch(@Nonnull final Value toBePushedValue,
                                                 @Nonnull final CorrelationIdentifier baseAlias,
                                                 @Nonnull final CorrelationIdentifier sourceAlias,
                                                 @Nonnull final CorrelationIdentifier targetAlias,
                                                 @Nonnull final Iterable<? extends Value> providedValuesFromIndex,
                                                 @Nonnull final Iterable<NonnullPair<Value, IndexEntryColumnValue>> computedColumns) {
        final AliasMap equivalenceMap = AliasMap.ofAliases(sourceAlias, baseAlias);

        if (!isOfPushableTypesOrConstant(toBePushedValue, sourceAlias, equivalenceMap, computedColumns)) {
            return Optional.empty();
        }

        final AliasMap toTargetAliasMap = AliasMap.ofAliases(sourceAlias, targetAlias);
        final AliasMap baseToTargetAliasMap = AliasMap.ofAliases(baseAlias, targetAlias);

        final var translatedValueOptional =
                toBePushedValue.<Value>mapMaybe((value, mappedChildren) -> {
                    for (final var providedValue : providedValuesFromIndex) {
                        if (value.semanticEquals(providedValue, equivalenceMap)) {
                            return value.withChildren(mappedChildren)
                                    .rebase(toTargetAliasMap);
                        }
                    }

                    // Replace a value that the index computes with the value that reads it out of an index entry.
                    for (final var computedColumn : computedColumns) {
                        if (value.semanticEquals(computedColumn.getKey(), equivalenceMap)) {
                            return computedColumn.getValue().rebase(baseToTargetAliasMap);
                        }
                    }
                    return value.withChildren(mappedChildren); // this may be correlated to sourceAlias
                });

        // the translation was successful if the translated value is not correlated to sourceAlias anymore
        return translatedValueOptional.filter(translatedValue -> !translatedValue.getCorrelatedTo().contains(sourceAlias));
    }

    private static boolean isOfPushableTypesOrConstant(@Nonnull final Value toBePushedValue,
                                                       @Nonnull final CorrelationIdentifier sourceAlias,
                                                       @Nonnull final AliasMap equivalenceMap,
                                                       @Nonnull final Iterable<NonnullPair<Value, IndexEntryColumnValue>> computedColumns) {
        if (!toBePushedValue.getCorrelatedTo().contains(sourceAlias)) {
            return true;
        }

        // Accept a value that the index computes, as it can be read out of an index entry.
        for (final var computedColumn : computedColumns) {
            if (toBePushedValue.semanticEquals(computedColumn.getKey(), equivalenceMap)) {
                return true;
            }
        }

        if (toBePushedValue instanceof FieldValue) {
            return true;
        } else if (toBePushedValue instanceof RecordConstructorValue) {
            return ((RecordConstructorValue)toBePushedValue).getColumns().stream()
                    .allMatch(column -> isOfPushableTypesOrConstant(column.getValue(), sourceAlias, equivalenceMap,
                            computedColumns));
        } else {
            // A value that combines pushable values, such as an arithmetic expression over a computed column, can be
            // evaluated on top of whatever those values are pushed to. So far, we only do that for values that read at
            // least one computed column, as those are the ones that cannot be recomputed from the partial record.
            //
            // A value that reads the record itself, or that has no children, is not pushable. This check is needed
            // because of values like the VersionValue, which aren't accessible without a record fetch, even if the
            // index entry contains a VersionValue. We should address this by attaching the version to a partial
            // record if there's one in the index entry, but until then, this prevents us from incorrectly applying a
            // covering optimization.
            final Iterable<? extends Value> children = toBePushedValue.getChildren();
            return !Iterables.isEmpty(children) &&
                   readsComputedColumn(toBePushedValue, equivalenceMap, computedColumns) &&
                   Streams.stream(children).allMatch(child ->
                           isOfPushableTypesOrConstant(child, sourceAlias, equivalenceMap, computedColumns));
        }
    }

    /**
     * Checks whether {@code value} or any value it is composed of is a value that the index computes, in other words
     * whether {@code value} reads a computed column.
     *
     * @param value the value to inspect
     * @param equivalenceMap the aliases that stand for the same record in {@code value} and in the computed columns
     * @param computedColumns the computed columns, as pairs of the computed value and the value that reads it back
     * @return {@code true} if {@code value} reads at least one computed column
     */
    private static boolean readsComputedColumn(@Nonnull final Value value,
                                               @Nonnull final AliasMap equivalenceMap,
                                               @Nonnull final Iterable<NonnullPair<Value, IndexEntryColumnValue>> computedColumns) {
        return value.preOrderStream().anyMatch(descendant ->
                Streams.stream(computedColumns)
                        .anyMatch(computedColumn -> descendant.semanticEquals(computedColumn.getKey(), equivalenceMap)));
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
        final var maybeFieldName = fieldValue.getLastFieldName();
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
        for (final var maybeFieldName : fieldValue.getFieldPrefix().getOptionalFieldNames()) {
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
        final var collector = new ColumnCollector(queriedRecordType, baseAlias, baseType);
        if (!collector.collect(indexKeyValues, IndexKeyValueToPartialRecord.TupleSource.KEY)) {
            return Optional.empty();
        }
        if (!collector.collect(indexValueValues, IndexKeyValueToPartialRecord.TupleSource.VALUE)) {
            return Optional.empty();
        }
        if (!collector.isValid()) {
            return Optional.empty();
        }
        return Optional.of(
                new ScanWithFetchMatchCandidate.IndexEntryToLogicalRecord(
                        queriedRecordType,
                        collector.indexKeyValueToPartialRecord(),
                        collector.logicalKeyValues(),
                        collector.logicalValueValues(),
                        collector.computedColumns(),
                        indexEntryToRecordValue(baseType, collector.covered())));
    }

    /**
     * Decodes the columns of the key and the value tuple of an index entry into a partial record, setting aside the
     * computed columns that the partial record has no field for.
     */
    final class ColumnCollector {
        @Nonnull
        private final CorrelationIdentifier baseAlias;
        @Nonnull
        private final QuantifiedObjectValue baseObjectValue;
        @Nonnull
        private final IndexKeyValueToPartialRecord.Builder builder;
        @Nonnull
        private final IndexEntryToRecordValueHelper covered = new IndexEntryToRecordValueHelper();
        @Nonnull
        private final ImmutableList.Builder<Value> logicalKeyValuesBuilder = ImmutableList.builder();
        @Nonnull
        private final ImmutableList.Builder<Value> logicalValueValuesBuilder = ImmutableList.builder();
        @Nonnull
        private final ImmutableList.Builder<NonnullPair<Value, IndexEntryColumnValue>> computedColumnsBuilder =
                ImmutableList.builder();

        ColumnCollector(@Nonnull final RecordType queriedRecordType, @Nonnull final CorrelationIdentifier baseAlias,
                        @Nonnull final Type baseType) {
            this.baseAlias = baseAlias;
            this.baseObjectValue = QuantifiedObjectValue.of(baseAlias, baseType);
            this.builder = IndexKeyValueToPartialRecord.newBuilder(queriedRecordType);
        }

        /**
         * Decodes each column of one tuple of an index entry into the partial record, or sets it aside as a computed
         * column if the partial record has no place for it.
         *
         * @param indexValues what the index reads from a record to produce each column of the tuple
         * @param source whether the tuple is the key or the value of an entry
         *
         * @return {@code false} if a column cannot be decoded, which rules out covering the entry
         */
        boolean collect(@Nonnull final List<Value> indexValues,
                        @Nonnull final IndexKeyValueToPartialRecord.TupleSource source) {
            final ImmutableList.Builder<Value> logicalValuesBuilder =
                    source == IndexKeyValueToPartialRecord.TupleSource.KEY ? logicalKeyValuesBuilder
                                                                           : logicalValueValuesBuilder;
            for (int i = 0; i < indexValues.size(); i++) {
                final Value indexValue = indexValues.get(i);
                final ImmutableIntArray ordinalPath = ImmutableIntArray.of(i);

                // Decode a column that the partial record has a field for.
                final var coveredColumn = tryExtractFromIndexEntry(indexValue, source, ordinalPath);
                if (coveredColumn != null) {
                    final FieldValue fieldValue = coveredColumn.getKey();
                    final Value extractFromIndexEntryValue = coveredColumn.getValue();
                    if (!addCoveringField(builder, fieldValue, extractFromIndexEntryValue)) {
                        return false;
                    }
                    recordCoveredField(covered, coveredColumn);
                    logicalValuesBuilder.add(fieldValue);
                    continue;
                }

                // For a column that the partial record has no field for, see if it can be read back out of an entry.
                final var computedColumn = tryExtractComputedColumn(indexValue, baseAlias, source, ordinalPath);
                if (computedColumn != null) {
                    computedColumnsBuilder.add(computedColumn);
                }
            }
            return true;
        }

        /**
         * Convenience wrapper around {@link Value#extractFromIndexEntryMaybe}.
         */
        @Nullable
        private NonnullPair<FieldValue, Value> tryExtractFromIndexEntry(
                @Nonnull final Value indexValue, @Nonnull final IndexKeyValueToPartialRecord.TupleSource source,
                @Nonnull final ImmutableIntArray ordinalPath) {
            return indexValue.extractFromIndexEntryMaybe(baseObjectValue, EvaluationContext.empty(),
                    AliasMap.emptyMap(), ImmutableSet.of(), source, ordinalPath).orElse(null);
        }

        boolean isValid() {
            return builder.isValid();
        }

        @Nonnull
        IndexKeyValueToPartialRecord indexKeyValueToPartialRecord() {
            return builder.build();
        }

        @Nonnull
        List<Value> logicalKeyValues() {
            return logicalKeyValuesBuilder.build();
        }

        @Nonnull
        List<Value> logicalValueValues() {
            return logicalValueValuesBuilder.build();
        }

        @Nonnull
        List<NonnullPair<Value, IndexEntryColumnValue>> computedColumns() {
            return computedColumnsBuilder.build();
        }

        @Nonnull
        IndexEntryToRecordValueHelper covered() {
            return covered;
        }
    }

    /**
     * Pairs an element of an index entry that the index computes from a record but that the record has no field for
     * (for example, {@code cardinality(_.arr)}) with the value that reads that element back out of an index entry.
     * A partial record has no place for such an element, but a covering scan flows the entry a record was decoded from,
     * so a query asking for the same computation can read the element instead of recomputing it from fields the index
     * does not carry.
     *
     * @param indexColumnValue what the index computes from a record to produce the element
     * @param baseAlias the alias standing for a record, which the returned pair is correlated to
     * @param source whether the element sits in the key or the value tuple of an entry
     * @param ordinalPath the position of the element in that tuple
     * @return the pair, or {@code null} if the element cannot be read back
     */
    @Nullable
    private static NonnullPair<Value, IndexEntryColumnValue> tryExtractComputedColumn(@Nonnull final Value indexColumnValue,
                                                                                      @Nonnull final CorrelationIdentifier baseAlias,
                                                                                      @Nonnull final IndexKeyValueToPartialRecord.TupleSource source,
                                                                                      @Nonnull final ImmutableIntArray ordinalPath) {
        // Refuse a plain field read. It belongs to the partial record, not to the computed columns.
        if (indexColumnValue instanceof FieldValue) {
            return null;
        }

        // Refuse a value of a type that a single tuple element cannot hold, such as an array or a record. The column
        // value reads exactly one tuple element, which is a primitive, an enum, or a UUID.
        final Type resultType = indexColumnValue.getResultType();
        if (!resultType.isPrimitive() && !resultType.isEnum() && !resultType.isUuid()) {
            return null;
        }

        // Refuse an element that the index does not compute from the record alone, such as one reading an exploded
        // repeated field or folding in a constant binding. It is correlated to something other than the base.
        if (!indexColumnValue.getCorrelatedTo().equals(ImmutableSet.of(baseAlias))) {
            return null;
        }

        return NonnullPair.of(
                indexColumnValue,
                new IndexEntryColumnValue(baseAlias, source, ordinalPath, resultType));
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
        node.withChild(fieldValue.getLastFieldName().orElseThrow()).cover(extraction.getValue());
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
     * <p>The computed columns are the entry columns that the record has no fields for, such as the column of an index
     * on {@code cardinality(_.arr)}, and that the partial record therefore cannot decode. Each one is a pair of the
     * value that the index computes from a record to produce the column and an {@link IndexEntryColumnValue} that
     * reads the column back out of an entry. A query that asks for the same computation can thus read the column from
     * the entry that a covering scan flows, instead of recomputing it from fields the index does not carry. See
     * {@code tryExtractComputedColumn()} for which columns qualify.
     *
     * @param queriedRecordType the record type an entry decodes into
     * @param indexKeyValueToPartialRecord the copiers that decode it
     * @param logicalKeyValues what each entry key column reads from the record
     * @param logicalValueValues what each entry value column reads from the record
     * @param computedColumns the computed columns, as pairs of the computed value and the value that reads it back
     * @param indexEntryToRecordValue the value that decodes it, or {@code null} if one cannot be built
     */
    record IndexEntryToLogicalRecord(@Nonnull RecordType queriedRecordType,
                                     @Nonnull IndexKeyValueToPartialRecord indexKeyValueToPartialRecord,
                                     @Nonnull List<Value> logicalKeyValues, @Nonnull List<Value> logicalValueValues,
                                     @Nonnull List<NonnullPair<Value, IndexEntryColumnValue>> computedColumns,
                                     @Nullable Value indexEntryToRecordValue) {
    }
}
