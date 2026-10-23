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
import com.apple.foundationdb.record.query.plan.cascades.values.IndexEntryColumnValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
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
     *
     * @return the rewritten value, or an empty optional if the value cannot be pushed
     */
    @Nonnull
    static Optional<Value> pushValueThroughFetch(@Nonnull final Value toBePushedValue,
                                                 @Nonnull final CorrelationIdentifier baseAlias,
                                                 @Nonnull final CorrelationIdentifier sourceAlias,
                                                 @Nonnull final CorrelationIdentifier targetAlias,
                                                 @Nonnull final Iterable<? extends Value> providedValuesFromIndex,
                                                 @Nonnull final Iterable<NonnullPair<Value, Value>> computedColumns) {
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
                                                       @Nonnull final Iterable<NonnullPair<Value, Value>> computedColumns) {
        if (!toBePushedValue.getCorrelatedTo().contains(sourceAlias)) {
            return true;
        }
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
            // Effectively, this check is needed because of values like the VersionValue, which aren't
            // accessible without a record fetch, even if the index entry contains a VersionValue. We
            // should address this by attaching the version to a partial record if there's one in the
            // index entry, but until then, this prevents us from incorrectly applying a covering
            // optimization.
            return false;
        }
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
        final var builder = IndexKeyValueToPartialRecord.newBuilder(queriedRecordType);
        final var baseObjectValue = QuantifiedObjectValue.of(baseAlias, baseType);
        final var covered = new IndexEntryToRecordValueHelper();
        final var computedColumnsBuilder = ImmutableList.<NonnullPair<Value, Value>>builder();
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
                logicalKeyValuesBuilder.add(extractFromIndexEntryPair.getLeft());
            } else {
                final NonnullPair<Value, Value> computedColumn =
                        computedColumnOrNull(keyValue, baseAlias, IndexKeyValueToPartialRecord.TupleSource.KEY, i);
                if (computedColumn != null) {
                    computedColumnsBuilder.add(computedColumn);
                }
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
                logicalValueValuesBuilder.add(extractFromIndexEntryPair.getLeft());
            } else {
                final NonnullPair<Value, Value> computedColumn =
                        computedColumnOrNull(valueValue, baseAlias, IndexKeyValueToPartialRecord.TupleSource.VALUE, i);
                if (computedColumn != null) {
                    computedColumnsBuilder.add(computedColumn);
                }
            }
        }

        if (!builder.isValid()) {
            return Optional.empty();
        }

        return Optional.of(
                new ScanWithFetchMatchCandidate.IndexEntryToLogicalRecord(queriedRecordType, builder.build(),
                        logicalKeyValuesBuilder.build(), logicalValueValuesBuilder.build(),
                        computedColumnsBuilder.build(), indexEntryToRecordValue(baseType, covered)));
    }

    /**
     * Pairs an element of an index entry that the index computes from a record but that no field of the record holds
     * (for example, {@code cardinality(_.arr)}) with the value that reads that element back out of an index entry. A
     * partial record has no place for such an element, but a covering scan flows the entry a record was decoded from,
     * so a query asking for the same computation can read the element instead of recomputing it from fields the index
     * does not carry.
     * <p>
     * The pairing only holds for an element the index computes from the record alone. An element reading an exploded
     * repeated field, or one folding in a constant binding, is correlated to something other than the base and is
     * refused here. An element that is a plain field read is refused too: it belongs to the partial record, and
     * {@code extractFromIndexEntryMaybe} having turned it down means it cannot be restored, the record version being
     * the case in point.
     *
     * @param indexColumnValue what the index computes from a record to produce the element
     * @param baseAlias the alias standing for a record, which the returned pair is correlated to
     * @param source whether the element sits in the key or the value tuple of an entry
     * @param ordinal the position of the element in that tuple
     *
     * @return the pair, or {@code null} if the element cannot be read back
     */
    @Nullable
    private static NonnullPair<Value, Value> computedColumnOrNull(@Nonnull final Value indexColumnValue,
                                                                  @Nonnull final CorrelationIdentifier baseAlias,
                                                                  @Nonnull final IndexKeyValueToPartialRecord.TupleSource source,
                                                                  final int ordinal) {
        if (indexColumnValue instanceof FieldValue) {
            return null;
        }
        final Type resultType = indexColumnValue.getResultType();
        if (!resultType.isPrimitive() && !resultType.isEnum() && !resultType.isUuid()) {
            return null;
        }
        if (!indexColumnValue.getCorrelatedTo().equals(ImmutableSet.of(baseAlias))) {
            return null;
        }
        return NonnullPair.of(indexColumnValue,
                new IndexEntryColumnValue(baseAlias, source, ImmutableIntArray.of(ordinal), resultType));
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
     * @param queriedRecordType the record type an entry decodes into
     * @param indexKeyValueToPartialRecord the copiers that decode it
     * @param logicalKeyValues what each entry key column reads from the record
     * @param logicalValueValues what each entry value column reads from the record
     * @param computedColumns what each entry column that a partial record has no place for computes from the record,
     *        paired with the value reading that column back out of an entry
     * @param indexEntryToRecordValue the value that decodes it, or {@code null} if one cannot be built
     */
    record IndexEntryToLogicalRecord(@Nonnull RecordType queriedRecordType,
                                     @Nonnull IndexKeyValueToPartialRecord indexKeyValueToPartialRecord,
                                     @Nonnull List<Value> logicalKeyValues, @Nonnull List<Value> logicalValueValues,
                                     @Nonnull List<NonnullPair<Value, Value>> computedColumns,
                                     @Nullable Value indexEntryToRecordValue) {
    }
}
