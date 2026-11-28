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
                                                 @Nonnull final Iterable<? extends Value> providedValuesFromIndex) {
        if (!isOfPushableTypesOrConstant(toBePushedValue, sourceAlias)) {
            return Optional.empty();
        }

        final AliasMap equivalenceMap = AliasMap.ofAliases(sourceAlias, baseAlias);
        final AliasMap toTargetAliasMap = AliasMap.ofAliases(sourceAlias, targetAlias);

        final var translatedValueOptional =
                toBePushedValue.<Value>mapMaybe((value, mappedChildren) -> {
                    for (final var providedValue : providedValuesFromIndex) {
                        if (value.semanticEquals(providedValue, equivalenceMap)) {
                            return value.withChildren(mappedChildren)
                                    .rebase(toTargetAliasMap);
                        }
                    }
                    return value.withChildren(mappedChildren); // this may be correlated to sourceAlias
                });

        // the translation was successful if the translated value is not correlated to sourceAlias anymore
        return translatedValueOptional.filter(translatedValue -> !translatedValue.getCorrelatedTo().contains(sourceAlias));
    }

    private static boolean isOfPushableTypesOrConstant(@Nonnull final Value toBePushedValue,
                                                       @Nonnull final CorrelationIdentifier sourceAlias) {
        if (!toBePushedValue.getCorrelatedTo().contains(sourceAlias)) {
            return true;
        }
        if (toBePushedValue instanceof FieldValue) {
            return true;
        } else if (toBePushedValue instanceof RecordConstructorValue) {
            return ((RecordConstructorValue)toBePushedValue).getColumns().stream()
                    .allMatch(column -> isOfPushableTypesOrConstant(column.getValue(), sourceAlias));
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
        final var logicalKeyValuesBuilder = ImmutableList.<Value>builder();
        for (int i = 0; i < indexKeyValues.size(); i++) {
            if (!addDatum(builder, covered, logicalKeyValuesBuilder, baseObjectValue,
                    IndexKeyValueToPartialRecord.TupleSource.KEY, indexKeyValues.get(i), ImmutableIntArray.of(i))) {
                return Optional.empty();
            }
        }
        final var logicalValueValuesBuilder = ImmutableList.<Value>builder();
        for (int i = 0; i < indexValueValues.size(); i++) {
            if (!addDatum(builder, covered, logicalValueValuesBuilder, baseObjectValue,
                    IndexKeyValueToPartialRecord.TupleSource.VALUE, indexValueValues.get(i), ImmutableIntArray.of(i))) {
                return Optional.empty();
            }
        }

        if (!builder.isValid()) {
            return Optional.empty();
        }

        return Optional.of(
                new ScanWithFetchMatchCandidate.IndexEntryToLogicalRecord(queriedRecordType, builder.build(),
                        logicalKeyValuesBuilder.build(), logicalValueValuesBuilder.build(),
                        indexEntryToRecordValue(baseType, covered)));
    }

    /**
     * Reads a datum of an index entry into the field of the record it holds, if it holds one. A datum is a column of
     * the entry or, if the column holds a nested tuple, one of the columns of that tuple, one level deeper.
     *
     * @param builder the copiers reading the entry into a partial record
     * @param covered what the entry covers so far, for the value reading it into a record
     * @param logicalValuesBuilder the fields of the record read from the entry so far
     * @param baseObjectValue the record the index is over
     * @param source whether the datum is in the key or the value of the entry
     * @param value the value the expansion of the index registered for the datum
     * @param ordinalPath the path to the datum in the tuple of {@code source}
     * @return {@code false} if the datum holds a field the partial record cannot hold, {@code true} otherwise
     */
    private static boolean addDatum(@Nonnull final IndexKeyValueToPartialRecord.Builder builder,
                                    @Nonnull final IndexEntryToRecordValueHelper covered,
                                    @Nonnull final ImmutableList.Builder<Value> logicalValuesBuilder,
                                    @Nonnull final Value baseObjectValue,
                                    @Nonnull final IndexKeyValueToPartialRecord.TupleSource source,
                                    @Nonnull final Value value,
                                    @Nonnull final ImmutableIntArray ordinalPath) {
        // An index key or value registered as a record constructor comes from the expansion of a list key expression
        // (see KeyExpressionExpansionVisitor#visitExpression(ListKeyExpression)): the entry holds a nested tuple
        // there, rather than a field the record can be read from as a whole, so each of its columns is read instead.
        if (value instanceof final RecordConstructorValue recordConstructorValue) {
            final var columns = recordConstructorValue.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                final var columnPath = ImmutableIntArray.builder(ordinalPath.length() + 1).addAll(ordinalPath).add(i).build();
                if (!addDatum(builder, covered, logicalValuesBuilder, baseObjectValue, source,
                        columns.get(i).getValue(), columnPath)) {
                    return false;
                }
            }
            return true;
        }

        final var extraction = value.extractFromIndexEntryMaybe(baseObjectValue, EvaluationContext.empty(),
                AliasMap.emptyMap(), ImmutableSet.of(), source, ordinalPath).orElse(null);
        if (extraction == null) {
            return true;
        }
        if (!addCoveringField(builder, extraction.getKey(), extraction.getValue())) {
            return false;
        }
        recordCoveredField(covered, extraction);
        logicalValuesBuilder.add(extraction.getKey());
        return true;
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
     */
    record IndexEntryToLogicalRecord(@Nonnull RecordType queriedRecordType,
                                     @Nonnull IndexKeyValueToPartialRecord indexKeyValueToPartialRecord,
                                     @Nonnull List<Value> logicalKeyValues, @Nonnull List<Value> logicalValueValues,
                                     @Nullable Value indexEntryToRecordValue) {
    }
}
