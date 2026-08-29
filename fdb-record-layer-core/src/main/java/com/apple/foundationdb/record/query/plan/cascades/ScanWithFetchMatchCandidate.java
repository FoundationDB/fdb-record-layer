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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.ImmutableIntArray;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Interface to represent a candidate that replaces with an index scan.
 */
public interface ScanWithFetchMatchCandidate extends WithPrimaryKeyMatchCandidate {
    Optional<Value> pushValueThroughFetch(Value value,
                                          CorrelationIdentifier sourceAlias,
                                          CorrelationIdentifier targetAlias);

    static Optional<Value> pushValueThroughFetch(final Value toBePushedValue,
                                                 final CorrelationIdentifier baseAlias,
                                                 final CorrelationIdentifier sourceAlias,
                                                 final CorrelationIdentifier targetAlias,
                                                 final Iterable<? extends Value> providedValuesFromIndex) {
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

    private static boolean isOfPushableTypesOrConstant(final Value toBePushedValue,
                                                       final CorrelationIdentifier sourceAlias) {
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

    static boolean addCoveringField(final IndexKeyValueToPartialRecord.Builder builder,
                                    final FieldValue fieldValue,
                                    final Value extractFromIndexEntryValue) {
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

    private static Optional<IndexKeyValueToPartialRecord.Builder> getParentBuilderForFieldMaybe(IndexKeyValueToPartialRecord.Builder builder,
                                                                                                final FieldValue fieldValue) {
        // TODO field names are for debugging purposes only, we should probably use field ordinals here instead.
        for (final var maybeFieldName : fieldValue.getFieldPrefix().getOptionalFieldNames()) {
            if (maybeFieldName.isEmpty()) {
                return Optional.empty();
            }
            builder = builder.getFieldBuilder(maybeFieldName.get());
        }

        return Optional.of(builder);
    }

    static Optional<ScanWithFetchMatchCandidate.IndexEntryToLogicalRecord> computeIndexEntryToLogicalRecord(final Collection<RecordType> queriedRecordTypes,
                                                                                                            final CorrelationIdentifier baseAlias,
                                                                                                            final Type baseType,
                                                                                                            final List<Value> indexKeyValues,
                                                                                                            final List<Value> indexValueValues) {
        if (queriedRecordTypes.size() > 1) {
            return Optional.empty();
        }
        final var queriedRecordType = Iterables.getOnlyElement(queriedRecordTypes);
        final var builder = IndexKeyValueToPartialRecord.newBuilder(queriedRecordType);
        final var baseObjectValue = QuantifiedObjectValue.of(baseAlias, baseType);
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
                logicalValueValuesBuilder.add(extractFromIndexEntryPair.getLeft());
            }
        }

        if (!builder.isValid()) {
            return Optional.empty();
        }

        return Optional.of(
                new ScanWithFetchMatchCandidate.IndexEntryToLogicalRecord(queriedRecordType, builder.build(),
                        logicalKeyValuesBuilder.build(), logicalValueValuesBuilder.build()));
    }

    /**
     * Helper structure that allows us to precompute the mapping from index entry to the logical (partial record).
     */
    class IndexEntryToLogicalRecord {
        private final RecordType queriedRecordType;
        private final IndexKeyValueToPartialRecord indexKeyValueToPartialRecord;
        private final List<Value> logicalKeyValues;
        private final List<Value> logicalValueValues;

        public IndexEntryToLogicalRecord(final RecordType queriedRecordType,
                                         final IndexKeyValueToPartialRecord indexKeyValueToPartialRecord,
                                         final List<Value> logicalKeyValues,
                                         final List<Value> logicalValueValues) {
            this.queriedRecordType = queriedRecordType;
            this.indexKeyValueToPartialRecord = indexKeyValueToPartialRecord;
            this.logicalKeyValues = logicalKeyValues;
            this.logicalValueValues = logicalValueValues;
        }

        public RecordType getQueriedRecordType() {
            return queriedRecordType;
        }

        public IndexKeyValueToPartialRecord getIndexKeyValueToPartialRecord() {
            return indexKeyValueToPartialRecord;
        }

        public List<Value> getLogicalKeyValues() {
            return logicalKeyValues;
        }

        public List<Value> getLogicalValueValues() {
            return logicalValueValues;
        }
    }
}
