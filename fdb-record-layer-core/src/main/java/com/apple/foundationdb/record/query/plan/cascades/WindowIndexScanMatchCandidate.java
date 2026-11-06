/*
 * MatchCandidate.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.RankScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Ordering.Binding;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.OrderingValueComputationRuleSet;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Case class to represent a match candidate that is backed by a windowed index such as a rank index.
 */
public class WindowIndexScanMatchCandidate implements ScanWithFetchMatchCandidate, WithBaseQuantifierMatchCandidate, ValueIndexLikeMatchCandidate {
    /**
     * Index metadata structure.
     */
    @Nonnull
    private final Index index;

    /**
     * Record types this index is defined over.
     */
    private final List<RecordType> queriedRecordTypes;

    /**
     * Base type.
     */
    @Nonnull
    private final Type.Record baseType;

    /**
     * Base alias.
     */
    @Nonnull
    private final CorrelationIdentifier baseAlias;

    /**
     * Holds the grouping aliases for all groupings that can to be bound during matching.
     */
    @Nonnull
    private final List<CorrelationIdentifier> parameters;

    /**
     * Traversal object of the expanded index scan graph.
     */
    @Nonnull
    private final Traversal traversal;

    @Nonnull
    private final KeyExpression fullKeyExpression;

    @Nullable
    private final KeyExpression primaryKey;
    @Nonnull
    private final Value postWindowResultValue;

    /**
     * The columns of the index entry's key, in entry key order, as values over the base record.
     */
    @Nonnull
    private final List<Value> indexKeyValues;

    /**
     * The record the merge value reads the columns an index entry does not carry from, in the post-window shape.
     */
    @Nonnull
    private final Value defaultRecord;

    @Nonnull
    private final Supplier<Optional<List<Value>>> primaryKeyValuesSupplier;

    @Nonnull
    private final Supplier<Optional<IndexEntryToLogicalRecord>> indexEntryToLogicalRecordOptionalSupplier;

    public WindowIndexScanMatchCandidate(@Nonnull Index index,
                                         @Nonnull Collection<RecordType> queriedRecordTypes,
                                         @Nonnull final Traversal traversal,
                                         @Nonnull final Type.Record baseType,
                                         @Nonnull final CorrelationIdentifier baseAlias,
                                         @Nonnull final List<CorrelationIdentifier> parameters,
                                         @Nonnull final List<Value> indexKeyValues,
                                         @Nonnull final KeyExpression fullKeyExpression,
                                         @Nullable final KeyExpression primaryKey,
                                         @Nonnull final Value postWindowResultValue,
                                         @Nonnull final Value defaultRecord) {
        this.index = index;
        this.queriedRecordTypes = ImmutableList.copyOf(queriedRecordTypes);
        this.traversal = traversal;
        this.baseType = baseType;
        this.baseAlias = baseAlias;
        this.parameters = ImmutableList.copyOf(parameters);
        this.fullKeyExpression = fullKeyExpression;
        this.primaryKey = primaryKey;
        this.postWindowResultValue = postWindowResultValue;
        this.defaultRecord = defaultRecord;
        this.indexKeyValues = ImmutableList.copyOf(indexKeyValues);
        this.primaryKeyValuesSupplier = Suppliers.memoize(() -> MatchCandidate.computePrimaryKeyValuesMaybe(primaryKey, baseType));
        this.indexEntryToLogicalRecordOptionalSupplier =
                Suppliers.memoize(() -> ScanWithFetchMatchCandidate.computeIndexEntryToLogicalRecord(queriedRecordTypes,
                        baseAlias, baseType, indexKeyValues, ImmutableList.of()));
    }

    /**
     * The result value of the post-window select, that is the shape this candidate's entries are decoded into. Its
     * trailing column is the window function itself, which no index entry key carries; the columns before it follow the
     * entry's key, in key order.
     *
     * @return the result value of the post-window select
     */
    @Nonnull
    public Value getPostWindowResultValue() {
        return postWindowResultValue;
    }

    @Override
    public int getColumnSize() {
        return index.getColumnSize();
    }

    @Override
    public boolean isUnique() {
        return index.isUnique();
    }

    @Nonnull
    @Override
    public String getName() {
        return index.getName();
    }

    @Nonnull
    @Override
    public List<RecordType> getQueriedRecordTypes() {
        return queriedRecordTypes;
    }

    @Nonnull
    @Override
    public Traversal getTraversal() {
        return traversal;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getSargableAliases() {
        return ImmutableList.<CorrelationIdentifier>builder().addAll(parameters).build();
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getOrderingAliases() {
        return getSargableAliases();
    }

    @Nonnull
    @Override
    public Type.Record getBaseType() {
        return baseType;
    }

    @Nonnull
    @Override
    public KeyExpression getFullKeyExpression() {
        return fullKeyExpression;
    }

    @Override
    public String toString() {
        return "Windowed[" + getName() + "]";
    }

    @Override
    public boolean createsDuplicates() {
        return index.getRootExpression().createsDuplicates();
    }

    @Nonnull
    @Override
    public Optional<List<Value>> getPrimaryKeyValuesMaybe() {
        return primaryKeyValuesSupplier.get();
    }

    @Nonnull
    private Optional<IndexEntryToLogicalRecord> getIndexEntryToLogicalRecordMaybe() {
        return indexEntryToLogicalRecordOptionalSupplier.get();
    }

    @Override
    public boolean isScopedToSingleType() {
        return queriedRecordTypes.size() == 1 || hasAndOrderedByRecordTypeKey();
    }

    @Nonnull
    @Override
    public Ordering computeOrderingFromScanComparisons(@Nonnull final ScanComparisons scanComparisons, final boolean isReverse, final boolean isDistinct) {
        final var bindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
        final var normalizedKeyExpressions = getFullKeyExpression().normalizeKeyForPositions();
        final var equalityComparisons = scanComparisons.getEqualityComparisons();
        final var groupingExpression = (GroupingKeyExpression)index.getRootExpression();
        final var scoreOrdinal = groupingExpression.getGroupingCount();

        // We keep a set for normalized values in order to check for duplicate values in the index definition.
        // We correct here for the case where an index is defined over {a, a} since its order is still just {a}.
        final var seenValues = Sets.newHashSetWithExpectedSize(normalizedKeyExpressions.size());

        for (var i = 0; i < equalityComparisons.size(); i++) {
            final var normalizedKeyExpression = normalizedKeyExpressions.get(i);
            final var comparison = equalityComparisons.get(i);

            if (normalizedKeyExpression.createsDuplicates()) {
                continue;
            }

            final var normalizedValue =
                    new ScalarTranslationVisitor(normalizedKeyExpression).toResultValue(Quantifier.current(),
                            getBaseType());

            seenValues.add(normalizedValue);
            if (i == scoreOrdinal) {
                bindingMapBuilder.put(normalizedValue, Binding.fixed(new Comparisons.OpaqueEqualityComparison()));
                seenValues.add(normalizedValue);
            } else {
                final var simplifiedComparisonPairOptional =
                        MatchCandidate.simplifyComparisonMaybe(normalizedValue, comparison);
                if (simplifiedComparisonPairOptional.isEmpty()) {
                    continue;
                }
                final var simplifiedComparisonPair = simplifiedComparisonPairOptional.get();
                bindingMapBuilder.put(simplifiedComparisonPair.getLeft(), Binding.fixed(simplifiedComparisonPair.getRight()));
                seenValues.add(simplifiedComparisonPair.getLeft());
            }
        }

        final var orderingSequenceBuilder = ImmutableList.<Value>builder();
        for (int i = scanComparisons.getEqualitySize(); i < normalizedKeyExpressions.size(); i++) {
            final KeyExpression normalizedKeyExpression = normalizedKeyExpressions.get(i);

            if (normalizedKeyExpression.createsDuplicates()) {
                break;
            }

            //
            // Note that it is not really important here if the keyExpression can be normalized in a lossless way
            // or not. A key expression containing repeated fields is sort-compatible with its normalized key
            // expression. We used to refuse to compute the sort order in the presence of repeats, however,
            // I think that restriction can be relaxed.
            //
            final var normalizedValue =
                    new ScalarTranslationVisitor(normalizedKeyExpression).toResultValue(Quantifier.current(),
                            getBaseType());

            final var providedOrderingPart =
                    normalizedValue.deriveOrderingPart(EvaluationContext.empty(), AliasMap.emptyMap(),
                            ImmutableSet.of(), OrderingPart.ProvidedOrderingPart::new,
                            OrderingValueComputationRuleSet.usingProvidedOrderingParts());

            final var providedOrderingValue = providedOrderingPart.getValue();
            if (!seenValues.contains(providedOrderingValue)) {
                seenValues.add(providedOrderingValue);
                bindingMapBuilder.put(providedOrderingValue,
                        Binding.sorted(providedOrderingPart.getSortOrder()
                                .flipIfReverse(isReverse)));
                orderingSequenceBuilder.add(providedOrderingValue);
            }
        }

        return Ordering.ofOrderingSequence(bindingMapBuilder.build(), orderingSequenceBuilder.build(), isDistinct);
    }

    @Nonnull
    @Override
    public RecordQueryPlan toEquivalentPlan(@Nonnull final PartialMatch partialMatch,
                                            @Nonnull final PlanContext planContext,
                                            @Nonnull final Memoizer memoizer,
                                            @Nonnull final List<ComparisonRange> comparisonRanges,
                                            final boolean reverseScanOrder) {
        //
        // The entry routes to a record and carries the rank besides: the scan is asked to report the rank in the
        // entry's value and the merge value reads it from there, so the two halves of that contract are set together.
        // The record is fetched as usual and supplies every column the entry does not carry.
        //
        final var scanComparisons = toScanComparisons(comparisonRanges);
        return new RecordQueryIndexPlan(index.getName(),
                primaryKey,
                new RankScanComparisons(scanTypeFor(scanComparisons), scanComparisons, true),
                planContext.getPlannerConfiguration().getIndexFetchMethod(),
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                reverseScanOrder,
                false,
                Optional.of(partialMatch.getMatchCandidate()),
                postWindowResultValue.getResultType(),
                partialMatch.getRegularMatchInfo().getConstraint(),
                createIndexEntryToRecordValue());
    }

    /**
     * Which of a rank index's two traversals the given comparisons permit.
     *
     * <p>A {@code BY_RANK} scan turns a range over ranks into a range over scores, and to do that it has to know which
     * ranked set to consult. It takes the group from the common prefix of the range endpoints, and refuses the scan if
     * the endpoints do not pin one down -- either because they are shorter than the grouping key ("ranked scan range
     * does not include group") or because they disagree on it ("ranked scan range crosses groups"). So
     * {@code BY_RANK} is available only when every partitioning column is bound by <em>equality</em>: an unbound
     * partitioning column fails the first way, and one bound by inequality fails the second. Anything less has to be
     * read as a plain key range over the index's primary subspace, which is what {@code BY_VALUE} does.
     *
     * <p>Either traversal returns the same entries, and the rank is reported in the entry value either way: the
     * maintainer looks it up per entry from that entry's own group rather than from the scan range, so a scan
     * spanning several partitions still reports each row's rank within its own partition.
     *
     * @param scanComparisons the comparisons the scan is built from
     *
     * @return the scan type to use
     */
    @Nonnull
    private IndexScanType scanTypeFor(@Nonnull final ScanComparisons scanComparisons) {
        final int partitioningCount = ((GroupingKeyExpression)index.getRootExpression()).getGroupingCount();
        return scanComparisons.getEqualitySize() >= partitioningCount
                ? IndexScanType.BY_RANK
                : IndexScanType.BY_VALUE;
    }

    /**
     * Builds the value that reads an index entry into the shape the post-window select describes, in place of the
     * copiers of an {@link IndexKeyValueToPartialRecord}. See
     * <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2907">issue 2907</a>.
     *
     * <p>The post-window shape and the entry key are two different orders over overlapping sets of columns. The shape
     * lists the partitioning columns, then every other column of the base record, then the window value; the entry key
     * lists the index key, then whatever the primary key adds to it. For {@code partition by b order by c} over a table
     * keyed on {@code (a, c)} the shape is {@code (b, c, a, d, rank)} while the key is {@code (b, c, recordTypeKey,
     * a)}. So a field cannot be read from the key ordinal it happens to sit at; it is read from the ordinal holding the
     * column it actually is, found by matching the field's base record expression against {@link #indexKeyValues}. A
     * field no key column matches -- {@code d} above -- is left to the record, and a key column no field matches --
     * {@code recordTypeKey} -- is simply never read. This mirrors
     * {@link AggregateIndexMatchCandidate#indexEntryToRecordValue}, where the grouping columns come from the entry key
     * and the aggregate from the entry value.
     *
     * <p>Between the two sit the base record columns the entry does not carry. The post-window select flows them so that a
     * query may select any column, not only the ones the index key mentions, and they are read from the record the
     * entry points to, which is fetched alongside it. The primary key is not among them: the expansion appends
     * whatever of it the index key does not already cover, so those columns are part of the entry key and are read
     * from it like any other.
     *
     * <p>The window value is no column of the entry key: a {@code BY_RANK} scan turns a rank range into a score range and
     * hands back ordinary entries. It is read from the entry's value, which a rank index otherwise leaves empty and
     * which {@link com.apple.foundationdb.record.provider.foundationdb.RankScanBounds} asks to be filled with the rank.
     *
     * @return the value computing a post-window record from an index entry
     */
    @Nonnull
    private Value createIndexEntryToRecordValue() {
        final var postWindowResultType = (Type.Record)postWindowResultValue.getResultType();
        final var fields = postWindowResultType.getFields();
        final int partitioningCount = ((GroupingKeyExpression)index.getRootExpression()).getGroupingCount();
        Verify.verify(fields.size() > partitioningCount,
                "the post-window select flows fewer columns than the index partitions by");

        // the alias is local to the extractions below, it only has to be the one the field values are rooted at
        final var entryObjectValue = QuantifiedObjectValue.of(Quantifier.current(), postWindowResultType);
        final var covered = new IndexEntryToRecordValueHelper();

        //
        // The entry's key columns first, at the same ordinals they occupy in the entry, and the window value last, read
        // from the entry's value. The fields in between are base record columns the entry does not carry, and are read
        // from the record the entry points to, which is fetched alongside it.
        //
        final int windowValueOrdinal = fields.size() - 1;
        final var columnsOverBaseRecord = Values.deconstructRecord(defaultRecord);
        for (int ordinal = 0; ordinal < windowValueOrdinal; ordinal++) {
            final int keyOrdinal = entryKeyOrdinalOf(columnsOverBaseRecord.get(ordinal));
            if (keyOrdinal >= 0) {
                cover(covered, entryObjectValue, fields.get(ordinal),
                        IndexKeyValueToPartialRecord.TupleSource.KEY, keyOrdinal);
            }
        }
        cover(covered, entryObjectValue, fields.get(windowValueOrdinal),
                IndexKeyValueToPartialRecord.TupleSource.VALUE, 0);

        return covered.toRecordValue(postWindowResultType, defaultRecord);
    }

    /**
     * Where in the entry key the given column sits, or {@code -1} if the entry key does not carry it. The column is
     * expressed over the record the merge value reads from, {@link #indexKeyValues} over the base record the index was
     * expanded against, so the two are compared with those two aliases held equal.
     *
     * @param columnOverBaseRecord one column of {@link #defaultRecord}
     *
     * @return the ordinal of that column in the entry key, or {@code -1}
     */
    private int entryKeyOrdinalOf(@Nonnull final Value columnOverBaseRecord) {
        final var toBaseRecord = AliasMap.ofAliases(baseAlias, IndexEntryToRecordValueHelper.baseRecordAlias());
        for (int keyOrdinal = 0; keyOrdinal < indexKeyValues.size(); keyOrdinal++) {
            if (indexKeyValues.get(keyOrdinal).semanticEquals(columnOverBaseRecord, toBaseRecord)) {
                return keyOrdinal;
            }
        }
        return -1;
    }

    private static void cover(@Nonnull final IndexEntryToRecordValueHelper covered,
                              @Nonnull final Value entryObjectValue,
                              @Nonnull final Type.Record.Field field,
                              @Nonnull final IndexKeyValueToPartialRecord.TupleSource tupleSource,
                              final int ordinal) {
        final var fieldName = field.getFieldName();
        covered.withChild(fieldName)
                .cover(IndexEntryToRecordValueHelper.entryColumn(entryObjectValue, fieldName, tupleSource, ordinal));
    }

    @Nonnull
    @Override
    public Optional<Value> pushValueThroughFetch(@Nonnull Value toBePushedValue,
                                                 @Nonnull CorrelationIdentifier sourceAlias,
                                                 @Nonnull CorrelationIdentifier targetAlias) {
        final var indexEntryToLogicalRecord =
                getIndexEntryToLogicalRecordMaybe().orElseThrow(() -> new RecordCoreException("need index entry to logical record"));

        return ScanWithFetchMatchCandidate.pushValueThroughFetch(toBePushedValue,
                baseAlias,
                sourceAlias,
                targetAlias,
                indexEntryToLogicalRecord.logicalKeyValues());
    }

    @Nonnull
    private static ScanComparisons toScanComparisons(@Nonnull final List<ComparisonRange> comparisonRanges) {
        ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (ComparisonRange comparisonRange : comparisonRanges) {
            builder.addComparisonRange(comparisonRange);
        }
        return builder.build();
    }
}
