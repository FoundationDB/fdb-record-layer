/*
 * VectorIndexScanMatchCandidate.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Case class to represent a match candidate that is backed by an index.
 */
public class VectorIndexScanMatchCandidate implements ScanWithFetchMatchCandidate, ValueIndexLikeMatchCandidate {
    /**
     * Index metadata structure.
     */
    @Nonnull
    private final Index index;

    /**
     * Record types this index is defined over.
     */
    private final List<RecordType> queriedRecordTypes;

    @Nonnull
    private final List<CorrelationIdentifier> parameters;

    @Nonnull
    private final Set<CorrelationIdentifier> parametersRequiredForBinding;

    /**
     * Base type.
     */
    @Nonnull
    private final Type baseType;

    /**
     * Base alias.
     */
    @Nonnull
    private final CorrelationIdentifier baseAlias;

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
    private final Supplier<Optional<List<Value>>> primaryKeyValuesOptionalSupplier;

    @Nonnull
    private final Supplier<Optional<IndexEntryToLogicalRecord>> indexEntryToLogicalRecordOptionalSupplier;

    public VectorIndexScanMatchCandidate(@Nonnull final Index index,
                                         @Nonnull final Collection<RecordType> queriedRecordTypes,
                                         @Nonnull final Traversal traversal,
                                         @Nonnull final List<CorrelationIdentifier> parameters,
                                         @Nonnull final Set<CorrelationIdentifier> parametersRequiredForBinding,
                                         @Nonnull final Type baseType,
                                         @Nonnull final CorrelationIdentifier baseAlias,
                                         @Nonnull final List<Value> indexKeyValues,
                                         @Nonnull final List<Value> indexValueValues,
                                         @Nonnull final KeyExpression fullKeyExpression,
                                         @Nullable final KeyExpression primaryKey) {
        this.index = index;
        this.queriedRecordTypes = ImmutableList.copyOf(queriedRecordTypes);
        this.traversal = traversal;
        this.parameters = ImmutableList.copyOf(parameters);
        this.parametersRequiredForBinding = ImmutableSet.copyOf(parametersRequiredForBinding);
        this.baseType = baseType;
        this.baseAlias = baseAlias;
        this.fullKeyExpression = fullKeyExpression;
        this.primaryKey = primaryKey;
        this.primaryKeyValuesOptionalSupplier =
                Suppliers.memoize(() -> MatchCandidate.computePrimaryKeyValuesMaybe(primaryKey, baseType));
        this.indexEntryToLogicalRecordOptionalSupplier =
                Suppliers.memoize(() -> ScanWithFetchMatchCandidate.computeIndexEntryToLogicalRecord(queriedRecordTypes,
                        baseAlias, baseType, indexKeyValues, indexValueValues));
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
        return parameters;
    }

    @Nonnull
    public Set<CorrelationIdentifier> getSargableAliasesRequiredForBinding() {
        return parametersRequiredForBinding;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getOrderingAliases() {
        return getSargableAliases();
    }

    @Nonnull
    @Override
    public Type getBaseType() {
        return baseType;
    }

    @Nonnull
    public CorrelationIdentifier getBaseAlias() {
        return baseAlias;
    }

    @Nonnull
    @Override
    public KeyExpression getFullKeyExpression() {
        return fullKeyExpression;
    }

    @Override
    public String toString() {
        return "value[" + getName() + "]";
    }

    @Override
    public boolean createsDuplicates() {
        return index.getRootExpression().createsDuplicates();
    }

    @Nonnull
    @Override
    public Optional<List<Value>> getPrimaryKeyValuesMaybe() {
        return primaryKeyValuesOptionalSupplier.get();
    }

    @Nonnull
    private Optional<IndexEntryToLogicalRecord> getIndexEntryToLogicalRecordMaybe() {
        return indexEntryToLogicalRecordOptionalSupplier.get();
    }

    @Nonnull
    @Override
    public RecordQueryPlan toEquivalentPlan(@Nonnull final PartialMatch partialMatch,
                                            @Nonnull final PlanContext planContext,
                                            @Nonnull final Memoizer memoizer,
                                            @Nonnull final List<ComparisonRange> comparisonRanges,
                                            final boolean reverseScanOrder) {
        final var matchInfo = partialMatch.getRegularMatchInfo();
        final var vectorIndexScanComparison = toVectorIndexScanComparisons(comparisonRanges);
        final var baseRecordType =
                Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(queriedRecordTypes));
        return tryFetchCoveringIndexScan(partialMatch, planContext, memoizer, vectorIndexScanComparison, reverseScanOrder, baseRecordType)
                .orElseGet(() ->
                        new RecordQueryIndexPlan(index.getName(),
                                primaryKey,
                                vectorIndexScanComparison,
                                planContext.getPlannerConfiguration().getIndexFetchMethod(),
                                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                                reverseScanOrder,
                                false,
                                partialMatch.getMatchCandidate(),
                                baseRecordType,
                                matchInfo.getConstraint()));
    }

    @Nonnull
    private Optional<RecordQueryPlan> tryFetchCoveringIndexScan(@Nonnull final PartialMatch partialMatch,
                                                                @Nonnull final PlanContext planContext,
                                                                @Nonnull final Memoizer memoizer,
                                                                @Nonnull final VectorIndexScanComparisons vectorIndexScanComparisons,
                                                                final boolean isReverse,
                                                                @Nonnull Type.Record baseRecordType) {
        final var indexEntryToLogicalRecordOptional = getIndexEntryToLogicalRecordMaybe();
        if (indexEntryToLogicalRecordOptional.isEmpty()) {
            return Optional.empty();
        }
        final var indexEntryToLogicalRecord = indexEntryToLogicalRecordOptional.get();
        final var indexPlan =
                new RecordQueryIndexPlan(index.getName(),
                        primaryKey,
                        vectorIndexScanComparisons,
                        planContext.getPlannerConfiguration().getIndexFetchMethod(),
                        RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                        isReverse,
                        false,
                        partialMatch.getMatchCandidate(),
                        baseRecordType,
                        partialMatch.getRegularMatchInfo().getConstraint());

        final var coveringIndexPlan = new RecordQueryCoveringIndexPlan(indexPlan,
                indexEntryToLogicalRecord.getQueriedRecordType().getName(),
                AvailableFields.NO_FIELDS, // not used except for old planner properties
                indexEntryToLogicalRecord.getIndexKeyValueToPartialRecord());

        return Optional.of(new RecordQueryFetchFromPartialRecordPlan(Quantifier.physical(memoizer.memoizePlan(coveringIndexPlan)),
                coveringIndexPlan::pushValueThroughFetch, baseRecordType, RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY));
    }

    @Nonnull
    @Override
    public Optional<Value> pushValueThroughFetch(@Nonnull final Value toBePushedValue,
                                                 @Nonnull final CorrelationIdentifier sourceAlias,
                                                 @Nonnull final CorrelationIdentifier targetAlias) {
        final var indexEntryToLogicalRecord =
                getIndexEntryToLogicalRecordMaybe().orElseThrow(() -> new RecordCoreException("need index entry to logical record"));

        return ScanWithFetchMatchCandidate.pushValueThroughFetch(toBePushedValue,
                baseAlias,
                sourceAlias,
                targetAlias,
                Iterables.concat(indexEntryToLogicalRecord.getLogicalKeyValues(),
                        indexEntryToLogicalRecord.getLogicalValueValues()));
    }

    @Nonnull
    private static VectorIndexScanComparisons toVectorIndexScanComparisons(@Nonnull final List<ComparisonRange> comparisonRanges) {
        final var scanRangesBuilder = ImmutableList.<ComparisonRange>builder();
        final var distanceRankComparisonsBuilder = ImmutableList.<Comparisons.DistanceRankValueComparison>builder();

        comparisonRanges.forEach(comparisonRange -> {
            if (comparisonRange.isEquality()
                    && comparisonRange.getEqualityComparison() instanceof Comparisons.DistanceRankValueComparison) {
                distanceRankComparisonsBuilder.add((Comparisons.DistanceRankValueComparison)comparisonRange.getEqualityComparison());
            } else if (comparisonRange.isInequality()
                    && comparisonRange.getInequalityComparisons()
                    .stream().allMatch(comp -> comp instanceof Comparisons.DistanceRankValueComparison)) {
                distanceRankComparisonsBuilder.addAll(comparisonRange.getInequalityComparisons().stream()
                        .map(comp -> (Comparisons.DistanceRankValueComparison)comp)
                        .collect(ImmutableList.toImmutableList()));
            } else {
                scanRangesBuilder.add(comparisonRange);
            }
        });
        final var rankComparisons = distanceRankComparisonsBuilder.build();
        // currently, exactly one distance rank comparison is supported by the index.
        Verify.verify(rankComparisons.size() == 1, "attempt to create vector scan comparison without any rank comparison");

        return VectorIndexScanComparisons.byDistance(toScanComparisons(scanRangesBuilder.build()),
                distanceRankComparisonsBuilder.build().get(0));
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
