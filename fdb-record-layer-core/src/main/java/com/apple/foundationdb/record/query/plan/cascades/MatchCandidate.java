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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Interface to represent a match candidate. A match candidate on code level is just a name and a data flow graph
 * that can be matched against a query graph. The match candidate does not keep the root to the graph to be matched but
 * rather an instance of {@link Traversal} to allow for navigation of references within the candidate.
 *
 * Match candidates also allow for creation of scans over the materialized data, e.g. the index for an
 * {@link ValueIndexScanMatchCandidate} or the primary range for a {@link PrimaryScanMatchCandidate}, given appropriate
 * {@link ComparisonRange}s which usually are the direct result of graph matching.
 */
public interface MatchCandidate {

    @Nonnull
    Logger LOGGER = LoggerFactory.getLogger(MatchCandidate.class);

    /**
     * Returns the name of the match candidate. If this candidate represents an index, it will be the name of the index.
     *
     * @return the name of this match candidate
     */
    @Nonnull
    String getName();

    /**
     * Returns the traversal object for this candidate. The traversal object can either be computed up-front when
     * the candidate is created or lazily when this method is invoked. It is, however, necessary that the traversal
     * once computed is stable, meaning the object returned by implementors of this method must always return the
     * same object.
     * @return the traversal associated for this match candidate
     */
    @Nonnull
    Traversal getTraversal();

    /**
     * Returns a list of parameter names for sargable parameters that can to be bound during matching.
     * @return a list of {@link CorrelationIdentifier}s for all sargable parameters in this match candidate
     */
    @Nonnull
    List<CorrelationIdentifier> getSargableAliases();

    /**
     * Returns the parameter names for the resulting order for parameters that can be bound during matching
     * (sargable and residual).
     * @return a list of {@link CorrelationIdentifier}s describing the ordering of the result set of this match candidate
     */
    @Nonnull
    List<CorrelationIdentifier> getOrderingAliases();

    /**
     * This method returns a key expression that can be used to actually compute the keys of this candidate for a
     * given record.
     * The current expression hierarchy cannot be evaluated at runtime (in general). This key expression helps
     * represent compensation or part of compensation if needed.
     * @return a key expression that can be evaluated based on a base record
     */
    @Nonnull
    KeyExpression getFullKeyExpression();

    boolean createsDuplicates();

    /**
     * Computes a map from {@link CorrelationIdentifier} to {@link ComparisonRange} that is physically compatible with
     * a scan over the materialized version of the match candidate, so e.g. for an {@link ValueIndexScanMatchCandidate} that
     * would be the scan over the index.
     * As matching progresses it finds mappings from parameters to corresponding comparison ranges. Matching, however,
     * is not sensitive to whether such a binding could actually be used in an index scan. In fact, in a different maybe
     * future record layer with improved physical operators this method should be revised to account for those improvements.
     * For now, we only consider a prefix of said mappings that consist of n equality-bound mappings and stops either
     * at an inequality bound parameter or before a unbound parameter.
     * @param matchInfo match info
     * @return a map containing parameter to comparison range mappings for a prefix of parameters that is compatible
     *         with a physical scan over the materialized view (of the candidate)
     */
    default Map<CorrelationIdentifier, ComparisonRange> computeBoundParameterPrefixMap(@Nonnull final MatchInfo matchInfo) {
        final var prefixMap = Maps.<CorrelationIdentifier, ComparisonRange>newHashMap();
        final var parameterBindingMap = matchInfo.getParameterBindingMap();

        final var parameters = getSargableAliases();
        for (final var parameter : parameters) {
            Objects.requireNonNull(parameter);
            @Nullable final var comparisonRange = parameterBindingMap.get(parameter);
            if (comparisonRange == null) {
                return ImmutableMap.copyOf(prefixMap);
            }
            if (prefixMap.containsKey(parameter)) {
                Verify.verify(prefixMap.get(parameter).equals(comparisonRange));
                continue;
            }

            switch (comparisonRange.getRangeType()) {
                case EQUALITY:
                    prefixMap.put(parameter, comparisonRange);
                    break;
                case INEQUALITY:
                    prefixMap.put(parameter, comparisonRange);
                    return ImmutableMap.copyOf(prefixMap);
                case EMPTY:
                default:
                    return ImmutableMap.copyOf(prefixMap);
            }
        }

        return ImmutableMap.copyOf(prefixMap);
    }

    /**
     * Compute a list of {@link MatchedOrderingPart}s which forms a bridge to relate {@link KeyExpression}s and
     * {@link QueryPredicate}s.
     * @param matchInfo a pre-existing match info structure
     * @param sortParameterIds the parameter IDs which the query should be ordered by
     * @param isReverse reversed-ness of the order
     * @return a list of bound key parts that express the order of the outgoing data stream and their respective mappings
     *         between query and match candidate
     */
    @Nonnull
    List<MatchedOrderingPart> computeMatchedOrderingParts(@Nonnull MatchInfo matchInfo,
                                                          @Nonnull List<CorrelationIdentifier> sortParameterIds,
                                                          boolean isReverse);

    @Nonnull
    Ordering computeOrderingFromScanComparisons(@Nonnull ScanComparisons scanComparisons,
                                                boolean isReverse,
                                                boolean isDistinct);

    /**
     * Creates a {@link RecordQueryPlan} that represents a scan over the materialized candidate data.
     * @param partialMatch the match to be used
     * @param planContext the plan context
     * @param memoizer the memoizer
     * @param reverseScanOrder {@code true} if and only if a reverse scan is to be built
     * @return a new {@link RecordQueryPlan}
     */
    @SuppressWarnings("java:S135")
    default RecordQueryPlan toEquivalentPlan(@Nonnull final PartialMatch partialMatch,
                                             @Nonnull final PlanContext planContext,
                                             @Nonnull final Memoizer memoizer,
                                             final boolean reverseScanOrder) {
        final var matchInfo = partialMatch.getMatchInfo();
        final var prefixMap = computeBoundParameterPrefixMap(matchInfo);

        final var comparisonRangesForScanBuilder = ImmutableList.<ComparisonRange>builder();

        // iterate through the parameters in order -- stop:
        // 1. if the current mapping does not exist
        // 2. the current mapping is EMPTY
        // 3. after the current mapping if the mapping is an INEQUALITY
        for (final var parameterAlias : getSargableAliases()) {
            // get the mapped side
            if (!prefixMap.containsKey(parameterAlias)) {
                break;
            }
            comparisonRangesForScanBuilder.add(prefixMap.get(parameterAlias));
        }

        return toEquivalentPlan(partialMatch, planContext, memoizer, comparisonRangesForScanBuilder.build(), reverseScanOrder);
    }

    /**
     * Creates a {@link RecordQueryPlan} that represents a scan over the materialized candidate data. This method is
     * expected to be implemented by specific implementations of {@link MatchCandidate}.
     * @param partialMatch the {@link PartialMatch} that matched the query and the candidate
     * @param planContext the plan context
     * @param memoizer the memoizer
     * @param comparisonRanges a {@link List} of {@link ComparisonRange}s to be applied
     * @param reverseScanOrder {@code true} if and only if a reverse scan is to be built
     * @return a new {@link RecordQueryPlan}
     */
    @Nonnull
    RecordQueryPlan toEquivalentPlan(@Nonnull PartialMatch partialMatch,
                                     @Nonnull PlanContext planContext,
                                     @Nonnull Memoizer memoizer,
                                     @Nonnull List<ComparisonRange> comparisonRanges,
                                     boolean reverseScanOrder);

    @Nonnull
    @SuppressWarnings("java:S1452")
    default SetMultimap<Reference, RelationalExpression> findReferencingExpressions(@Nonnull final ImmutableList<? extends Reference> references) {
        final var traversal = getTraversal();

        final var refToExpressionMap =
                Multimaps.<Reference, RelationalExpression>newSetMultimap(new LinkedIdentityMap<>(), LinkedIdentitySet::new);

        // going up may yield duplicates -- deduplicate with this multimap
        for (final Reference rangesOverRef : references) {
            final var partialMatchesForCandidate = rangesOverRef.getPartialMatchesForCandidate(this);
            for (final var partialMatch : partialMatchesForCandidate) {
                for (final var parentReferencePath : traversal.getParentRefPaths(partialMatch.getCandidateRef())) {
                    refToExpressionMap.put(parentReferencePath.getReference(), parentReferencePath.getExpression());
                }
            }
        }
        return refToExpressionMap;
    }

    @Nonnull
    List<RecordType> getQueriedRecordTypes();

    int getColumnSize();

    boolean isUnique();

    @Nonnull
    default Set<String> getQueriedRecordTypeNames() {
        return getQueriedRecordTypes().stream()
                .map(RecordType::getName)
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    static Iterable<MatchCandidate> fromIndexDefinition(@Nonnull final RecordMetaData metaData,
                                                        @Nonnull final Index index,
                                                        final boolean isReverse) {
        final var resultBuilder = ImmutableList.<MatchCandidate>builder();
        final var queriedRecordTypes = metaData.recordTypesForIndex(index);
        final var commonPrimaryKeyForIndex = RecordMetaData.commonPrimaryKey(queriedRecordTypes);

        final var queriedRecordTypeNames =
                queriedRecordTypes
                        .stream()
                        .map(RecordType::getName)
                        .collect(ImmutableSet.toImmutableSet());

        final var recordTypeMap = metaData.getRecordTypes();
        final var availableRecordTypeNames = recordTypeMap.keySet();
        final var availableRecordTypes = recordTypeMap.values();

        final var indexType = index.getType();

        switch (indexType) {
            case IndexTypes.VALUE:
            case IndexTypes.VERSION:
                expandValueIndexMatchCandidate(
                        index,
                        availableRecordTypeNames,
                        availableRecordTypes,
                        queriedRecordTypeNames,
                        queriedRecordTypes,
                        isReverse,
                        commonPrimaryKeyForIndex
                ).ifPresent(resultBuilder::add);
                break;
            case IndexTypes.RANK:
                // For rank() we need to create at least two candidates. One for BY_RANK scans and one for BY_VALUE scans.
                expandValueIndexMatchCandidate(
                        index,
                        availableRecordTypeNames,
                        availableRecordTypes,
                        queriedRecordTypeNames,
                        queriedRecordTypes,
                        isReverse,
                        commonPrimaryKeyForIndex
                ).ifPresent(resultBuilder::add);

                expandIndexMatchCandidate(
                        index,
                        availableRecordTypeNames,
                        availableRecordTypes,
                        queriedRecordTypeNames,
                        queriedRecordTypes,
                        isReverse,
                        commonPrimaryKeyForIndex,
                        new WindowedIndexExpansionVisitor(index, queriedRecordTypes)
                ).ifPresent(resultBuilder::add);
                break;
            case IndexTypes.MIN_EVER_TUPLE: // fallthrough
            case IndexTypes.MAX_EVER_TUPLE: // fallthrough
            case IndexTypes.MAX_EVER_LONG: // fallthrough
            case IndexTypes.MIN_EVER_LONG: // fallthrough
            case IndexTypes.SUM: // fallthrough
            case IndexTypes.COUNT: // fallthrough
            case IndexTypes.COUNT_NOT_NULL:
                expandAggregateIndexMatchCandidate(
                        index,
                        availableRecordTypeNames,
                        availableRecordTypes,
                        queriedRecordTypeNames,
                        queriedRecordTypes,
                        isReverse
                ).ifPresent(resultBuilder::add);
                break;
            case IndexTypes.PERMUTED_MAX: // fallthrough
            case IndexTypes.PERMUTED_MIN:
                // For permuted min and max, we use the value index expansion for BY_VALUE scans and we use
                // the aggregate index expansion for BY_GROUP scans
                expandValueIndexMatchCandidate(
                        index,
                        availableRecordTypeNames,
                        availableRecordTypes,
                        queriedRecordTypeNames,
                        queriedRecordTypes,
                        isReverse,
                        commonPrimaryKeyForIndex
                ).ifPresent(resultBuilder::add);
                expandAggregateIndexMatchCandidate(
                        index,
                        availableRecordTypeNames,
                        availableRecordTypes,
                        queriedRecordTypeNames,
                        queriedRecordTypes,
                        isReverse
                ).ifPresent(resultBuilder::add);
                break;
            default:
                break;
        }
        return resultBuilder.build();
    }

    private static Optional<MatchCandidate> expandValueIndexMatchCandidate(@Nonnull final Index index,
                                                                           @Nonnull final Set<String> availableRecordTypeNames,
                                                                           @Nonnull final Collection<RecordType> availableRecordTypes,
                                                                           @Nonnull final Set<String> queriedRecordTypeNames,
                                                                           @Nonnull final Collection<RecordType> queriedRecordTypes,
                                                                           final boolean isReverse,
                                                                           @Nullable final KeyExpression commonPrimaryKeyForIndex) {
        return expandIndexMatchCandidate(index,
                availableRecordTypeNames,
                availableRecordTypes,
                queriedRecordTypeNames,
                queriedRecordTypes,
                isReverse,
                commonPrimaryKeyForIndex,
                new ValueIndexExpansionVisitor(index, queriedRecordTypes)
        );
    }

    private static Optional<MatchCandidate> expandAggregateIndexMatchCandidate(@Nonnull final Index index,
                                                                               @Nonnull final Set<String> availableRecordTypeNames,
                                                                               @Nonnull final Collection<RecordType> availableRecordTypes,
                                                                               @Nonnull final Set<String> queriedRecordTypeNames,
                                                                               @Nonnull final Collection<RecordType> queriedRecordTypes,
                                                                               final boolean isReverse) {
        return expandIndexMatchCandidate(index,
                availableRecordTypeNames,
                availableRecordTypes,
                queriedRecordTypeNames,
                queriedRecordTypes,
                isReverse,
                null,
                new AggregateIndexExpansionVisitor(index, queriedRecordTypes)
        );
    }

    @Nonnull
    private static Optional<MatchCandidate> expandIndexMatchCandidate(@Nonnull final Index index,
                                                                      @Nonnull final Set<String> availableRecordTypeNames,
                                                                      @Nonnull final Collection<RecordType> availableRecordTypes,
                                                                      @Nonnull final Set<String> queriedRecordTypeNames,
                                                                      @Nonnull final Collection<RecordType> queriedRecordTypes,
                                                                      final boolean isReverse,
                                                                      @Nullable final KeyExpression commonPrimaryKeyForIndex,
                                                                      @Nonnull final ExpansionVisitor<?> expansionVisitor) {
        final var baseRef = createBaseRef(availableRecordTypeNames, availableRecordTypes, queriedRecordTypeNames, queriedRecordTypes, new IndexAccessHint(index.getName()));
        try {
            return Optional.of(expansionVisitor.expand(() -> Quantifier.forEach(baseRef), commonPrimaryKeyForIndex, isReverse));
        } catch (final UnsupportedOperationException uOE) {
            // just log and return empty
            if (LOGGER.isDebugEnabled()) {
                final String message =
                        KeyValueLogMessage.of("unsupported index",
                                "reason", uOE.getMessage(),
                                "indexName", index.getName());
                LOGGER.debug(message, uOE);
            }
        }
        return Optional.empty();
    }

    @Nonnull
    static Optional<MatchCandidate> fromPrimaryDefinition(@Nonnull final RecordMetaData metaData,
                                                          @Nonnull final Set<String> queriedRecordTypeNames,
                                                          @Nullable KeyExpression primaryKey,
                                                          final boolean isReverse) {
        if (primaryKey != null) {
            final var availableRecordTypes = metaData.getRecordTypes().values();
            final var queriedRecordTypes =
                    availableRecordTypes.stream()
                            .filter(recordType -> queriedRecordTypeNames.contains(recordType.getName()))
                            .collect(ImmutableList.toImmutableList());

            final var baseRef = createBaseRef(metaData.getRecordTypes().keySet(), availableRecordTypes, queriedRecordTypeNames, queriedRecordTypes, new PrimaryAccessHint());
            final var expansionVisitor = new PrimaryAccessExpansionVisitor(availableRecordTypes, queriedRecordTypes);
            return Optional.of(expansionVisitor.expand(() -> Quantifier.forEach(baseRef), primaryKey, isReverse));
        }

        return Optional.empty();
    }

    @Nonnull
    static Reference createBaseRef(@Nonnull final Set<String> availableRecordTypeNames,
                                   @Nonnull final Collection<RecordType> availableRecordTypes,
                                   @Nonnull final Set<String> queriedRecordTypeNames,
                                   @Nonnull final Collection<RecordType> queriedRecordTypes,
                                   @Nonnull AccessHint accessHint) {
        final var quantifier =
                Quantifier.forEach(
                        Reference.of(
                                new FullUnorderedScanExpression(availableRecordTypeNames,
                                        new Type.AnyRecord(false),
                                        new AccessHints(accessHint))));
        return Reference.of(
                new LogicalTypeFilterExpression(queriedRecordTypeNames,
                        quantifier,
                        Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(queriedRecordTypes))));
    }

    @Nonnull
    static Optional<List<Value>> computePrimaryKeyValuesMaybe(@Nullable KeyExpression primaryKey, @Nonnull Type flowedType) {
        if (primaryKey == null) {
            return Optional.empty();
        }

        return Optional.of(ScalarTranslationVisitor.translateKeyExpression(primaryKey, flowedType));
    }
}
