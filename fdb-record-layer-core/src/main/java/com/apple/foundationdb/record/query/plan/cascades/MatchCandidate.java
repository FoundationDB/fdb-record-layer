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
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Interface to represent a match candidate. A match candidate on code level is just a name and a data flow graph
 * that can be matched against a query graph. The match candidate does not keep the root to the graph to be matched but
 * rather an instance of {@link ExpressionRefTraversal} to allow for navigation of references within the candidate.
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
    ExpressionRefTraversal getTraversal();

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
    KeyExpression getAlternativeKeyExpression();

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
     * Compute a list of {@link BoundKeyPart}s which forms a bridge to relate {@link KeyExpression}s and
     * {@link QueryPredicate}s.
     * @param matchInfo a pre-existing match info structure
     * @param sortParameterIds the query should be ordered by
     * @param isReverse reversed-ness of the order
     * @return a list of bound key parts that express the order of the outgoing data stream and their respective mappings
     *         between query and match candidate
     */
    @Nonnull
    List<BoundKeyPart> computeBoundKeyParts(@Nonnull MatchInfo matchInfo,
                                            @Nonnull List<CorrelationIdentifier> sortParameterIds,
                                            boolean isReverse);

    @Nonnull
    Ordering computeOrderingFromScanComparisons(@Nonnull ScanComparisons scanComparisons,
                                                boolean isReverse,
                                                boolean isDistinct);

    /**
     * Creates a logical expression that represents a scan over the materialized candidate data.
     * @param recordMetaData the metadata used by the planner
     * @param partialMatch the match to be used
     * @return a new {@link RelationalExpression}
     */
    @SuppressWarnings("java:S135")
    default RelationalExpression toEquivalentExpression(@Nonnull RecordMetaData recordMetaData,
                                                        @Nonnull final PartialMatch partialMatch,
                                                        @Nonnull final PlanContext planContext) {
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

        return toEquivalentExpression(recordMetaData, partialMatch, planContext, comparisonRangesForScanBuilder.build());
    }

    /**
     * Creates a logical expression that represents a scan over the materialized candidate data. This method is expected
     * to be implemented by specific implementations of {@link MatchCandidate}.
     * @param recordMetaData the metadata available to the planner
     * @param partialMatch the {@link PartialMatch} that matched th query and the candidate
     * @param comparisonRanges a {@link List} of {@link ComparisonRange}s to be applied
     * @return a new {@link RelationalExpression}
     */
    @Nonnull
    RelationalExpression toEquivalentExpression(@Nonnull RecordMetaData recordMetaData,
                                                @Nonnull PartialMatch partialMatch,
                                                @Nonnull PlanContext planContext,
                                                @Nonnull List<ComparisonRange> comparisonRanges);

    @Nonnull
    @SuppressWarnings("java:S1452")
    default SetMultimap<ExpressionRef<? extends RelationalExpression>, RelationalExpression> findReferencingExpressions(@Nonnull final ImmutableList<? extends ExpressionRef<? extends RelationalExpression>> references) {
        final var traversal = getTraversal();

        final var refToExpressionMap =
                Multimaps.<ExpressionRef<? extends RelationalExpression>, RelationalExpression>newSetMultimap(new LinkedIdentityMap<>(), LinkedIdentitySet::new);

        // going up may yield duplicates -- deduplicate with this multimap
        for (final ExpressionRef<? extends RelationalExpression> rangesOverRef : references) {
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
    static Iterable<MatchCandidate> fromIndexDefinition(@Nonnull final RecordMetaData metaData,
                                                        @Nonnull final Index index,
                                                        final boolean isReverse) {
        final var resultBuilder = ImmutableList.<MatchCandidate>builder();
        final var recordTypesForIndex = metaData.recordTypesForIndex(index);
        final var commonPrimaryKeyForIndex = RecordMetaData.commonPrimaryKey(recordTypesForIndex);

        final var recordTypeNamesForIndex =
                recordTypesForIndex
                        .stream()
                        .map(RecordType::getName)
                        .collect(ImmutableSet.toImmutableSet());

        final var availableRecordTypes = metaData.getRecordTypes().keySet();

        final var type = index.getType();

        if (IndexTypes.VALUE.equals(type)) {
            expandIndexMatchCandidate(
                    metaData,
                    index,
                    recordTypeNamesForIndex,
                    availableRecordTypes,
                    isReverse,
                    commonPrimaryKeyForIndex,
                    new ValueIndexExpansionVisitor(index, recordTypesForIndex)).ifPresent(resultBuilder::add);
        }

        if (IndexTypes.RANK.equals(type)) {
            // For rank() we need to create at least two candidates. One for BY_RANK scans and one for BY_VALUE scans.
            expandIndexMatchCandidate(
                    metaData,
                    index,
                    recordTypeNamesForIndex,
                    availableRecordTypes,
                    isReverse,
                    commonPrimaryKeyForIndex,
                    new ValueIndexExpansionVisitor(index, recordTypesForIndex)).ifPresent(resultBuilder::add);

            expandIndexMatchCandidate(metaData,
                    index,
                    recordTypeNamesForIndex,
                    availableRecordTypes,
                    isReverse,
                    commonPrimaryKeyForIndex,
                    new WindowedIndexExpansionVisitor(index, recordTypesForIndex))
                    .ifPresent(resultBuilder::add);
        }

        return resultBuilder.build();
    }

    @Nonnull
    private static Optional<MatchCandidate> expandIndexMatchCandidate(@Nonnull RecordMetaData recordMetaData,
                                                                      @Nonnull final Index index,
                                                                      @Nonnull final ImmutableSet<String> recordTypeNamesForIndex,
                                                                      @Nonnull final Set<String> availableRecordTypes,
                                                                      final boolean isReverse,
                                                                      @Nullable final KeyExpression commonPrimaryKeyForIndex,
                                                                      @Nonnull final ExpansionVisitor<?> expansionVisitor) {
        final var baseRef = createBaseRef(recordMetaData, availableRecordTypes, recordTypeNamesForIndex, new IndexAccessHint(index.getName()));
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
                                                          @Nonnull final Set<String> recordTypes,
                                                          @Nullable KeyExpression commonPrimaryKey,
                                                          final boolean isReverse) {
        if (commonPrimaryKey != null) {
            final var availableRecordTypes = metaData.getRecordTypes().keySet();
            final var baseRef = createBaseRef(metaData, availableRecordTypes, recordTypes, new PrimaryAccessHint());
            final var expansionVisitor = new PrimaryAccessExpansionVisitor(availableRecordTypes, recordTypes);
            return Optional.of(expansionVisitor.expand(() -> Quantifier.forEach(baseRef), commonPrimaryKey, isReverse));
        }

        return Optional.empty();
    }

    @Nonnull
    static GroupExpressionRef<RelationalExpression> createBaseRef(@Nonnull RecordMetaData metaData, @Nonnull final Set<String> allAvailableRecordTypes, @Nonnull final Set<String> recordTypesForIndex, @Nonnull AccessHint accessHint) {
        final var quantifier =
                Quantifier.forEach(
                        GroupExpressionRef.of(
                                new FullUnorderedScanExpression(allAvailableRecordTypes,
                                        Type.Record.fromFieldDescriptorsMap(metaData.getFieldDescriptorMapFromNames(allAvailableRecordTypes)),
                                        new AccessHints(accessHint))));
        return GroupExpressionRef.of(
                new LogicalTypeFilterExpression(recordTypesForIndex,
                        quantifier,
                        Type.Record.fromFieldDescriptorsMap(metaData.getFieldDescriptorMapFromNames(recordTypesForIndex))));
    }
}
