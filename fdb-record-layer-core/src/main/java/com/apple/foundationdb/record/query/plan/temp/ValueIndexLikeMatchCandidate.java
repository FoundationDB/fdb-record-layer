/*
 * ValueIndexLikeMatchCandidate.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Trait to implement some default logic for {@link MatchCandidate}s that are defined over value index-like
 * data structures such as {@link PrimaryScanMatchCandidate} and {@link ValueIndexScanMatchCandidate}.
 */
public interface ValueIndexLikeMatchCandidate extends MatchCandidate {

    /**
     This synthesizes a list of {@link BoundKeyPart}s from the partial match and the ordering information
     * passed in. Using a list of parameter ids, each {@link BoundKeyPart} links together the
     * (1) normalized key expression that originally produced the key (from index, or common primary key)
     * (2) a comparison range for this parameter which is contained in the already existent partial match
     * (3) the predicate on the query part that participated and bound this parameter (and implicitly was used to
     *     synthesize the comparison range in (2)
     * (4) the candidate predicate on the candidate side that is the placeholder predicate for the parameter
     * @param matchInfo a pre-existing match info structure
     * @param sortParameterIds the query should be ordered by
     * @param isReverse reversed-ness of the order
     * @return a list of bound key parts that express the order of the outgoing data stream and their respective mappings
     *         between query and match candidate
     */
    @Nonnull
    @Override
    default List<BoundKeyPart> computeBoundKeyParts(@Nonnull MatchInfo matchInfo,
                                                    @Nonnull List<CorrelationIdentifier> sortParameterIds,
                                                    boolean isReverse) {
        final var parameterBindingMap = matchInfo.getParameterBindingMap();
        final var parameterBindingPredicateMap = matchInfo.getParameterPredicateMap();

        final var normalizedKeys =
                getAlternativeKeyExpression().normalizeKeyForPositions();

        final var builder = ImmutableList.<BoundKeyPart>builder();
        final var candidateParameterIds = getOrderingAliases();

        for (final var parameterId : sortParameterIds) {
            final var ordinalInCandidate = candidateParameterIds.indexOf(parameterId);
            Verify.verify(ordinalInCandidate >= 0);
            final var normalizedKey = normalizedKeys.get(ordinalInCandidate);

            Objects.requireNonNull(parameterId);
            Objects.requireNonNull(normalizedKey);
            @Nullable final var comparisonRange = parameterBindingMap.get(parameterId);
            @Nullable final var queryPredicate = parameterBindingPredicateMap.get(parameterId);

            Verify.verify(comparisonRange == null || comparisonRange.getRangeType() == ComparisonRange.Type.EMPTY || queryPredicate != null);

            builder.add(
                    BoundKeyPart.of(normalizedKey,
                            comparisonRange == null ? ComparisonRange.Type.EMPTY : comparisonRange.getRangeType(),
                            queryPredicate,
                            isReverse));
        }

        return builder.build();
    }

    @Nonnull
    @Override
    default Ordering computeOrderingFromScanComparisons(@Nonnull final ScanComparisons scanComparisons,
                                                        final boolean isReverse,
                                                        final boolean isDistinct) {
        return computeOrderingFromKeyAndScanComparisons(getAlternativeKeyExpression(), scanComparisons, isReverse, isDistinct);
    }

    @Nonnull
    static Ordering computeOrderingFromKeyAndScanComparisons(@Nonnull final KeyExpression keyExpression,
                                                             @Nonnull final ScanComparisons scanComparisons,
                                                             final boolean isReverse,
                                                             final boolean isDistinct) {
        final var equalityBoundKeyMapBuilder = ImmutableSetMultimap.<KeyExpression, Comparisons.Comparison>builder();
        final var normalizedKeyExpressions = keyExpression.normalizeKeyForPositions();
        final var equalityComparisons = scanComparisons.getEqualityComparisons();

        for (var i = 0; i < equalityComparisons.size(); i++) {
            final var normalizedKeyExpression = normalizedKeyExpressions.get(i);
            final var comparison = equalityComparisons.get(i);

            equalityBoundKeyMapBuilder.put(normalizedKeyExpression, comparison);
        }

        final var result = ImmutableList.<KeyPart>builder();
        for (var i = scanComparisons.getEqualitySize(); i < normalizedKeyExpressions.size(); i++) {
            final var currentKeyExpression = normalizedKeyExpressions.get(i);

            //
            // Note that it is not really important here if the keyExpression can be normalized in a lossless way
            // or not. A key expression containing repeated fields is sort-compatible with its normalized key
            // expression. We used to refuse to compute the sort order in the presence of repeats, however,
            // I think that restriction can be relaxed.
            //
            result.add(KeyPart.of(currentKeyExpression, isReverse));
        }

        return new Ordering(equalityBoundKeyMapBuilder.build(), result.build(), isDistinct);
    }
}
