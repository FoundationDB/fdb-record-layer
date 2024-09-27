/*
 * MaxMatchMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.translation;

import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Represents a max match between a (rewritten) query result {@link Value} and the candidate result {@link Value}.
 */
public class MaxMatchMap {
    @Nonnull
    private final BiMap<Value, Value> mapping;
    @Nonnull
    private final Value queryResultValue; // in terms of the candidate quantifiers.
    @Nonnull
    private final Value candidateResultValue;
    @Nonnull
    private final QueryPlanConstraint queryPlanConstraint;
    @Nonnull
    private final ValueEquivalence valueEquivalence;

    /**
     * Creates a new instance of {@link MaxMatchMap}.
     * @param mapping the {@link Value} mapping
     * @param queryResult the query result from which the mapping keys originate
     * @param candidateResult the candidate result from which the mapping values originate
     * @param valueEquivalence a {@link ValueEquivalence} that was used to match up query and candidate values
     */
    MaxMatchMap(@Nonnull final Map<Value, Value> mapping,
                @Nonnull final Value queryResult,
                @Nonnull final Value candidateResult,
                @Nonnull final QueryPlanConstraint queryPlanConstraint,
                @Nonnull final ValueEquivalence valueEquivalence) {
        this.mapping = ImmutableBiMap.copyOf(mapping);
        this.queryResultValue = queryResult;
        this.candidateResultValue = candidateResult;
        this.queryPlanConstraint = queryPlanConstraint;
        this.valueEquivalence = valueEquivalence;
    }

    @Nonnull
    public Map<Value, Value> getMapping() {
        return mapping;
    }

    @Nonnull
    public Value getCandidateResultValue() {
        return candidateResultValue;
    }

    @Nonnull
    public Value getQueryResultValue() {
        return queryResultValue;
    }

    @Nonnull
    public QueryPlanConstraint getQueryPlanConstraint() {
        return queryPlanConstraint;
    }

    @Nonnull
    public ValueEquivalence getValueEquivalence() {
        return valueEquivalence;
    }

    @Nonnull
    public Optional<Value> translateQueryValueMaybe(@Nonnull final CorrelationIdentifier candidateCorrelation) {
        final var candidateResultValue = getCandidateResultValue();
        final var pulledUpCandidateSide =
                candidateResultValue.pullUp(mapping.values(),
                        AliasMap.emptyMap(),
                        ImmutableSet.of(), candidateCorrelation);
        //
        // We now have the right side pulled up, specifically we have a map from each candidate value below,
        // to a candidate value pulled up along the candidateCorrelation. We also have this max match map, which
        // encapsulates a map from query values to candidate value.
        // In other words we have in this max match map m1 := MAP(queryValues over q -> candidateValues over q') and
        // we just computed m2 := MAP(candidateValues over p' -> candidateValues over candidateCorrelation). We now
        // chain these two maps to get m1 ○ m2 := MAP(queryValues over q -> candidateValues over candidateCorrelation).
        // As we will use this map in the subsequent step to look up values over semantic equivalency using
        // equivalencesMap, we immediately create m1 ○ m2 using a boundEquivalence based on equivalencesMap.
        //
        final var pulledUpMaxMatchMapBuilder =
                ImmutableMap.<Value, Value>builder();
        for (final var entry : mapping.entrySet()) {
            final var queryPart = entry.getKey();
            final var candidatePart = entry.getValue();
            final var pulledUpdateCandidatePart = pulledUpCandidateSide.get(candidatePart);
            if (pulledUpdateCandidatePart == null) {
                return Optional.empty();
            }
            pulledUpMaxMatchMapBuilder.put(queryPart, pulledUpdateCandidatePart);
        }
        final var pulledUpMaxMatchMap = pulledUpMaxMatchMapBuilder.build();

        final var queryResultValueFromBelow = getQueryResultValue();
        final var translatedQueryResultValue = Objects.requireNonNull(queryResultValueFromBelow.replace(value -> {
            final var maxMatchValue = pulledUpMaxMatchMap.get(value);
            return maxMatchValue == null ? value : maxMatchValue;
        }));
        return Optional.of(translatedQueryResultValue);
    }

    /**
     * Calculates the maximum sub-{@link Value}s in {@code queryResultValue} that has an exact match in the
     * {@code candidateValue}.
     *
     * @param queryResultValue the query result {@code Value}.
     * @param candidateResultValue the candidate result {@code Value} we want to search for maximum matches.
     *
     * @return a {@link  MaxMatchMap} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap calculate(@Nonnull final Value queryResultValue,
                                        @Nonnull final Value candidateResultValue) {
        return calculate(queryResultValue,
                candidateResultValue,
                ValueEquivalence.empty());
    }

    /**
     * Calculates the maximum sub-{@link Value}s in {@code rewrittenQueryValue} that has an exact match in the
     * {@code candidateValue}.
     * <br>
     * For certain shapes of {@code Value}s, multiple matches can be found, this method is guaranteed to always find the
     * maximum part of the candidate sub-{@code Value} that matches with a sub-{@code Value} on the query side. For
     * example, assume we have a query {@code Value} {@code R = RCV(s+t)} and a candidate {@code Value} that is
     * {@code R` = RCV(s, t, (s+t))}, with R ≡ R`. We could have the following matches:
     * <ul>
     *     <li>{@code R.0 --> R`.0 + R`.1}</li>
     *     <li>{@code R -> R`.0.0}</li>
     * </ul>
     * The first match is not <i>maximum</i>, because it involves matching smaller constituents of the index and summing
     * them together, the other match however, is much better because it matches the entire query {@code Value} with a
     * single part of the index. The algorithm will always prefer the maximum match.
     *
     * @param queryResultValue the query result {@code Value}.
     * @param candidateResultValue the candidate result {@code Value} we want to search for maximum matches.
     * @param valueEquivalence an {@link ValueEquivalence} that informs the logic about equivalent value subtrees
     *
     * @return a {@link  MaxMatchMap} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap calculate(@Nonnull final Value queryResultValue,
                                        @Nonnull final Value candidateResultValue,
                                        @Nonnull final ValueEquivalence valueEquivalence) {
        final var recursionResult =
                recurseQueryResultValue(queryResultValue, candidateResultValue,
                        valueEquivalence, ImmutableBiMap.of(), new LinkedIdentitySet<>());

        return new MaxMatchMap(recursionResult.getValueMap(), recursionResult.getNewCurrentValue(),
                candidateResultValue, recursionResult.getQueryPlanConstraint(), valueEquivalence);
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static RecursionResult recurseQueryResultValue(@Nonnull final Value currentQueryValue,
                                                           @Nonnull final Value candidateResultValue,
                                                           @Nonnull final ValueEquivalence valueEquivalence,
                                                           @Nonnull final BiMap<Value, Value> knownValueMap,
                                                           @Nonnull final Set<Value> expandedValues) {
        final var queryPlanConstraintsBuilder = ImmutableList.<QueryPlanConstraint>builder();

        var currentMatchingPair =
                findMatchingCandidateValue(currentQueryValue,
                        candidateResultValue,
                        valueEquivalence);
        var isFound = currentMatchingPair.getKey();
        if (isFound.isTrue()) {
            queryPlanConstraintsBuilder.add(isFound.getConstraint());
            //
            // We found a match to the candidate side.
            //
            return new RecursionResult(ImmutableBiMap.of(currentQueryValue, Objects.requireNonNull(currentMatchingPair.getValue())),
                    currentQueryValue, QueryPlanConstraint.composeConstraints(queryPlanConstraintsBuilder.build()));
        }

        boolean areAllChildrenSame = true;
        final var newChildrenBuilder = ImmutableList.<Value>builder();
        final var knownNestedValueMap = HashBiMap.<Value, Value>create();
        knownNestedValueMap.putAll(knownValueMap);
        final var resultValueMapBuilder = ImmutableBiMap.<Value, Value>builder();
        for (final var child : currentQueryValue.getChildren()) {
            final var recursionResult =
                    recurseQueryResultValue(child, candidateResultValue, valueEquivalence, knownNestedValueMap, expandedValues);

            final var newChild = recursionResult.getNewCurrentValue();
            newChildrenBuilder.add(newChild);
            areAllChildrenSame = areAllChildrenSame && (child == newChild);

            final var nestedNewValueMap = recursionResult.getValueMap();
            knownNestedValueMap.putAll(nestedNewValueMap);
            resultValueMapBuilder.putAll(nestedNewValueMap);

            queryPlanConstraintsBuilder.add(recursionResult.getQueryPlanConstraint());
        }

        final var resultCurrentValue =
                areAllChildrenSame
                ? currentQueryValue
                : currentQueryValue.withChildren(newChildrenBuilder.build());

        final var resultValueMap = resultValueMapBuilder.build();

        if (knownValueMap.containsKey(resultCurrentValue)) {
            return new RecursionResult(ImmutableBiMap.of(), resultCurrentValue, QueryPlanConstraint.tautology());
        }

        currentMatchingPair =
                findMatchingCandidateValue(resultCurrentValue,
                        candidateResultValue,
                        valueEquivalence);
        isFound = currentMatchingPair.getKey();
        if (isFound.isTrue()) {
            queryPlanConstraintsBuilder.add(isFound.getConstraint());
            //
            // We found a match to the candidate side, this supersedes everything that was already found in the
            // subtree recursion.
            //
            return new RecursionResult(ImmutableBiMap.of(resultCurrentValue, Objects.requireNonNull(currentMatchingPair.getValue())),
                    resultCurrentValue, QueryPlanConstraint.composeConstraints(queryPlanConstraintsBuilder.build()));
        }

        if (!expandedValues.contains(currentQueryValue) &&
                currentQueryValue.getResultType().isRecord() &&
                !(currentQueryValue instanceof RecordConstructorValue)) {
            expandedValues.add(currentQueryValue);
            try {
                final var expandedCurrentQueryValue = expandValueToRcvOverFields(currentQueryValue);
                return recurseQueryResultValue(expandedCurrentQueryValue, candidateResultValue, valueEquivalence,
                        knownValueMap, expandedValues);
            } finally {
                expandedValues.remove(currentQueryValue);
            }
        }

        return new RecursionResult(resultValueMap, resultCurrentValue,
                QueryPlanConstraint.composeConstraints(queryPlanConstraintsBuilder.build()));
    }

    @Nonnull
    private static RecordConstructorValue expandValueToRcvOverFields(@Nonnull final Value quantifiedValue) {
        final var resultType = quantifiedValue.getResultType();
        Verify.verify(resultType.isRecord());
        Verify.verify(resultType instanceof Type.Record);
        final Type.Record resultRecordType = (Type.Record)resultType;

        final List<Type.Record.Field> fields = Objects.requireNonNull(resultRecordType.getFields());
        final var resultBuilder = ImmutableList.<Column<? extends Value>>builder();
        for (int i = 0; i < fields.size(); i++) {
            //final var field = fields.get(i);
            resultBuilder.add(Column.unnamedOf(FieldValue.ofOrdinalNumberAndFuseIfPossible(quantifiedValue, i)));
            //resultBuilder.add(Column.of(field, FieldValue.ofOrdinalNumberAndFuseIfPossible(quantifiedValue, i)));
        }
        return RecordConstructorValue.ofColumns(resultBuilder.build());
    }

    @Nonnull
    private static Pair<BooleanWithConstraint, Value> findMatchingCandidateValue(@Nonnull final Value currentQueryValue,
                                                                                 @Nonnull final Value candidateResultValue,
                                                                                 @Nonnull final ValueEquivalence valueEquivalence) {
        for (final var currentCandidateValue : candidateResultValue
                // when traversing the candidate in pre-order, only descend into structures that can be referenced
                // from the top expression. For example, RCV's components can be referenced however an Arithmetic
                // operator's children can not be referenced.
                // It is crucial to do this in pre-order to guarantee matching the maximum (sub-)value of the candidate.
                .preOrderIterable(v -> v instanceof RecordConstructorValue)) {
            final var semanticEquals =
                    currentQueryValue.semanticEquals(currentCandidateValue, valueEquivalence);
            if (semanticEquals.isTrue()) {
                return Pair.of(semanticEquals, currentCandidateValue);
            }
        }
        return Pair.of(BooleanWithConstraint.falseValue(), null);
    }

    private static class RecursionResult {
        @Nonnull
        private final BiMap<Value, Value> valueMap;
        @Nonnull
        private final Value newCurrentValue;
        @Nonnull
        private final QueryPlanConstraint queryPlanConstraint;

        public RecursionResult(@Nonnull final BiMap<Value, Value> valueMap,
                               @Nonnull final Value newCurrentValue,
                               @Nonnull final QueryPlanConstraint queryPlanConstraint) {
            this.valueMap = valueMap;
            this.newCurrentValue = newCurrentValue;
            this.queryPlanConstraint = queryPlanConstraint;
        }

        @Nonnull
        public BiMap<Value, Value> getValueMap() {
            return valueMap;
        }

        @Nonnull
        public Value getNewCurrentValue() {
            return newCurrentValue;
        }

        @Nonnull
        public QueryPlanConstraint getQueryPlanConstraint() {
            return queryPlanConstraint;
        }
    }
}
