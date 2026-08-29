/*
 * PullUp.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.Set;

/**
 * Chain to pull up {@link Value} trees through a series of relational expressions.
 */
public class PullUp {
    @Nullable
    private final PullUp parentPullUp;
    private final CorrelationIdentifier candidateAlias;
    private final Value pullThroughValue;
    private final Set<CorrelationIdentifier> rangedOverAliases;
    private final PullUp rootPullUp;

    private PullUp(@Nullable final PullUp parentPullUp,
                   final CorrelationIdentifier candidateAlias,
                   final Value pullThroughValue,
                   final Set<CorrelationIdentifier> rangedOverAliases) {
        this.parentPullUp = parentPullUp;
        this.candidateAlias = candidateAlias;
        this.pullThroughValue = pullThroughValue;
        this.rangedOverAliases = ImmutableSet.copyOf(rangedOverAliases);
        this.rootPullUp = parentPullUp == null ? this : parentPullUp.getRootPullUp();
    }

    @Nullable
    public PullUp getParentPullUp() {
        return parentPullUp;
    }

    public PullUp getRootPullUp() {
        return rootPullUp;
    }

    public CorrelationIdentifier getCandidateAlias() {
        return candidateAlias;
    }

    public boolean isRoot() {
        return parentPullUp == null;
    }

    public Value getPullThroughValue() {
        return pullThroughValue;
    }

    public Set<CorrelationIdentifier> getRangedOverAliases() {
        return rangedOverAliases;
    }

    private static PullUp forMatch(@Nullable final PullUp parentPullUp,
                                   final CorrelationIdentifier candidateAlias,
                                   final CorrelationIdentifier lowerAlias,
                                   final Type lowerType,
                                   final Set<CorrelationIdentifier> rangedOverAliases) {
        return forMatch(parentPullUp, candidateAlias, QuantifiedObjectValue.of(lowerAlias, lowerType),
                rangedOverAliases);
    }

    private static PullUp forMatch(@Nullable final PullUp parentPullUp,
                                   final CorrelationIdentifier candidateAlias,
                                   final Value lowerPullThroughValue,
                                   final Set<CorrelationIdentifier> rangedOverAliases) {
        return new MatchPullUp(parentPullUp, candidateAlias, lowerPullThroughValue, rangedOverAliases);
    }

    public static UnificationPullUp forUnification(final CorrelationIdentifier candidateAlias,
                                                   final Value lowerPullThroughValue,
                                                   final Set<CorrelationIdentifier> rangedOverAliases) {
        return new UnificationPullUp(null, candidateAlias, lowerPullThroughValue, rangedOverAliases);
    }

    /**
     * Pull up a {@link Value} expressed in the appropriate scope of matching (some expression in the matching path)
     * using this {@link PullUp}. The current pull-up is maintained during the computation of compensation. Whenever,
     * we need to e.g. reapply a predicate or in general compute a {@link Value} compensating for a shortcoming of
     * an e.g. index scan, we need to reapply that {@link Value} in terms of the result of the index scan, not in terms
     * of the intermediate match target of some nested
     * {@link com.apple.foundationdb.record.query.plan.cascades.PartialMatch} in the matching path. If there is such a
     * pulled up value, the compensation can be expressed and realized by a filter or map or similar. Note that it is
     * possible that the passed in value cannot be pulled up. This method returns {@code Optional.empty()} in such case.
     * @param value the {@link Value} to be pulled up
     * @return an optional contained the pulled up {@link Value} of {@code value} or {@code Optional.empty()} if
     *         {@code value} could not be pulled up.
     */
    public Optional<Value> pullUpValueMaybe(final Value value) {
        //
        // The following loop would probably be more self-explanatory if it were written as a recursion but
        // this unrolled version probably performs better as this may prove to be a tight loop.
        //
        var currentValue = value;
        for (var currentPullUp = this; ; currentPullUp = currentPullUp.getParentPullUp()) {
            final var maxMatchMap =
                    MaxMatchMap.compute(currentValue, currentPullUp.getPullThroughValue(),
                            currentPullUp.getRangedOverAliases());
            final var currentValueOptional =
                    maxMatchMap.translateQueryValueMaybe(currentPullUp.getCandidateAlias());
            if (currentValueOptional.isEmpty()) {
                return Optional.empty();
            }
            currentValue = currentValueOptional.get()
                    .simplify(EvaluationContext.empty(), AliasMap.emptyMap(), currentPullUp.getRangedOverAliases());

            if (currentPullUp.getParentPullUp() == null) {
                return Optional.of(currentValue);
            }
        }
    }

    public Optional<Value> pullUpCandidateValueMaybe(final Value value) {
        //
        // The following loop would probably be more self-explanatory if it were written as a recursion but
        // this unrolled version probably performs better as this may prove to be a tight loop.
        //
        var currentValue = value;
        for (var currentPullUp = this; ; currentPullUp = currentPullUp.getParentPullUp()) {
            final var currentPullThroughValue = currentPullUp.getPullThroughValue();
            final var currentRangedOverAliases = currentPullUp.getRangedOverAliases();
            final var currentCandidateAlias = currentPullUp.getCandidateAlias();
            final var candidatePullUpMap =
                    currentPullThroughValue.pullUp(ImmutableList.of(currentValue),
                            EvaluationContext.empty(),
                            AliasMap.emptyMap(),
                            Sets.difference(currentValue.getCorrelatedToWithoutChildren(),
                                    currentRangedOverAliases),
                            currentCandidateAlias);
            final var pulledUpCandidateAggregateValue = candidatePullUpMap.get(currentValue);
            if (pulledUpCandidateAggregateValue.isEmpty()) {
                return Optional.empty();
            }
            currentValue = Iterables.getOnlyElement(pulledUpCandidateAggregateValue);

            if (currentPullUp.getParentPullUp() == null) {
                return Optional.of(currentValue);
            }
        }
    }

    public static RelationalExpressionVisitor<PullUp> visitor(@Nullable final PullUp parentPullUp,
                                                              final CorrelationIdentifier candidateAlias) {
        return new PullUpVisitor(parentPullUp, candidateAlias);
    }

    private static class PullUpVisitor implements RelationalExpressionVisitorWithDefaults<PullUp> {
        @Nullable
        private final PullUp parentPullUp;
        private final CorrelationIdentifier candidateAlias;

        public PullUpVisitor(@Nullable final PullUp parentPullUp,
                             final CorrelationIdentifier candidateAlias) {
            this.parentPullUp = parentPullUp;
            this.candidateAlias = candidateAlias;
        }

        @Override
        public PullUp visitLogicalTypeFilterExpression(final LogicalTypeFilterExpression logicalTypeFilterExpression) {
            return forMatch(parentPullUp, candidateAlias, logicalTypeFilterExpression.getInnerQuantifier().getAlias(),
                    logicalTypeFilterExpression.getInnerQuantifier().getFlowedObjectType(),
                    Quantifiers.aliases(logicalTypeFilterExpression.getQuantifiers()));
        }

        @Override
        public PullUp visitDefault(final RelationalExpression relationalExpression) {
            return forMatch(parentPullUp, candidateAlias, relationalExpression.getResultValue(),
                    Quantifiers.aliases(relationalExpression.getQuantifiers()));
        }
    }

    public static class MatchPullUp extends PullUp {
        public MatchPullUp(@Nullable final PullUp parentPullUp,
                           final CorrelationIdentifier candidateAlias,
                           final Value pullThroughValue,
                           final Set<CorrelationIdentifier> rangedOverAliases) {
            super(parentPullUp, candidateAlias, pullThroughValue, rangedOverAliases);
        }
    }

    public static class UnificationPullUp extends PullUp {
        public UnificationPullUp(@Nullable final PullUp parentPullUp,
                                 final CorrelationIdentifier candidateAlias,
                                 final Value pullThroughValue,
                                 final Set<CorrelationIdentifier> rangedOverAliases) {
            super(parentPullUp, candidateAlias, pullThroughValue, rangedOverAliases);
        }
    }
}
