/*
 * SelectDataAccessRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.MatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MatchPartitionMatchers.ofExpressionAndMatches;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PartialMatchMatchers.completeMatch;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.ofType;

/**
 * A rule that utilizes index matching information compiled by {@link CascadesPlanner} to create one or more
 * expressions for data access specifically for a {@link SelectExpression}. A {@link SelectExpression} behaves
 * different compared to essentially all other expressions in a way that we can conceptually deconstruct such an expression
 * on the fly and only replace the matched part of the original expression with the scan over the materialized view.
 * That allows us to relax restrictions (.e.g. to match all quantifiers the select expression owns) while matching
 * select expressions.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class SelectDataAccessRule extends AbstractDataAccessRule<SelectExpression> {
    private static final BindingMatcher<PartialMatch> completeMatchMatcher = completeMatch();
    private static final BindingMatcher<SelectExpression> expressionMatcher = ofType(SelectExpression.class);

    private static final BindingMatcher<MatchPartition> rootMatcher =
            ofExpressionAndMatches(expressionMatcher, some(completeMatchMatcher));

    public SelectDataAccessRule() {
        super(rootMatcher, completeMatchMatcher, expressionMatcher);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var completeMatches = bindings.getAll(getCompleteMatchMatcher());
        if (completeMatches.isEmpty()) {
            return;
        }

        final var expression = bindings.get(getExpressionMatcher());
        final var correlatedTo = expression.getCorrelatedTo();

        //
        // return if there is no pre-determined interesting ordering
        //
        final var requestedOrderingsOptional = call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }

        final var requestedOrderings = requestedOrderingsOptional.get();
        final var aliasToQuantifierMap = Quantifiers.aliasToQuantifierMap(expression.getQuantifiers());
        final var aliases = aliasToQuantifierMap.keySet();

        // group all successful matches by their sets of compensated aliases
        final var matchPartitionByMatchAliasMap =
                completeMatches
                        .stream()
                        .flatMap(match -> {
                            final var compensatedAliases = match.getCompensatedAliases();
                            if (!compensatedAliases.containsAll(aliases)) {
                                return Stream.empty();
                            }
                            final Set<CorrelationIdentifier> matchedForEachAliases =
                                    compensatedAliases.stream()
                                            .filter(matchedAlias -> Objects.requireNonNull(aliasToQuantifierMap.get(matchedAlias)) instanceof Quantifier.ForEach)
                                            .collect(ImmutableSet.toImmutableSet());
                            if (matchedForEachAliases.size() == 1) {
                                return Stream.of(NonnullPair.of(Iterables.getOnlyElement(matchedForEachAliases), match));
                            }
                            return Stream.empty();
                        })
                        .collect(Collectors.groupingBy(
                                Pair::getLeft,
                                LinkedHashMap::new,
                                Collectors.mapping(Pair::getRight, ImmutableList.toImmutableList())));

        // loop through all compensated alias sets and their associated match partitions
        for (final var matchPartitionByMatchAliasEntry : matchPartitionByMatchAliasMap.entrySet()) {
            final var matchedAlias = matchPartitionByMatchAliasEntry.getKey();
            final var matchPartitionForMatchedAlias = matchPartitionByMatchAliasEntry.getValue();

            //
            // Pull down the requested orderings along the matchedAlias
            //
            final var pushedRequestedOrderings =
                    requestedOrderings.stream()
                            .map(requestedOrdering -> requestedOrdering.pushDown(expression.getResultValue(), matchedAlias, AliasMap.emptyMap(), correlatedTo))
                            .collect(ImmutableSet.toImmutableSet());

            //
            // We do know that local predicates (which includes predicates only using the matchedAlias quantifier)
            // are definitely handled by the logic expressed by the partial matches of the current match partition.
            // Join predicates are different in a sense that there will be matches that handle those predicates and
            // there will be matches where these predicates will not be handled. We further need to sub-partition the
            // current match partition, by the predicates that are being handled by the matches.
            //
            // TODO this should just be exactly one key
            final var matchPartitionsForAliasesByPredicates =
                    matchPartitionForMatchedAlias
                            .stream()
                            .collect(Collectors.groupingBy(match -> new LinkedIdentitySet<>(match.getMatchInfo().getPredicateMap().keySet()),
                                    HashMap::new,
                                    ImmutableList.toImmutableList()));

            //
            // Note that this works because there is only one for-each and potentially 0 - n existential quantifiers
            // that are covered by the match partition. Even though that logically forms a join, the existential
            // quantifiers do not mutate the result of the join, they only cause filtering, that is, the resulting
            // record is exactly what the for each quantifier produced filtered by the predicates expressed on the
            // existential quantifiers.
            //
            for (final var matchPartitionEntry : matchPartitionsForAliasesByPredicates.entrySet()) {
                final var matchPartition = matchPartitionEntry.getValue();

                //
                // The current match partition covers all matches that match the aliases in matchedAliases
                // as well as all predicates in matchedPredicates. In other words we now have to compensate
                // for all the remaining quantifiers and all remaining predicates.
                //
                final var dataAccessExpressions =
                        dataAccessForMatchPartition(call,
                                pushedRequestedOrderings,
                                matchPartition);
                call.yieldExpression(dataAccessExpressions);
            }
        }
    }
}
