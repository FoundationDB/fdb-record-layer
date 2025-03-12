/*
 * PushFilterThroughFetchRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.TranslateValueFunction;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilter;

/**
 * A rule that pushes a set of predicates that can be evaluated on a partial record of keys and values that were
 * retrieved by an index scan underneath a {@link RecordQueryFetchFromPartialRecordPlan} in order to filter out records
 * not satisfying the pushed predicates prior to a potentially expensive fetch operation.
 * <br>
 * A predicate can be pushed if and only if the predicate's leaves which (in the prior of this rule refer to entities
 * in the fetched full record) can be translated to counterparts in the index record. The translation is specific to
 * the kind of index and therefore the kind of {@link com.apple.foundationdb.record.query.plan.cascades.MatchCandidate}.
 * <br>
 * This rule defers to a <em>push function</em> to translate values in an appropriate way. See
 * {@link TranslateValueFunction} for
 * details.
 * <br>
 * A filter can have a number of predicates which can be separately pushed or not pushed, e.g. if the filter uses
 * {@code x > 5 AND y < 10} we may be able to push {@code x > 5} since {@code x} is covered by the underlying index, but
 * we cannot push {@code y < 10} since {@code y} is not covered by the index. We therefore partition all conjuncts of
 * the filter and try the maximum set of predicates which can be pushed.
 * <br>
 * Once the classification of predicate conjuncts is done leading to a partitioning of all conjuncts into pushable and
 * not pushable (residual), we need to consider three distinct cases:
 * <ol>
 *   <li>pushed predicates list is empty -- don't yield anything and just return</li>
 *   <li>pushed predicates list is not empty, residual predicates list is empty; yield {@code FETCH(FILTER(inner, pushedPredicates))}</li>
 *   <li>pushed predicates list is not empty, but residual predicates list is also not empty; yield
 *       {@code FILTER(FETCH(FILTER(inner, pushedPredicates)), residualPredicates)}</li>
 * </ol>
 * <pre>
 * Case 1
 * {@code
 *         +------------------------+                                +------------------------------+
 *         |                        |                                |                              |
 *         |  PredicatesFilterPlan  |                                |  FetchFromPartialRecordPlan  |
 *         |            predicates  |                                |                              |
 *         |                        |                                +---------------+--------------+
 *         +-----------+------------+                                                |
 *                     |                                                             |
 *                     |  overFetch                                                  |
 *                     |                    +------------------->                    |
 *     +---------------+--------------+                              +---------------+---------------+
 *     |                              |                              |                               |
 *     |  FetchFromPartialRecordPlan  |                              |  PredicatesFilterPlan         |
 *     |                              |                              |             pushedPredicates  |
 *     +---------------+--------------+                              |                               |
 *                     |                                             +----------------+--------------+
 *                     |                                                              |
 *                     |                                                              |
 *              +------+------+                                                       |
 *              |             |                                                       |
 *              |  innerPlan  |   +---------------------------------------------------+
 *              |             |
 *              +-------------+
 * }
 * </pre>
 * <pre>
 * Case 2
 * {@code
 *         +------------------------+                               +---------------------------------+
 *         |                        |                               |                                 |
 *         |  PredicatesFilterPlan  |                               |  PredicatesFilterPlan           |
 *         |            predicates  |                               |             residualPredicates  |
 *         |                        |                               |                                 |
 *         +-----------+------------+                               +----------------+----------------+
 *                     |                                                             |
 *                     |  overFetch                                                  |
 *                     |                    +------------------->                    |
 *     +---------------+--------------+                              +---------------+--------------+
 *     |                              |                              |                              |
 *     |  FetchFromPartialRecordPlan  |                              |  FetchFromPartialRecordPlan  |
 *     |                              |                              |                              |
 *     +---------------+--------------+                              +---------------+--------------+
 *                     |                                                             |
 *                     |                                                             |
 *                     |                                                             |
 *                     |                                             +---------------+---------------+
 *                     |                                             |                               |
 *                     |                                             |  PredicatesFilterPlan         |
 *                     |                                             |             pushedPredicates  |
 *                     |                                             |                               |
 *                     |                                             +----------------+--------------+
 *                     |                                                              |
 *                     |                                                              |
 *              +------+------+                                                       |
 *              |             |                                                       |
 *              |  innerPlan  |   +---------------------------------------------------+
 *              |             |
 *              +-------------+
 * }
 * </pre>
 */
@API(API.Status.EXPERIMENTAL)
public class PushFilterThroughFetchRule extends ImplementationCascadesRule<RecordQueryPredicatesFilterPlan> {
    @Nonnull
    private static final BindingMatcher<RecordQueryPlan> innerPlanMatcher = anyPlan();
    @Nonnull
    private static final BindingMatcher<RecordQueryFetchFromPartialRecordPlan> fetchPlanMatcher =
            RecordQueryPlanMatchers.fetchFromPartialRecordPlan(innerPlanMatcher);
    @Nonnull
    private static final BindingMatcher<Quantifier.Physical> quantifierOverFetchMatcher =
            physicalQuantifier(fetchPlanMatcher);
    @Nonnull
    private static final BindingMatcher<RecordQueryPredicatesFilterPlan> root =
            predicatesFilter(quantifierOverFetchMatcher);

    public PushFilterThroughFetchRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();

        final RecordQueryPredicatesFilterPlan filterPlan = bindings.get(root);
        final RecordQueryFetchFromPartialRecordPlan fetchPlan = bindings.get(fetchPlanMatcher);
        final Quantifier.Physical quantifierOverFetch = bindings.get(quantifierOverFetchMatcher);
        final RecordQueryPlan innerPlan = bindings.get(innerPlanMatcher);

        final List<? extends QueryPredicate> queryPredicates = filterPlan.getPredicates();

        final ImmutableList.Builder<QueryPredicate> pushedPredicatesBuilder = ImmutableList.builder();
        final ImmutableList.Builder<QueryPredicate> residualPredicatesBuilder = ImmutableList.builder();

        final CorrelationIdentifier newInnerAlias = Quantifier.uniqueId();

        for (final QueryPredicate queryPredicate : queryPredicates) {
            final Optional<QueryPredicate> pushedPredicateOptional =
                    queryPredicate.replaceLeavesMaybe(leafPredicate -> pushLeafPredicate(fetchPlan, quantifierOverFetch.getAlias(), newInnerAlias, leafPredicate));

            if (pushedPredicateOptional.isPresent()) {
                pushedPredicatesBuilder.add(pushedPredicateOptional.get());
            } else {
                residualPredicatesBuilder.add(queryPredicate);
            }
        }

        final ImmutableList<QueryPredicate> pushedPredicates = pushedPredicatesBuilder.build();
        final ImmutableList<QueryPredicate> residualPredicates = residualPredicatesBuilder.build();
        Verify.verify(pushedPredicates.size() + residualPredicates.size() == queryPredicates.size());

        // case 1
        if (pushedPredicates.isEmpty()) {
            return;
        }

        // for case 2 and case 3 we can at least build a FILTER(inner, pushedPredicates) as that is
        // required both for case 2 and 3

        final Quantifier.Physical newInnerQuantifier = Quantifier.physical(call.memoizePlan(innerPlan), newInnerAlias);

        final RecordQueryPredicatesFilterPlan pushedFilterPlan =
                new RecordQueryPredicatesFilterPlan(newInnerQuantifier, pushedPredicates);

        final Quantifier.Physical newQuantifierOverFilter = Quantifier.physical(call.memoizePlan(pushedFilterPlan));

        final RecordQueryFetchFromPartialRecordPlan newFetchPlan =
                new RecordQueryFetchFromPartialRecordPlan(newQuantifierOverFilter,
                        fetchPlan.getPushValueFunction(),
                        Type.Relation.scalarOf(fetchPlan.getResultType()),
                        fetchPlan.getFetchIndexRecords());

        if (residualPredicates.isEmpty()) {
            // case 2
            call.yieldPlan(newFetchPlan);
        } else {
            // case 3
            // create yet another physical quantifier on top of the fetch
            final Quantifier.Physical newQuantifierOverFetch = Quantifier.physical(call.memoizePlan(newFetchPlan));

            final AliasMap translationMap = AliasMap.ofAliases(quantifierOverFetch.getAlias(), newQuantifierOverFetch.getAlias());

            // rebase all residual predicates to use that quantifier's alias
            final ImmutableList<QueryPredicate> rebasedResidualPredicates = residualPredicates.stream()
                    .map(residualPredicate -> residualPredicate.rebase(translationMap))
                    .collect(ImmutableList.toImmutableList());

            call.yieldPlan(new RecordQueryPredicatesFilterPlan(newQuantifierOverFetch, rebasedResidualPredicates));
        }
    }

    @Nullable
    private QueryPredicate pushLeafPredicate(@Nonnull final RecordQueryFetchFromPartialRecordPlan fetchPlan,
                                             @Nonnull final CorrelationIdentifier oldInnerAlias,
                                             @Nonnull final CorrelationIdentifier newInnerAlias,
                                             @Nonnull final QueryPredicate leafPredicate) {
        if (!(leafPredicate instanceof PredicateWithValue)) {
            // Only values depend on aliases -- returning this leaf is ok as it
            // appears to be pushable as is.
            return leafPredicate;
        }
        final var predicateWithValue = (PredicateWithValue)leafPredicate;
        final var pushedLeafPredicateOptional =
                predicateWithValue.translateValueAndComparisonsMaybe(
                        value -> fetchPlan.pushValue(value, oldInnerAlias, newInnerAlias),
                        comparison -> comparison.replaceValuesMaybe(value -> {
                            return fetchPlan.pushValue(value, oldInnerAlias, newInnerAlias);
                        }));
        // Something went wrong when attempting to push this value through the fetch.
        // We must return null to prevent pushing of this conjunct.
        return pushedLeafPredicateOptional.orElse(null);
    }
}
