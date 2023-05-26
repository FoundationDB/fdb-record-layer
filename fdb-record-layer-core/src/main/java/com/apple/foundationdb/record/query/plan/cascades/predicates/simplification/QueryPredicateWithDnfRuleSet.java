/*
 * QueryPredicateWithDnfRuleSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class QueryPredicateWithDnfRuleSet extends DefaultQueryPredicateRuleSet {
    protected static final QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, QueryPredicate> dnfRule = new NormalFormRule(BooleanPredicateNormalizer.getDefaultInstanceForDnf());

    protected static final Set<QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, ? extends QueryPredicate>> COMPUTATION_WITH_DNF_RULES =
            ImmutableSet.<QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, ? extends QueryPredicate>>builder()
                    .addAll(COMPUTATION_RULES)
                    .add(dnfRule)
                    .build();

    protected static final SetMultimap<QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, ? extends QueryPredicate>, QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, ? extends QueryPredicate>> COMPUTATION_WITH_DNF_DEPENDS_ON;

    static {
        final var computationDependsOnBuilder =
                ImmutableSetMultimap.<QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, ? extends QueryPredicate>, QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, ? extends QueryPredicate>>builder();
        computationDependsOnBuilder.putAll(COMPUTATION_DEPENDS_ON);

        COMPUTATION_RULES.forEach(existingRule -> computationDependsOnBuilder.put(existingRule, dnfRule));
        COMPUTATION_WITH_DNF_DEPENDS_ON = computationDependsOnBuilder.build();
    }

    private QueryPredicateWithDnfRuleSet() {
        this(COMPUTATION_WITH_DNF_RULES, COMPUTATION_WITH_DNF_DEPENDS_ON);
    }

    protected QueryPredicateWithDnfRuleSet(@Nonnull final Set<? extends AbstractQueryPredicateRule<Pair<QueryPredicate, List<QueryPlanConstraint>>, QueryPredicateComputationRuleCall<EvaluationContext, List<QueryPlanConstraint>>, ? extends QueryPredicate>> abstractQueryPredicateRules,
                                           @Nonnull final SetMultimap<? extends AbstractQueryPredicateRule<Pair<QueryPredicate, List<QueryPlanConstraint>>, QueryPredicateComputationRuleCall<EvaluationContext, List<QueryPlanConstraint>>, ? extends QueryPredicate>, ? extends AbstractQueryPredicateRule<Pair<QueryPredicate, List<QueryPlanConstraint>>, QueryPredicateComputationRuleCall<EvaluationContext, List<QueryPlanConstraint>>, ? extends QueryPredicate>> dependsOn) {
        super(abstractQueryPredicateRules, dependsOn);
    }

    public static QueryPredicateWithDnfRuleSet ofComputationRules() {
        return new QueryPredicateWithDnfRuleSet();
    }
}
