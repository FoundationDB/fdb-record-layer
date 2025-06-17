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
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
public class QueryPredicateWithDnfRuleSet extends DefaultQueryPredicateRuleSet {
    protected static final QueryPredicateSimplificationRule<QueryPredicate> dnfRule = new NormalFormRule(BooleanPredicateNormalizer.getDefaultInstanceForDnf());

    protected static final Set<QueryPredicateSimplificationRule<? extends QueryPredicate>> COMPUTATION_WITH_DNF_RULES =
            ImmutableSet.<QueryPredicateSimplificationRule<? extends QueryPredicate>>builder()
                    .addAll(SIMPLIFICATION_RULES)
                    .add(dnfRule)
                    .build();

    protected static final SetMultimap<QueryPredicateSimplificationRule<? extends QueryPredicate>, QueryPredicateSimplificationRule<? extends QueryPredicate>> COMPUTATION_WITH_DNF_DEPENDS_ON;

    static {
        final var computationDependsOnBuilder =
                ImmutableSetMultimap.<QueryPredicateSimplificationRule<? extends QueryPredicate>, QueryPredicateSimplificationRule<? extends QueryPredicate>>builder();
        computationDependsOnBuilder.putAll(SIMPLIFICATION_DEPENDS_ON);

        SIMPLIFICATION_RULES.forEach(existingRule -> computationDependsOnBuilder.put(existingRule, dnfRule));
        COMPUTATION_WITH_DNF_DEPENDS_ON = computationDependsOnBuilder.build();
    }

    private QueryPredicateWithDnfRuleSet() {
        this(COMPUTATION_WITH_DNF_RULES, COMPUTATION_WITH_DNF_DEPENDS_ON);
    }

    protected QueryPredicateWithDnfRuleSet(@Nonnull final Set<? extends AbstractQueryPredicateRule<QueryPredicate, QueryPredicateSimplificationRuleCall, ? extends QueryPredicate>> abstractQueryPredicateRules,
                                           @Nonnull final SetMultimap<? extends AbstractQueryPredicateRule<QueryPredicate, QueryPredicateSimplificationRuleCall, ? extends QueryPredicate>, ? extends AbstractQueryPredicateRule<QueryPredicate, QueryPredicateSimplificationRuleCall, ? extends QueryPredicate>> dependsOn) {
        super(abstractQueryPredicateRules, dependsOn);
    }

    @Nonnull
    public static QueryPredicateWithDnfRuleSet ofSimplificationRules() {
        return new QueryPredicateWithDnfRuleSet();
    }
}
