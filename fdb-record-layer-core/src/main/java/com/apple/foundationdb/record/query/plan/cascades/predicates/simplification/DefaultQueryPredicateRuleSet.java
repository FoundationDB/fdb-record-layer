/*
 * DefaultQueryPredicateRuleSet.java
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
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class DefaultQueryPredicateRuleSet extends AbstractQueryPredicateRuleSet<QueryPredicate, QueryPredicateSimplificationRuleCall> {
    protected static final QueryPredicateSimplificationRule<OrPredicate> identityOrRule = new IdentityOrRule();
    protected static final QueryPredicateSimplificationRule<OrPredicate> annulmentOrRule = new AnnulmentOrRule();
    protected static final QueryPredicateSimplificationRule<AndPredicate> identityAndRule = new IdentityAndRule();
    protected static final QueryPredicateSimplificationRule<AndPredicate> annulmentAndRule = new AnnulmentAndRule();
    protected static final QueryPredicateSimplificationRule<AndPredicate> absorptionAndRule = AbsorptionRule.withMajor(AndPredicate.class);
    protected static final QueryPredicateSimplificationRule<OrPredicate> absorptionOrRule = AbsorptionRule.withMajor(OrPredicate.class);
    protected static final QueryPredicateSimplificationRule<NotPredicate> notOverComparisonRule = new NotOverComparisonRule();
    protected static final QueryPredicateSimplificationRule<NotPredicate> deMorganNotOverAndRule = DeMorgansTheoremRule.withMajor(AndPredicate.class);
    protected static final QueryPredicateSimplificationRule<NotPredicate> deMorganNotOverOrRule = DeMorgansTheoremRule.withMajor(OrPredicate.class);
    protected static final Set<QueryPredicateSimplificationRule<? extends QueryPredicate>> SIMPLIFICATION_RULES =
            ImmutableSet.of(identityOrRule,
                    annulmentOrRule,
                    identityAndRule,
                    annulmentAndRule,
                    absorptionAndRule,
                    absorptionOrRule,
                    notOverComparisonRule,
                    deMorganNotOverAndRule,
                    deMorganNotOverOrRule);

    protected static final SetMultimap<QueryPredicateSimplificationRule<? extends QueryPredicate>, QueryPredicateSimplificationRule<? extends QueryPredicate>> SIMPLIFICATION_DEPENDS_ON =
            ImmutableSetMultimap.of();

    public DefaultQueryPredicateRuleSet() {
        this(SIMPLIFICATION_RULES, SIMPLIFICATION_DEPENDS_ON);
    }

    public DefaultQueryPredicateRuleSet(@Nonnull final Set<? extends AbstractQueryPredicateRule<QueryPredicate, QueryPredicateSimplificationRuleCall, ? extends QueryPredicate>> abstractQueryPredicateRules,
                                        @Nonnull final SetMultimap<? extends AbstractQueryPredicateRule<QueryPredicate, QueryPredicateSimplificationRuleCall, ? extends QueryPredicate>, ? extends AbstractQueryPredicateRule<QueryPredicate, QueryPredicateSimplificationRuleCall, ? extends QueryPredicate>> dependsOn) {
        super(abstractQueryPredicateRules, dependsOn);
    }

    @Nonnull
    public static DefaultQueryPredicateRuleSet ofSimplificationRules() {
        return new DefaultQueryPredicateRuleSet();
    }
}
