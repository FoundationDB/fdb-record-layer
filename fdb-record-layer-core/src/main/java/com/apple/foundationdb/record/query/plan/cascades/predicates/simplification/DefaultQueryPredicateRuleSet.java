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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class DefaultQueryPredicateRuleSet extends QueryPredicateComputationRuleSet<EvaluationContext, List<QueryPlanConstraint>> {
    protected static final QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, OrPredicate> identityOrRule = new IdentityOrRule();
    protected static final QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, OrPredicate> annulmentOrRule = new AnnulmentOrRule();
    protected static final QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, AndPredicate> identityAndRule = new IdentityAndRule();
    protected static final QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, AndPredicate> annulmentAndRule = new AnnulmentAndRule();
    protected static final QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, AndPredicate> absorptionAndRule = AbsorptionRule.withMajor(AndPredicate.class);
    protected static final QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, OrPredicate> absorptionOrRule = AbsorptionRule.withMajor(OrPredicate.class);
    protected static final QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, NotPredicate> notOverComparisonRule = new NotOverComparisonRule();
    protected static final QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, NotPredicate> deMorganNotOverAndRule = DeMorgansTheoremRule.withMajor(AndPredicate.class);
    protected static final QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, NotPredicate> deMorganNotOverOrRule = DeMorgansTheoremRule.withMajor(OrPredicate.class);
    protected static final Set<QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, ? extends QueryPredicate>> COMPUTATION_RULES =
            ImmutableSet.of(identityOrRule,
                    annulmentOrRule,
                    identityAndRule,
                    annulmentAndRule,
                    absorptionAndRule,
                    absorptionOrRule,
                    notOverComparisonRule,
                    deMorganNotOverAndRule,
                    deMorganNotOverOrRule);

    protected static final SetMultimap<QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, ? extends QueryPredicate>, QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, ? extends QueryPredicate>> COMPUTATION_DEPENDS_ON =
            ImmutableSetMultimap.of();

    public DefaultQueryPredicateRuleSet() {
        this(COMPUTATION_RULES, COMPUTATION_DEPENDS_ON);
    }

    public DefaultQueryPredicateRuleSet(@Nonnull final Set<? extends AbstractQueryPredicateRule<NonnullPair<QueryPredicate, List<QueryPlanConstraint>>, QueryPredicateComputationRuleCall<EvaluationContext, List<QueryPlanConstraint>>, ? extends QueryPredicate>> abstractQueryPredicateRules,
                                        @Nonnull final SetMultimap<? extends AbstractQueryPredicateRule<NonnullPair<QueryPredicate, List<QueryPlanConstraint>>, QueryPredicateComputationRuleCall<EvaluationContext, List<QueryPlanConstraint>>, ? extends QueryPredicate>, ? extends AbstractQueryPredicateRule<NonnullPair<QueryPredicate, List<QueryPlanConstraint>>, QueryPredicateComputationRuleCall<EvaluationContext, List<QueryPlanConstraint>>, ? extends QueryPredicate>> dependsOn) {
        super(abstractQueryPredicateRules, dependsOn);
    }

    public static DefaultQueryPredicateRuleSet ofComputationRules() {
        return new DefaultQueryPredicateRuleSet();
    }
}
