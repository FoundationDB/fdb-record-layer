/*
 * DefaultQueryPredicateRuleSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerConstraint;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.util.List;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class DefaultQueryPredicateRuleSet extends QueryPredicateComputationRuleSet<EvaluationContext, List<PlannerConstraint<?>>> {
    protected static final QueryPredicateComputationRule<EvaluationContext, List<PlannerConstraint<?>>, OrPredicate> matchValueRule = new OrWithTautologyRule();

    protected static final Set<QueryPredicateComputationRule<EvaluationContext, List<PlannerConstraint<?>>, ? extends QueryPredicate>> COMPUTATION_RULES =
            ImmutableSet.of(matchValueRule);

    protected static final SetMultimap<QueryPredicateComputationRule<EvaluationContext, List<PlannerConstraint<?>>, ? extends QueryPredicate>, QueryPredicateComputationRule<EvaluationContext, List<PlannerConstraint<?>>, ? extends QueryPredicate>> COMPUTATION_DEPENDS_ON =
            ImmutableSetMultimap.of();

    public DefaultQueryPredicateRuleSet() {
        super(COMPUTATION_RULES, COMPUTATION_DEPENDS_ON);
    }

    public static DefaultQueryPredicateRuleSet ofComputationRules() {
        return new DefaultQueryPredicateRuleSet();
    }
}
