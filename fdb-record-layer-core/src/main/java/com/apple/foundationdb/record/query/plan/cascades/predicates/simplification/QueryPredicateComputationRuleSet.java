/*
 * QueryPredicateComputationRuleSet.java
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
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 * @param <ARGUMENT> the type of argument that rules in this set consume
 * @param <RESULT> the type of result that rules in this set (or subclasses thereof) produce
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public abstract class QueryPredicateComputationRuleSet<ARGUMENT, RESULT> extends AbstractQueryPredicateRuleSet<Pair<QueryPredicate, RESULT>, QueryPredicateComputationRuleCall<ARGUMENT, RESULT>> {

    public QueryPredicateComputationRuleSet(@Nonnull final Set<? extends AbstractQueryPredicateRule<Pair<QueryPredicate, RESULT>, QueryPredicateComputationRuleCall<ARGUMENT, RESULT>, ? extends QueryPredicate>> rules,
                                            @Nonnull final SetMultimap<? extends AbstractQueryPredicateRule<Pair<QueryPredicate, RESULT>, QueryPredicateComputationRuleCall<ARGUMENT, RESULT>, ? extends QueryPredicate>, ? extends AbstractQueryPredicateRule<Pair<QueryPredicate, RESULT>, QueryPredicateComputationRuleCall<ARGUMENT, RESULT>, ? extends QueryPredicate>> dependsOn) {
        super(rules, dependsOn);
    }
}
