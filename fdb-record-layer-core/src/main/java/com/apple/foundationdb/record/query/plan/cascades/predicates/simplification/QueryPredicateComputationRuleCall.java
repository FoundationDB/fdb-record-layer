/*
 * QueryPredicateComputationRuleCall.java
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.AbstractRule;
import com.apple.foundationdb.record.util.pair.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.Function;

/**
 * A rule call implementation for the computation of a result while traversing {@link QueryPredicate} trees.
 * @param <ARGUMENT> the type of the arguments to this rule call
 * @param <RESULT> the type of result this rule call produces
 */
@API(API.Status.EXPERIMENTAL)
public class QueryPredicateComputationRuleCall<ARGUMENT, RESULT> extends AbstractQueryPredicateRuleCall<Pair<QueryPredicate, RESULT>, QueryPredicateComputationRuleCall<ARGUMENT, RESULT>> {

    @Nullable
    private final ARGUMENT argument;

    @Nonnull
    private final Function<QueryPredicate, Pair<QueryPredicate, RESULT>> retrieveResultFunction;

    public QueryPredicateComputationRuleCall(@Nonnull final AbstractRule<Pair<QueryPredicate, RESULT>, QueryPredicateComputationRuleCall<ARGUMENT, RESULT>, QueryPredicate, ? extends QueryPredicate> rule,
                                             @Nonnull final QueryPredicate root,
                                             @Nonnull final QueryPredicate current,
                                             @Nullable final ARGUMENT argument,
                                             @Nonnull final PlannerBindings bindings,
                                             @Nonnull final AliasMap aliasMap,
                                             @Nonnull final Set<CorrelationIdentifier> constantAliases,
                                             @Nonnull final Function<QueryPredicate, Pair<QueryPredicate, RESULT>> retrieveResultFunction) {
        super(rule, root, current, bindings, aliasMap, constantAliases);
        this.argument = argument;
        this.retrieveResultFunction = retrieveResultFunction;
    }

    @Nullable
    public ARGUMENT getArgument() {
        return argument;
    }

    @Nullable
    public Pair<QueryPredicate, RESULT> getResult(@Nonnull final QueryPredicate predicate) {
        return retrieveResultFunction.apply(predicate);
    }

    public void yieldPredicate(@Nonnull final QueryPredicate predicate, @Nonnull final RESULT result) {
        super.yieldExpression(Pair.of(predicate, result));
    }

    public void yieldPredicateAndReExplore(@Nonnull final QueryPredicate predicate, @Nonnull final RESULT result) {
        super.yieldAndReExplore(Pair.of(predicate, result));
    }
}
