/*
 * AnnulmentOrRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;

/**
 * A rule that matches a {@link com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate} (with the argument values)
 * that applies the annulment law.
 * {@code X v T = T}
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class AnnulmentOrRule extends QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, OrPredicate> {
    @Nonnull
    private static final BindingMatcher<QueryPredicate> orTermMatcher = anyPredicate();

    @Nonnull
    private static final BindingMatcher<OrPredicate> rootMatcher = QueryPredicateMatchers.orPredicate(all(orTermMatcher));

    public AnnulmentOrRule() {
        super(rootMatcher);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.of(OrPredicate.class);
    }

    @Override
    public void onMatch(@Nonnull final QueryPredicateComputationRuleCall<EvaluationContext, List<QueryPlanConstraint>> call) {
        final var bindings = call.getBindings();
        final var orTerms = bindings.getAll(orTermMatcher);

        if (orTerms.stream().anyMatch(QueryPredicate::isTautology)) {
            call.yield(new ConstantPredicate(true), ImmutableList.of());
        }
    }
}
