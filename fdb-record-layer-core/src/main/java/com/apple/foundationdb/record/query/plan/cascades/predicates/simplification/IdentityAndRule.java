/*
 * IdentityAndRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;

/**
 * A rule that matches a {@link AndPredicate} (with the argument values).
 * {@code X ^ T = X}
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class IdentityAndRule extends QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, AndPredicate> {
    @Nonnull
    private static final BindingMatcher<QueryPredicate> andTermMatcher = anyPredicate();

    @Nonnull
    private static final BindingMatcher<AndPredicate> rootMatcher = QueryPredicateMatchers.andPredicate(all(andTermMatcher));

    public IdentityAndRule() {
        super(rootMatcher);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.of(AndPredicate.class);
    }

    @Override
    public void onMatch(@Nonnull final QueryPredicateComputationRuleCall<EvaluationContext, List<QueryPlanConstraint>> call) {
        final var bindings = call.getBindings();
        final var terms = bindings.getAll(andTermMatcher);

        final var resultTermsBuilder = ImmutableList.<QueryPredicate>builder();
        int count = 0;
        for (final var term : terms) {
            if (!term.isTautology()) {
                count ++;
                if (count == terms.size()) {
                    return;
                }

                // term is still needed
                resultTermsBuilder.add(term);
            }
        }
        final var resultTerms = resultTermsBuilder.build();
        final var simplifiedPredicate = AndPredicate.and(resultTerms);
        call.yieldPredicate(simplifiedPredicate, ImmutableList.of());
    }
}
