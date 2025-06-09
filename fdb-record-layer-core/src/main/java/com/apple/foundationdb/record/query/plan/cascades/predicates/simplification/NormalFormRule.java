/*
 * NormalFormRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;

/**
 * A rule that transforms a boolean expression into a normal form.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class NormalFormRule extends QueryPredicateSimplificationRule<QueryPredicate> {
    @Nonnull
    private static final BindingMatcher<QueryPredicate> anyPredicateMatcher = anyPredicate();
    @Nonnull
    private final BooleanPredicateNormalizer normalizer;

    public NormalFormRule(@Nonnull final BooleanPredicateNormalizer normalizer) {
        super(anyPredicateMatcher);
        this.normalizer = normalizer;
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        // always-rule
        return Optional.empty();
    }

    @Override
    public void onMatch(@Nonnull final QueryPredicateSimplificationRuleCall call) {
        if (!call.isRoot()) {
            return;
        }

        final var bindings = call.getBindings();
        final var predicate = bindings.get(anyPredicateMatcher);

        final var normalizedPredicateMaybe = normalizer.normalize(predicate, false);
        normalizedPredicateMaybe.ifPresent(normalizedPredicate ->
                call.yieldAndReExplore(normalizedPredicateMaybe.get()));
    }
}
