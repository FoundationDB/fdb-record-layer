/*
 * DeMorgansTheoremRule.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndOrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.notPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.ofTypeWithChildren;

/**
 * A rule that matches a {@link NotPredicate} over an {@link AndPredicate} or {@link OrPredicate} and applies a
 * transformation utilizing deMorgan's law.
 * @param <P> the type representing the major of the law, i.e. {@code and} or {@code or}
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class DeMorgansTheoremRule<P extends AndOrPredicate> extends QueryPredicateSimplificationRule<NotPredicate> {
    @Nonnull
    private final Class<P> majorClass;
    @Nonnull
    private final BindingMatcher<QueryPredicate> termMatcher;
    @Nonnull
    private final BindingMatcher<P> andOrPredicateMatcher;

    public DeMorgansTheoremRule(@Nonnull final Class<P> majorClass,
                                @Nonnull final BindingMatcher<QueryPredicate> termMatcher,
                                @Nonnull final BindingMatcher<P> andOrPredicateMatcher,
                                @Nonnull final BindingMatcher<NotPredicate> rootMatcher) {
        super(rootMatcher);
        this.majorClass = majorClass;
        this.andOrPredicateMatcher = andOrPredicateMatcher;
        this.termMatcher = termMatcher;
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.of(NotPredicate.class);
    }

    @Override
    public void onMatch(@Nonnull final QueryPredicateSimplificationRuleCall call) {
        final var bindings = call.getBindings();
        final var majorTerms = bindings.getAll(termMatcher);

        final var minorTerms =
                majorTerms.stream()
                        .map(NotPredicate::not)
                        .collect(ImmutableList.toImmutableList());

        call.yieldResultBuilder()
                .addConstraintsFrom(bindings.get(getMatcher()), bindings.get(andOrPredicateMatcher))
                .yieldResultAndReExplore(minorWith(minorTerms));
    }

    private QueryPredicate minorWith(@Nonnull final Collection<? extends QueryPredicate> terms) {
        if (majorClass == AndPredicate.class) {
            return OrPredicate.or(terms);
        } else if (majorClass == OrPredicate.class) {
            return AndPredicate.and(terms);
        }
        throw new RecordCoreException("unsupported major");
    }

    public static <P extends AndOrPredicate> DeMorgansTheoremRule<P> withMajor(@Nonnull final Class<P> majorClass) {
        final var termMatcher = anyPredicate();
        final var andOrPredicateMatcher = ofTypeWithChildren(majorClass, all(termMatcher));
        return new DeMorgansTheoremRule<>(majorClass,
                termMatcher,
                andOrPredicateMatcher,
                notPredicate(ListMatcher.exactly(andOrPredicateMatcher)));
    }
}
