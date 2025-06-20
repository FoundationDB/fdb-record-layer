/*
 * AbsorptionRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndOrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.ofTypeWithChildren;

/**
 * A rule that matches an {@link AndPredicate} or {@link OrPredicate} and attempts to simplify the terms underneath
 * according to the boolean absorption law.
 * <ul>
 *     <li>{@code (X^Y) v X = X}</li>
 *     <li>{@code (XvY) ^ X = X}</li>
 * </ul>
 * @param <P> the type representing the major of the law, i.e. {@code and} or {@code or}
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class AbsorptionRule<P extends AndOrPredicate> extends QueryPredicateSimplificationRule<P> {
    @Nonnull
    private final Class<P> majorClass;
    @Nonnull
    private final BindingMatcher<QueryPredicate> termMatcher;

    public AbsorptionRule(@Nonnull final Class<P> majorClass,
                          @Nonnull final BindingMatcher<QueryPredicate> termMatcher,
                          @Nonnull final BindingMatcher<P> rootMatcher) {
        super(rootMatcher);
        this.majorClass = majorClass;
        this.termMatcher = termMatcher;
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.empty();
    }

    @Override
    public void onMatch(@Nonnull final QueryPredicateSimplificationRuleCall call) {
        final var bindings = call.getBindings();
        final var majorTerms = bindings.getAll(termMatcher);

        final var minorClass = minorForMajor(majorClass);
        final var majorOfMinors =
                majorTerms.stream()
                        .map(term -> {
                            if (minorClass.isInstance(term)) {
                                return (Collection<? extends QueryPredicate>)Lists.newArrayList(term.getChildren());
                            } else {
                                return (Collection<? extends QueryPredicate>)Lists.newArrayList(term);
                            }
                        })
                        .collect(Collectors.toList());

        final var absorbed = BooleanPredicateNormalizer.applyAbsorptionLaw(majorOfMinors);
        if (absorbed.size() < majorOfMinors.size()) {
            final var simplifiedPredicate =
                    with(majorClass,
                            absorbed.stream()
                                    .map(minor -> with(minorClass, minor))
                                    .collect(Collectors.toList()));

            // Find all predicates that have not been retained
            final var allMajorOfMinors =
                    majorOfMinors.stream()
                            .flatMap(Collection::stream)
                            .collect(LinkedIdentitySet.toLinkedIdentitySet());

            final var retainedMajorOfMinors =
                    absorbed.stream()
                            .flatMap(Collection::stream)
                            .collect(LinkedIdentitySet.toLinkedIdentitySet());

            call.yieldResultBuilder()
                    .addConstraintsFrom(bindings.get(getMatcher()))
                    .addConstraintsFrom(Sets.difference(allMajorOfMinors, retainedMajorOfMinors))
                    .yieldResult(simplifiedPredicate);
        }
    }

    @Nonnull
    private QueryPredicate with(@Nonnull final Class<? extends AndOrPredicate> majorOrMinorClass,
                                @Nonnull final Collection<? extends QueryPredicate> terms) {
        if (majorOrMinorClass == OrPredicate.class) {
            return OrPredicate.or(terms);
        } else if (majorOrMinorClass == AndPredicate.class) {
            return AndPredicate.and(terms);
        }
        throw new RecordCoreException("unsupported major or minor");
    }

    @Nonnull
    private static Class<? extends AndOrPredicate> minorForMajor(@Nonnull final Class<? extends AndOrPredicate> majorClass) {
        if (majorClass == AndPredicate.class) {
            return OrPredicate.class;
        } else if (majorClass == OrPredicate.class) {
            return AndPredicate.class;
        }
        throw new RecordCoreException("unsupported major");
    }

    @Nonnull
    public static <P extends AndOrPredicate> AbsorptionRule<P> withMajor(@Nonnull final Class<P> majorClass) {
        final var termMatcher = anyPredicate();
        return new AbsorptionRule<>(majorClass,
                termMatcher,
                ofTypeWithChildren(majorClass, all(termMatcher)));
    }
}
