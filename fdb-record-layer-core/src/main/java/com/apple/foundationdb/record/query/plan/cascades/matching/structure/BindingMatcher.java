/*
 * BindingMatcher.java
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * A binding matcher is an object that can be matched against a complex object tree, while binding certain
 * references in the tree to matcher objects. The bindings can be retrieved from the {@link PlannerBindings} once one
 * or more bindings are established.
 * <br>
 * Extreme care should be taken when implementing binding matchers, since it can be very delicate.
 * In particular, binding matchers may (or may not) be reused between successive rule calls and should be stateless.
 * Additionally, implementors must use the (default) reference equals for any binding matcher as the
 * class {@link PlannerBindings} keeps an association between a matcher and the matched object(s) in a
 * multimap.
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public interface BindingMatcher<T> {
    /**
     * Indentation for {@link #explainMatcher(Class, String, String)}.
     */
    String INDENTATION = "    ";

    /**
     * Get a class that this matcher can match. Ideally, it should be the lowest such class, but it may not be.
     * A planner will generally use this method to quickly determine a set of rules that could match an expression,
     * without considering each rule and trying to apply it. A good implementation of this method helps the planner
     * match rules efficiently.
     * @return a class object for a class that is a super class of every planner expression this matcher can match
     */
    @Nonnull
    Class<T> getRootClass();

    /**
     * Attempt to match this matcher against the given object.
     * Note that implementations should only attempt to match the given object with this matcher and delegate
     * all other matching activity to other matchers.
     *
     * @param plannerConfiguration a planner configuration
     * @param outerBindings preexisting bindings to be used by the matcher
     * @param in the object we attempt to match
     * @return a stream of {@link PlannerBindings} containing the matched bindings, or an empty stream is no match was found
     */
    @Nonnull
    Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull PlannerBindings outerBindings, @Nonnull T in);

    /**
     * A matcher is completely agnostic to the actual class of an object that is being passed in meaning that
     * a matcher that can only match e.g. a {@link String} can be asked to match a {@link Long}. By contract, it must
     * not ever match that {@code Long} but the call to {@code bindMatches} is allowed and even encouraged.
     * This method is implemented as a {@code default} which should be {@code final} as well if Java allowed
     * {@code default}s to be {@code final}. The implementation checks the class of the object that is passed in to be
     * at least of the class returned by {@link #getRootClass()} and then calls
     * {@link #bindMatchesSafely(RecordQueryPlannerConfiguration, PlannerBindings, Object)} with a down-casted {@code in} argument (to {@code T)}.
     * @param plannerConfiguration a planner configuration
     * @param outerBindings the outer bindings passed in from the caller
     * @param in the object to match against
     * @return a stream of planner bindings
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    default Stream<PlannerBindings> bindMatches(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull PlannerBindings outerBindings, @Nonnull Object in) {
        if (getRootClass().isInstance(in)) {
            return bindMatchesSafely(plannerConfiguration, outerBindings, (T)in);
        } else {
            return Stream.empty();
        }
    }

    /**
     * Combinator to constrain the current matcher with the downstream matcher passed in.
     * @param downstream a matcher that is used to constrain this matcher
     * @return a new binding matcher.
     */
    default BindingMatcher<T> where(@Nonnull final BindingMatcher<? super T> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(getRootClass(),
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(getRootClass(), ImmutableList.of(this, downstream)));
    }

    /**
     * Combinator to constrain the current matcher with the downstream matchers passed in.
     * Note: As it is not allowed to declare a non-final instance method with {@link SafeVarargs} any callers of
     * this method we cause an <em>unchecked</em> warning at the call site which may have to be
     * suppressed for each call site separately. As an alternative suggestion, the caller can also string
     * matchers together using {@code m.where(m1).and(m2)} or directly call {@link #where(Collection)} instead.
     * @param downstreams a vararg of matchers that is used to constrain this matcher
     * @return a new binding matcher.
     */
    @SuppressWarnings({"varags", "unchecked"})
    default BindingMatcher<T> where(@Nonnull final BindingMatcher<? super T>... downstreams) {
        return where(AllOfMatcher.matchingAllOf(getRootClass(), Arrays.asList(downstreams)));
    }

    /**
     * Combinator to constrain the current matcher with the downstream matchers passed in.
     * @param downstreams a collection of matchers that is used to constrain this matcher
     * @return a new binding matcher.
     */
    default BindingMatcher<T> where(@Nonnull final Collection<? extends BindingMatcher<? super T>> downstreams) {
        return where(AllOfMatcher.matchingAllOf(getRootClass(), downstreams));
    }

    /**
     * Combinator which is just just a rename of {@link #where(BindingMatcher)}.
     * @param downstream a matcher that is used to constrain this matcher
     * @return a new binding matcher.
     */
    default BindingMatcher<T> and(@Nonnull final BindingMatcher<? super T> downstream) {
        return where(downstream);
    }

    /**
     * Combinator to return a matcher that matches the logical or of this matcher and the matcher passed in.
     * @param downstream a downstream matcher
     * @return a new binding matcher.
     */
    default BindingMatcher<T> or(@Nonnull final BindingMatcher<? super T> downstream) {
        return AnyOfMatcher.matchingAnyOf(getRootClass(), ImmutableList.of(this, downstream));
    }

    /**
     * Method that explain what this matcher does. By convention implementors use a somewhat Scala-esque syntax to
     * provide the explain string.
     * @param atLeastType the type that we know {@code boundId} is at least of
     * @param boundId an typedIdentifier for the object being matched.
     * @param indentation the current indentation as a string of spaces
     * @return a string explaining the semantics of this matcher
     */
    String explainMatcher(@Nonnull Class<?> atLeastType, @Nonnull String boundId, @Nonnull String indentation);

    /**
     * Method that attempts to match the current binding matcher against the object passed in and just returns whether
     * the matching process produced any matches at all thus forsaking all actual bindings accrued in the matching process.
     * This method is intended to be called e.g. from test code.
     * @param in an object to match against
     * @return {@code true} if the binding matcher produced any bindings for the object passed in,
     *         {@code false} otherwise
     */
    default boolean matches(@Nonnull Object in) {
        return bindMatches(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(), PlannerBindings.empty(), in).findAny().isPresent();
    }

    /**
     * Method that attempts to match the current binding matcher against the object passed in and just returns whether
     * the matching process produced one single match thus forsaking all actual bindings accrued in the matching process.
     * This method is intended to be called e.g. from test code.
     * As a example {@code any(equalsObject(5)).matchesExactly(ImmutableList.of(4, 5, 5))} returns {@code false} while
     * {@code any(equalsObject(5)).matches(ImmutableList.of(4, 5, 5))} returns {@code true}.
     * @param in an object to match against
     * @return {@code true} if the binding matcher produced any bindings for the object passed in,
     *         {@code false} otherwise
     */
    default boolean matchesExactly(@Nonnull Object in) {
        final List<PlannerBindings> plannerBindings =
                bindMatches(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(), PlannerBindings.empty(), in)
                        .collect(ImmutableList.toImmutableList());
        return plannerBindings.size() == 1;
    }

    default String identifierFromMatcher() {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, unboundRootClass().getSimpleName());
    }

    default Class<?> unboundRootClass() {
        try {
            return getRootClass();
        } catch (final RecordCoreException e) {
            return Object.class;
        }
    }

    /**
     * Create a unique instance that can be used to manually set a binding in a {@link PlannerBindings}. Normally
     * matchers match a pattern which (if matching is successful) causes a binding to be created between a matcher
     * and its matched data structure. This particular kind of instance of a matcher this method returns can be used to
     * pre-set a binding to be conveniently be picked up by other matchers. For instance, it can be useful to
     * communicate state information to the matching process (via the {@code outerBindings} parameter). Some rules
     * would like to e.g. match an expression if that expression also happens to be the root of the graph, which is
     * not matched but can be preset by the planner for the other matchers to be picked up.
     * @param <T> type parameter
     * @return a new instance of a matcher that can be used to manually set a binding in a {@link PlannerBindings}
     *         object
     */
    static <T> BindingMatcher<T> instance() {
        return new BindingMatcher<>() {
            @Nonnull
            @Override
            public Class<T> getRootClass() {
                throw new RecordCoreException("getRootClass() should not be called");
            }

            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final T in) {
                throw new RecordCoreException("bindMatchesSafely() should not be called");
            }

            @Override
            public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
                throw new RecordCoreException("explainMatcher() should not be called");
            }
        };
    }

    static String newLine(@Nonnull final String indent) {
        return "\n" + indent;
    }
}
