/*
 * TypedMatcher.java
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
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@code TypedMatcher} matches a data structure that is at least of a certain type.
 *
 * <p>
 * Extreme care should be taken when implementing <code>BindingMatcher</code>s, since it can be very delicate.
 * In particular, matchers may (or may not) be reused between successive rule calls and should be stateless.
 * Additionally, implementors of <code>TypedMatcher</code> must use the (default) reference equals.
 * </p>
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class TypedMatcher<T> implements BindingMatcher<T> {
    @Nonnull
    private final Class<T> bindableClass;
    @Nonnull
    private final Set<Class<?>> rootClasses;

    public TypedMatcher(@Nonnull final Class<T> bindableClass) {
        this(bindableClass, ImmutableSet.of(bindableClass));
    }

    public TypedMatcher(@Nonnull final Class<T> bindableClass,
                        @Nonnull final Set<? extends Class<?>> rootClasses) {
        // Sanity-check that every advertised root class is a (sub-)type of `bindableClass`.
        Debugger.sanityCheck(() -> rootClasses.forEach(c -> Verify.verify(bindableClass.isAssignableFrom(c),
                "rootClass %s is not assignable from bindableClass %s", c, bindableClass)));

        this.bindableClass = bindableClass;
        this.rootClasses = ImmutableSet.copyOf(rootClasses);
    }

    @Nonnull
    @Override
    public final Class<T> getRootClass() {
        return bindableClass;
    }

    @Nonnull
    @Override
    public final Set<Class<?>> getRootClasses() {
        return rootClasses;
    }

    /**
     * Returns whether the {@link #rootClasses} set contains only {@link #bindableClass} itself.
     */
    private boolean rootClassesIsTrivial() {
        return rootClasses.size() == 1 && rootClasses.contains(bindableClass);
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull PlannerBindings outerBindings, @Nonnull T in) {
        // If `rootClasses` is not the trivial default `{bindableClass}`, sanity-check that the class of `in` is
        // (exactly) one of the `rootClasses`. When `rootClasses` is a narrower set than the default, the matcher
        // relies on the rule-set indexing path to only invoke it for instances of one of those classes.
        //
        // In the trivial default case this check is skipped on purpose, as `bindableClass` may be an abstract base
        // class, and the check would then fail if `in.getClass()` is a concrete subclass.
        if (!rootClassesIsTrivial()) {
            Debugger.sanityCheck(() -> Verify.verify(rootClasses.contains(in.getClass()),
                    "`TypedMatcher` invoked with class `%s`; expected one of %s", in.getClass(), rootClasses));
        }

        return Stream.of(PlannerBindings.from(this, in));
    }

    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        // Don't bother emitting a redundant `case _: «RootClass»` type guard if it’s already established (according to
        // `atLeastType`) that `boundId` is at least of type `getRootClass()`. Just emit a `_` wildcard pattern.
        if (rootClassesIsTrivial() && getRootClass().isAssignableFrom(atLeastType)) {
            return "case _ => success ";
        }

        // Emit a typed wildcard pattern. Print a `(… | …)` union type if there are multiple root classes.
        final String alternatives = rootClasses.stream()
                .map(Class::getSimpleName)
                .collect(Collectors.joining(" | "));
        final String pattern = rootClasses.size() > 1 ? "(" + alternatives + ")" : alternatives;
        return "case _: " + pattern + " => success ";
    }

    @Nonnull
    public static <T> TypedMatcher<T> typed(@Nonnull final Class<T> bindableClass) {
        return new TypedMatcher<>(bindableClass);
    }
}
