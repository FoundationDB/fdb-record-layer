/*
 * CallSiteArguments.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

public sealed interface CallSiteArguments
        permits CallSiteArguments.PositionalArguments,
                CallSiteArguments.NamedArguments {

    CallSiteArguments EMPTY = new PositionalArguments(List.of(), ImmutableMap.of(), WindowSpecification.NONE);

    @Nonnull
    Iterable<Value> getArguments();

    /**
     * Returns the call-site arguments as a {@link List}. For {@link NamedArguments} the ordering follows the
     * underlying map's iteration order; callers that rely on positional semantics should only use this on
     * positional invocations.
     * @return the arguments as a list
     */
    @Nonnull
    default List<Value> getArgumentsList() {
        return ImmutableList.copyOf(getArguments());
    }

    @Nonnull
    Map<String, Object> getOptions();

    @Nonnull
    WindowSpecification getWindowSpecification();

    @Nonnull
    CallSiteArguments withArguments(@Nonnull Iterable<Value> newValues);

    @Nonnull
    CallSiteArguments withNamedArguments(@Nonnull Map<String, Value> newNamedValues);

    @Nonnull
    CallSiteArguments withOptions(@Nonnull Map<String, Object> newOptions);

    @Nonnull
    CallSiteArguments withWindowSpecification(@Nonnull WindowSpecification newWindowSpecification);

    default NamedArguments asNamedArguments() {
        return (NamedArguments)this;
    }

    default boolean isSimple() {
        return isSimplePositional() || isSimpleNamed();
    }

    default boolean isSimplePositional() {
        return this instanceof PositionalArguments && !isWindowed() && getOptions().isEmpty();
    }

    default boolean isSimpleNamed() {
        return isNamed() && !isWindowed() && getOptions().isEmpty();
    }

    default boolean isNamed() {
        return this instanceof NamedArguments;
    }

    default boolean isWindowed() {
        return !getWindowSpecification().isNone();
    }

    default boolean hasOptions() {
        return !getOptions().isEmpty();
    }

    default boolean isEmpty() {
        return Iterables.isEmpty(getArguments()) && getOptions().isEmpty() && !isWindowed();
    }

    default int arity() {
        return Iterables.size(getArguments());
    }

    default int size() {
        return Iterables.size(getArguments());
    }

    @Nonnull
    static CallSiteArguments empty() {
        return EMPTY;
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final List<? extends Value> values) {
        return new PositionalArguments(ImmutableList.copyOf(values), ImmutableMap.of(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final Value value) {
        return new PositionalArguments(ImmutableList.of(value), ImmutableMap.of(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final Value... values) {
        return new PositionalArguments(ImmutableList.copyOf(values), ImmutableMap.of(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final Iterable<? extends Value> values) {
        return new PositionalArguments(ImmutableList.copyOf(values), ImmutableMap.of(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofNamed(@Nonnull final Map<String, ? extends Value> namedValues) {
        return new NamedArguments(ImmutableMap.copyOf(namedValues), ImmutableMap.of(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofNamed(@Nonnull final String argumentName, Value argumentValue) {
        return new NamedArguments(ImmutableMap.of(argumentName, argumentValue), ImmutableMap.of(), WindowSpecification.NONE);
    }

    /**
     * Bundles the window-specific components of a call site that are not already modeled by the ordinary call-site
     * arguments: the {@code PARTITION BY} columns and the {@code ORDER BY} columns (as {@link WindowOrderingPart}s,
     * which pair each ordering value with its sort direction). Carrying these here lets a windowed function receive
     * its partitioning and ordering columns directly, rather than encoded positionally as an array of arrays. Use
     * {@link #NONE} for non-windowed call sites.
     * <p>
     * A {@code WindowFrameSpecification} component is intentionally omitted for now and can be added later without
     * disturbing existing call sites.
     * </p>
     *
     * @param partitioningValues the {@code PARTITION BY} columns
     * @param orderingParts the {@code ORDER BY} columns paired with their sort directions
     */
    record WindowSpecification(@Nonnull List<Value> partitioningValues,
                               @Nonnull List<WindowOrderingPart> orderingParts) {
        public static final WindowSpecification NONE = new WindowSpecification(List.of(), List.of());

        public boolean isNone() {
            return partitioningValues.isEmpty() && orderingParts.isEmpty();
        }
    }

    record PositionalArguments(@Nonnull Iterable<Value> values,
                               @Nonnull Map<String, Object> options,
                               @Nonnull WindowSpecification windowSpecification) implements CallSiteArguments {
        @Nonnull
        @Override
        public Iterable<Value> getArguments() {
            return values;
        }

        @Nonnull
        @Override
        public Map<String, Object> getOptions() {
            return options;
        }

        @Nonnull
        @Override
        public WindowSpecification getWindowSpecification() {
            return windowSpecification;
        }

        @Nonnull
        @Override
        public CallSiteArguments withArguments(@Nonnull final Iterable<Value> newValues) {
            return new PositionalArguments(newValues, options, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withNamedArguments(@Nonnull final Map<String, Value> newNamedValues) {
            return new NamedArguments(newNamedValues, options, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withOptions(@Nonnull final Map<String, Object> newOptions) {
            return new PositionalArguments(values, newOptions, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withWindowSpecification(@Nonnull final WindowSpecification newWindowSpecification) {
            return new PositionalArguments(values, options, newWindowSpecification);
        }
    }

    record NamedArguments(@Nonnull Map<String, Value> namedArguments,
                          @Nonnull Map<String, Object> options,
                          @Nonnull WindowSpecification windowSpecification) implements CallSiteArguments {
        @Nonnull
        @Override
        public List<Value> getArguments() {
            return List.copyOf(namedArguments.values());
        }

        @Nonnull
        @Override
        public Map<String, Object> getOptions() {
            return options;
        }

        @Nonnull
        @Override
        public WindowSpecification getWindowSpecification() {
            return windowSpecification;
        }

        @Nonnull
        @Override
        public CallSiteArguments withArguments(@Nonnull final Iterable<Value> newValues) {
            return new PositionalArguments(newValues, options, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withNamedArguments(@Nonnull final Map<String, Value> newNamedValues) {
            return new NamedArguments(newNamedValues, options, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withOptions(@Nonnull final Map<String, Object> newOptions) {
            return new NamedArguments(namedArguments, newOptions, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withWindowSpecification(@Nonnull final WindowSpecification newWindowSpecification) {
            return new NamedArguments(namedArguments, options, newWindowSpecification);
        }
    }
}
