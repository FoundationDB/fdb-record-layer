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

    CallSiteArguments EMPTY = new PositionalArguments(List.of(), ImmutableMap.of());

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
    CallSiteArguments withArguments(@Nonnull Iterable<Value> newValues);

    @Nonnull
    CallSiteArguments withNamedArguments(@Nonnull Map<String, Value> newNamedValues);

    @Nonnull
    CallSiteArguments withOptions(@Nonnull Map<String, Object> newOptions);

    default NamedArguments asNamedArguments() {
        return (NamedArguments)this;
    }

    default boolean isSimple() {
        return isSimplePositional() || isSimpleNamed();
    }

    default boolean isSimplePositional() {
        return this instanceof PositionalArguments && getOptions().isEmpty();
    }

    default boolean isSimpleNamed() {
        return isNamed() && getOptions().isEmpty();
    }

    default boolean isNamed() {
        return this instanceof NamedArguments;
    }

    default boolean hasOptions() {
        return !getOptions().isEmpty();
    }

    default boolean isEmpty() {
        return Iterables.isEmpty(getArguments()) && getOptions().isEmpty();
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
        return new PositionalArguments(ImmutableList.copyOf(values), ImmutableMap.of());
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final Value value) {
        return new PositionalArguments(ImmutableList.of(value), ImmutableMap.of());
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final Value... values) {
        return new PositionalArguments(ImmutableList.copyOf(values), ImmutableMap.of());
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final Iterable<? extends Value> values) {
        return new PositionalArguments(ImmutableList.copyOf(values), ImmutableMap.of());
    }

    @Nonnull
    static CallSiteArguments ofNamed(@Nonnull final Map<String, ? extends Value> namedValues) {
        return new NamedArguments(ImmutableMap.copyOf(namedValues), ImmutableMap.of());
    }

    @Nonnull
    static CallSiteArguments ofNamed(@Nonnull final String argumentName, Value argumentValue) {
        return new NamedArguments(ImmutableMap.of(argumentName, argumentValue), ImmutableMap.of());
    }

    record PositionalArguments(@Nonnull Iterable<Value> values,
                               @Nonnull Map<String, Object> options) implements CallSiteArguments {
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
        public CallSiteArguments withArguments(@Nonnull final Iterable<Value> newValues) {
            return new PositionalArguments(newValues, options);
        }

        @Nonnull
        @Override
        public CallSiteArguments withNamedArguments(@Nonnull final Map<String, Value> newNamedValues) {
            return new NamedArguments(newNamedValues, options);
        }

        @Nonnull
        @Override
        public CallSiteArguments withOptions(@Nonnull final Map<String, Object> newOptions) {
            return new PositionalArguments(values, newOptions);
        }
    }

    record NamedArguments(@Nonnull Map<String, Value> namedArguments,
                          @Nonnull Map<String, Object> options) implements CallSiteArguments {
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
        public CallSiteArguments withArguments(@Nonnull final Iterable<Value> newValues) {
            return new PositionalArguments(newValues, options);
        }

        @Nonnull
        @Override
        public CallSiteArguments withNamedArguments(@Nonnull final Map<String, Value> newNamedValues) {
            return new NamedArguments(newNamedValues, options);
        }

        @Nonnull
        @Override
        public CallSiteArguments withOptions(@Nonnull final Map<String, Object> newOptions) {
            return new NamedArguments(namedArguments, newOptions);
        }
    }
}
