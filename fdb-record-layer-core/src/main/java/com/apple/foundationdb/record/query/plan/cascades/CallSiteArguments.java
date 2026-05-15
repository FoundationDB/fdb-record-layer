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
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification;
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
    Iterable<Value> getValues();

    @Nonnull
    Map<String, Object> getOptions();

    @Nonnull
    WindowSpecification getWindowSpecification();

    @Nonnull
    CallSiteArguments withValues(@Nonnull Iterable<Value> newValues);

    @Nonnull
    CallSiteArguments withNamedValues(@Nonnull Map<String, Value> newNamedValues);

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

    default boolean isWindowed() {
        return !getWindowSpecification().isNone();
    }

    default boolean isWindowedPositional() {
        return this instanceof PositionalArguments && isWindowed();
    }

    default boolean isWindowedNamed() {
        return isNamed() && isWindowed();
    }

    default boolean isNamed() {
        return this instanceof NamedArguments;
    }

    default boolean hasOptions() {
        return !getOptions().isEmpty();
    }

    default boolean isEmpty() {
        return Iterables.isEmpty(getValues()) && getOptions().isEmpty() && !isWindowed();
    }

    default int arity() {
        return Iterables.size(getValues());
    }

    default int size() {
        return Iterables.size(getValues());
    }

    @Nonnull
    static CallSiteArguments empty() {
        return EMPTY;
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final List<Value> values) {
        return new PositionalArguments(values, ImmutableMap.of(), WindowSpecification.NONE);
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
    static CallSiteArguments ofPositional(@Nonnull final Iterable<Value> values,
                                          @Nonnull final WindowSpecification windowSpecification) {
        return new PositionalArguments(values, ImmutableMap.of(), windowSpecification);
    }

    @Nonnull
    static CallSiteArguments ofNamed(@Nonnull final Map<String, Value> namedValues) {
        return new NamedArguments(namedValues, ImmutableMap.of(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofNamed(@Nonnull final String argumentName, Value argumentValue) {
        return new NamedArguments(ImmutableMap.of(argumentName, argumentValue), ImmutableMap.of(), WindowSpecification.NONE);
    }

    /**
     * Bundles the three components of a SQL window clause: the frame specification, partitioning columns,
     * and ordering parts. Use {@link #NONE} for non-windowed call sites.
     *
     * @param frameSpecification the frame type, boundaries, and exclusion mode
     * @param partitioningValues the PARTITION BY columns
     * @param orderingParts the ORDER BY columns with sort directions
     */
    record WindowSpecification(@Nonnull WindowFrameSpecification frameSpecification,
                               @Nonnull List<Value> partitioningValues,
                               @Nonnull List<WindowOrderingPart> orderingParts) {
        static final WindowSpecification NONE = new WindowSpecification(
                WindowFrameSpecification.defaultSpecification(), List.of(), List.of());

        public boolean isNone() {
            return partitioningValues.isEmpty() && orderingParts.isEmpty() && frameSpecification.isDefault();
        }
    }

    record PositionalArguments(@Nonnull Iterable<Value> values,
                               @Nonnull Map<String, Object> options,
                               @Nonnull WindowSpecification windowSpecification) implements CallSiteArguments {
        @Nonnull
        @Override
        public Iterable<Value> getValues() {
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
        public CallSiteArguments withValues(@Nonnull final Iterable<Value> newValues) {
            return new PositionalArguments(newValues, options, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withNamedValues(@Nonnull final Map<String, Value> newNamedValues) {
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

    record NamedArguments(@Nonnull Map<String, Value> namedValues,
                          @Nonnull Map<String, Object> options,
                          @Nonnull WindowSpecification windowSpecification) implements CallSiteArguments {
        @Nonnull
        @Override
        public List<Value> getValues() {
            return List.copyOf(namedValues.values());
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
        public CallSiteArguments withValues(@Nonnull final Iterable<Value> newValues) {
            return new PositionalArguments(newValues, options, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withNamedValues(@Nonnull final Map<String, Value> newNamedValues) {
            return new NamedArguments(newNamedValues, options, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withOptions(@Nonnull final Map<String, Object> newOptions) {
            return new NamedArguments(namedValues, newOptions, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withWindowSpecification(@Nonnull final WindowSpecification newWindowSpecification) {
            return new NamedArguments(namedValues, options, newWindowSpecification);
        }
    }
}
