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

import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public sealed interface CallSiteArguments {

    CallSiteArguments EMPTY = new PositionalArguments(List.of(), Options.empty(), WindowSpecification.NONE);

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
    Options getOptions();

    @Nonnull
    WindowSpecification getWindowSpecification();

    @Nonnull
    CallSiteArguments withArguments(@Nonnull Iterable<Value> newValues);

    @Nonnull
    CallSiteArguments withNamedArguments(@Nonnull Map<String, Value> newNamedValues);

    @Nonnull
    CallSiteArguments withOptions(@Nonnull Options newOptions);

    @Nonnull
    CallSiteArguments withWindowSpecification(@Nonnull WindowSpecification newWindowSpecification);

    /**
     * Reads a call-site option. This is the typed way to get at an option: the value is handed back as the option's
     * declared type, so no caller needs to cast.
     * @param option the option to read
     * @param <T> the option's value type
     * @return the option's value, or {@link Optional#empty()} if the option was not supplied at this call site
     */
    @Nonnull
    default <T> Optional<T> getOption(@Nonnull final Option<T> option) {
        return getOptions().get(option);
    }

    /**
     * Returns call-site arguments with {@code option} set to {@code value}, in addition to any options already set.
     * @param option the option to set
     * @param value the option's value
     * @param <T> the option's value type
     * @return a new {@link CallSiteArguments} with the given option set
     */
    @Nonnull
    default <T> CallSiteArguments withOption(@Nonnull final Option<T> option, @Nonnull final T value) {
        return withOptions(getOptions().toBuilder().put(option, value).build());
    }

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
        return new PositionalArguments(ImmutableList.copyOf(values), Options.empty(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final Value value) {
        return new PositionalArguments(ImmutableList.of(value), Options.empty(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final Value... values) {
        return new PositionalArguments(ImmutableList.copyOf(values), Options.empty(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofPositional(@Nonnull final Iterable<? extends Value> values) {
        return new PositionalArguments(ImmutableList.copyOf(values), Options.empty(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofNamed(@Nonnull final Map<String, ? extends Value> namedValues) {
        return new NamedArguments(ImmutableMap.copyOf(namedValues), Options.empty(), WindowSpecification.NONE);
    }

    @Nonnull
    static CallSiteArguments ofNamed(@Nonnull final String argumentName, Value argumentValue) {
        return new NamedArguments(ImmutableMap.of(argumentName, argumentValue), Options.empty(), WindowSpecification.NONE);
    }

    /**
     * A typed key for a single option that may be supplied at a function call site, e.g. the {@code ef_search} of
     * {@code row_number() OVER (... OPTIONS ef_search = 100)}.
     * <p>
     * Options are <em>not</em> a global namespace: a function declares the options it understands through
     * {@link CatalogedFunction#getSupportedOptions()}, and the keys it declares are the only authority on an option's
     * name and value type. Two unrelated functions are therefore free to declare an option of the same name with
     * different value types, because an option is only ever resolved against the declared options of the function
     * actually being called.
     * </p>
     * <p>
     * Keys are value-typed on their {@link #getName() name}: two keys are equal iff their names are equal. Names must
     * therefore be unique within one function's declared option set. Instances are meant to be declared once as
     * {@code static final} constants next to the function that accepts them.
     * </p>
     *
     * @param <T> the value type of the option
     */
    final class Option<T> {
        @Nonnull
        private final String name;
        @Nonnull
        private final Class<T> type;
        @Nonnull
        private final Coercer<T> coercer;

        private Option(@Nonnull final String name, @Nonnull final Class<T> type, @Nonnull final Coercer<T> coercer) {
            this.name = name;
            this.type = type;
            this.coercer = coercer;
        }

        /**
         * The name this option is supplied under at a call site.
         * @return the option name
         */
        @Nonnull
        public String getName() {
            return name;
        }

        /**
         * The value type of the option.
         * @return the value type
         */
        @Nonnull
        public Class<T> getType() {
            return type;
        }

        /**
         * Converts a raw value supplied at a call site to this option's value type. A value that already is of the
         * option's type is returned as-is; otherwise the option's type-specific conversion is applied, which accepts
         * the neighbouring representations a front end may produce (e.g. a {@code Long} literal for an
         * {@link #ofInteger} option) as long as the value is exactly representable.
         * <p>
         * The conversion is idempotent, so it is safe to apply on every read as well as once up front.
         * </p>
         * @param rawValue the raw value supplied at the call site
         * @return the value converted to this option's type
         */
        @Nonnull
        public T coerce(@Nullable final Object rawValue) {
            if (rawValue == null) {
                throw nullOptionValue(name);
            }
            if (type.isInstance(rawValue)) {
                return type.cast(rawValue);
            }
            return coercer.coerce(name, rawValue);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof final Option<?> that)) {
                return false;
            }
            return name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return name;
        }

        @Nonnull
        public static Option<Integer> ofInteger(@Nonnull final String name) {
            return new Option<>(name, Integer.class, Option::coerceInteger);
        }

        @Nonnull
        public static Option<Long> ofLong(@Nonnull final String name) {
            return new Option<>(name, Long.class, (optionName, rawValue) -> toLong(optionName, Long.class, rawValue));
        }

        @Nonnull
        public static Option<Double> ofDouble(@Nonnull final String name) {
            return new Option<>(name, Double.class, Option::coerceDouble);
        }

        @Nonnull
        public static Option<Boolean> ofBoolean(@Nonnull final String name) {
            return new Option<>(name, Boolean.class, Option::coerceBoolean);
        }

        @Nonnull
        public static Option<String> ofString(@Nonnull final String name) {
            return new Option<>(name, String.class, Option::coerceString);
        }

        @Nonnull
        public static <E extends Enum<E>> Option<E> ofEnum(@Nonnull final String name,
                                                           @Nonnull final Class<E> enumType) {
            return new Option<>(name, enumType, (optionName, rawValue) -> coerceEnum(optionName, enumType, rawValue));
        }

        /**
         * Creates an option with a bespoke conversion, for value types the typed factories do not cover.
         * @param name the option name
         * @param type the option's value type
         * @param coercer the conversion from a raw call-site value to the option's value type
         * @param <T> the option's value type
         * @return a new option
         */
        @Nonnull
        public static <T> Option<T> of(@Nonnull final String name, @Nonnull final Class<T> type,
                                       @Nonnull final Coercer<T> coercer) {
            return new Option<>(name, type, coercer);
        }

        @Nonnull
        private static Integer coerceInteger(@Nonnull final String optionName, @Nonnull final Object rawValue) {
            final long valueAsLong = toLong(optionName, Integer.class, rawValue);
            if (valueAsLong < Integer.MIN_VALUE || valueAsLong > Integer.MAX_VALUE) {
                throw optionValueOutOfRange(optionName, Integer.class, rawValue);
            }
            return (int)valueAsLong;
        }

        private static long toLong(@Nonnull final String optionName, @Nonnull final Class<?> expectedType,
                                   @Nonnull final Object rawValue) {
            //
            // Only exactly-representable integral values are accepted; a fractional literal is a different option
            // value, not an invitation to round.
            //
            if (rawValue instanceof Byte || rawValue instanceof Short || rawValue instanceof Integer
                    || rawValue instanceof Long) {
                return ((Number)rawValue).longValue();
            }
            throw unexpectedOptionValueType(optionName, expectedType, rawValue);
        }

        @Nonnull
        private static Double coerceDouble(@Nonnull final String optionName, @Nonnull final Object rawValue) {
            if (rawValue instanceof Number) {
                return ((Number)rawValue).doubleValue();
            }
            throw unexpectedOptionValueType(optionName, Double.class, rawValue);
        }

        @Nonnull
        private static Boolean coerceBoolean(@Nonnull final String optionName, @Nonnull final Object rawValue) {
            // a boolean option only accepts an actual boolean; Boolean.parseBoolean() would map any garbage to false
            throw unexpectedOptionValueType(optionName, Boolean.class, rawValue);
        }

        @Nonnull
        private static String coerceString(@Nonnull final String optionName, @Nonnull final Object rawValue) {
            if (rawValue instanceof CharSequence) {
                return rawValue.toString();
            }
            throw unexpectedOptionValueType(optionName, String.class, rawValue);
        }

        @Nonnull
        private static <E extends Enum<E>> E coerceEnum(@Nonnull final String optionName,
                                                        @Nonnull final Class<E> enumType,
                                                        @Nonnull final Object rawValue) {
            if (rawValue instanceof CharSequence) {
                final var rawValueAsString = rawValue.toString();
                for (final E enumConstant : enumType.getEnumConstants()) {
                    if (enumConstant.name().equalsIgnoreCase(rawValueAsString)) {
                        return enumConstant;
                    }
                }
            }
            throw unexpectedOptionValueType(optionName, enumType, rawValue);
        }

        @Nonnull
        private static SemanticException nullOptionValue(@Nonnull final String optionName) {
            final var semanticException = SemanticException.newException(SemanticException.ErrorCode.INCOMPATIBLE_TYPE,
                    "option value must not be null");
            semanticException.addLogInfo(LogMessageKeys.OPTION_NAME, optionName);
            return semanticException;
        }

        @Nonnull
        private static SemanticException unexpectedOptionValueType(@Nonnull final String optionName,
                                                                   @Nonnull final Class<?> expectedType,
                                                                   @Nonnull final Object rawValue) {
            final var semanticException = SemanticException.newException(SemanticException.ErrorCode.INCOMPATIBLE_TYPE,
                    "option value is of an unexpected type");
            semanticException.addLogInfo(LogMessageKeys.OPTION_NAME, optionName,
                    LogMessageKeys.EXPECTED_TYPE, expectedType.getSimpleName(),
                    LogMessageKeys.ACTUAL_TYPE, rawValue.getClass().getSimpleName(),
                    LogMessageKeys.OPTION_VALUE, rawValue);
            return semanticException;
        }

        @Nonnull
        private static SemanticException optionValueOutOfRange(@Nonnull final String optionName,
                                                               @Nonnull final Class<?> expectedType,
                                                               @Nonnull final Object rawValue) {
            final var semanticException = SemanticException.newException(SemanticException.ErrorCode.INCOMPATIBLE_TYPE,
                    "option value is out of range for the option's type");
            semanticException.addLogInfo(LogMessageKeys.OPTION_NAME, optionName,
                    LogMessageKeys.EXPECTED_TYPE, expectedType.getSimpleName(),
                    LogMessageKeys.OPTION_VALUE, rawValue);
            return semanticException;
        }

        /**
         * Converts a raw value supplied at a call site to an option's value type.
         * @param <T> the option's value type
         */
        @FunctionalInterface
        public interface Coercer<T> {
            /**
             * Converts {@code rawValue} to the option's value type.
             * @param optionName the name of the option being converted, for diagnostics
             * @param rawValue the raw value supplied at the call site
             * @return the converted value
             */
            @Nonnull
            T coerce(@Nonnull String optionName, @Nonnull Object rawValue);
        }
    }

    /**
     * The immutable set of options supplied at a function call site. Values go in through an {@link Option} (or, for a
     * front end that only has a name and a literal at parse time, under a name through
     * {@link Builder#putRaw(String, Object)}) and come out through an {@link Option}, so no caller performs an
     * unchecked cast.
     * <p>
     * Options are held by name and resolved against the called function's declared options when the call is
     * encapsulated (see {@link CatalogedFunction#getSupportedOptions()}). Holding them by name rather than by key is
     * what keeps options decentralized: a front end builds a call site's options before the function has even been
     * looked up, so resolving a name to a key at that point would require a global registry of every option that every
     * function might accept.
     * </p>
     */
    final class Options {
        private static final Options EMPTY_OPTIONS = new Options(ImmutableMap.of());

        @Nonnull
        private final Map<String, Object> optionsByName;

        private Options(@Nonnull final Map<String, Object> optionsByName) {
            this.optionsByName = optionsByName;
        }

        /**
         * The empty set of options.
         * @return options with nothing set
         */
        @Nonnull
        public static Options empty() {
            return EMPTY_OPTIONS;
        }

        /**
         * Creates a builder for a new set of options.
         * @return a new builder
         */
        @Nonnull
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Creates a builder pre-populated with these options.
         * @return a new builder
         */
        @Nonnull
        public Builder toBuilder() {
            final var builder = new Builder();
            builder.putAll(optionsByName);
            return builder;
        }

        public boolean isEmpty() {
            return optionsByName.isEmpty();
        }

        public int size() {
            return optionsByName.size();
        }

        /**
         * The names the options are set under. Useful for diagnostics and for validating a call site against the
         * options a function declares.
         * @return the option names
         */
        @Nonnull
        public Set<String> names() {
            return optionsByName.keySet();
        }

        /**
         * Whether {@code option} is set.
         * @param option the option to look for
         * @return {@code true} if the option is set
         */
        public boolean contains(@Nonnull final Option<?> option) {
            return optionsByName.containsKey(option.getName());
        }

        /**
         * Reads an option, converting the stored value to the option's declared type.
         * @param option the option to read
         * @param <T> the option's value type
         * @return the option's value, or {@link Optional#empty()} if the option is not set
         */
        @Nonnull
        public <T> Optional<T> get(@Nonnull final Option<T> option) {
            final var value = optionsByName.get(option.getName());
            return value == null ? Optional.empty() : Optional.of(option.coerce(value));
        }

        /**
         * Reads an option, falling back to {@code defaultValue} when it is not set.
         * @param option the option to read
         * @param defaultValue the value to return when the option is not set
         * @param <T> the option's value type
         * @return the option's value, or {@code defaultValue}
         */
        @Nonnull
        public <T> T getOrDefault(@Nonnull final Option<T> option, @Nonnull final T defaultValue) {
            return get(option).orElse(defaultValue);
        }

        /**
         * Resolves every option against the options a function declares, converting each value to its declared type
         * and rejecting any option the function does not understand. Package-private because validation is driven by
         * {@link CatalogedFunction}, which knows both the declared options and the function name to report.
         *
         * @param supportedOptionsByName the called function's declared options, by name
         * @param functionName the called function's name, for diagnostics
         * @return equivalent options whose values are of their declared types
         */
        @Nonnull
        Options resolve(@Nonnull final Map<String, Option<?>> supportedOptionsByName,
                        @Nonnull final String functionName) {
            if (optionsByName.isEmpty()) {
                return this;
            }
            final var resolvedOptionsBuilder =
                    ImmutableMap.<String, Object>builderWithExpectedSize(optionsByName.size());
            for (final var option : optionsByName.entrySet()) {
                final var supportedOption = supportedOptionsByName.get(option.getKey());
                if (supportedOption == null) {
                    throw unsupportedOption(functionName, option.getKey());
                }
                resolvedOptionsBuilder.put(option.getKey(), supportedOption.coerce(option.getValue()));
            }
            return new Options(resolvedOptionsBuilder.build());
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof final Options that)) {
                return false;
            }
            return optionsByName.equals(that.optionsByName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(optionsByName);
        }

        @Override
        public String toString() {
            // sorted by name, so that the rendering does not depend on the order the options were supplied in
            return new TreeMap<>(optionsByName).entrySet().stream()
                    .map(option -> option.getKey() + ": " + option.getValue())
                    .collect(Collectors.joining(", ", "[", "]"));
        }

        @Nonnull
        private static SemanticException unsupportedOption(@Nonnull final String functionName,
                                                           @Nonnull final String optionName) {
            final var semanticException = SemanticException.newException(
                    SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                    "unsupported option for function");
            semanticException.addLogInfo(LogMessageKeys.FUNCTION, functionName,
                    LogMessageKeys.OPTION_NAME, optionName);
            return semanticException;
        }

        /**
         * Builder for {@link Options}.
         */
        public static final class Builder {
            @Nonnull
            private final Map<String, Object> optionsByName = new LinkedHashMap<>();

            private Builder() {
            }

            /**
             * Sets an option to a value of the option's declared type.
             * @param option the option to set
             * @param value the option's value
             * @param <T> the option's value type
             * @return this builder
             */
            @Nonnull
            public <T> Builder put(@Nonnull final Option<T> option, @Nonnull final T value) {
                return putInternal(option.getName(), option.coerce(value));
            }

            /**
             * Sets an option by name to a value that has not been type-checked yet. This is for front ends that only
             * have an option's name and its literal value at parse time; the value is type-checked against the called
             * function's declared options when the call is encapsulated.
             * @param name the option name
             * @param rawValue the raw option value
             * @return this builder
             */
            @Nonnull
            public Builder putRaw(@Nonnull final String name, @Nullable final Object rawValue) {
                if (rawValue == null) {
                    throw Option.nullOptionValue(name);
                }
                return putInternal(name, rawValue);
            }

            /**
             * Builds the options.
             * @return the options
             */
            @Nonnull
            public Options build() {
                return optionsByName.isEmpty() ? empty() : new Options(ImmutableMap.copyOf(optionsByName));
            }

            private void putAll(@Nonnull final Map<String, Object> otherOptionsByName) {
                optionsByName.putAll(otherOptionsByName);
            }

            @Nonnull
            private Builder putInternal(@Nonnull final String name, @Nonnull final Object value) {
                if (optionsByName.putIfAbsent(name, value) != null) {
                    throw duplicateOption(name);
                }
                return this;
            }

            @Nonnull
            private static SemanticException duplicateOption(@Nonnull final String optionName) {
                final var semanticException = SemanticException.newException(
                        SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                        "option specified more than once");
                semanticException.addLogInfo(LogMessageKeys.OPTION_NAME, optionName);
                return semanticException;
            }
        }
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
                               @Nonnull Options options,
                               @Nonnull WindowSpecification windowSpecification) implements CallSiteArguments {
        @Nonnull
        @Override
        public Iterable<Value> getArguments() {
            return values;
        }

        @Nonnull
        @Override
        public Options getOptions() {
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
        public CallSiteArguments withOptions(@Nonnull final Options newOptions) {
            return new PositionalArguments(values, newOptions, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withWindowSpecification(@Nonnull final WindowSpecification newWindowSpecification) {
            return new PositionalArguments(values, options, newWindowSpecification);
        }
    }

    record NamedArguments(@Nonnull Map<String, Value> namedArguments,
                          @Nonnull Options options,
                          @Nonnull WindowSpecification windowSpecification) implements CallSiteArguments {
        @Nonnull
        @Override
        public List<Value> getArguments() {
            return List.copyOf(namedArguments.values());
        }

        @Nonnull
        @Override
        public Options getOptions() {
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
        public CallSiteArguments withOptions(@Nonnull final Options newOptions) {
            return new NamedArguments(namedArguments, newOptions, windowSpecification);
        }

        @Nonnull
        @Override
        public CallSiteArguments withWindowSpecification(@Nonnull final WindowSpecification newWindowSpecification) {
            return new NamedArguments(namedArguments, options, newWindowSpecification);
        }
    }
}
