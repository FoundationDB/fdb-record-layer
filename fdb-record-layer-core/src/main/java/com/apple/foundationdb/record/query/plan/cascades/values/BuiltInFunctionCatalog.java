/*
 * FunctionCatalog.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.util.ServiceLoaderProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A catalog of functions that provides {@link BuiltInFunction}s.
 */
public class BuiltInFunctionCatalog {
    private static final Logger logger = LoggerFactory.getLogger(BuiltInFunctionCatalog.class);

    private static final Supplier<Map<FunctionKey, BuiltInFunction<? extends Typed>>> catalogSupplier =
            Suppliers.memoize(BuiltInFunctionCatalog::loadFunctions);

    private static final Supplier<Map<Class<BuiltInFunction<? extends Typed>>, BuiltInFunction<? extends Typed>>> functionsByClassSupplier =
            Suppliers.memoize(BuiltInFunctionCatalog::computeFunctionsByClass);
    
    private BuiltInFunctionCatalog() {
        // prevent instantiation
    }

    private static Map<FunctionKey, BuiltInFunction<? extends Typed>> getFunctionCatalog() {
        return catalogSupplier.get();
    }

    @SuppressWarnings({"unchecked", "rawtypes", "java:S3457"})
    private static Map<FunctionKey, BuiltInFunction<? extends Typed>> loadFunctions() {
        final ImmutableMap.Builder<FunctionKey, BuiltInFunction<? extends Typed>> catalogBuilder = ImmutableMap.builder();
        final Iterable<BuiltInFunction> loader
                = ServiceLoaderProvider.load(BuiltInFunction.class);

        loader.forEach(builtInFunction -> {
            catalogBuilder.put(FunctionKey.entry(builtInFunction.getFunctionName(), builtInFunction.getParameterTypes().size(),
                    builtInFunction.getDefaultValuesCount(), builtInFunction.hasVariadicSuffix()), builtInFunction);
            if (logger.isTraceEnabled()) {
                logger.trace("loaded function " + builtInFunction);
            }
        });
        
        return catalogBuilder.build();
    }

    @Nonnull
    @SuppressWarnings("java:S1066")
    public static Optional<BuiltInFunction<? extends Typed>> resolve(@Nonnull final String functionName, int numberOfArguments) {
        BuiltInFunction<? extends Typed> builtInFunction = getFunctionCatalog().get(FunctionKey.invocation(functionName, numberOfArguments));
        return Optional.ofNullable(builtInFunction);
    }

    @Nonnull
    private static Map<Class<BuiltInFunction<? extends Typed>>, BuiltInFunction<? extends Typed>> getFunctionsByClass() {
        return functionsByClassSupplier.get();
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    private static Map<Class<BuiltInFunction<? extends Typed>>, BuiltInFunction<? extends Typed>> computeFunctionsByClass() {
        final var functionCatalog = getFunctionCatalog();
        final var resultBuilder = ImmutableMap.<Class<BuiltInFunction<? extends Typed>>, BuiltInFunction<? extends Typed>>builder();
        for (final var singleton : functionCatalog.values()) {
            resultBuilder.put((Class<BuiltInFunction<? extends Typed>>)singleton.getClass(), singleton);
        }
        return resultBuilder.build();
    }

    /**
     * Get the loaded representative function singleton for a function class.
     * @param clazz the class to look up
     * @return an {@link Optional} containing the function singleton or {@code Optional.empty()}
     */
    @Nonnull
    public static Optional<BuiltInFunction<? extends Typed>> getFunctionSingleton(@Nonnull final Class<? extends BuiltInFunction<? extends Typed>> clazz) {
        return Optional.ofNullable(getFunctionsByClass().get(clazz));
    }

    /**
     * Internal key class used for function catalog lookups, supporting matching between function invocations
     * and function definitions based on name and argument count.
     *
     * <p>This class serves a dual purpose:
     * <ul>
     *   <li><b>Catalog Entry Key:</b> Represents a function definition in the catalog, capturing the valid
     *       range of arguments that the function accepts based on its signature (required parameters,
     *       optional parameters with defaults, and variadic parameters).</li>
     *   <li><b>Invocation Key:</b> Represents a function call with a specific number of arguments, used
     *       to look up matching function definitions in the catalog.</li>
     * </ul>
     *
     * <p><b>The {@code requiredArgParamRange} Field:</b>
     * <br>
     * The {@code requiredArgParamRange} captures the valid range of arguments that must be provided by
     * the user when invoking the function. This range is determined by the function's signature:
     *
     * <ul>
     *   <li><b>Functions with optional parameters:</b> If a function is defined as
     *       {@code f1(col1 int, col2 int default = 32)}, then the user must provide between 1 and 2
     *       arguments (inclusive). The range would be {@code [1, 2]}.</li>
     *
     *   <li><b>Variadic functions:</b> If a function is defined as {@code f1(col1 int, col2 int, col3 int...)},
     *       then the user must provide at least 2 arguments with no upper limit. The range would be
     *       {@code [2, ∞)}.</li>
     *
     *   <li><b>Fixed-parameter functions:</b> If a function is defined as {@code f1(col1 int, col2 int)},
     *       then the user must provide exactly 2 arguments. The range would be {@code [2, 2]}.</li>
     *
     *   <li><b>Invocation keys:</b> When representing a function call with a specific argument count
     *       (e.g., 3 arguments), the range is a singleton {@code [3, 3]}.</li>
     * </ul>
     *
     * <p><b>Equality Semantics:</b>
     * <br>
     * The {@link #equals(Object)} method implements special matching logic to support function resolution:
     * <ul>
     *   <li>When comparing two keys of the same type (both invocations or both entries), equality requires
     *       matching function names and identical ranges.</li>
     *   <li>When comparing an invocation key to an entry key, the invocation matches if its argument count
     *       falls within the entry's acceptable range. This enables function overload resolution.</li>
     * </ul>
     *
     * <p><b>Example:</b>
     * <pre>{@code
     * // Entry for f(x int, y int default = 10)
     * FunctionKey.entry("f", 2, 1, false)  // Range: [1, 2]
     *
     * // Invocation with 1 argument
     * FunctionKey.invocation("f", 1)       // Range: [1, 1] - matches entry
     *
     * // Invocation with 2 arguments
     * FunctionKey.invocation("f", 2)       // Range: [2, 2] - matches entry
     *
     * // Entry for g(x int, y int, z int...)
     * FunctionKey.entry("g", 3, 0, true)   // Range: [3, ∞)
     *
     * // Invocation with 5 arguments
     * FunctionKey.invocation("g", 5)       // Range: [5, 5] - matches entry
     * }</pre>
     */
    @VisibleForTesting
    public static final class FunctionKey {
        @Nonnull
        private final String functionName;

        @Nonnull
        private final Range<Integer> requiredArgParamRange;

        private final boolean isInvocation;

        private FunctionKey(@Nonnull final String functionName, @Nonnull final Range<Integer> requiredArgParamRange,
                            final boolean isInvocation) {
            this.functionName = functionName;
            this.requiredArgParamRange = requiredArgParamRange;
            this.isInvocation = isInvocation;
        }

        @Nonnull
        public String getFunctionName() {
            return functionName;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FunctionKey)) {
                return false;
            }
            final FunctionKey that = (FunctionKey)o;

            if (!getFunctionName().equals(that.getFunctionName())) {
                return false;
            }

            if (this.isInvocation == that.isInvocation) {
                return requiredArgParamRange.equals(that.requiredArgParamRange);
            }

            if (this.isInvocation) {
                return that.requiredArgParamRange.encloses(requiredArgParamRange);
            } else {
                return requiredArgParamRange.encloses(that.requiredArgParamRange);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(getFunctionName());
        }

        @Nonnull
        public static FunctionKey invocation(@Nonnull final String functionName, final int numArguments) {
            Verify.verify(numArguments >= 0);
            final Range<Integer> argRange = Range.singleton(numArguments);
            return new FunctionKey(functionName, argRange, true);
        }

        @Nonnull
        public static FunctionKey entry(@Nonnull final String functionName, final int numParameters,
                                        int numParametersWithDefaultValues, final boolean isVariadic) {
            Verify.verify(numParametersWithDefaultValues >= 0);
            Verify.verify(numParameters >= numParametersWithDefaultValues);
            final var minRequiredParams = numParameters - numParametersWithDefaultValues;
            final Range<Integer> paramRange;
            if (isVariadic) {
                paramRange = Range.atLeast(minRequiredParams);
            } else {
                paramRange = Range.closed(minRequiredParams, numParameters);
            }
            return new FunctionKey(functionName, paramRange, false);
        }

        @Override
        public String toString() {
            if (isInvocation) {
                Verify.verify(requiredArgParamRange.lowerEndpoint().equals(requiredArgParamRange.upperEndpoint()));
                final int argCount = requiredArgParamRange.lowerEndpoint();
                return functionName + "(" + IntStream.range(1, argCount + 1)
                        .mapToObj(i -> "arg" + i)
                        .collect(Collectors.joining(", ")) + ")";
            } else {
                final int lowerBound = requiredArgParamRange.lowerEndpoint();
                StringBuilder sb = new StringBuilder();
                sb.append(functionName).append("(").append(IntStream.range(1, lowerBound + 1)
                        .mapToObj(i -> "param" + i)
                        .collect(Collectors.joining(", ")));
                if (requiredArgParamRange.hasUpperBound()) {
                    final var upperBound = requiredArgParamRange.upperEndpoint();
                    if (upperBound.equals(lowerBound)) {
                        return sb.append(")").toString();
                    }
                    if (lowerBound > 0) {
                        sb.append(", ");
                    }
                    return sb.append(IntStream.range(lowerBound + 1, upperBound + 1)
                            .mapToObj(i -> "param" + i + " (with default)")
                            .collect(Collectors.joining(", "))).append(")").toString();
                }
                // variadic.
                if (lowerBound > 0) {
                    sb.append(", ");
                }
                return sb.append("...)").toString();
            }
        }
    }
}
