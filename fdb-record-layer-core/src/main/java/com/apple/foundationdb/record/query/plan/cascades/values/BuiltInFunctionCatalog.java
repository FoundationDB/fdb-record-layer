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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

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
            catalogBuilder.put(new FunctionKey(builtInFunction.getFunctionName(), builtInFunction.getParameterTypes().size(), builtInFunction.hasVariadicSuffix()), builtInFunction);
            if (logger.isDebugEnabled()) {
                logger.debug("loaded function " + builtInFunction);
            }
        });
        
        return catalogBuilder.build();
    }

    @Nonnull
    @SuppressWarnings("java:S1066")
    public static Optional<BuiltInFunction<? extends Typed>> resolve(@Nonnull final String functionName, int numberOfArguments) {
        BuiltInFunction<? extends Typed> builtInFunction = getFunctionCatalog().get(new FunctionKey(functionName, numberOfArguments, false));
        if (builtInFunction == null) {
            // try again as a variadic function
            builtInFunction = getFunctionCatalog().get(new FunctionKey(functionName, numberOfArguments, true));
            if (builtInFunction != null) {
                // we should have at least as many arguments as there are declared fixed parameters in the function
                if (builtInFunction.getParameterTypes().size() > numberOfArguments) {
                    return Optional.empty();
                }
            }
        }

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

    private static class FunctionKey {
        @Nonnull
        final String functionName;

        final int numParameters;

        final boolean isVariadic;

        public FunctionKey(@Nonnull final String functionName, final int numParameters, final boolean isVariadic) {
            this.functionName = functionName;
            this.numParameters = numParameters;
            this.isVariadic = isVariadic;
        }

        @Nonnull
        public String getFunctionName() {
            return functionName;
        }

        public int getNumParameters() {
            return numParameters;
        }

        public boolean isVariadic() {
            return isVariadic;
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

            if (isVariadic()) {
                return that.isVariadic() && getFunctionName().equals(that.getFunctionName());
            } else {
                return !that.isVariadic() && getNumParameters() == that.getNumParameters() && getFunctionName().equals(that.getFunctionName());
            }
        }

        @Override
        public int hashCode() {
            if (isVariadic()) {
                return Objects.hash(getFunctionName());
            } else {
                return Objects.hash(getFunctionName(), getNumParameters());
            }
        }
    }
}
