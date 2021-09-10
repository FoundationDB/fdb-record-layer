/*
 * FunctionCatalog.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.norse;

import com.apple.foundationdb.record.query.predicates.Typed;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Supplier;

public class FunctionCatalog {
    private static final Logger logger = LoggerFactory.getLogger(FunctionCatalog.class);

    private FunctionCatalog() {
        // prevent instantiation
    }

    private static final Supplier<ImmutableMap<FunctionKey, BuiltInFunction<? extends Typed>>> catalogSupplier =
            Suppliers.memoize(FunctionCatalog::loadFunctions);

    private static ImmutableMap<FunctionKey, BuiltInFunction<? extends Typed>> getFunctionCatalog() {
        return catalogSupplier.get();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ImmutableMap<FunctionKey, BuiltInFunction<? extends Typed>> loadFunctions() {
        final ImmutableMap.Builder<FunctionKey, BuiltInFunction<? extends Typed>> catalogBuilder = ImmutableMap.builder();
        final ServiceLoader<BuiltInFunction> loader
                = ServiceLoader.load(BuiltInFunction.class);

        loader.forEach(builtInFunction -> {
            catalogBuilder.put(new FunctionKey(builtInFunction.getFunctionName(), builtInFunction.getParameterTypes().size()), builtInFunction);
            logger.info("loaded function " + builtInFunction);
        });

        return catalogBuilder.build();
    }

    public static Optional<BuiltInFunction<? extends Typed>> resolveFunction(@Nonnull final String functionName, int numberOfParameters) {
        final BuiltInFunction<? extends Typed> builtInFunction = getFunctionCatalog().get(new FunctionKey(functionName, numberOfParameters));
        return Optional.ofNullable(builtInFunction);
    }

    private static class FunctionKey {
        @Nonnull
        final String functionName;

        final int numParameters;

        public FunctionKey(@Nonnull final String functionName, final int numParameters) {
            this.functionName = functionName;
            this.numParameters = numParameters;
        }

        @Nonnull
        public String getFunctionName() {
            return functionName;
        }

        public int getNumParameters() {
            return numParameters;
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
            return getNumParameters() == that.getNumParameters() && getFunctionName().equals(that.getFunctionName());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getFunctionName(), getNumParameters());
        }
    }
}
