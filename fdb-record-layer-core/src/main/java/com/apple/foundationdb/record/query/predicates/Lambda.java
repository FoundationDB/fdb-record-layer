/*
 * Lambda.java
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.record.query.norse.DelayedEncapsulationFunction;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Lambda implements Atom {
    @Nonnull
    private final List<Optional<String>> parameterNameOptionals;
    @Nonnull
    private final DelayedEncapsulationFunction<GraphExpansion> delayedEncapsulationFunction;

    public Lambda(@Nonnull final List<Optional<String>> parameterNameOptionals,
                  @Nonnull final DelayedEncapsulationFunction<GraphExpansion> delayedEncapsulationFunction) {
        this.parameterNameOptionals = ImmutableList.copyOf(parameterNameOptionals);
        this.delayedEncapsulationFunction = delayedEncapsulationFunction;
    }

    @Nonnull
    public List<Optional<String>> getParameterNameOptionals() {
        return parameterNameOptionals;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return new Type.Function();
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        throw new IllegalStateException("a lambda is unified and should not exist in a finished graph");
    }

    @Nonnull
    public GraphExpansion encapsulate(@Nonnull final Set<CorrelationIdentifier> visibleAliases,
                                      @Nonnull final Map<String, Value> boundIdentifiers) {
        return delayedEncapsulationFunction.encapsulate(visibleAliases, boundIdentifiers);
    }

    @Nonnull
    public GraphExpansion unifyBody(@Nonnull List<? extends Value> argumentValues) {
        final Map<String, Value> boundIdentifiers;
        if (parameterNameOptionals.isEmpty() && !argumentValues.isEmpty()) {
            // implied lambda -- bind "_"/"_1", etc.

            if (argumentValues.size() == 1) {
                boundIdentifiers = ImmutableMap.of("_", argumentValues.get(0));
            } else {
                final ImmutableMap.Builder<String, Value> boundIdentifiersBuilder = ImmutableMap.builder();
                for (int i = 0; i < argumentValues.size(); i ++) {
                    boundIdentifiersBuilder.put("_" + (i + 1), argumentValues.get(i));
                }
                boundIdentifiers = boundIdentifiersBuilder.build();
            }
        } else {
            if (argumentValues.size() != parameterNameOptionals.size()) {
                throw new IllegalArgumentException("number of provided parameters for lambda does not match number of elements in tuple");
            }
            final ImmutableMap.Builder<String, Value> boundIdentifiersBuilder = ImmutableMap.builder();
            for (int i = 0; i < argumentValues.size(); i ++) {
                final Optional<String> parameterNameOptional = parameterNameOptionals.get(i);
                if (parameterNameOptional.isPresent()) {
                    boundIdentifiersBuilder.put(parameterNameOptional.get(), argumentValues.get(i));
                }
            }
            boundIdentifiers = boundIdentifiersBuilder.build();
        }

        return encapsulate(ImmutableSet.of(), boundIdentifiers);
    }

    @Override
    public String toString() {
        return getResultType().toString();
    }
}
