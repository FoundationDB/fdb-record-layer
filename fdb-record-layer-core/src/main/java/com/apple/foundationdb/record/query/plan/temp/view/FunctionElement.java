/*
 * FunctionElement.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.view;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An element that represents the value of a named function on its arguments, which are other {@link Element}s.
 */
@API(API.Status.EXPERIMENTAL)
public class FunctionElement implements Element {
    @Nonnull
    private final String name;
    @Nonnull
    private final List<Element> arguments;

    public FunctionElement(@Nonnull String name, @Nonnull List<Element> arguments) {
        this.name = name;
        this.arguments = arguments;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Override
    public Set<Source> getAncestralSources() {
        final ImmutableSet.Builder<Source> argumentSources = ImmutableSet.builder();
        arguments.forEach(argument -> argumentSources.addAll(argument.getAncestralSources()));
        return argumentSources.build();
    }

    @Nonnull
    @Override
    public Optional<ComparisonRange> matchWith(@Nonnull ComparisonRange existingComparisons, @Nonnull ElementPredicate predicate) {
        if (equals(predicate.getElement())) {
            return existingComparisons.tryToAdd(predicate.getComparison());
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<ViewExpressionComparisons> matchSourcesWith(@Nonnull ViewExpressionComparisons viewExpressionComparisons,
                                                                @Nonnull Element element) {
        if (element instanceof FunctionElement) {
            final FunctionElement functionElement = (FunctionElement)element;
            if (functionElement.getName().equals(name)) {
                Optional<ViewExpressionComparisons> matchedSoFar = Optional.of(viewExpressionComparisons);
                final Iterator<Element> thisArguments = arguments.iterator();
                final Iterator<Element> thatArguments = functionElement.arguments.iterator();
                while (thisArguments.hasNext() && thatArguments.hasNext() && matchedSoFar.isPresent()) {
                    matchedSoFar = thisArguments.next().matchSourcesWith(matchedSoFar.get(), thatArguments.next());
                }

                if (!thisArguments.hasNext() && !thatArguments.hasNext()) {
                    return matchedSoFar;
                }
            }
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Element withSourceMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource) {
        return new FunctionElement(name, arguments.stream()
                .map(arg -> arg.withSourceMappedInto(originalSource, duplicateSource))
                .collect(Collectors.toList()));
    }

    @Nullable
    @Override
    public Object eval(@Nonnull SourceEntry sourceEntry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int planHash() {
        return name.hashCode() + PlanHashable.planHash(arguments);
    }
}
