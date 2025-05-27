/*
 * ReferencesAndDependenciesProperty.java
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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.record.query.combinatorics.PartiallyOrderedSet;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * This property establishes a partial order over the expressions contained in a subgraph.
 */
public class ReferencesAndDependenciesProperty implements ExpressionProperty<PartiallyOrderedSet<Reference>> {
    private static final ReferencesAndDependenciesProperty REFERENCES_AND_DEPENDENCIES =
            new ReferencesAndDependenciesProperty();

    private ReferencesAndDependenciesProperty() {
        // prevent outside instantiation
    }

    @Nonnull
    @Override
    public ReferencesAndDependenciesVisitor createVisitor() {
        return new ReferencesAndDependenciesVisitor();
    }

    @Nonnull
    public PartiallyOrderedSet<Reference> evaluate(@Nonnull Iterable<? extends Reference> references) {
        final var refResults =
                Streams.stream(references)
                        .map(this::evaluate)
                        .collect(ImmutableList.toImmutableList());

        return mergePartialOrders(refResults);
    }

    @Nonnull
    public PartiallyOrderedSet<Reference> evaluate(@Nonnull Reference reference) {
        return Objects.requireNonNull(reference.acceptVisitor(createVisitor()));
    }

    @Nonnull
    public PartiallyOrderedSet<Reference> evaluate(@Nonnull RelationalExpression expression) {
        return Objects.requireNonNull(expression.acceptVisitor(createVisitor()));
    }

    @Nonnull
    public static ReferencesAndDependenciesProperty referencesAndDependencies() {
        return REFERENCES_AND_DEPENDENCIES;
    }

    @Nonnull
    private static PartiallyOrderedSet<Reference> mergePartialOrders(@Nonnull final Iterable<PartiallyOrderedSet<Reference>> partialOrders) {
        final var setBuilder = ImmutableSet.<Reference>builder();
        final var dependencyMapBuilder = ImmutableSetMultimap.<Reference, Reference>builder();

        for (final var partialOrder : partialOrders) {
            setBuilder.addAll(partialOrder.getSet());
            dependencyMapBuilder.putAll(partialOrder.getDependencyMap().entries());
        }

        return PartiallyOrderedSet.of(setBuilder.build(), dependencyMapBuilder.build());
    }

    public static class ReferencesAndDependenciesVisitor implements SimpleExpressionVisitor<PartiallyOrderedSet<Reference>> {
        @Nonnull
        @Override
        public PartiallyOrderedSet<Reference> evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<PartiallyOrderedSet<Reference>> childResults) {
            return mergePartialOrders(childResults);
        }

        @Nonnull
        @Override
        public PartiallyOrderedSet<Reference> evaluateAtRef(@Nonnull Reference ref, @Nonnull List<PartiallyOrderedSet<Reference>> memberResults) {
            final var membersPartialOrder = mergePartialOrders(memberResults);

            final var membersSet = membersPartialOrder.getSet();
            final var membersDependencyMap = membersPartialOrder.getDependencyMap();

            final var setBuilder = ImmutableSet.<Reference>builder();
            final var dependencyMapBuilder = ImmutableSetMultimap.<Reference, Reference>builder();

            setBuilder.addAll(membersSet);
            setBuilder.add(ref);
            dependencyMapBuilder.putAll(membersDependencyMap.entries());

            for (final var member : ref.getAllMemberExpressions()) {
                for (final var quantifier : member.getQuantifiers()) {
                    dependencyMapBuilder.put(ref, quantifier.getRangesOver());
                }
            }

            return PartiallyOrderedSet.of(setBuilder.build(), dependencyMapBuilder.build());
        }
    }
}
