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

import com.apple.foundationdb.record.query.combinatorics.PartialOrder;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * This property establishes a partial order over the expressions contained in a subgraph.
 */
public class ReferencesAndDependenciesProperty implements ExpressionProperty<PartialOrder<ExpressionRef<? extends RelationalExpression>>> {

    @Nonnull
    @Override
    public PartialOrder<ExpressionRef<? extends RelationalExpression>> evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<PartialOrder<ExpressionRef<? extends RelationalExpression>>> childResults) {
        return mergePartialOrders(childResults);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    @Override
    public PartialOrder<ExpressionRef<? extends RelationalExpression>> evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<PartialOrder<ExpressionRef<? extends RelationalExpression>>> memberResults) {
        final var membersPartialOrder = mergePartialOrders(memberResults);

        final var membersSet = membersPartialOrder.getSet();
        final var membersDependencyMap = membersPartialOrder.getDependencyMap();

        final var setBuilder = ImmutableSet.<ExpressionRef<? extends RelationalExpression>>builder();
        final var dependencyMapBuilder = ImmutableSetMultimap.<ExpressionRef<? extends RelationalExpression>, ExpressionRef<? extends RelationalExpression>>builder();

        setBuilder.addAll(membersSet);
        setBuilder.add(ref);
        dependencyMapBuilder.putAll(membersDependencyMap.entries());

        for (final var member : ref.getMembers()) {
            for (final var quantifier : member.getQuantifiers()) {
                dependencyMapBuilder.put(ref, quantifier.getRangesOver());
            }
        }

        return PartialOrder.of(setBuilder.build(), dependencyMapBuilder.build());
    }

    @SuppressWarnings("UnstableApiUsage")
    private PartialOrder<ExpressionRef<? extends RelationalExpression>> mergePartialOrders(@Nonnull Iterable<PartialOrder<ExpressionRef<? extends RelationalExpression>>> partialOrders) {
        final var setBuilder = ImmutableSet.<ExpressionRef<? extends RelationalExpression>>builder();
        final var dependencyMapBuilder = ImmutableSetMultimap.<ExpressionRef<? extends RelationalExpression>, ExpressionRef<? extends RelationalExpression>>builder();

        for (final var partialOrder : partialOrders) {
            setBuilder.addAll(partialOrder.getSet());
            dependencyMapBuilder.putAll(partialOrder.getDependencyMap().entries());
        }

        return PartialOrder.of(setBuilder.build(), dependencyMapBuilder.build());
    }

    @Nonnull
    public static PartialOrder<ExpressionRef<? extends RelationalExpression>> evaluate(@Nonnull ExpressionRef<? extends RelationalExpression> ref) {
        @Nullable final var nullableResult =
                ref.acceptPropertyVisitor(new ReferencesAndDependenciesProperty());
        return Objects.requireNonNull(nullableResult);
    }

    @Nonnull
    public static PartialOrder<ExpressionRef<? extends RelationalExpression>> evaluate(@Nonnull Iterable<? extends ExpressionRef<? extends RelationalExpression>> refs) {
        final var property = new ReferencesAndDependenciesProperty();
        final var refResults = Streams.stream(refs)
                .map(ref -> Objects.requireNonNull(ref.acceptPropertyVisitor(property)))
                .collect(ImmutableList.toImmutableList());

        return property.mergePartialOrders(refResults);
    }
}
