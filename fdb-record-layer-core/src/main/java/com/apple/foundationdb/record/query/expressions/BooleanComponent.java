/*
 * BooleanComponent.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A boolean component is a component representing an operation in the boolean algebra, e.g. and, or, not.
 * This class is more of a tag interface from that perspective.
 *
 * Within the {@link QueryComponent} hierarchy, the query components are structured into islands of functionally
 * evaluated boolean components as well as structural components that e.g. span a new level of nesting, represent
 * something that itself can evaluated to to a boolean (e.g. a comparison). This class contains helpers for navigation
 * within and for the collection of useful information from an arbitrary query component tree.
 */
public interface BooleanComponent extends QueryComponent {
    /**
     * Starting from this boolean component find all sub components that itself are not considered {@link BooleanComponent}s.
     * @return a stream of non-boolean sub components.
     */
    @Nonnull
    default Stream<QueryComponent> nonBooleanSubComponents() {
        if (this instanceof ComponentWithChildren) {
            final ComponentWithChildren componentWithChildren = (ComponentWithChildren)this;

            return componentWithChildren.getChildren()
                    .stream()
                    .flatMap(child -> {
                        if (child instanceof BooleanComponent) {
                            return ((BooleanComponent)child).nonBooleanSubComponents();
                        }
                        return Stream.of(child);
                    });
        } else if (this instanceof ComponentWithSingleChild) {
            final ComponentWithSingleChild componentWithSingleChild = (ComponentWithSingleChild)this;
            final QueryComponent child = componentWithSingleChild.getChild();

            if (child instanceof BooleanComponent) {
                return ((BooleanComponent)child).nonBooleanSubComponents();
            }
            return Stream.of(child);
        }

        return Stream.empty();
    }

    /**
     * Starting from an arbitrary query component collect all {@link ComponentWithComparison}s within the sub tree
     * spanned by the {@code queryComponent} handed in and group each of these components with all comparisons
     * referring to them.
     * @param queryComponent the {@link QueryComponent} as the root of the search
     * @return a stream of pairs of {@link ComponentWithComparison} and associated {@link Comparisons.Comparison}s
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    static Stream<Pair<String, List<Comparisons.ComparisonWithParameter>>> groupedComparisons(@Nonnull final QueryComponent queryComponent) {
        final Stream<BooleanComponent> booleanComponents = BooleanComponent.topBooleanComponents(queryComponent);
        return booleanComponents
                .flatMap(booleanComponent -> {
                    // find all the comparisons within this boolean component
                    final List<QueryComponent> nonBooleanSubComponents =
                            booleanComponent.nonBooleanSubComponents().collect(ImmutableList.toImmutableList());

                    final Stream<Pair<String, List<Comparisons.ComparisonWithParameter>>> nestedGroupedComparisons =
                            nestedComponents(nonBooleanSubComponents.stream())
                            .flatMap(BooleanComponent::groupedComparisons);

                    final Stream<ComponentWithComparison> comparisons = comparisons(nonBooleanSubComponents.stream());
                    final Map<String, ? extends List<Comparisons.ComparisonWithParameter>> partitionedComparisonsMap =
                            groupedComparisonsMap(comparisons);

                    //
                    // Each entry in that map represents a comparison that may have a number of occurrences:
                    //
                    // x < :p1 and x > :p2
                    // would manifest itself in the map as such:
                    // x -> { x < :p1, x > :p2 }
                    //
                    // Note that each entry is considered independent to each other entry.
                    //
                    final Stream<Pair<String, List<Comparisons.ComparisonWithParameter>>> dependentComparisons =
                            partitionedComparisonsMap.entrySet()
                                    .stream()
                                    .map(entry -> Pair.of(entry.getKey(), entry.getValue()));

                    return Streams.concat(nestedGroupedComparisons, dependentComparisons);
                });
    }

    @Nonnull
    static Map<String, ImmutableList<Comparisons.ComparisonWithParameter>> groupedComparisonsMap(final Stream<ComponentWithComparison> comparisons) {
        return comparisons
                .filter(componentWithComparison -> componentWithComparison.getComparison() instanceof Comparisons.ComparisonWithParameter)
                .collect(Collectors.groupingBy(ComponentWithComparison::getName,
                        Maps::newLinkedHashMap,
                        Collectors.mapping(componentWithComparison -> (Comparisons.ComparisonWithParameter)componentWithComparison.getComparison(),
                                ImmutableList.toImmutableList())));
    }
    
    @Nonnull
    static Stream<ComponentWithComparison> comparisons(@Nonnull final Stream<QueryComponent> nonBooleanSubComponents) {
        return nonBooleanSubComponents
                .filter(queryComponent -> queryComponent instanceof ComponentWithComparison)
                .map(queryComponent -> (ComponentWithComparison)queryComponent);
    }

    @Nonnull
    static Stream<QueryComponent> nestedComponents(@Nonnull final Stream<QueryComponent> nonBooleanSubComponents) {
        return nonBooleanSubComponents
                .filter(queryComponent -> !(queryComponent instanceof ComponentWithComparison));
    }

    @Nonnull
    static Stream<BooleanComponent> topBooleanComponents(@Nonnull final QueryComponent queryComponent) {
        if (queryComponent instanceof BooleanComponent) {
            return Stream.of((BooleanComponent)queryComponent);
        }

        if (queryComponent instanceof ComponentWithChildren) {
            final ComponentWithChildren componentWithChildren = (ComponentWithChildren)queryComponent;
            return componentWithChildren.getChildren()
                    .stream()
                    .flatMap(BooleanComponent::topBooleanComponents);
        } else if (queryComponent instanceof ComponentWithSingleChild) {
            final ComponentWithSingleChild componentWithSingleChild = (ComponentWithSingleChild)queryComponent;
            final QueryComponent child = componentWithSingleChild.getChild();
            return topBooleanComponents(child);
        }

        return Stream.empty();
    }
}
