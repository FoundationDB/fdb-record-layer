/*
 * RequestedOrderingConstraint.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

/**
 * A constraint requesting a set of sort orderings.
 */
public class RequestedOrderingConstraint implements PlannerConstraint<Set<RequestedOrdering>> {
    public static final PlannerConstraint<Set<RequestedOrdering>> REQUESTED_ORDERING = new RequestedOrderingConstraint();

    @Nonnull
    @Override
    public Optional<Set<RequestedOrdering>> combine(@Nonnull final Set<RequestedOrdering> currentConstraint,
                                                    @Nonnull final Set<RequestedOrdering> newConstraint) {
        final var newRequestedOrderings = Sets.newLinkedHashSet(newConstraint);
        for (final var newRequestedOrdering : newConstraint) {
            for (final var currentRequestedOrdering : currentConstraint) {
                // try to figure out if current already subsumes new
                if (newRequestedOrdering.isDistinct() != currentRequestedOrdering.isDistinct()) {
                    continue;
                }
                final var newOrderingParts = newRequestedOrdering.getOrderingParts();
                final var currentOrderingParts = currentRequestedOrdering.getOrderingParts();

                if (!currentRequestedOrdering.isExhaustive() && newRequestedOrdering.isExhaustive()) {
                    continue;
                }
                if (currentRequestedOrdering.isExhaustive() && newOrderingParts.size() >= currentOrderingParts.size()) {
                    if (newOrderingParts.subList(0, currentOrderingParts.size()).equals(currentOrderingParts)) {
                        newRequestedOrderings.remove(newRequestedOrdering);
                    }
                    continue;
                }
                if (newOrderingParts.equals(currentOrderingParts)) {
                    newRequestedOrderings.remove(newRequestedOrdering);
                }
            }
        }

        if (newRequestedOrderings.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(ImmutableSet.<RequestedOrdering>builder()
                .addAll(currentConstraint)
                .addAll(newRequestedOrderings)
                .build());
    }
}
