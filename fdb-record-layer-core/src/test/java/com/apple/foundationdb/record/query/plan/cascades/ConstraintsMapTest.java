/*
 * ConstraintsMapTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ConstraintsMap}.
 */
class ConstraintsMapTest {
    /**
     * Tests that a constraints map that inherits the constraints of another one does not share the state of the
     * constraints with it.
     */
    @Test
    void inheritFromOtherDoesNotShareConstraints() {
        final var original = new ConstraintsMap();
        original.pushProperty(RequestedOrderingConstraint.REQUESTED_ORDERING,
                ImmutableSet.of(RequestedOrdering.preserve()));
        final var inheriting = new ConstraintsMap();
        inheriting.inheritFromOther(original);

        inheriting.pushProperty(RequestedOrderingConstraint.REQUESTED_ORDERING,
                ImmutableSet.of(RequestedOrdering.ofParts(ImmutableList.of(), RequestedOrdering.Distinctness.DISTINCT,
                        false, ImmutableSet.of())));

        assertThat(inheriting.getConstraintOptional(RequestedOrderingConstraint.REQUESTED_ORDERING))
                .hasValueSatisfying(orderings -> assertThat(orderings).hasSize(2));
        assertThat(original.getConstraintOptional(RequestedOrderingConstraint.REQUESTED_ORDERING))
                .hasValueSatisfying(orderings -> assertThat(orderings).hasSize(1));
    }
}
