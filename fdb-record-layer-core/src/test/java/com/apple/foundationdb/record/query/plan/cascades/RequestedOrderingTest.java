/*
 * RequestedOrderingTest.java
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

import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueTestHelpers;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of {@link RequestedOrdering}.
 */
class RequestedOrderingTest {
    /**
     * Tests that an empty suffix is returned as the prefix itself, which is the case of an aggregate without an in-call
     * {@code ORDER BY} clause.
     */
    @Test
    void concatWithoutDuplicates1() {
        final var qov = ValueTestHelpers.qov();
        final Value a = ValueTestHelpers.field(qov, "a");
        final List<RequestedOrderingPart> prefix = ImmutableList.of(part(a, RequestedSortOrder.ANY));

        assertThat(RequestedOrdering.concatWithoutDuplicates(prefix, ImmutableList.of())).isEqualTo(prefix);
    }

    /**
     * Tests that a suffix part is appended, and that its own sort order is kept rather than the {@code ANY} the
     * grouping prefix carries.
     */
    @Test
    void concatWithoutDuplicates2() {
        final var qov = ValueTestHelpers.qov();
        final Value a = ValueTestHelpers.field(qov, "a");
        final Value b = ValueTestHelpers.field(qov, "b");

        assertThat(RequestedOrdering.concatWithoutDuplicates(
                ImmutableList.of(part(a, RequestedSortOrder.ANY)),
                ImmutableList.of(part(b, RequestedSortOrder.DESCENDING))))
                .containsExactly(part(a, RequestedSortOrder.ANY), part(b, RequestedSortOrder.DESCENDING));
    }

    /**
     * Tests that a suffix part whose value the prefix already orders by is dropped. Such a value is constant from that
     * point on, so requesting an order for it again would ask for an ordering nothing can provide.
     */
    @Test
    void concatWithoutDuplicates3() {
        final var qov = ValueTestHelpers.qov();
        final Value a = ValueTestHelpers.field(qov, "a");
        final Value b = ValueTestHelpers.field(qov, "b");

        assertThat(RequestedOrdering.concatWithoutDuplicates(
                ImmutableList.of(part(a, RequestedSortOrder.ANY)),
                ImmutableList.of(part(a, RequestedSortOrder.DESCENDING), part(b, RequestedSortOrder.ASCENDING))))
                .containsExactly(part(a, RequestedSortOrder.ANY), part(b, RequestedSortOrder.ASCENDING));
    }

    /**
     * Tests that a value repeated <em>within</em> the suffix is dropped as well, keeping only its first occurrence.
     */
    @Test
    void concatWithoutDuplicates4() {
        final var qov = ValueTestHelpers.qov();
        final Value a = ValueTestHelpers.field(qov, "a");
        final Value b = ValueTestHelpers.field(qov, "b");

        assertThat(RequestedOrdering.concatWithoutDuplicates(
                ImmutableList.of(part(a, RequestedSortOrder.ANY)),
                ImmutableList.of(part(b, RequestedSortOrder.ASCENDING), part(b, RequestedSortOrder.DESCENDING))))
                .containsExactly(part(a, RequestedSortOrder.ANY), part(b, RequestedSortOrder.ASCENDING));
    }

    private static RequestedOrderingPart part(final Value value, final RequestedSortOrder sortOrder) {
        return new RequestedOrderingPart(value, sortOrder);
    }
}
