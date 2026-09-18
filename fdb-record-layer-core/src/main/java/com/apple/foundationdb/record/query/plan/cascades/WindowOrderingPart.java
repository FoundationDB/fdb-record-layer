/*
 * WindowOrderingPart.java
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

import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * An ordering column of a window specification: a {@link Value} paired with its requested sort direction. Carried
 * compile-time on {@link CallSiteArguments.WindowSpecification} so a windowed call site can describe its {@code ORDER
 * BY} columns together with their sort directions.
 */
public class WindowOrderingPart {
    @Nonnull
    private final Value value;

    @Nonnull
    private final OrderingPart.RequestedSortOrder sortOrder;

    @SuppressWarnings("this-escape")
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    public WindowOrderingPart(@Nonnull final Value value, @Nonnull final OrderingPart.RequestedSortOrder sortOrder) {
        this.value = value;
        this.sortOrder = sortOrder;
    }

    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nonnull
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return value.getCorrelatedTo();
    }

    @Nonnull
    public OrderingPart.RequestedSortOrder getSortOrder() {
        return sortOrder;
    }

    @Nonnull
    public OrderingPart.RequestedSortOrder getDirectionalSortOrderOrDefault(@Nonnull final OrderingPart.RequestedSortOrder defaultSortOrder) {
        if (sortOrder.isDirectional()) {
            return sortOrder;
        }
        return defaultSortOrder;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof final WindowOrderingPart keyPart)) {
            return false;
        }
        return getValue().equals(keyPart.getValue()) &&
               getSortOrder() == keyPart.getSortOrder();
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    public int computeHashCode() {
        return Objects.hash(getValue(), getSortOrder().name());
    }

    @Override
    public String toString() {
        return getValue() + getSortOrder().getArrowIndicator();
    }
}
