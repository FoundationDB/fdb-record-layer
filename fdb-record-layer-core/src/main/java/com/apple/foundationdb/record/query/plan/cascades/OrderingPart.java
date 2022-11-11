/*
 * OrderingPart.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value that is used to express ordered-ness.
 */
public class OrderingPart {
    @Nonnull
    private final Value value;

    private final boolean isReverse;

    private final Supplier<Integer> hashCodeSupplier;

    protected OrderingPart(@Nonnull final Value value, final boolean isReverse) {
        this.value = checkValue(value);
        this.isReverse = isReverse;
        this.hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
    }

    @Nonnull
    public Value getValue() {
        return value;
    }

    public boolean isReverse() {
        return isReverse;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OrderingPart)) {
            return false;
        }
        final var keyPart = (OrderingPart)o;
        return getValue().equals(keyPart.getValue()) &&
               isReverse() == keyPart.isReverse();
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    public int computeHashCode() {
        return Objects.hash(getValue(), isReverse());
    }

    @Override
    public String toString() {
        return getValue() + (isReverse() ? "↓" : "↑") ;
    }

    @Nonnull
    public static List<OrderingPart> prefix(@Nonnull final List<? extends OrderingPart> keyParts, final int endExclusive) {
        return ImmutableList.copyOf(keyParts.subList(0, endExclusive));
    }

    @Nonnull
    public static OrderingPart of(@Nonnull final Value orderByValue) {
        return OrderingPart.of(orderByValue, false);
    }

    @Nonnull
    public static OrderingPart of(@Nonnull final Value orderByValue,
                                  final boolean isReverse) {
        return new OrderingPart(orderByValue, isReverse);
    }

    @Nonnull
    private static Value checkValue(@Nonnull final Value value) {
        final var correlatedTo = value.getCorrelatedTo();
        Verify.verify(correlatedTo.size() <= 1);
        Verify.verify(correlatedTo.isEmpty() || Iterables.getOnlyElement(correlatedTo).equals(Quantifier.current()));
        return value;
    }
}
