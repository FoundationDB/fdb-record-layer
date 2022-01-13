/*
 * RequestedOrdering.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * This class captures a requested ordering. Instances of this class are used to communicate ordering properties
 * towards the sources during planning.
 *
 * There are two flows of information at work during planning:
 * <ul>
 *     <li>
 *         a {@link RequestedOrdering} is used to convey to an upstream expression (closer to source) that a particular
 *         order is useful or even required in order for the operation downstream to properly plan
 *     </li>
 *     <li>
 *         an {@link Ordering} which e.g. is attached to plans (or can be computed of plans) that declares the ordering
 *         of that plan
 *     </li>
 * </ul>
 *
 * An {@link Ordering} satisfies a {@link RequestedOrdering} iff it adheres to the constraints given in the request.
 */
public class RequestedOrdering {
    /**
     * A list of {@link KeyExpression}s where none of the contained expressions is equality-bound. This list
     * defines the actual order of records.
     */
    @Nonnull
    private final List<KeyPart> orderingKeyParts;

    private final Distinctness distinctness;

    public RequestedOrdering(@Nonnull final List<KeyPart> orderingKeyParts, final Distinctness distinctness) {
        this.orderingKeyParts = ImmutableList.copyOf(orderingKeyParts);
        this.distinctness = distinctness;
    }

    public Distinctness getDistinctness() {
        return distinctness;
    }

    public boolean isDistinct() {
        return distinctness == Distinctness.DISTINCT;
    }

    /**
     * When expressing a requirement (see also {@link OrderingAttribute}), the requirement may be to preserve
     * the order of records that are being encountered. This is represented by a special value here.
     * @return {@code true} if the ordering needs to be preserved
     */
    public boolean isPreserve() {
        return orderingKeyParts.isEmpty();
    }

    @Nonnull
    public List<KeyPart> getOrderingKeyParts() {
        return orderingKeyParts;
    }

    public int size() {
        return orderingKeyParts.size();
    }

    @Nonnull
    public RequestedOrdering removePrefix(int prefixSize) {
        if (prefixSize >= size()) {
            return preserve();
        }

        return new RequestedOrdering(getOrderingKeyParts().subList(prefixSize, size()), getDistinctness());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RequestedOrdering)) {
            return false;
        }
        final RequestedOrdering ordering = (RequestedOrdering)o;
        return getOrderingKeyParts().equals(ordering.getOrderingKeyParts()) &&
               getDistinctness() == ordering.getDistinctness();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getOrderingKeyParts(), getDistinctness());
    }

    /**
     * Method to create an ordering instance that preserves the order of records.
     * @return a new ordering that preserves the order of records
     */
    @Nonnull
    public static RequestedOrdering preserve() {
        return new RequestedOrdering(ImmutableList.of(), Distinctness.PRESERVE_DISTINCTNESS);
    }

    public enum Distinctness {
        DISTINCT,
        NOT_DISTINCT,
        PRESERVE_DISTINCTNESS
    }
}
