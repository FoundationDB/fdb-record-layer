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
 * This class captures an interesting ordering.
 * Instances of this class are used to communicate required ordering properties during planning.
 *
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RequestedOrdering)) {
            return false;
        }
        final RequestedOrdering ordering = (RequestedOrdering)o;
        return getOrderingKeyParts().equals(ordering.getOrderingKeyParts());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getOrderingKeyParts());
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
        NO_DISTINCT,
        PRESERVE_DISTINCTNESS
    }
}
