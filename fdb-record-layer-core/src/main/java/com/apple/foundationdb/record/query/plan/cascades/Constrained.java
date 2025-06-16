/*
 * Constrained.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;

/**
 * A container that can carry a {@link QueryPlanConstraint}.
 * @param <T> type parameter of the type of object this container wraps.
 */
public interface Constrained<T> {
    /**
     * Method that returns the wrapped object.
     * @return the wrapped object
     */
    @Nonnull
    T get();

    @Nonnull
    default T getUnconstrained() {
        Verify.verify(!getConstraint().isConstrained());
        return get();
    }

    @Nonnull
    QueryPlanConstraint getConstraint();

    /**
     * Method to compose this boolean with a {@link QueryPlanConstraint}. It is assumed that the caller just wants to
     * strengthen the constraint already recorded in {@code this}.
     * @param constraint a {@link QueryPlanConstraint}
     * @return a new {@link Constrained} that is {@code false} if {@code this} is false,
     *         and that is {@code true} otherwise under the composed constraint from the constraint of {@code this} and
     *         the {@code constraint}
     */
    @Nonnull
    Constrained<T> composeWithConstraint(@Nonnull QueryPlanConstraint constraint);

    /**
     * Helper method to create an unconstrained wrapper.
     * @param object object to wrap
     * @return a new unconditional {@link Constrained}
     */
    @Nonnull
    static <T> Constrained<T> unconstrained(@Nonnull final T object) {
        return DefaultConstrained.of(object);
    }

    /**
     * Helper method to create an unconstrained wrapper.
     * @param object object to wrap
     * @param queryPlanConstraint the query plan constraint
     * @return a new unconditional {@link Constrained}
     */
    @Nonnull
    static <T> Constrained<T> ofConstrainedObject(@Nonnull final T object,
                                                  @Nonnull final QueryPlanConstraint queryPlanConstraint) {
        return DefaultConstrained.ofConstrainedObject(object, queryPlanConstraint);
    }
}
