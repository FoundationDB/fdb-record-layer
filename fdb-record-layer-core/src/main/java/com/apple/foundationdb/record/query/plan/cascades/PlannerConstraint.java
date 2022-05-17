/*
 * PlannerConstraint.java
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

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Base interface to capture constraints for the planner.
 * An instance of this usually class serves as a key in maps, e.g. {@link ConstraintsMap}, much like an
 * enum, but provides strong typing.
 * @param <T> the type representing the actual property
 */
public interface PlannerConstraint<T> {
    /**
     * Method to narrow the type from {@link Object}, e.g. from {@link ConstraintsMap}, to the declared
     * type of the attribute. Note that {@link ConstraintsMap} guarantees that the narrowing is well-defined
     * and successful.
     * @param object an object that actually is of dynamic type {@code T}
     * @return the narrowed object of type {@code T}
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default T narrowConstraint(@Nonnull final Object object) {
        return (T)object;
    }

    /**
     * Method to be provided by the implementor to provide a mechanism to combine different constraints of the same kind
     * into one. Most constraints are collections, or more specifically sets of things. E.g. a set is idempotent with
     * respect to the add operation under equality. This combine method is a generalization of adding new property
     * information to existing ones under more general considerations than just equality.
     * For instance a constraining {@link Ordering} may already express an ordering of {@code a, b}. That would result in
     * plans that whose records are ordered by {@code a, b, xxx} meaning plans with an ordering of e.g. {@code a, b, c}
     * would also be created. A subsequent interesting ordering of {@code a, b, c} can just be dropped as the
     * interesting ordering {@code a, b} subsumes the new interesting property.
     * Note that repeated invocations of this method should behave in an idempotent way, meaning that the second
     * invocation of {@code combine()} with the same {@code newConstraint} should result in {@code Optional.empty()}.
     * @param currentConstraint current property
     * @param newConstraint a new property that should be combined with the {@code currentProperty}
     * @return an optional containing the combined constraint, or {@code Optional.empty()} if the implementation of this
     *         attribute was unable to create a new property in a meaningful way or if the current constraint already
     *         subsumes the new property.
     */
    @Nonnull
    Optional<T> combine(@Nonnull final T currentConstraint, @Nonnull final T newConstraint);
}
