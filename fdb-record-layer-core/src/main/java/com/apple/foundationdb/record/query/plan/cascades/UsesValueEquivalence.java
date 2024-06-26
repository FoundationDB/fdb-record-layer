/*
 * AliasMap.java
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

import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Tag interface to provide a common base for all classes implementing a semantic equals using a
 * {@link ValueEquivalence}.
 * @param <T> type parameter that {@link #semanticEquals} operates on.
 */
public interface UsesValueEquivalence<T extends UsesValueEquivalence<T>> {

    @SuppressWarnings("unchecked")
    @Nonnull
    default Optional<QueryPlanConstraint> semanticEquals(@Nullable final Object other, @Nonnull final ValueEquivalence valueEquivalence) {
        if (this == other) {
            return ValueEquivalence.alwaysEqual();
        }

        final var thisClass = this.getClass();
        if (other == null || thisClass != other.getClass()) {
            return Optional.empty();
        }

        // This cast is safe!
        return semanticEqualsTyped((T)thisClass.cast(other), valueEquivalence);
    }

    @Nonnull
    Optional<QueryPlanConstraint> semanticEqualsTyped(@Nonnull T other, @Nonnull final ValueEquivalence valueEquivalence);
}
