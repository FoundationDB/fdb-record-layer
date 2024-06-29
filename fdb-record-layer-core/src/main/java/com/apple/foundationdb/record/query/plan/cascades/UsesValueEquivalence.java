/*
 * UsesValueEquivalence.java
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

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Tag interface to provide a common base for all classes implementing a semantic equals using a
 * {@link ValueEquivalence}.
 * @param <T> type parameter that {@link #semanticEquals} operates on.
 */
public interface UsesValueEquivalence<T extends UsesValueEquivalence<T>> {

    /**
     * Method to determine the semantic equality under a given {@link ValueEquivalence}. A value equivalence can
     * define axiomatic equalities between e.g. aliases or
     * {@link com.apple.foundationdb.record.query.plan.cascades.values.Value} trees. The outcome of this method is either
     * {@code false} or {@code true} under the assumption that a verifiable
     * {@link com.apple.foundationdb.record.query.plan.QueryPlanConstraint} is satisfied before the plan is executed
     * and/or re-executed.
     * <br>
     * Note that this method must be reflexive ({@code a R a}), and it must be strongly symmetric using a slightly
     * altered notion of symmetry:
     * <pre>
     * {@code
     * this.semanticEquals(other, valueEquivalence) iff other.semanticEquals(this, valueEquivalence.inverse())
     * }
     * Note that the client can use a symmetric value equivalence in which case the inverse of the value equivalence is
     * identical to the value equivalence itself and we arrive back at {code aRb iff bRa}.
     * </pre>
     * @param other the other object to compare this object to
     * @param valueEquivalence the value equivalence
     * @return a boolean monad {@link BooleanWithConstraint} that is either effectively {@code false} or {@code true}
     *         under the assumption that a contained query plan constraint is satisfied
     */
    @Nonnull
    @SuppressWarnings({"unchecked", "PMD.CompareObjectsWithEquals"})
    default BooleanWithConstraint semanticEquals(@Nullable final Object other,
                                                 @Nonnull final ValueEquivalence valueEquivalence) {
        if (this == other) {
            return BooleanWithConstraint.alwaysTrue();
        }

        final var thisClass = this.getClass();
        if (other == null || thisClass != other.getClass()) {
            return BooleanWithConstraint.falseValue();
        }

        // This cast is safe!
        final var thisOther = semanticEqualsTyped((T)other, valueEquivalence);

        Debugger.sanityCheck(() -> {
            final var inverseValueEquivalenceMaybe = valueEquivalence.inverseMaybe();
            Verify.verify(inverseValueEquivalenceMaybe.isPresent());
            final var otherThis =
                    ((T)other).semanticEqualsTyped((T)this, inverseValueEquivalenceMaybe.get());
            Verify.verify(thisOther.isTrue() == otherThis.isTrue());
        });

        return thisOther;
    }

    /**
     * Method that is overridden by implementors. The difference to {@link #semanticEquals(Object, ValueEquivalence)}
     * is that this method is typed to {@code T}, {@code other} is not {@code null}, and {@code other} is of the same
     * class as {@code this}.
     * @param other other object of type {@code T}
     * @param valueEquivalence a value equivalence
     * @return a {@link BooleanWithConstraint}
     */
    @Nonnull
    BooleanWithConstraint semanticEqualsTyped(@Nonnull T other, @Nonnull ValueEquivalence valueEquivalence);
}
