/*
 * ConstrainedBoolean.java
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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A boolean container that can if the boolean is true carry a {@link QueryPlanConstraint}.
 * <br>
 * The meaning of an object of this class is:
 * <ul>
 *     <li>The boolean is {@code false} (there is no constraint).</li>
 *     <li>
 *         The boolean is {@code true} under the condition that the recorded {@link QueryPlanConstraint}
 *         evaluates to {@code true}.
 *     </li>
 * </ul>
 * Note that the {@link QueryPlanConstraint} can be {@link QueryPlanConstraint#noConstraint()} which means that this
 * boolean is effectively unconditionally true. {@link QueryPlanConstraint}s are usually proven to be true for this
 * invocation of the planner but refer to an external environment that may change if a plan were to be re-executed in
 * a different environment.
 * <br>
 * This class is a monad which conceptually wraps around a boolean and a {@link QueryPlanConstraint}. There are two
 * combinators {@link #composeWithOther(ConstrainedBoolean)} and {@link #filter(Predicate)}.
 */
public class ConstrainedBoolean implements Constrained<Boolean> {
    private static final ConstrainedBoolean FALSE = new ConstrainedBoolean(false, QueryPlanConstraint.noConstraint());
    private static final ConstrainedBoolean ALWAYS_TRUE = new ConstrainedBoolean(true, QueryPlanConstraint.noConstraint());

    private final boolean isTrue;

    /**
     * The query plan constraint recorded for this boolean.
     */
    @Nonnull
    private final QueryPlanConstraint queryPlanConstraint;

    private ConstrainedBoolean(final boolean isTrue, @Nonnull final QueryPlanConstraint queryPlanConstraint) {
        Verify.verify(isTrue || !queryPlanConstraint.isConstrained());
        this.isTrue = isTrue;
        this.queryPlanConstraint = queryPlanConstraint;
    }

    /**
     * Returns iff this boolean is considered to be true.
     *
     * @return {@code true} if this boolean is true, {@code false} otherwise.
     */
    public boolean isTrue() {
        return isTrue;
    }

    /**
     * Returns iff this boolean is considered to be false. This method is just provided to avoid {@code !isTrue()} in
     * client code.
     *
     * @return {@code true} if this boolean is false, {@code false} otherwise.
     */
    public boolean isFalse() {
        return !isTrue();
    }

    @Nonnull
    @Override
    public Boolean get() {
        return isTrue();
    }

    @Nonnull
    @Override
    public QueryPlanConstraint getConstraint() {
        return queryPlanConstraint;
    }

    /**
     * Method to compose this boolean with another {@link ConstrainedBoolean}.
     * @param other another {@link ConstrainedBoolean}
     * @return a new {@link ConstrainedBoolean} that is {@code false} if {@code this} or {@code other} is false,
     *         and that is {@code true} otherwise under the composed constraint from the constraint of {@code this} and
     *         the constraint of {@code other}
     */
    @Nonnull
    public ConstrainedBoolean composeWithOther(@Nonnull final ConstrainedBoolean other) {
        if (other.isFalse()) {
            return falseValue();
        }

        return composeWithConstraint(other.getConstraint());
    }

    /**
     * Method to compose this boolean with a {@link QueryPlanConstraint}. It is assumed that the caller just wants to
     * strengthen the constraint already recorded in {@code this}.
     * @param constraint a {@link QueryPlanConstraint}
     * @return a new {@link ConstrainedBoolean} that is {@code false} if {@code this} is false,
     *         and that is {@code true} otherwise under the composed constraint from the constraint of {@code this} and
     *         the {@code constraint}
     */
    @Nonnull
    @Override
    public ConstrainedBoolean composeWithConstraint(@Nonnull final QueryPlanConstraint constraint) {
        if (!this.isTrue()) {
            return falseValue();
        }

        return trueWithConstraint(Objects.requireNonNull(queryPlanConstraint).compose(constraint));
    }

    /**
     * Monadic method to filter {@code this} by applying a {@link Predicate} to the recorded plan constraint if
     * applicable.
     * @param predicate to be applied as filter
     * @return {@link ConstrainedBoolean#falseValue()} if {@link #isFalse()} is true or the {@link Predicate}
     *         evaluates to {@code false}, {@code this} otherwise
     */
    @Nonnull
    public ConstrainedBoolean filter(@Nonnull final Predicate<? super QueryPlanConstraint> predicate) {
        if (isFalse() || !predicate.test(getConstraint())) {
            return falseValue();
        }
        return this;
    }

    /**
     * Helper method to interface with {@link Optional}s. A mapper that is passed in by the client to map the
     * recorded {@link QueryPlanConstraint} to an object of a type {@code U} which is applied when {@code this} object
     * is transformed to an {@link Optional}.
     * @param mapper a function to map from {@link QueryPlanConstraint} to type {@code U}
     * @param <U> type parameter of the resulting {@link Optional}
     * @return an {@link Optional} of type {@code U} that is {@link Optional#empty()} if {@code this} is false; it is
     *         {@code Optional.of(mapper.apply(getConstraint))} otherwise.
     */
    @Nonnull
    public <U> Optional<U> mapToOptional(@Nonnull final Function<? super QueryPlanConstraint, ? extends U> mapper) {
        if (isFalse()) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(mapper.apply(getConstraint()));
        }
    }

    /**
     * Monadic method to compose {@code this} by applying a {@link Function} to the recorded plan constraint if
     * applicable.
     * @param composeFunction function that maps the current {@link QueryPlanConstraint} to a new
     *        {@link ConstrainedBoolean}
     * @return {@link ConstrainedBoolean#falseValue()} if {@link #isFalse()} is true or the
     *         {@link ConstrainedBoolean} that the {@code composeFunction} returned
     */
    @Nonnull
    public ConstrainedBoolean compose(@Nonnull final Function<? super QueryPlanConstraint, ? extends ConstrainedBoolean> composeFunction) {
        if (isFalse()) {
            return ConstrainedBoolean.falseValue();
        } else {
            final var result = composeFunction.apply(getConstraint());
            if (result.isFalse()) {
                return ConstrainedBoolean.falseValue();
            }
            return composeWithOther(result);
        }
    }

    /**
     * Factory method to return a {@code false}.
     * @return {@code false}
     */
    @Nonnull
    public static ConstrainedBoolean falseValue() {
        return FALSE;
    }

    /**
     * Factory method to create an unconditional {@code true}.
     * @return a {@link ConstrainedBoolean} that is unconditionally {@code true}, i.e. that is {@code true} using
     *         a {@link QueryPlanConstraint#noConstraint()}
     */
    @Nonnull
    public static ConstrainedBoolean alwaysTrue() {
        return ALWAYS_TRUE;
    }

    /**
     * Helper method to dispatch to {@link #falseValue()} or {@link #alwaysTrue()} based on a boolean value handed in.
     * @param isTrue {@code true} or {@code false}
     * @return the appropriate unconditional {@link ConstrainedBoolean}
     */
    @Nonnull
    public static ConstrainedBoolean ofBoolean(final boolean isTrue) {
        return isTrue ? alwaysTrue() : falseValue();
    }

    /**
     * Factory method to create a conditional {@code true} based on a {@link QueryPlanConstraint} that is also passed
     * in.
     * @param constraint the {@link QueryPlanConstraint} that this {@link ConstrainedBoolean} is constraint on.
     * @return a {@link ConstrainedBoolean} that is conditionally {@code true} under the constraint
     *         {@code constraint}.
     */
    @Nonnull
    public static ConstrainedBoolean trueWithConstraint(@Nonnull final QueryPlanConstraint constraint) {
        return new ConstrainedBoolean(true, constraint);
    }
}
