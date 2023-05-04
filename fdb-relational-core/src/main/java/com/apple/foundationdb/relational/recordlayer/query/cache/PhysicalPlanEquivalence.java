/*
 * PhysicalPlanEquivalence.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a key in the secondary cache with special logic for look up.
 * <br>
 * It is designed to hold <u>only</u> the items that can not be hashed or evaluated in constant-time. When used for
 * lookup, its holds {@link EvaluationContext} information as a needle and use it to check constraint implication
 * ({@link QueryPlanConstraint#compileTimeEval(EvaluationContext)}) against other objects in the haystack.
 * See how {@link PhysicalPlanEquivalence#equals(Object)} is implemented for more information.
 * Therefore, this class accepts _either_ an {@link EvaluationContext} or a {@link QueryPlanConstraint} as a member.
 * <br>
 *
 * @implNote I think a class similar to Scala's {@code Either} would serve us well in this case. Unfortunately, standard
 * Java libraries do not have (yet) an {@code Either} implementation. Moreover, due to the special equality behavior
 * it is not recommended to use this class outside the current scope of it acting as a key in the secondary cache.
 * For more information about the cache structure, see {@link MultiStageCache} and examine the unit tests of this class
 * in {@code PhysicalPlanEquivalenceTests}.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class PhysicalPlanEquivalence {

    @Nonnull
    private final Optional<QueryPlanConstraint> constraint;

    @Nonnull
    private final Optional<EvaluationContext> evaluationContext;

    @VisibleForTesting
    PhysicalPlanEquivalence(@Nonnull final Optional<QueryPlanConstraint> constraint,
                            @Nonnull final Optional<EvaluationContext> evaluationContext) {
        Assert.thatUnchecked(constraint.isPresent() ^ evaluationContext.isPresent(),
                "Either constraint or evaluation context must be set (but not both)",
                ErrorCode.INTERNAL_ERROR);
        this.constraint = constraint;
        this.evaluationContext = evaluationContext;
    }

    /**
     * Checks whether the passed equivalence is equal to {@code this}.
     * <br>
     * This implementation is nuanced to address two goals:
     * <ol>
     *     <li>realise constraint implications for a given {@link EvaluationContext} when used as a needle during
     *     cache lookup</li>
     *     <li>work seamlessly with {@link com.google.common.cache.Cache}, i.e. no special logic for lookup that
     *     might require changing cache behavior. </li>
     * </ol>
     * Let's use the following terminology to demonstrate the equality behavior: we denote a {@link PhysicalPlanEquivalence}
     * that has an {@link EvaluationContext} an <b>environment</b>, and we denote a {@link PhysicalPlanEquivalence} which
     * has a {@link QueryPlanConstraint} a <b>conditional</b>.
     * <br>
     * if either {@code this} or {@code object} is an environment, while the other is conditional, return {@code true}
     * if the conditional is implied under the environment's {@link EvaluationContext}, otherwise, return {@code false}.
     * <br>
     * return {@code false} in any other case (both environments or both conditional) as they are logically meaningless
     * in the context of cache lookup.
     *
     * @param object The object to check "equality" against.
     * @return {@code true} if objects are "equal", otherwise {@code false}.
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (!(object instanceof PhysicalPlanEquivalence)) {
            return false;
        }
        final var other = (PhysicalPlanEquivalence) object;

        if (other.constraint.isEmpty()) {
            if (constraint.isPresent()) {
                // special semantics for cache lookup.
                return constraint.get().compileTimeEval(other.evaluationContext.get());
            } else {
                // member-wise equality.
                if (evaluationContext.isEmpty() && other.evaluationContext.isEmpty()) {
                    return true;
                }
                if (evaluationContext.isEmpty() || other.evaluationContext.isEmpty()) {
                    return false;
                }
                final var constantBindings1 = (List<?>) evaluationContext.get().getBindings().get(Bindings.Internal.CONSTANT.bindingName(Quantifier.constant().getId()));
                final var constantBindings2 = (List<?>) other.evaluationContext.get().getBindings().get(Bindings.Internal.CONSTANT.bindingName(Quantifier.constant().getId()));
                return Objects.equals(constantBindings1, constantBindings2);
            }
        } else {
            // special semantics for cache lookup.
            // member-wise equality.
            return evaluationContext.map(context -> other.constraint.get().compileTimeEval(context)).orElseGet(() -> other.constraint.equals(constraint));
        }
    }

    @Override
    public int hashCode() {
        // (yhatem): this is intentional, due to special semantics of equality. See equals() for more info.
        return 0;
    }

    @Nonnull
    public PhysicalPlanEquivalence withConstraint(@Nonnull final QueryPlanConstraint constraint) {
        return new PhysicalPlanEquivalence(Optional.of(constraint), Optional.empty());
    }

    @Nonnull
    public static PhysicalPlanEquivalence of(@Nonnull final EvaluationContext evaluationContext) {
        return new PhysicalPlanEquivalence(Optional.empty(), Optional.of(evaluationContext));
    }

    @Nonnull
    public static PhysicalPlanEquivalence of(@Nonnull final QueryPlanConstraint constraint) {
        return new PhysicalPlanEquivalence(Optional.of(constraint), Optional.empty());
    }

    @Override
    public String toString() {
        return constraint.map(queryPlanConstraint -> "constraint " + queryPlanConstraint).orElseGet(() -> "environment " + evaluationContext.get());
    }
}
