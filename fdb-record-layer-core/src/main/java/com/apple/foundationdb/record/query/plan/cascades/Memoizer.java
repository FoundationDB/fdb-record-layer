/*
 * Memoizer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;

/**
 * An interface for memoizing {@link Reference}s and their member {@link RelationalExpression}s. The methods declared in
 * this interface mostly have one thing in common. They expect among their parameters an expression or a collection of
 * expressions which are to be memoized and return a {@link Reference} which may be a new reference that was just
 * created or an already existing reference that was previously memoized by this {@code Memoizer} and that was deemed
 * to be compatible to be reused.
 * <br>
 * There are numerous considerations that determine if a reference can be safely reused. Most of these considerations
 * can be derived from the individual use case and the context the method is called from. Each individual method
 * declaration in this interface will also indicate (via java doc) if the method can return a reused expression or
 * if the caller can always expect a fresh reference to be returned. Note that the terminology used here is that
 * a <em>memoized expression</em> indicates that the memoization structures of the planner are aware of this expression.
 * A reference (not an expression) can be reused as an effect of memoization of the given expressions (depending on
 * use case and context).
 */
@API(API.Status.EXPERIMENTAL)
public interface Memoizer extends ExploratoryMemoizer, FinalMemoizer {

    /**
     * Memoize the given two collections of {@link RelationalExpression}s, one for exploratory and one for final
     * expressions. A new reference is created and returned to the caller.
     *
     * @param exploratoryExpressions the collection of exploratory expressions to memoize
     * @param finalExpressions the collection of exploratory expressions to memoize
     * @return a new or reused reference
     * @see #memoizeExploratoryExpression(RelationalExpression)
     * */
    @Nonnull
    Reference memoizeExpressions(@Nonnull Collection<? extends RelationalExpression> exploratoryExpressions,
                                 @Nonnull Collection<? extends RelationalExpression> finalExpressions);

    @Nonnull
    static Memoizer noMemoization(@Nonnull final PlannerStage plannerStage) {
        return new Memoizer() {
            @Nonnull
            @Override
            public Reference memoizeExpressions(@Nonnull final Collection<? extends RelationalExpression> exploratoryExpressions,
                                                @Nonnull final Collection<? extends RelationalExpression> finalExpressions) {
                return Reference.of(plannerStage, exploratoryExpressions, finalExpressions);
            }

            @Nonnull
            @Override
            public Reference memoizeExploratoryExpression(@Nonnull final RelationalExpression expression) {
                return Reference.ofExploratoryExpression(plannerStage, expression);
            }

            @Nonnull
            @Override
            public Reference memoizeExploratoryExpressions(@Nonnull final Collection<? extends RelationalExpression> expressions) {
                return Reference.ofExploratoryExpressions(plannerStage, expressions);
            }

            @Nonnull
            @Override
            public ReferenceBuilder memoizeExploratoryExpressionBuilder(@Nonnull final RelationalExpression expression) {
                return new ReferenceBuilder() {
                    @Nonnull
                    @Override
                    public Reference reference() {
                        return Reference.ofExploratoryExpression(plannerStage, expression);
                    }

                    @Nonnull
                    @Override
                    public Set<? extends RelationalExpression> members() {
                        final var newMembersSet = new LinkedIdentitySet<RelationalExpression>();
                        newMembersSet.add(expression);
                        return newMembersSet;
                    }
                };
            }

            @Nonnull
            @Override
            public Reference memoizeFinalExpressionsFromOther(@Nonnull final Reference reference,
                                                              @Nonnull final Collection<? extends RelationalExpression> expressions) {
                return Reference.ofFinalExpressions(plannerStage, expressions);
            }

            @Nonnull
            @Override
            public Reference memoizeFinalExpression(@Nonnull final RelationalExpression expression) {
                return Reference.ofFinalExpression(plannerStage, expression);
            }

            @Nonnull
            @Override
            public Reference memoizeFinalExpressions(@Nonnull final Collection<RelationalExpression> expressions) {
                return Reference.ofFinalExpressions(plannerStage, expressions);
            }

            @Nonnull
            @Override
            public Reference memoizeUnknownExpression(@Nonnull final RelationalExpression expression) {
                Verify.verify(plannerStage == PlannerStage.PLANNED);
                if (expression instanceof RecordQueryPlan) {
                    return memoizeFinalExpression(expression);
                }
                return memoizeExploratoryExpression(expression);
            }

            @Nonnull
            @Override
            public Reference memoizeMemberPlansFromOther(@Nonnull final Reference reference,
                                                         @Nonnull final Collection<? extends RecordQueryPlan> plans) {
                return memoizeFinalExpressionsFromOther(reference, plans);
            }

            @Nonnull
            @Override
            public Reference memoizePlan(@Nonnull final RecordQueryPlan plan) {
                return memoizeFinalExpression(plan);
            }

            @Nonnull
            @Override
            public ReferenceBuilder memoizeFinalExpressionsBuilder(@Nonnull final Collection<? extends RelationalExpression> expressions) {
                return new ReferenceBuilder() {
                    @Nonnull
                    @Override
                    public Reference reference() {
                        return Reference.ofFinalExpressions(plannerStage, expressions);
                    }

                    @Nonnull
                    @Override
                    public Set<? extends RelationalExpression> members() {
                        return new LinkedIdentitySet<>(expressions);
                    }
                };
            }

            @Nonnull
            @Override
            public ReferenceOfPlansBuilder memoizeMemberPlansBuilder(@Nonnull final Reference reference,
                                                                     @Nonnull final Collection<? extends RecordQueryPlan> plans) {
                return new ReferenceOfPlansBuilder() {
                    @Nonnull
                    @Override
                    public Set<? extends RecordQueryPlan> members() {
                        return new LinkedIdentitySet<>(plans);
                    }

                    @Nonnull
                    @Override
                    public Reference reference() {
                        return Reference.ofFinalExpressions(plannerStage, plans);
                    }
                };
            }

            @Nonnull
            @Override
            public ReferenceOfPlansBuilder memoizePlansBuilder(@Nonnull final Collection<? extends RecordQueryPlan> plans) {
                return new ReferenceOfPlansBuilder() {
                    @Nonnull
                    @Override
                    public Set<? extends RecordQueryPlan> members() {
                        return new LinkedIdentitySet<>(plans);
                    }

                    @Nonnull
                    @Override
                    public Reference reference() {
                        return Reference.ofFinalExpressions(plannerStage, plans);
                    }
                };
            }

            @Override
            public String toString() {
                return "no-memo";
            }
        };
    }

    /**
     * Builder for references.
     */
    interface ReferenceBuilder {
        @Nonnull
        Reference reference();

        @Nonnull
        Set<? extends RelationalExpression> members();
    }

    /**
     * Builder for references.
     */
    interface ReferenceOfPlansBuilder extends ReferenceBuilder {
        @Nonnull
        @Override
        Set<? extends RecordQueryPlan> members();
    }
}
