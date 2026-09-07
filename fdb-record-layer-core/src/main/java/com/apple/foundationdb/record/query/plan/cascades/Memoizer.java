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
    Reference memoizeExpressions(Collection<? extends RelationalExpression> exploratoryExpressions,
                                 Collection<? extends RelationalExpression> finalExpressions);

    static Memoizer noMemoization(final PlannerStage plannerStage) {
        return new Memoizer() {
            @Override
            public Reference memoizeExpressions(final Collection<? extends RelationalExpression> exploratoryExpressions,
                                                final Collection<? extends RelationalExpression> finalExpressions) {
                return Reference.of(plannerStage, exploratoryExpressions, finalExpressions);
            }

            @Override
            public Reference memoizeExploratoryExpression(final RelationalExpression expression) {
                return Reference.ofExploratoryExpression(plannerStage, expression);
            }

            @Override
            public Reference memoizeExploratoryExpressions(final Collection<? extends RelationalExpression> expressions) {
                return Reference.ofExploratoryExpressions(plannerStage, expressions);
            }

            @Override
            public ReferenceBuilder memoizeExploratoryExpressionBuilder(final RelationalExpression expression) {
                return new ReferenceBuilder() {
                    @Override
                    public Reference reference() {
                        return Reference.ofExploratoryExpression(plannerStage, expression);
                    }

                    @Override
                    public Set<? extends RelationalExpression> members() {
                        final var newMembersSet = new LinkedIdentitySet<RelationalExpression>();
                        newMembersSet.add(expression);
                        return newMembersSet;
                    }
                };
            }

            @Override
            public Reference memoizeFinalExpressionsFromOther(final Reference reference,
                                                              final Collection<? extends RelationalExpression> expressions) {
                return Reference.ofFinalExpressions(plannerStage, expressions);
            }

            @Override
            public Reference memoizeFinalExpression(final RelationalExpression expression) {
                return Reference.ofFinalExpression(plannerStage, expression);
            }

            @Override
            public Reference memoizeFinalExpressions(final Collection<RelationalExpression> expressions) {
                return Reference.ofFinalExpressions(plannerStage, expressions);
            }

            @Override
            public Reference memoizeUnknownExpression(final RelationalExpression expression) {
                Verify.verify(plannerStage == PlannerStage.PLANNED);
                if (expression instanceof RecordQueryPlan) {
                    return memoizeFinalExpression(expression);
                }
                return memoizeExploratoryExpression(expression);
            }

            @Override
            public Reference memoizeMemberPlansFromOther(final Reference reference,
                                                         final Collection<? extends RecordQueryPlan> plans) {
                return memoizeFinalExpressionsFromOther(reference, plans);
            }

            @Override
            public Reference memoizePlan(final RecordQueryPlan plan) {
                return memoizeFinalExpression(plan);
            }

            @Override
            public ReferenceBuilder memoizeFinalExpressionsBuilder(final Collection<? extends RelationalExpression> expressions) {
                return new ReferenceBuilder() {
                    @Override
                    public Reference reference() {
                        return Reference.ofFinalExpressions(plannerStage, expressions);
                    }

                    @Override
                    public Set<? extends RelationalExpression> members() {
                        return new LinkedIdentitySet<>(expressions);
                    }
                };
            }

            @Override
            public ReferenceOfPlansBuilder memoizeMemberPlansBuilder(final Reference reference,
                                                                     final Collection<? extends RecordQueryPlan> plans) {
                return new ReferenceOfPlansBuilder() {
                    @Override
                    public Set<? extends RecordQueryPlan> members() {
                        return new LinkedIdentitySet<>(plans);
                    }

                    @Override
                    public Reference reference() {
                        return Reference.ofFinalExpressions(plannerStage, plans);
                    }
                };
            }

            @Override
            public ReferenceOfPlansBuilder memoizePlansBuilder(final Collection<? extends RecordQueryPlan> plans) {
                return new ReferenceOfPlansBuilder() {
                    @Override
                    public Set<? extends RecordQueryPlan> members() {
                        return new LinkedIdentitySet<>(plans);
                    }

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
        Reference reference();

        Set<? extends RelationalExpression> members();
    }

    /**
     * Builder for references.
     */
    interface ReferenceOfPlansBuilder extends ReferenceBuilder {
        @Override
        Set<? extends RecordQueryPlan> members();
    }
}
