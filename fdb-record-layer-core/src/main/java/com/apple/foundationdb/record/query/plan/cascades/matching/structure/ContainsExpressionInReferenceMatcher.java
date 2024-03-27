/*
 * ContainsExpressionInReferenceMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

/**
 * A typed matcher that matches the current expression if the current expression is contained in a
 * group reference binding designated by another {@link BindingMatcher} (which was bound earlier on a reference).
 */
@API(API.Status.EXPERIMENTAL)
public class ContainsExpressionInReferenceMatcher extends TypedMatcher<RelationalExpression> {
    @Nonnull
    private final BindingMatcher<? extends Reference> otherMatcher;

    private ContainsExpressionInReferenceMatcher(@Nonnull final BindingMatcher<? extends Reference> otherMatcher) {
        super(RelationalExpression.class);
        this.otherMatcher = otherMatcher;
    }

    /**
     * Attempt to match this matcher against the given expression.
     * Note that implementations of {@code matchWith()} should only attempt to match the given root with this planner
     * expression or attempt to access the members of the given reference.
     *
     * @param plannerConfiguration planner configuration
     * @param outerBindings preexisting bindings to be used by the matcher
     * @param in the object of type {@code T} we attempt to match
     * @return a stream of {@link PlannerBindings} containing the matched bindings, or an empty stream is no match was found
     */
    @Nonnull
    @Override
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final RelationalExpression in) {
        Verify.verify(outerBindings.containsKey(otherMatcher));

        final Reference reference = outerBindings.get(otherMatcher);
        if (reference.containsExactly(in)) {
            return Stream.of(PlannerBindings.from(this, in));
        } else {
            return Stream.empty();
        }
    }

    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        return "case _: " + getRootClass().getSimpleName() + " if " + boundId + " is bound in other matcher => success ";
    }

    @Nonnull
    public static BindingMatcher<RelationalExpression> containsExpressionInReference(@Nonnull final BindingMatcher<? extends Reference> otherMatcher) {
        return new ContainsExpressionInReferenceMatcher(otherMatcher);
    }
}
