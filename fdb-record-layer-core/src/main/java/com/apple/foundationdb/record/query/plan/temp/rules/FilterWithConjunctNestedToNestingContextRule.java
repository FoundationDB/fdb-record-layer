/*
 * FilterWithConjunctNestedToNestingContextRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.SingleNestedContext;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.NestedContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.NestedContextExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildWithRestMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A rule that looks for a logical filter that contains a conjunction that includes a non-repeated nested predicate
 * ({@link NestedField}) and transforms the inner expression to a version with respect to the {@link NestedContext}
 * defined by that nested field. The other filters in the conjunct are left above the produced
 * {@link NestedContextExpression} and need to be handled separately.
 */
@API(API.Status.EXPERIMENTAL)
public class FilterWithConjunctNestedToNestingContextRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<ExpressionRef<RelationalPlannerExpression>> innerMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<ExpressionRef<QueryComponent>> nestedFilterMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<NestedField> nestedFieldMatcher = TypeMatcher.of(NestedField.class, nestedFilterMatcher);
    private static final ReferenceMatcher<QueryComponent> otherConjunctMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class,
            TypeMatcher.of(AndComponent.class, AnyChildWithRestMatcher.anyMatchingWithRest(
                    nestedFieldMatcher, otherConjunctMatcher)), innerMatcher);

    public FilterWithConjunctNestedToNestingContextRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<RelationalPlannerExpression> inner = call.get(innerMatcher);
        final NestedField nestedField = call.get(nestedFieldMatcher);
        final List<ExpressionRef<QueryComponent>> otherConjuncts = call.getBindings().getAll(otherConjunctMatcher);
        final ExpressionRef<QueryComponent> nestedFilter = call.get(nestedFilterMatcher);
        call.yield(call.ref(call.get(root)));

        final NestedContext nestedContext = new SingleNestedContext(nestedField.getFieldName());
        final ExpressionRef<RelationalPlannerExpression> nestedInner = nestedContext.getNestedRelationalPlannerExpression(inner);

        if (nestedInner == null) {
            return ChangesMade.NO_CHANGE;
        }

        final ExpressionRef<RelationalPlannerExpression> nestedExpression = call.ref(
                new NestedContextExpression(nestedContext, new LogicalFilterExpression(nestedFilter, nestedInner)));
        if (otherConjuncts.isEmpty()) {
            call.yield(nestedExpression);
        } else {
            final ExpressionRef<QueryComponent> residualFilter;
            if (otherConjuncts.size() == 1) {
                residualFilter = otherConjuncts.get(0);
            } else {
                residualFilter = call.ref(new AndComponent(otherConjuncts));
            }
            call.yield(call.ref(new LogicalFilterExpression(residualFilter, nestedExpression)));
        }
        return ChangesMade.MADE_CHANGES;
    }
}
