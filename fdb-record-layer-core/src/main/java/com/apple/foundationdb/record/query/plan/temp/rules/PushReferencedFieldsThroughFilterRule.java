/*
 * PushReferencedFieldsThroughFilterRule.java
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
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.ReferencedFieldsAttribute;
import com.apple.foundationdb.record.query.plan.temp.ReferencedFieldsAttribute.ReferencedFields;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatchers;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.logicalFilterExpression;

/**
 * A rule that pushes an interesting {@link ReferencedFields} through a {@link LogicalFilterExpression}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushReferencedFieldsThroughFilterRule extends PlannerRule<LogicalFilterExpression> implements PreOrderRule {
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> lowerRefMatcher = ReferenceMatchers.anyRef();
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    private static final BindingMatcher<QueryPredicate> predicateMatcher = anyPredicate();
    private static final BindingMatcher<LogicalFilterExpression> root =
            logicalFilterExpression(all(predicateMatcher), exactly(innerQuantifierMatcher));

    public PushReferencedFieldsThroughFilterRule() {
        super(root, ImmutableSet.of(ReferencedFieldsAttribute.REFERENCED_FIELDS));
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final List<? extends QueryPredicate> predicates = bindings.getAll(predicateMatcher);

        final Set<FieldValue> fieldValuesFromPredicates =
                RelationalExpressionWithPredicates.fieldValuesFromPredicates(predicates);

        final ExpressionRef<? extends RelationalExpression> lowerRef = bindings.get(lowerRefMatcher);
        final ImmutableSet<FieldValue> allReferencedValues = ImmutableSet.<FieldValue>builder()
                .addAll(call.getInterestingProperty(ReferencedFieldsAttribute.REFERENCED_FIELDS)
                        .map(ReferencedFields::getReferencedFieldValues)
                        .orElse(ImmutableSet.of()))
                .addAll(fieldValuesFromPredicates)
                .build();
        call.pushRequirement(lowerRef,
                ReferencedFieldsAttribute.REFERENCED_FIELDS,
                new ReferencedFields(allReferencedValues));
    }
}
