/*
 * PushReferencedFieldsThroughSelectRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.ReferencedFieldsConstraint;
import com.apple.foundationdb.record.query.plan.cascades.ReferencedFieldsConstraint.ReferencedFields;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that pushes a {@link ReferencedFieldsConstraint} through a {@link SelectExpression}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushReferencedFieldsThroughSelectRule extends CascadesRule<SelectExpression> implements PreOrderRule {
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> lowerRefMatcher = ReferenceMatchers.anyRef();
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    private static final BindingMatcher<QueryPredicate> predicateMatcher = anyPredicate();
    private static final BindingMatcher<SelectExpression> root =
            selectExpression(all(predicateMatcher), exactly(innerQuantifierMatcher));

    public PushReferencedFieldsThroughSelectRule() {
        super(root, ImmutableSet.of(ReferencedFieldsConstraint.REFERENCED_FIELDS));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final SelectExpression selectExpression = bindings.get(root);
        final List<? extends QueryPredicate> predicates = bindings.getAll(predicateMatcher);

        final Set<FieldValue> fieldValuesFromPredicates =
                RelationalExpressionWithPredicates.fieldValuesFromPredicates(predicates);

        final Set<FieldValue> fieldValuesFromResultValues =
                getFieldValuesFromResultValues(selectExpression);

        final ExpressionRef<? extends RelationalExpression> lowerRef = bindings.get(lowerRefMatcher);
        final ImmutableSet<FieldValue> allReferencedValues = ImmutableSet.<FieldValue>builder()
                .addAll(call.getPlannerConstraint(ReferencedFieldsConstraint.REFERENCED_FIELDS)
                        .map(ReferencedFields::getReferencedFieldValues)
                        .orElse(ImmutableSet.of()))
                .addAll(fieldValuesFromPredicates)
                .addAll(fieldValuesFromResultValues)
                .build();
        call.pushConstraint(lowerRef,
                ReferencedFieldsConstraint.REFERENCED_FIELDS,
                new ReferencedFields(allReferencedValues));
    }

    /**
     * Return all {@link FieldValue}s contained in the result values of this expression.
     * @return a set of {@link FieldValue}s
     */
    private ImmutableSet<FieldValue> getFieldValuesFromResultValues(@Nonnull final SelectExpression selectExpression) {
        return selectExpression.getResultValue().preOrderStream()
                .filter(FieldValue.class::isInstance)
                .map(value -> (FieldValue)value)
                .collect(ImmutableSet.toImmutableSet());
    }
}
