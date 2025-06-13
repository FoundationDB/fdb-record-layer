/*
 * RemoveRangeOneRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RangeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamingValue;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.members;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.tableFunctionExpression;

@SuppressWarnings("PMD.TooManyStaticImports")
public class RemoveRangeOneRule extends ExplorationCascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<TableFunctionExpression> tfExpression = tableFunctionExpression();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> middleQun = forEachQuantifierOverRef(members(any(tfExpression)));
    @Nonnull
    private static final BindingMatcher<SelectExpression> root = selectExpression(any(middleQun));

    @Nonnull
    private static final RangeValue EXPECTED = (RangeValue) new RangeValue.RangeFn().encapsulate(List.of(LiteralValue.ofScalar(1L)));

    public RemoveRangeOneRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
        final SelectExpression select = call.get(root);
        if (select.getQuantifiers().size() == 1) {
            // There must be at least one other quantifier. Otherwise, removing this quantifier
            // can result in cardinality changes
            return;
        }

        //
        // Check that the value matches the RANGE(0, 1) inserted to handle values
        //
        final TableFunctionExpression tf = call.get(tfExpression);
        final StreamingValue value = tf.getValue();
        if (!(value instanceof RangeValue)) {
            return;
        }
        final RangeValue rangeValue = (RangeValue) value;
        if (!EXPECTED.equals(rangeValue)) {
            return;
        }

        //
        // Validate that nothing is quantified to this value
        //
        final Quantifier.ForEach qun = call.get(middleQun);
        final CorrelationIdentifier id = qun.getAlias();
        if (select.getResultValue().isCorrelatedTo(id)
                || select.getPredicates().stream().anyMatch(predicate -> predicate.isCorrelatedTo(id))
                || select.getQuantifiers().stream().anyMatch(childQun -> childQun != qun && childQun.isCorrelatedTo(id))) {
            return;
        }

        //
        // Here we have a quantifier over RANGE(0, 1) whose result value is never referenced.
        // We can remove the quantifier from this SELECT.
        //
        call.yieldExploratoryExpression(new SelectExpression(
                select.getResultValue(),
                select.getQuantifiers().stream().filter(otherQun -> otherQun != qun).collect(ImmutableList.toImmutableList()),
                select.getPredicates()
        ));
    }
}
