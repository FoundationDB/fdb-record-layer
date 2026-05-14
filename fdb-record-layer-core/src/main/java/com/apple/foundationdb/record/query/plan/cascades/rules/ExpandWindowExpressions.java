/*
 * ExpandWindowExpressions.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.WindowExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.TransientWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;

import java.util.Objects;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.atLeastOne;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpressionWithOutput;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyTransientWindowValue;

public class ExpandWindowExpressions extends ImplementationCascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<Reference> lowerRefMatcher = ReferenceMatchers.anyRef();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    @Nonnull
    private static final BindingMatcher<Value> windowValuesMatcher = anyTransientWindowValue();
    @Nonnull
    private static final BindingMatcher<SelectExpression> root = selectExpressionWithOutput(atLeastOne(windowValuesMatcher), exactly(innerQuantifierMatcher));

    public ExpandWindowExpressions() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var transientWindowValues = call.getBindings().getAll(windowValuesMatcher).stream()
                .flatMap(value -> value.preOrderStream().filter(TransientWindowValue.class::isInstance))
                .map(TransientWindowValue.class::cast)
                .distinct()
                .collect(ImmutableList.toImmutableList());
        if (transientWindowValues.size() != 1) {
            return;
        }

        final var selectExpression = call.get(root);
        final var lowerRef = call.get(lowerRefMatcher);
        final var oldQun = Iterables.getOnlyElement(selectExpression.getQuantifiers());
        final var newLowerQun = Quantifier.forEach(lowerRef);
        final var rebaseToNew = AliasMap.ofAliases(oldQun.getAlias(), newLowerQun.getAlias());

        final var transientWindowValue = Iterables.getOnlyElement(transientWindowValues);
        final var partitioningValues = transientWindowValue.getPartitioningValues().stream()
                .map(v -> v.rebase(rebaseToNew)).collect(ImmutableList.toImmutableList());
        final var passThroughValues = selectExpression.getResultValues().stream()
                .filter(v -> !v.isTransient()).map(v -> v.rebase(rebaseToNew)).collect(ImmutableList.toImmutableList());
        final var windowValue = (WindowValue)transientWindowValue.toWindowValue().rebase(rebaseToNew);
        final var requestedOrdering = WindowOrderingPart.toRequestedOrdering(
                transientWindowValue.getOrderingParts(), lowerRef.getCorrelatedTo());

        final var windowExpression = new WindowExpression(windowValue, partitioningValues,
                passThroughValues, requestedOrdering, newLowerQun);
        final var windowQun = Quantifier.forEach(call.memoizeFinalExpression(windowExpression));

        final var upperSelectOutput = Objects.requireNonNull(MaxMatchMap.compute(selectExpression.getResultValue().rebase(rebaseToNew),
                windowExpression.getResultValue(), newLowerQun.getCorrelatedTo())
                .translateQueryValueMaybe(windowQun.getAlias())
                .orElseThrow()
                .replace(part -> {
                    if (part instanceof TransientWindowValue) {
                        return createFieldValueReferenceToWindowExpression(windowExpression.getResultValue(), windowQun).orElseThrow();
                    }
                    return part;
                }));
        final var upperSelect = new SelectExpression(upperSelectOutput, ImmutableList.of(windowQun), ImmutableList.of());
        call.yieldFinalExpression(upperSelect);
    }

    @Nonnull
    private static Optional<FieldValue> createFieldValueReferenceToWindowExpression(@Nonnull final Value windowExpressionOutput,
                                                                                    @Nonnull final Quantifier windowExpressionQun) {
        final var fieldValues = Values.deconstructRecord(windowExpressionOutput);
        for (int i = 0; i < fieldValues.size(); i++) {
            if (fieldValues.get(i) instanceof WindowValue) {
                return Optional.of(FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(windowExpressionQun), i));
            }
        }
        return Optional.empty();
    }
}
