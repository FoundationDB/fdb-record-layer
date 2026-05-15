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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.WithValue;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.WindowExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
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
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.transientSelectExpression;

/**
 * Matches a {@link SelectExpression} containing a single {@link TransientWindowValue} and rewrites it into a
 * {@link WindowExpression} fed by the original input, wrapped by an upper {@link SelectExpression} that projects
 * the window result alongside any pass-through columns.
 */
public class ExpandWindowExpressions extends ImplementationCascadesRule<SelectExpression> {
    private static final BindingMatcher<Reference> lowerRefMatcher = ReferenceMatchers.anyRef();
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    private static final BindingMatcher<SelectExpression> root = transientSelectExpression(exactly(innerQuantifierMatcher));

    public ExpandWindowExpressions() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var select = call.get(root);
        final var lowerRef = call.get(lowerRefMatcher);
        final var twvs = collectTransientWindowValues(select);
        if (twvs.size() != 1) {
            return;
        }

        final var oldQun = Iterables.getOnlyElement(select.getQuantifiers());
        final var newLowerQun = Quantifier.forEach(lowerRef);
        final var rebase = AliasMap.ofAliases(oldQun.getAlias(), newLowerQun.getAlias());
        final var twv = Iterables.getOnlyElement(twvs);

        // Build WindowExpression.
        final var windowExpr = new WindowExpression(
                (WindowValue)twv.toWindowValue().rebase(rebase),
                twv.getPartitioningValues().stream().map(v -> v.rebase(rebase)).collect(ImmutableList.toImmutableList()),
                select.getResultValues().stream().filter(v -> !v.isTransient()).map(v -> v.rebase(rebase)).collect(ImmutableList.toImmutableList()),
                WindowOrderingPart.toRequestedOrdering(twv.getOrderingParts(), lowerRef.getCorrelatedTo()),
                newLowerQun);
        final var windowQun = Quantifier.forEach(call.memoizeFinalExpression(windowExpr));
        final var constants = lowerRef.getCorrelatedTo();

        // Pull result and predicates up through the window expression.
        final var upperOutput = pullUp(select.getResultValue(), windowExpr.getResultValue(), windowQun, rebase, constants);
        final var upperPredicates = select.getPredicates().stream()
                .map(p -> p instanceof final PredicateWithValue pwv
                        ? pwv.withValue(pullUp(Objects.requireNonNull(pwv.getValue()), windowExpr.getResultValue(), windowQun, rebase, constants))
                        : p)
                .collect(ImmutableList.toImmutableList());

        call.yieldFinalExpression(new SelectExpression(upperOutput, ImmutableList.of(windowQun), upperPredicates));
    }

    @Nonnull
    private static ImmutableList<TransientWindowValue> collectTransientWindowValues(@Nonnull final SelectExpression select) {
        return Stream.concat(
                        select.getResultValue().preOrderStream(),
                        select.getPredicates().stream()
                                .filter(WithValue.class::isInstance).map(WithValue.class::cast)
                                .flatMap(wv -> Objects.requireNonNull(wv.getValue()).preOrderStream()))
                .filter(TransientWindowValue.class::isInstance).map(TransientWindowValue.class::cast)
                .distinct()
                .collect(ImmutableList.toImmutableList());
    }

    /** Rebases a value into the window's alias space, then translates it to reference the window output columns. */
    @Nonnull
    private static Value pullUp(@Nonnull final Value value, @Nonnull final Value windowResult,
                                @Nonnull final Quantifier windowQun, @Nonnull final AliasMap rebase,
                                @Nonnull final Set<CorrelationIdentifier> constants) {
        return Objects.requireNonNull(
                MaxMatchMap.compute(value.rebase(rebase), windowResult, constants)
                        .translateQueryValueMaybe(windowQun.getAlias()).orElseThrow()
                        .replace(part -> part instanceof TransientWindowValue
                                ? windowFieldRef(windowResult, windowQun)
                                : part));
    }

    @Nonnull
    private static FieldValue windowFieldRef(@Nonnull final Value windowResult, @Nonnull final Quantifier windowQun) {
        final var fields = Values.deconstructRecord(windowResult);
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i) instanceof WindowValue) {
                return FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(windowQun), i);
            }
        }
        throw new IllegalStateException("window result does not contain a WindowValue field");
    }
}
