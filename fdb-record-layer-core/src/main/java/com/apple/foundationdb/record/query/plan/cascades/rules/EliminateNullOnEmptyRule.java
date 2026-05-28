/*
 * EliminateNullOnEmptyRule.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.ConstantPredicateFoldingUtil;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.atLeastOne;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierWithDefaultOnEmptyOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * Eliminates every null-on-empty quantifier underneath a {@link SelectExpression} whose predicates reject the null
 * tuple at that quantifier's alias. The rule fires once per matching {@code SelectExpression} and rewrites all
 * eligible null-on-empty quantifiers in a single yielded expression.
 */
public class EliminateNullOnEmptyRule extends ExplorationCascadesRule<SelectExpression> {

    @Nonnull
    private static final CollectionMatcher<Quantifier.ForEach> nullOnEmptyQuantifiers =
            atLeastOne(forEachQuantifierWithDefaultOnEmptyOverRef(anyRef()));

    @Nonnull
    private static final BindingMatcher<SelectExpression> root = selectExpression(nullOnEmptyQuantifiers);

    public EliminateNullOnEmptyRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final EvaluationContext evaluationContext = call.getEvaluationContext();
        final SelectExpression select = bindings.get(root);
        final List<? extends QueryPredicate> predicates = select.getPredicates();
        final Collection<Quantifier.ForEach> matches = bindings.get(nullOnEmptyQuantifiers);

        // Determine which null-on-empty flags are eliminable. A matched quantifier qualifies if some predicate of the
        // surrounding select provably rejects null at its alias.
        // Note: The set collects aliases rather than quantifiers because two null-on-empty for-each quantifiers ranging
        // over structurally identical references compare equal under `ForEach#equals`, and thus would get conflated.
        // Note: If `predicates` is empty to begin with, no quantifier qualifies. This is fine, as “no predicate”
        // effectively means the TRUE predicate, which is null-accepting.
        final Set<CorrelationIdentifier> aliases = matches.stream()
                .map(Quantifier::getAlias)
                .filter(alias -> predicates.stream().anyMatch(
                        predicate -> ConstantPredicateFoldingUtil.rejectsNull(predicate, alias, evaluationContext)))
                .collect(ImmutableSet.toImmutableSet());
        if (aliases.isEmpty()) {
            return;
        }

        // Replace each identified null-on-empty quantifier with a normal `ForEach` over the same underlying reference,
        // preserving the alias and leaving the sibling quantifiers untouched.
        final List<Quantifier> newQuantifiers = select.getQuantifiers().stream()
                .map(q -> {
                    if (!(q instanceof Quantifier.ForEach f) || !aliases.contains(f.getAlias())) {
                        return q;
                    }
                    Verify.verify(f.getFlowedObjectType().isNullable());
                    return Quantifier.forEachBuilder().withAlias(f.getAlias()).build(f.getRangesOver());
                })
                .collect(ImmutableList.toImmutableList());

        final SelectExpression newSelect = GraphExpansion.builder()
                .addAllQuantifiers(newQuantifiers)
                .addAllPredicates(predicates)
                .build()
                .buildSelectWithResultValue(select.getResultValue());
        call.yieldExploratoryExpression(newSelect);
    }
}
