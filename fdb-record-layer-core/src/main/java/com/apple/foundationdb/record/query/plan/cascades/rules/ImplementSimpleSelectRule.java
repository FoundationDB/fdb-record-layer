/*
 * ImplementSimpleSelectRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyCompensatablePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that implements a {@link SelectExpression} over a single inner quantifier as a chain of physical plans.
 *
 * <p>The emitted plan has up to four nodes, each node added only when needed:
 * <pre>{@code
 *   «inner plan»
 *   | ON EMPTY NULL          (if the inner quantifier is a null-on-empty for-each)
 *   | FIRST_OR_DEFAULT NULL  (if the inner quantifier is Existential)
 *   | FILTER <predicates>    (if there are non-tautology predicates)
 *   | MAP <resultValue>      (if the result value is not a passthrough of the inner quantifier)
 * }</pre>
 *
 * <p>The {@code MAP} node is omitted when the {@code resultValue} is a {@link QuantifiedObjectValue} referencing the
 * inner quantifier.
 *
 * <p>If none of the conditions apply, the rule simply yields the inner plan partition unchanged.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementSimpleSelectRule extends ImplementationCascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> innerReferenceMatcher =
            planPartitions(any(innerPlanPartitionMatcher));

    @Nonnull
    private static final BindingMatcher<Quantifier> innerQuantifierMatcher = anyQuantifierOverRef(innerReferenceMatcher);

    @Nonnull
    private static final BindingMatcher<QueryPredicate> predicateMatcher = anyCompensatablePredicate();

    @Nonnull
    private static final BindingMatcher<SelectExpression> root =
            selectExpression(all(predicateMatcher), exactly(innerQuantifierMatcher));

    public ImplementSimpleSelectRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);
        final var planPartition = bindings.get(innerPlanPartitionMatcher);
        final var innerReference = bindings.get(innerReferenceMatcher);
        final var quantifier = bindings.get(innerQuantifierMatcher);
        final var predicates = bindings.getAll(predicateMatcher);
        final var resultValue = selectExpression.getResultValue();
        final var referenceBuilder = implementSelectExpression(call, resultValue, predicates, innerReference, quantifier, planPartition);
        call.yieldPlans(referenceBuilder.members());
    }

    @Nonnull
    public static Memoizer.ReferenceOfPlansBuilder implementSelectExpression(@Nonnull final ImplementationCascadesRuleCall call,
                                                                             @Nonnull final Value result,
                                                                             @Nonnull final List<? extends QueryPredicate> predicates,
                                                                             @Nonnull final Reference innerReference,
                                                                             @Nonnull final Quantifier innerQuantifier,
                                                                             @Nonnull final PlanPartition innerPlanPartition) {
        Value resultValue = result;
        Memoizer.ReferenceOfPlansBuilder builder = call.memoizeMemberPlansBuilder(innerReference, innerPlanPartition.getPlans());

        // Check if the `resultValue` is a simple passthrough of the inner quantifier and the outer MAP can be omitted.
        // To skip the MAP, the passthrough must have *exactly* the same type that the inner quantifier flows; if the
        // type just “widens” the nullability, we keep the MAP so it expresses the type widening at runtime. Anything
        // beyond these two cases would indicate a retyping bug elsewhere and is caught by the sanity check below.
        final CorrelationIdentifier alias = innerQuantifier.getAlias();
        final boolean isPassthrough = QuantifiedObjectValue.isSimpleQuantifiedObjectValueOver(resultValue, alias);
        final boolean isSimpleResultValue = isPassthrough
                && resultValue.getResultType().equals(innerQuantifier.getFlowedObjectType());
        if (isPassthrough && !isSimpleResultValue) {
            final Type resultType = resultValue.getResultType();
            Debugger.sanityCheck(() -> Verify.verify(
                    resultType.equals(innerQuantifier.getFlowedObjectType().nullable()),
                    "result type %s does not match inner quantifier flowed type %s",
                    resultType, innerQuantifier.getFlowedObjectType()));
        }

        // Add a FIRST_OR_DEFAULT NULL if the quantifier is existential.
        // Add a ON EMPTY NULL if the quantifier is a null-on-empty for-each.
        if (innerQuantifier instanceof Quantifier.Existential) {
            builder = call.memoizePlanBuilder(
                    new RecordQueryFirstOrDefaultPlan(
                            Quantifier.physicalBuilder()
                                    .withAlias(alias)
                                    .build(builder.reference()),
                            new NullValue(innerQuantifier.getFlowedObjectType())));
        } else if (innerQuantifier instanceof Quantifier.ForEach forEach && forEach.isNullOnEmpty()) {
            builder = call.memoizePlanBuilder(
                    new RecordQueryDefaultOnEmptyPlan(
                            Quantifier.physicalBuilder()
                                    .withAlias(alias)
                                    .build(builder.reference()),
                            new NullValue(innerQuantifier.getFlowedObjectType())));
        }

        // Add a FILTER if there are non-tautology predicates.
        final var nonTautologyPredicates =
                predicates.stream()
                        .filter(predicate -> !predicate.isTautology())
                        .collect(ImmutableList.toImmutableList());
        if (!nonTautologyPredicates.isEmpty()) {
            builder = call.memoizePlanBuilder(
                    new RecordQueryPredicatesFilterPlan(
                            Quantifier.physicalBuilder()
                                    .withAlias(alias)
                                    .build(builder.reference()),
                            nonTautologyPredicates.stream()
                                    .map(QueryPredicate::toResidualPredicate)
                                    .collect(ImmutableList.toImmutableList())));
        }

        // Add a MAP if the result value is not a simple passthrough.
        if (!isSimpleResultValue) {
            final Quantifier.Physical beforeMapQuantifier;
            if (!nonTautologyPredicates.isEmpty()) {
                beforeMapQuantifier = Quantifier.physical(builder.reference());
                resultValue = resultValue.rebase(AliasMap.ofAliases(alias, beforeMapQuantifier.getAlias()));
            } else {
                beforeMapQuantifier = Quantifier.physicalBuilder()
                        .withAlias(alias)
                        .build(builder.reference());
            }

            builder = call.memoizePlanBuilder(new RecordQueryMapPlan(beforeMapQuantifier, resultValue));
        }

        return builder;
    }
}
