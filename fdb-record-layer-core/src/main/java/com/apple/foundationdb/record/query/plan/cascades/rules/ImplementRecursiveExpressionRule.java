/*
 * ImplementRecursiveRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursiveDfsPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifierOverRef;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.recursiveExpression;

/**
 * A rule that implements an existential nested loop join of its (already implemented) children.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementRecursiveExpressionRule extends ImplementationCascadesRule<RecursiveExpression> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(ImplementRecursiveExpressionRule.class);

    @Nonnull
    private static final BindingMatcher<PlanPartition> rootPlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> rootReferenceMatcher = planPartitions(rollUpPartitions(all(rootPlanPartitionsMatcher)));
    @Nonnull
    private static final BindingMatcher<Quantifier> rootQuantifierMatcher = anyQuantifierOverRef(rootReferenceMatcher);
    @Nonnull
    private static final BindingMatcher<PlanPartition> childPlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> childReferenceMatcher = planPartitions(rollUpPartitions(all(childPlanPartitionsMatcher)));
    @Nonnull
    private static final BindingMatcher<Quantifier> childQuantifierMatcher = anyQuantifierOverRef(childReferenceMatcher);
    @Nonnull
    private static final BindingMatcher<RecursiveExpression> root =
            recursiveExpression(exactly(rootQuantifierMatcher, childQuantifierMatcher));

    public ImplementRecursiveExpressionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var recursiveExpression = bindings.get(root);
        Debugger.withDebugger(debugger -> logger.debug(KeyValueLogMessage.of("matched RecursiveExpression", "legs", recursiveExpression.getQuantifiers().size())));

        final var rootQuantifier = bindings.get(rootQuantifierMatcher);
        final var childQuantifier = bindings.get(childQuantifierMatcher);

        final var rootReference = bindings.get(rootReferenceMatcher);
        final var childReference = bindings.get(childReferenceMatcher);

        final var rootPartition = bindings.get(rootPlanPartitionsMatcher);
        final var childPartition = bindings.get(childPlanPartitionsMatcher);

        final var rootAlias = rootQuantifier.getAlias();
        final var childAlias = childQuantifier.getAlias();

        var rootRef = call.memoizeMemberPlansFromOther(rootReference, rootPartition.getPlans());
        final var newRootQuantifier = Quantifier.physicalBuilder().withAlias(rootAlias).build(rootRef);

        var childRef = call.memoizeMemberPlansFromOther(childReference, childPartition.getPlans());
        final var newChildQuantifier = Quantifier.physicalBuilder().withAlias(childAlias).build(childRef);

        //call.yieldPlan(new RecordQueryRecursiveDfsPlan(newRootQuantifier, newChildQuantifier, recursiveExpression.getResultValue()));
    }
}
