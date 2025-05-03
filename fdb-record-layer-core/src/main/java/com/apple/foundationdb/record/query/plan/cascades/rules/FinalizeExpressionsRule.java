/*
 * ImplementUnorderedUnionRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.ExpressionPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.anyExpressionPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.expressionPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.anyExploratoryExpression;

/**
 * A rule that implements an unordered union of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalUnionExpression} and create a
 * {@link RecordQueryUnorderedUnionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class FinalizeExpressionsRule extends CascadesRule<RelationalExpression> {
    @Nonnull
    private static final BindingMatcher<ExpressionPartition<RelationalExpression>> childPartitionsMatcher =
            anyExpressionPartition();

    @Nonnull
    private static final BindingMatcher<Reference> childReferenceMatcher =
            expressionPartitions(rollUpPartitions(any(childPartitionsMatcher)));

    @Nonnull
    private static final CollectionMatcher<Quantifier> allQuantifiersMatcher =
            all(anyQuantifierOverRef(childReferenceMatcher));

    @Nonnull
    private static final BindingMatcher<RelationalExpression> root = anyExploratoryExpression(allQuantifiersMatcher);

    public FinalizeExpressionsRule() {
        super(root);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        // this is an all-rule
        return Optional.empty();
    }

    @Override
    @SuppressWarnings("UnstableApiUsage")
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var exploratoryExpression = bindings.get(root);
        final var partitions = bindings.getAll(childPartitionsMatcher);
        final var allQuantifiers = bindings.get(allQuantifiersMatcher);

        final var newQuantifiers =
                Streams.zip(partitions.stream(), allQuantifiers.stream(),
                                (partition, quantifier) -> {
                                    final var reference = call.memoizeMemberExpressions(quantifier.getRangesOver(), partition.getExpressions());
                                    return quantifier.toBuilder().build(reference);
                                })
                        .collect(ImmutableList.toImmutableList());

        call.yieldFinalExpression(exploratoryExpression.withQuantifiers(newQuantifiers));
    }
}
