/*
 * ImplementIntersectionRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.PlanPropertiesMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.filterPlanPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.rollUpPartitionsTo;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.logicalIntersectionExpression;

/**
 * A rule that implements an intersection of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalIntersectionExpression} and create a
 * {@link RecordQueryIntersectionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementIntersectionRule extends ImplementationCascadesRule<LogicalIntersectionExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> intersectionLegPlanPartitionMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> intersectionLegReferenceMatcher =
            planPartitions(filterPlanPartitions(planPartition -> planPartition.getPartitionPropertyValue(StoredRecordProperty.storedRecord()),
                    rollUpPartitionsTo(any(intersectionLegPlanPartitionMatcher), PlanPropertiesMap.allAttributesExcept(DistinctRecordsProperty.distinctRecords(), OrderingProperty.ordering()))));
    @Nonnull
    private static final CollectionMatcher<Quantifier.ForEach> allForEachQuantifiersMatcher =
            all(forEachQuantifierOverRef(intersectionLegReferenceMatcher));

    @Nonnull
    private static final BindingMatcher<LogicalIntersectionExpression> root =
            logicalIntersectionExpression(allForEachQuantifiersMatcher);

    public ImplementIntersectionRule() {
        super(root);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var logicalIntersectionExpression = bindings.get(root);
        final var allQuantifiers = bindings.get(allForEachQuantifiersMatcher);
        final var planPartitionsByQuantifier = bindings.getAll(intersectionLegPlanPartitionMatcher);

        //
        // create new references
        //
        final ImmutableList<Quantifier.Physical> newQuantifiers =
                Streams.zip(planPartitionsByQuantifier.stream(), allQuantifiers.stream(),
                                (planPartition, quantifier) -> call.memoizeMemberPlansFromOther(quantifier.getRangesOver(), planPartition.getPlans()))
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList());

        call.yieldPlan(RecordQueryIntersectionPlan.fromQuantifiers(newQuantifiers,
                logicalIntersectionExpression.getComparisonKeyProvidedOrderingParts(), Quantifiers.isReversed(newQuantifiers)));
    }
}
