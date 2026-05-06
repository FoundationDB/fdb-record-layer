/*
 * PlanPartitionsTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.PlanPropertiesMap.allAttributesExcept;
import static org.assertj.core.api.Assertions.assertThat;


class PlanPartitionsTest {
    @Test
    void rollUpByTrackedProperties() {
        final var firstPlan = indexScan("a");
        final var secondPlan = indexScan("b");

        final ExpressionProperty<Integer> firstTrackedProperty = newProperty();
        final ExpressionProperty<Integer> secondTrackedProperty = newProperty();
        final ExpressionProperty<Integer> untrackedProperty = newProperty();

        final var firstPlanPartition = PlanPartition.ofPlans(
                ImmutableMap.of(firstTrackedProperty, 1, secondTrackedProperty, 2),
                ImmutableMap.of(firstPlan, Map.of(untrackedProperty, 1)));
        final var secondPlanPartition = PlanPartition.ofPlans(
                ImmutableMap.of(firstTrackedProperty, 1, secondTrackedProperty, 22),
                ImmutableMap.of(secondPlan, Map.of(untrackedProperty, 2)));

        final var rolledUpPartitions = PlanPartitions.rollUpTo(
                List.of(firstPlanPartition, secondPlanPartition), firstTrackedProperty);

        assertThat(rolledUpPartitions).singleElement().satisfies(partition -> {
            assertThat(partition.getPlans()).containsExactly(firstPlan, secondPlan);
            assertThat(partition.getPartitionPropertiesMap()).isEqualTo(Map.of(firstTrackedProperty, 1));
            assertThat(partition.getNonPartitioningPropertiesMap()).containsExactly(
                    Map.entry(firstPlan, Map.of(untrackedProperty, 1)),
                    Map.entry(secondPlan, Map.of(untrackedProperty, 2))
            );
        });
    }

    @Test
    void rollUpByUntrackedProperties() {
        final ExpressionProperty<Integer> firstTrackedProperty = newProperty();
        final ExpressionProperty<Integer> secondTrackedProperty = newProperty();
        final ExpressionProperty<Integer> untrackedProperty = newProperty();
        final ExpressionProperty<Boolean> untrackedPartitioningProperty = newProperty(true);

        final var firstPlan = indexScan("a");
        final var secondPlan = indexScan("b");

        final var firstPlanPartition = PlanPartition.ofPlans(
                Map.of(firstTrackedProperty, 1, secondTrackedProperty, 2),
                Map.of(firstPlan, Map.of(untrackedProperty, 1)));
        final var secondPlanPartition = PlanPartition.ofPlans(
                Map.of(firstTrackedProperty, 1, secondTrackedProperty, 22),
                Map.of(secondPlan, Map.of(untrackedProperty, 2)));

        final var rolledUpPartitions = PlanPartitions.rollUpTo(
                List.of(firstPlanPartition, secondPlanPartition), untrackedPartitioningProperty);

        assertThat(rolledUpPartitions).singleElement().satisfies(partition -> {
            assertThat(partition.getPlans()).containsExactly(firstPlan, secondPlan);
            assertThat(partition.getPartitionPropertiesMap()).isEqualTo(Map.of(untrackedPartitioningProperty, true));
            assertThat(partition.getNonPartitioningPropertiesMap()).containsExactly(
                    Map.entry(firstPlan, Map.of(untrackedProperty, 1)),
                    Map.entry(secondPlan, Map.of(untrackedProperty, 2))
            );
        });
    }

    @Test
    void rollUpByUntrackedAndTrackedProperties() {
        final ExpressionProperty<Integer> firstTrackedProperty = newProperty();
        final ExpressionProperty<Integer> secondTrackedProperty = newProperty();
        final ExpressionProperty<Integer> untrackedProperty = newProperty();
        final ExpressionProperty<Boolean> untrackedPartitioningProperty = newProperty(true);

        final var firstPlan = indexScan("a");
        final var secondPlan = indexScan("b");

        final var firstPlanPartition = PlanPartition.ofPlans(
                Map.of(firstTrackedProperty, 1, secondTrackedProperty, 2),
                Map.of(firstPlan, Map.of(untrackedProperty, 1)));
        final var secondPlanPartition = PlanPartition.ofPlans(
                Map.of(firstTrackedProperty, 1, secondTrackedProperty, 22),
                Map.of(secondPlan, Map.of(untrackedProperty, 2)));

        final var rolledUpPartitions = PlanPartitions.rollUpTo(
                List.of(firstPlanPartition, secondPlanPartition),
                Set.of(secondTrackedProperty, untrackedPartitioningProperty));

        assertThat(rolledUpPartitions).hasSize(2).anySatisfy(partition -> {
            assertThat(partition.getPlans()).containsExactly(firstPlan);
            assertThat(partition.getPartitionPropertiesMap()).isEqualTo(Map.of(
                    untrackedPartitioningProperty, true,
                    secondTrackedProperty, 2
            ));
            assertThat(partition.getNonPartitioningPropertiesMap()).isEqualTo(Map.of(
                    firstPlan, Map.of(untrackedProperty, 1)
            ));
        }).anySatisfy(partition -> {
            assertThat(partition.getPlans()).containsExactly(secondPlan);
            assertThat(partition.getPartitionPropertiesMap()).isEqualTo(Map.of(
                    untrackedPartitioningProperty, true,
                    secondTrackedProperty, 22
            ));
            assertThat(partition.getNonPartitioningPropertiesMap()).containsExactly(
                    Map.entry(secondPlan, Map.of(untrackedProperty, 2))
            );
        });
    }

    @Test
    void toPartitionsWithEmptyMap() {
        final var propertiesMap = new PlanPropertiesMap();
        final var partitions = PlanPartitions.toPartitions(propertiesMap);
        assertThat(partitions).isEmpty();
    }

    @Test
    void toPartitionsWithSinglePlan() {
        final var plan = indexScan("a");
        final var propertiesMap = new PlanPropertiesMap(List.of(plan));

        final var partitions = PlanPartitions.toPartitions(propertiesMap);

        assertThat(partitions).singleElement().satisfies(partition -> {
            assertThat(partition.getPlans()).containsExactly(plan);
            assertThat(partition.getNonPartitioningPropertiesMap()).containsExactly(Map.entry(plan, Map.of()));
        });
    }

    @Test
    void toPartitionsPlansWithSamePropertiesAreGroupedTogether() {
        // Two index scans built identically (same type, same scan parameters) should share
        // the same property values and therefore land in a single partition.
        final var firstPlan = indexScan("a");
        final var secondPlan = indexScan("b");
        final var propertiesMap = new PlanPropertiesMap(List.of(firstPlan, secondPlan));

        final var partitions = PlanPartitions.toPartitions(propertiesMap);

        assertThat(partitions)
                .singleElement()
                .satisfies(partition -> {
                    assertThat(partition.getPlans()).containsExactly(firstPlan, secondPlan);
                    assertThat(partition.getNonPartitioningPropertiesMap()).containsExactly(
                            Map.entry(firstPlan, Map.of()),
                            Map.entry(secondPlan, Map.of())
                    );
                    assertThat(partition.getPartitionPropertiesMap()).containsOnlyKeys(allAttributesExcept());
                });
    }

    private static RecordQueryIndexPlan indexScan(@Nonnull final String indexName) {
        return new RecordQueryIndexPlan(indexName, null, IndexScanComparisons.byValue(),
                IndexFetchMethod.SCAN_AND_FETCH, RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                false, false, Optional.empty(),
                RuleTestHelper.TYPE_S, QueryPlanConstraint.noConstraint());
    }

    private static <T> ExpressionProperty<T> newProperty() {
        return newProperty(null);
    }

    private static <T> ExpressionProperty<T> newProperty(T propertyValue) {
        return new ExpressionProperty<>() {
            @Nonnull
            @Override
            @SuppressWarnings("unchecked")
            public T narrowAttribute(@Nonnull final Object object) {
                return (T)object;
            }

            @Nonnull
            @Override
            public RelationalExpressionVisitor<T> createVisitor() {
                return (RelationalExpressionVisitorWithDefaults<T>)element -> propertyValue;
            }
        };
    }
}
