/*
 * FDBRepeatedFieldQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.Column;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Tag;

import java.util.Optional;

/**
 * Tests of query planning and execution for queries on records with repeated fields.
 */
@Tag(Tags.RequiresFDB)
public class FDBSimpleQueryGraphTest extends FDBRecordStoreQueryTestBase {
    @DualPlannerTest
    public void testSimplePlanGraph() throws Exception {
        final CascadesPlanner cascadesPlanner;

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);

            if (!(planner instanceof CascadesPlanner)) {
                return;
            }

            cascadesPlanner = (CascadesPlanner)planner;

            TestRecords4Proto.RestaurantReviewer reviewer = TestRecords4Proto.RestaurantReviewer.newBuilder()
                    .setId(1L)
                    .setName("Javert")
                    .setEmail("inspecteur@policier.fr")
                    .setStats(TestRecords4Proto.ReviewerStats.newBuilder()
                            .setStartDate(100L)
                            .setHometown("Toulon")
                    )
                    .build();
            recordStore.saveRecord(reviewer);

            reviewer = TestRecords4Proto.RestaurantReviewer.newBuilder()
                    .setId(2L)
                    .setName("M. le Maire")
                    .setStats(TestRecords4Proto.ReviewerStats.newBuilder()
                            .setStartDate(120L)
                            .setHometown("Montreuil-sur-mer")
                    )
                    .build();
            recordStore.saveRecord(reviewer);

            TestRecords4Proto.RestaurantRecord restaurant = TestRecords4Proto.RestaurantRecord.newBuilder()
                    .setRestNo(1000L)
                    .setName("Chez Thénardier")
                    .addReviews(
                            TestRecords4Proto.RestaurantReview.newBuilder()
                                    .setReviewer(1L)
                                    .setRating(100)
                    )
                    .addReviews(
                            TestRecords4Proto.RestaurantReview.newBuilder()
                                    .setReviewer(2L)
                                    .setRating(0)
                    )
                    .addTags(
                            TestRecords4Proto.RestaurantTag.newBuilder()
                                    .setValue("l'atmosphère")
                                    .setWeight(10)
                    )
                    .addTags(
                            TestRecords4Proto.RestaurantTag.newBuilder()
                                    .setValue("les aliments")
                                    .setWeight(70)
                    )
                    .addCustomer("jean")
                    .addCustomer("fantine")
                    .addCustomer("cosette")
                    .addCustomer("éponine")
                    .build();
            recordStore.saveRecord(restaurant);

            restaurant = TestRecords4Proto.RestaurantRecord.newBuilder()
                    .setRestNo(1001L)
                    .setName("ABC")
                    .addReviews(
                            TestRecords4Proto.RestaurantReview.newBuilder()
                                    .setReviewer(1L)
                                    .setRating(34)
                    )
                    .addReviews(
                            TestRecords4Proto.RestaurantReview.newBuilder()
                                    .setReviewer(2L)
                                    .setRating(110)
                    )
                    .addTags(
                            TestRecords4Proto.RestaurantTag.newBuilder()
                                    .setValue("l'atmosphère")
                                    .setWeight(40)
                    )
                    .addTags(
                            TestRecords4Proto.RestaurantTag.newBuilder()
                                    .setValue("les aliments")
                                    .setWeight(20)
                    )
                    .addCustomer("gavroche")
                    .addCustomer("enjolras")
                    .addCustomer("éponine")
                    .build();
            recordStore.saveRecord(restaurant);

            commit(context);
        }

        final var plan = cascadesPlanner.planGraph(
                () -> {
                    var qun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(ImmutableSet.of("RestaurantRecord",
                                            "RestaurantReviewer"))));

                    qun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                    qun,
                                    Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()))));

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            new FieldValue(qun.getFlowedObjectValue(), ImmutableList.of("name"));
                    final var restNoValue =
                            new FieldValue(qun.getFlowedObjectValue(), ImmutableList.of("rest_no"));

                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(nameValue.getResultType(), Optional.of("nameNew")), nameValue));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(restNoValue.getResultType(), Optional.of("restNoNew")), nameValue));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(null, false, qun));
                },
                Optional.of(ImmutableSet.of("RestaurantRecord")),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());
        System.out.println("plan " + plan);
    }
}
