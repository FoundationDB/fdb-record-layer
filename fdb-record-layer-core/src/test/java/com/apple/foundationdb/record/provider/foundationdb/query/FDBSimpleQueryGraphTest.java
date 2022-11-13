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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.IndexAccessHint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.InsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.UpdateExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue.LightArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.containsAll;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.descendantPlans;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.flatMapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapResult;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.recordTypes;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.typeFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.fieldValueWithFieldNames;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;
import static com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue.LightArrayConstructorValue.emptyArray;

/**
 * Tests of query planning and execution for queries on records with repeated fields.
 */
@Tag(Tags.RequiresFDB)
public class FDBSimpleQueryGraphTest extends FDBRecordStoreQueryTestBase {
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testSimplePlanGraph() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();
        // no index hints, plan a query
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    final var allRecordTypes =
                            ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
                    var qun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints())));

                    qun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                    qun,
                                    Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()))));

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(nameValue.getResultType(), Optional.of("nameNew")), nameValue));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(restNoValue.getResultType(), Optional.of("restNoNew")), restNoValue));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());

        assertMatchesExactly(plan,
                mapPlan(
                        typeFilterPlan(
                                scanPlan()
                                        .where(scanComparisons(range("([1],>")))))
                        .where(mapResult(recordConstructorValue(exactly(ValueMatchers.fieldValueWithFieldNames("name"), ValueMatchers.fieldValueWithFieldNames("rest_no"))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testSimplePlanGraphWithNullableArray() throws Exception {
        CascadesPlanner cascadesPlanner = setUpWithNullableArray();
        // no index hints, plan a query
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    final var allRecordTypes =
                            ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
                    var qun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints())));

                    qun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                    qun,
                                    Type.Record.fromDescriptor(TestRecords4WrapperProto.RestaurantRecord.getDescriptor()))));

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");
                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(nameValue.getResultType(), Optional.of("nameNew")), nameValue));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(restNoValue.getResultType(), Optional.of("restNoNew")), restNoValue));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());
        assertMatchesExactly(plan,
                mapPlan(
                        typeFilterPlan(
                                scanPlan()
                                        .where(scanComparisons(range("([1],>")))))
                        .where(mapResult(recordConstructorValue(exactly(fieldValueWithFieldNames("name"), fieldValueWithFieldNames("rest_no"))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testFailWithBadIndexHintGraph() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        final Optional<Collection<String>> allowedIndexesOptional = Optional.empty();
        final ParameterRelationshipGraph parameterRelationshipGraph = ParameterRelationshipGraph.empty();

        // with index hints ("review_rating"), cannot plan a query
        Assertions.assertThrows(RecordCoreException.class, () -> cascadesPlanner.planGraph(
                () -> {
                    final var allRecordTypes =
                            ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");

                    var qun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints(new IndexAccessHint("review_rating")))));

                    qun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                    qun,
                                    Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()))));

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(nameValue.getResultType(), Optional.of("nameNew")), nameValue));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(restNoValue.getResultType(), Optional.of("restNoNew")), restNoValue));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                allowedIndexesOptional,
                IndexQueryabilityFilter.TRUE,
                false,
                parameterRelationshipGraph));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanDifferentWithIndexHintGraph() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        // with index hints (RestaurantRecord$name), plan a different query
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    final var allRecordTypes =
                            ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");

                    var qun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints(new IndexAccessHint("RestaurantRecord$name")))));

                    qun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                    qun,
                                    Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()))));

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(nameValue.getResultType(), Optional.of("nameNew")), nameValue));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(restNoValue.getResultType(), Optional.of("restNoNew")), restNoValue));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());

        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                mapPlan(
                        fetchFromPartialRecordPlan(
                                predicatesFilterPlan(
                                        coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("RestaurantRecord$name")).and(scanComparisons(unbounded())))))
                                        .where(predicates(only(valuePredicate(ValueMatchers.fieldValueWithFieldNames("rest_no"), new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)))))));

        assertMatchesExactly(plan, planMatcher);
    }
    
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanSimpleJoin() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        // with index hints (RestaurantRecord$name), plan a different query
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    final var allRecordTypes =
                            ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");

                    var graphExpansionBuilder = GraphExpansion.builder();

                    var outerQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints())));

                    outerQun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                    outerQun,
                                    Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()))));

                    final var explodeQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()), "reviews"))));

                    graphExpansionBuilder.addQuantifier(outerQun);
                    graphExpansionBuilder.addQuantifier(explodeQun);
                    graphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()), "name"),
                            new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "name")));

                    final var explodeResultValue = QuantifiedObjectValue.of(explodeQun.getAlias(), explodeQun.getFlowedObjectType());
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(explodeResultValue.getResultType(), Optional.of("review")), explodeResultValue));
                    outerQun = Quantifier.forEach(GroupExpressionRef.of(
                            graphExpansionBuilder.build().buildSelect()));

                    graphExpansionBuilder = GraphExpansion.builder();
                    graphExpansionBuilder.addQuantifier(outerQun);

                    var innerQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints())));

                    innerQun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantReviewer"),
                                    innerQun,
                                    Type.Record.fromDescriptor(TestRecords4Proto.RestaurantReviewer.getDescriptor()))));

                    graphExpansionBuilder.addQuantifier(innerQun);

                    final var outerQuantifiedValue = QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType());
                    final var innerQuantifiedValue = QuantifiedObjectValue.of(innerQun.getAlias(), innerQun.getFlowedObjectType());

                    final var outerReviewerIdValue = FieldValue.ofFieldNames(outerQuantifiedValue, ImmutableList.of("review", "reviewer"));
                    final var innerReviewerIdValue = FieldValue.ofFieldName(innerQuantifiedValue, "id");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(outerReviewerIdValue, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, innerReviewerIdValue)));

                    final var reviewerNameValue = FieldValue.ofFieldName(innerQuantifiedValue, "name");
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(reviewerNameValue.getResultType(), Optional.of("reviewerName")), reviewerNameValue));
                    final var reviewRatingValue = FieldValue.ofFieldNames(outerQuantifiedValue, ImmutableList.of("review", "rating"));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(reviewRatingValue.getResultType(), Optional.of("reviewRating")), reviewRatingValue));

                    final var qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());

        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                descendantPlans(
                        flatMapPlan(
                                descendantPlans(
                                        indexPlan()
                                                .where(indexName("RestaurantRecord$name"))
                                                .and(scanComparisons(range("[[name],[name]]")))),
                                descendantPlans(typeFilterPlan(scanPlan().where(scanComparisons(unbounded())))
                                        .where(recordTypes(containsAll(ImmutableSet.of("RestaurantReviewer")))))));

        assertMatchesExactly(plan, planMatcher);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanFiveWayJoin() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        // find restaurants that where at least reviewed by two common reviewers
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    final var allRecordTypes =
                            ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");

                    var graphExpansionBuilder = GraphExpansion.builder();

                    var reviewer1Qun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints())));

                    reviewer1Qun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantReviewer"),
                                    reviewer1Qun,
                                    Type.Record.fromDescriptor(TestRecords4Proto.RestaurantReviewer.getDescriptor()))));
                    graphExpansionBuilder.addQuantifier(reviewer1Qun);

                    var reviewer2Qun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints())));

                    reviewer2Qun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantReviewer"),
                                    reviewer2Qun,
                                    Type.Record.fromDescriptor(TestRecords4Proto.RestaurantReviewer.getDescriptor()))));
                    graphExpansionBuilder.addQuantifier(reviewer2Qun);

                    var restaurantQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints())));

                    restaurantQun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                    restaurantQun,
                                    Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()))));
                    graphExpansionBuilder.addQuantifier(restaurantQun);

                    var reviewsGraphExpansionBuilder = GraphExpansion.builder();

                    var explodeReviewsQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "reviews"))));

                    reviewsGraphExpansionBuilder.addQuantifier(explodeReviewsQun);
                    reviewsGraphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType()), "reviewer"),
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, FieldValue.ofFieldName(QuantifiedObjectValue.of(reviewer1Qun.getAlias(), reviewer1Qun.getFlowedObjectType()), "id"))));

                    var explodeResultValue = QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType());
                    reviewsGraphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(explodeResultValue.getResultType(), Optional.of("review")), explodeResultValue));
                    final var existential1Quantifier = Quantifier.existential(GroupExpressionRef.of(reviewsGraphExpansionBuilder.build().buildSelect()));

                    graphExpansionBuilder.addQuantifier(existential1Quantifier);
                    graphExpansionBuilder.addPredicate(new ExistsPredicate(existential1Quantifier.getAlias()));

                    reviewsGraphExpansionBuilder = GraphExpansion.builder();

                    explodeReviewsQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "reviews"))));

                    reviewsGraphExpansionBuilder.addQuantifier(explodeReviewsQun);
                    reviewsGraphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType()), "reviewer"),
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, FieldValue.ofFieldName(QuantifiedObjectValue.of(reviewer1Qun.getAlias(), reviewer1Qun.getFlowedObjectType()), "id"))));

                    explodeResultValue = QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType());
                    reviewsGraphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(explodeResultValue.getResultType(), Optional.of("review")), explodeResultValue));
                    var existential2Quantifier = Quantifier.existential(GroupExpressionRef.of(reviewsGraphExpansionBuilder.build().buildSelect()));

                    graphExpansionBuilder.addQuantifier(existential2Quantifier);
                    graphExpansionBuilder.addPredicate(new ExistsPredicate(existential2Quantifier.getAlias()));

                    final var reviewer1QuantifiedValue = QuantifiedObjectValue.of(reviewer1Qun.getAlias(), reviewer1Qun.getFlowedObjectType());
                    final var reviewer2QuantifiedValue = QuantifiedObjectValue.of(reviewer2Qun.getAlias(), reviewer2Qun.getFlowedObjectType());
                    final var restaurantQuantifiedValue = QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType());

                    final var reviewer1NameValue = FieldValue.ofFieldName(reviewer1QuantifiedValue, "name");
                    final var reviewer2NameValue = FieldValue.ofFieldName(reviewer2QuantifiedValue, "name");
                    final var restaurantNameValue = FieldValue.ofFieldName(restaurantQuantifiedValue, "name");
                    final var restaurantNoValue = FieldValue.ofFieldName(restaurantQuantifiedValue, "rest_no");

                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(reviewer1NameValue.getResultType(), Optional.of("reviewer1Name")), reviewer1NameValue));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(reviewer2NameValue.getResultType(), Optional.of("reviewer2Name")), reviewer2NameValue));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(restaurantNameValue.getResultType(), Optional.of("restaurantName")), restaurantNameValue));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(restaurantNoValue.getResultType(), Optional.of("restaurantNo")), restaurantNameValue));

                    final var qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());

        // TODO write a matcher when this plan becomes more stable
        Assertions.assertTrue(plan instanceof RecordQueryMapPlan);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testSimplePlanWithConstantPredicateGraph() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();
        // no index hints, plan a query
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    final var allRecordTypes =
                            ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
                    var qun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints())));

                    qun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                    qun,
                                    Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()))));

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addPredicate(new ConstantPredicate(true));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(nameValue.getResultType(), Optional.of("nameNew")), nameValue));
                    graphExpansionBuilder.addResultColumn(Column.of(Type.Record.Field.of(restNoValue.getResultType(), Optional.of("restNoNew")), restNoValue));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());

        assertMatchesExactly(plan,
                mapPlan(predicatesFilterPlan(
                        typeFilterPlan(
                                scanPlan()
                                        .where(scanComparisons(range("([1],>"))))))
                        .where(mapResult(recordConstructorValue(exactly(ValueMatchers.fieldValueWithFieldNames("name"), ValueMatchers.fieldValueWithFieldNames("rest_no"))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanUpdateExpression() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();
        // no index hints, plan a query
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    final var allRecordTypes =
                            ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
                    var qun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints())));

                    final var restaurantType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor());
                    qun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                    qun,
                                    restaurantType)));

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelectWithResultValue(QuantifiedObjectValue.of(qun))));

                    // make accessors and resolve them
                    final var namePath = FieldValue.resolveFieldPath(qun.getFlowedObjectType(), ImmutableList.of(new FieldValue.Accessor("name", -1)));
                    //final var customerPath = FieldValue.resolveFieldPath(qun.getFlowedObjectType(), ImmutableList.of(new FieldValue.Accessor("customer", -1)));

                    qun = Quantifier.forEach(GroupExpressionRef.of(new UpdateExpression(qun,
                            "RestaurantRecord",
                            restaurantType,
                            TestRecords4Proto.RestaurantRecord.getDescriptor(),
                            ImmutableMap.of(namePath, LiteralValue.ofScalar("newName")))));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());

        plan.show(false);
        assertMatchesExactly(plan,
                mapPlan(
                        typeFilterPlan(
                                scanPlan()
                                        .where(scanComparisons(range("([1],>")))))
                        .where(mapResult(recordConstructorValue(exactly(ValueMatchers.fieldValueWithFieldNames("name"), ValueMatchers.fieldValueWithFieldNames("rest_no"))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanInsertExpression() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        final var plan = cascadesPlanner.planGraph(
                FDBSimpleQueryGraphTest::insertGraph,
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());

//        plan.show(false);
//        assertMatchesExactly(plan,
//                mapPlan(
//                        typeFilterPlan(
//                                scanPlan()
//                                        .where(scanComparisons(range("([1],>")))))
//                        .where(mapResult(recordConstructorValue(exactly(ValueMatchers.fieldValueWithFieldNames("name"), ValueMatchers.fieldValueWithFieldNames("rest_no"))))));

        final var resultValues = fetchResultValues(plan, this::openNestedRecordStore, record -> {
//            final var x = record.toByteArray();
//            try {
//                record = DynamicMessage.parseFrom(record.getDescriptorForType(), x);
//            } catch (InvalidProtocolBufferException e) {
//                throw new RuntimeException(e);
//            }
            final var recordDescriptor = record.getDescriptorForType();
            final var newRecordField = recordDescriptor.findFieldByName("new");
            final var newRecordDescriptor = newRecordField.getMessageType();
            final var newRecord = (Message)record.getField(newRecordField);
            final var rest_no = newRecordDescriptor.findFieldByName("rest_no");
            final var name = newRecordDescriptor.findFieldByName("name");
            switch ((int)(long)newRecord.getField(rest_no)) {
                case 100:
                    Assertions.assertEquals("Burger King", newRecord.getField(name));
                    break;
                case 200:
                    Assertions.assertEquals("Heirloom Cafe", newRecord.getField(name));
                    break;
                default:
                    Assertions.fail("unexpected record");
            }
            return record;
        });
        Assertions.assertEquals(2, resultValues.size());
    }

    @Nonnull
    private static GroupExpressionRef<RelationalExpression> insertGraph() {
        final var reviewsType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantReview.getDescriptor());
        final var tagsType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantTag.getDescriptor());
        final var customerType = Type.primitiveType(Type.TypeCode.STRING);

        final var bananaRecord = RecordConstructorValue.ofUnnamed(
                ImmutableList.of(LiteralValue.ofScalar(100L),
                        LiteralValue.ofScalar("Burger King"),
                        emptyArray(reviewsType),
                        emptyArray(tagsType),
                        emptyArray(customerType)));
        final var bestRecord = RecordConstructorValue.ofUnnamed(
                ImmutableList.of(LiteralValue.ofScalar(200L),
                        LiteralValue.ofScalar("Heirloom Cafe"),
                        emptyArray(reviewsType), // empty array
                        emptyArray(tagsType), // empty array
                        emptyArray(customerType))); // empty array
        final var explodeExpression = new ExplodeExpression(LightArrayConstructorValue.of(bananaRecord, bestRecord));
        var qun = Quantifier.forEach(GroupExpressionRef.of(explodeExpression));

        qun = Quantifier.forEach(GroupExpressionRef.of(new InsertExpression(qun,
                "RestaurantRecord",
                Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()),
                TestRecords4Proto.RestaurantRecord.getDescriptor())));
        return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
    }

    /**
     * Tests that an insert using NULLs for non-nullable columns fails.
     * @throws Exception if test fails
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanInsertExpressionBadNullAssignments() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        final var plan = cascadesPlanner.planGraph(
                () -> {
                    final var reviewsType = new Type.Array(Type.Record.fromDescriptor(TestRecords4Proto.RestaurantReview.getDescriptor()));
                    final var tagsType = new Type.Array(Type.Record.fromDescriptor(TestRecords4Proto.RestaurantTag.getDescriptor()));
                    final var customerType = new Type.Array(Type.primitiveType(Type.TypeCode.STRING));

                    final var bananaRecord = RecordConstructorValue.ofUnnamed(
                            ImmutableList.of(LiteralValue.ofScalar(100L),
                                    LiteralValue.ofScalar("Burger King"),
                                    new NullValue(reviewsType),
                                    new NullValue(tagsType),
                                    new NullValue(customerType)));
                    final var bestRecord = RecordConstructorValue.ofUnnamed(
                            ImmutableList.of(LiteralValue.ofScalar(200L),
                                    LiteralValue.ofScalar("Heirloom Cafe"),
                                    new NullValue(reviewsType),
                                    new NullValue(tagsType),
                                    new NullValue(customerType)));
                    final var explodeExpression = new ExplodeExpression(LightArrayConstructorValue.of(bananaRecord, bestRecord));
                    var qun = Quantifier.forEach(GroupExpressionRef.of(explodeExpression));

                    qun = Quantifier.forEach(GroupExpressionRef.of(new InsertExpression(qun,
                            "RestaurantRecord",
                            Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()),
                            TestRecords4Proto.RestaurantRecord.getDescriptor())));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());

        Assertions.assertThrows(RecordCoreException.class, () -> fetchResultValues(plan, this::openNestedRecordStore, Function.identity()));
    }

    @Nonnull
    private static GroupExpressionRef<RelationalExpression> selectRecordsGraph(@Nonnull final RecordMetaData metaData, Function<Quantifier.ForEach, QueryPredicate> predicateCreator) {
        final var restaurantType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor());

        final var allRecordTypes =
                ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
        var qun =
                Quantifier.forEach(GroupExpressionRef.of(
                        new FullUnorderedScanExpression(allRecordTypes,
                                Type.Record.fromFieldDescriptorsMap(metaData.getFieldDescriptorMapFromNames(allRecordTypes)),
                                new AccessHints())));

        qun = Quantifier.forEach(GroupExpressionRef.of(
                new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                        qun,
                        restaurantType)));

        var graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(qun);
        final var predicate = predicateCreator.apply(qun);
        if (predicate != null) {
            graphExpansionBuilder.addPredicate(predicate);
        }
        final var selectExpression = graphExpansionBuilder.build().buildSelectWithResultValue(qun.getFlowedObjectValue());
        qun = Quantifier.forEach(GroupExpressionRef.of(selectExpression));
        return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
    }

    @Nonnull
    private static QueryPredicate whereReviewsIsNullGraph(@Nonnull Quantifier.ForEach qun) {
        final var reviewsValue =
                FieldValue.ofFieldName(qun.getFlowedObjectValue(), "reviews");

        return new ValuePredicate(reviewsValue, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, ImmutableList.of()));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanUpsertGraph() throws Exception {

        //
        // This plans, executes, and verifies the equivalent of these SQL queries
        //
        // INSERT INTO Restaurants(rec_no, name)
        // VALUES (100, 'Burger King'),
        //        (200, 'Heirloom Cafe');
        //
        // and subsequently:
        //
        // INSERT INTO Restaurants(rec_no, name)
        // VALUES (300, 'Burger King'),
        //        (400, 'Bonita Burrito')
        // ON CONFLICT(name) SET name = 'duplicate';
        //
        // which is translated into
        //
        // INSERT INTO Restaurants(rec_no, name)
        // WHERE NOT EXISTS(UPDATE Restaurants r
        //                  WHERE v = r.name
        //                  SET name = 'duplicate')
        // FROM (VALUES (300, 'Burger King'),
        //              (400, 'Bonita Burrito')) v;
        //
        
        CascadesPlanner cascadesPlanner = setUp();

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);

            // insert 2 records
            var plan = cascadesPlanner.planGraph(
                    FDBSimpleQueryGraphTest::insertGraph,
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    false,
                    ParameterRelationshipGraph.empty());
            var resultValues = fetchResultValues(context, plan, Function.identity(), c -> { });

            plan = cascadesPlanner.planGraph(
                    () -> {
                        final var restaurantType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor());
                        final var reviewsType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantReview.getDescriptor());
                        final var tagsType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantTag.getDescriptor());
                        final var customerType = Type.primitiveType(Type.TypeCode.STRING);

                        final var bananaRecord = RecordConstructorValue.ofUnnamed(
                                ImmutableList.of(LiteralValue.ofScalar(300L),
                                        LiteralValue.ofScalar("Burger King"),
                                        emptyArray(reviewsType),
                                        emptyArray(tagsType),
                                        emptyArray(customerType)));
                        final var bestRecord = RecordConstructorValue.ofUnnamed(
                                ImmutableList.of(LiteralValue.ofScalar(400L),
                                        LiteralValue.ofScalar("Bonita Burrito"),
                                        emptyArray(reviewsType),
                                        emptyArray(tagsType),
                                        emptyArray(customerType)));
                        final var explodeExpression = new ExplodeExpression(LightArrayConstructorValue.of(bananaRecord, bestRecord));
                        var outerQun = Quantifier.forEach(GroupExpressionRef.of(explodeExpression));

                        final var allRecordTypes =
                                ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
                        var qun =
                                Quantifier.forEach(GroupExpressionRef.of(
                                        new FullUnorderedScanExpression(allRecordTypes,
                                                Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                                new AccessHints())));

                        qun = Quantifier.forEach(GroupExpressionRef.of(
                                new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                        qun,
                                        restaurantType)));

                        var graphExpansionBuilder = GraphExpansion.builder();

                        graphExpansionBuilder.addQuantifier(qun);
                        final var nameValue =
                                FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");

                        final var comparandValue = FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(outerQun), 1);
                        graphExpansionBuilder.addPredicate(new ValuePredicate(nameValue, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, comparandValue)));
                        qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelectWithResultValue(QuantifiedObjectValue.of(qun))));

                        // make accessors and resolve them
                        final var namePath = FieldValue.resolveFieldPath(qun.getFlowedObjectType(), ImmutableList.of(new FieldValue.Accessor("name", -1)));

                        final var innerQun = Quantifier.existential(GroupExpressionRef.of(new UpdateExpression(qun,
                                "RestaurantRecord",
                                restaurantType,
                                TestRecords4Proto.RestaurantRecord.getDescriptor(),
                                ImmutableMap.of(namePath, LiteralValue.ofScalar("duplicate")))));

                        graphExpansionBuilder = GraphExpansion.builder();
                        graphExpansionBuilder.addQuantifier(outerQun);
                        graphExpansionBuilder.addQuantifier(innerQun);
                        graphExpansionBuilder.addPredicate(new NotPredicate(new ExistsPredicate(innerQun.getAlias())));
                        qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelectWithResultValue(outerQun.getFlowedObjectValue())));

                        qun = Quantifier.forEach(GroupExpressionRef.of(new InsertExpression(qun,
                                "RestaurantRecord",
                                Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()),
                                TestRecords4Proto.RestaurantRecord.getDescriptor())));

                        return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                    },
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    false,
                    ParameterRelationshipGraph.empty());

            plan.show(false);
//        assertMatchesExactly(plan,
//                mapPlan(
//                        typeFilterPlan(
//                                scanPlan()
//                                        .where(scanComparisons(range("([1],>")))))
//                        .where(mapResult(recordConstructorValue(exactly(ValueMatchers.fieldValueWithFieldNames("name"), ValueMatchers.fieldValueWithFieldNames("rest_no"))))));

            resultValues = fetchResultValues(context, plan, Function.identity(), c -> { });
            //System.out.println(resultValues);

            plan = cascadesPlanner.planGraph(() -> selectRecordsGraph(cascadesPlanner.getRecordMetaData(), FDBSimpleQueryGraphTest::whereReviewsIsNullGraph),
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    false,
                    ParameterRelationshipGraph.empty());
            resultValues = fetchResultValues(context, plan, Function.identity(), c -> { });

            System.out.println(resultValues);
        }
    }

    @Nonnull
    private CascadesPlanner setUp() throws Exception {
        final CascadesPlanner cascadesPlanner;

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);

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
        return cascadesPlanner;
    }

    @Nonnull
    private CascadesPlanner setUpWithNullableArray() throws Exception {
        final CascadesPlanner cascadesPlanner;

        try (FDBRecordContext context = openContext()) {
            openNestedWrappedArrayRecordStore(context);

            cascadesPlanner = (CascadesPlanner)planner;

            TestRecords4WrapperProto.RestaurantReviewer reviewer = TestRecords4WrapperProto.RestaurantReviewer.newBuilder()
                    .setId(1L)
                    .setName("Javert")
                    .setEmail("inspecteur@policier.fr")
                    .setStats(TestRecords4WrapperProto.ReviewerStats.newBuilder()
                            .setStartDate(100L)
                            .setHometown("Toulon")
                    )
                    .build();
            recordStore.saveRecord(reviewer);

            reviewer = TestRecords4WrapperProto.RestaurantReviewer.newBuilder()
                    .setId(2L)
                    .setName("M. le Maire")
                    .setStats(TestRecords4WrapperProto.ReviewerStats.newBuilder()
                            .setStartDate(120L)
                            .setHometown("Montreuil-sur-mer")
                    )
                    .build();
            recordStore.saveRecord(reviewer);

            TestRecords4WrapperProto.RestaurantRecord restaurant = TestRecords4WrapperProto.RestaurantRecord.newBuilder()
                    .setRestNo(1000L)
                    .setName("Chez Thénardier")
                    .setReviews(TestRecords4WrapperProto.RestaurantReviewList.newBuilder()
                            .addValues(TestRecords4WrapperProto.RestaurantReview.newBuilder().setReviewer(1L).setRating(100))
                            .addValues(TestRecords4WrapperProto.RestaurantReview.newBuilder().setReviewer(2L).setRating(0)))
                    .setTags(TestRecords4WrapperProto.RestaurantTagList.newBuilder()
                            .addValues(TestRecords4WrapperProto.RestaurantTag.newBuilder().setValue("l'atmosphère").setWeight(10))
                            .addValues(TestRecords4WrapperProto.RestaurantTag.newBuilder().setValue("les aliments").setWeight(70)))
                    .setCustomer(TestRecords4WrapperProto.StringList.newBuilder()
                            .addValues("jean")
                            .addValues("fantine")
                            .addValues("cosette")
                            .addValues("éponine"))
                    .build();
            recordStore.saveRecord(restaurant);

            restaurant = TestRecords4WrapperProto.RestaurantRecord.newBuilder()
                    .setRestNo(1001L)
                    .setName("ABC")
                    .setReviews(TestRecords4WrapperProto.RestaurantReviewList.newBuilder()
                            .addValues(TestRecords4WrapperProto.RestaurantReview.newBuilder().setReviewer(1L).setRating(34))
                            .addValues(TestRecords4WrapperProto.RestaurantReview.newBuilder().setReviewer(2L).setRating(110)))
                    .setTags(TestRecords4WrapperProto.RestaurantTagList.newBuilder()
                            .addValues(TestRecords4WrapperProto.RestaurantTag.newBuilder().setValue("l'atmosphère").setWeight(40))
                            .addValues(TestRecords4WrapperProto.RestaurantTag.newBuilder().setValue("les aliments").setWeight(20)))
                    .setCustomer(TestRecords4WrapperProto.StringList.newBuilder()
                            .addValues("gavroche")
                            .addValues("enjolras")
                            .addValues("éponine"))
                    .build();
            recordStore.saveRecord(restaurant);

            commit(context);
        }
        return cascadesPlanner;
    }
}
