/*
 * FDBSimpleQueryGraphTest.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.containsAll;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.descendantPlans;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests of query planning and execution for queries that use a query graph to define a query instead of
 * {@link com.apple.foundationdb.record.query.RecordQuery}.
 */
@Tag(Tags.RequiresFDB)
public class FDBSimpleQueryGraphTest extends FDBRecordStoreQueryTestBase {
    @Nonnull
    static Quantifier fullScan(@Nonnull RecordMetaData metaData, AccessHints hints) {
        Set<String> allRecordTypes = ImmutableSet.copyOf(metaData.getRecordTypes().keySet());
        return Quantifier.forEach(GroupExpressionRef.of(
                new FullUnorderedScanExpression(allRecordTypes,
                        Type.Record.fromFieldDescriptorsMap(metaData.getFieldDescriptorMapFromNames(allRecordTypes)),
                        hints)));
    }

    @Nonnull
    static Quantifier fullScan(@Nonnull RecordMetaData metaData) {
        return fullScan(metaData, new AccessHints());
    }

    @Nonnull
    static Quantifier fullTypeScan(@Nonnull RecordMetaData metaData, @Nonnull String typeName, @Nonnull Quantifier fullScanQun) {
        return Quantifier.forEach(GroupExpressionRef.of(
                new LogicalTypeFilterExpression(ImmutableSet.of(typeName),
                        fullScanQun,
                        Type.Record.fromDescriptor(metaData.getRecordType(typeName).getDescriptor()))));
    }

    @Nonnull
    static Quantifier fullTypeScan(@Nonnull RecordMetaData metaData, @Nonnull String typeName) {
        return fullTypeScan(metaData, typeName, fullScan(metaData));
    }

    @Nonnull
    static <V extends Value> Column<V> resultColumn(@Nonnull V value, @Nullable String name) {
        return Column.of(Type.Record.Field.of(value.getResultType(), Optional.ofNullable(name)), value);
    }

    static RecordCursor<QueryResult> executeCascades(FDBRecordStore store, RecordQueryPlan plan) {
        Set<Type> usedTypes = UsedTypesProperty.evaluate(plan);
        TypeRepository typeRepository = TypeRepository.newBuilder()
                .addAllTypes(usedTypes)
                .build();
        EvaluationContext evaluationContext = EvaluationContext.forTypeRepository(typeRepository);
        return plan.executePlan(store, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE);
    }

    static <T> T getField(QueryResult result, Class<T> type, String... path) {
        Message message = result.getMessage();
        for (int i = 0; i < path.length; i++) {
            String fieldName = path[i];
            Descriptors.Descriptor messageDescriptor = message.getDescriptorForType();
            Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName(fieldName);
            assertNotNull(fieldDescriptor, () -> String.format("expected to find field %s in descriptor: %s", fieldName, messageDescriptor));
            Object field = message.getField(fieldDescriptor);
            if (i < path.length - 1) {
                assertThat(field, Matchers.instanceOf(Message.class));
                message = (Message) field;
            } else {
                if (field == null) {
                    return null;
                }
                assertThat(field, Matchers.instanceOf(type));
                return type.cast(field);
            }
        }
        return null;
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testSimplePlanGraph() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();
        // no index hints, plan a query
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    var qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addResultColumn(resultColumn(nameValue, "nameNew"));
                    graphExpansionBuilder.addResultColumn(resultColumn(restNoValue, "restNoNew"));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                EvaluationContext.empty()).getPlan();

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
                    var qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");
                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addResultColumn(resultColumn(nameValue, "nameNew"));
                    graphExpansionBuilder.addResultColumn(resultColumn(restNoValue, "restNoNew"));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                EvaluationContext.empty()).getPlan();
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
        final EvaluationContext parameterRelationshipGraph = EvaluationContext.empty();

        // with index hints ("review_rating"), cannot plan a query
        Assertions.assertThrows(RecordCoreException.class, () -> cascadesPlanner.planGraph(
                () -> {
                    IndexAccessHint badHint = new IndexAccessHint("review_rating");
                    var qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord", fullScan(cascadesPlanner.getRecordMetaData(), new AccessHints(badHint)));

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addResultColumn(resultColumn(nameValue, "nameNew"));
                    graphExpansionBuilder.addResultColumn(resultColumn(restNoValue, "restNoNew"));
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
                    IndexAccessHint nameHint = new IndexAccessHint("RestaurantRecord$name");
                    var qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord", fullScan(cascadesPlanner.getRecordMetaData(), new AccessHints(nameHint)));

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addResultColumn(resultColumn(nameValue, "nameNew"));
                    graphExpansionBuilder.addResultColumn(resultColumn(restNoValue, "restNoNew"));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                EvaluationContext.empty()).getPlan();

        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                mapPlan(
                        predicatesFilterPlan(
                                coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("RestaurantRecord$name")).and(scanComparisons(unbounded())))))
                                .where(predicates(only(valuePredicate(ValueMatchers.fieldValueWithFieldNames("rest_no"), new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L))))));

        assertMatchesExactly(plan, planMatcher);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanCrossProductJoin() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        // with index hints (RestaurantRecord$name), plan a different query
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    var outerQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");
                    final var explodeQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()), "reviews"))));

                    var graphExpansionBuilder = GraphExpansion.builder();
                    graphExpansionBuilder.addQuantifier(outerQun);
                    graphExpansionBuilder.addQuantifier(explodeQun);
                    graphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()), "name"),
                            new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "name")));

                    final var explodeResultValue = QuantifiedObjectValue.of(explodeQun.getAlias(), explodeQun.getFlowedObjectType());
                    graphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
                    outerQun = Quantifier.forEach(GroupExpressionRef.of(
                            graphExpansionBuilder.build().buildSelect()));

                    graphExpansionBuilder = GraphExpansion.builder();
                    graphExpansionBuilder.addQuantifier(outerQun);

                    var innerQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantReviewer");
                    graphExpansionBuilder.addQuantifier(innerQun);

                    final var outerQuantifiedValue = QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType());
                    final var innerQuantifiedValue = QuantifiedObjectValue.of(innerQun.getAlias(), innerQun.getFlowedObjectType());

                    final var reviewerNameValue = FieldValue.ofFieldName(innerQuantifiedValue, "name");
                    graphExpansionBuilder.addResultColumn(resultColumn(reviewerNameValue, "reviewerName"));
                    final var reviewRatingValue = FieldValue.ofFieldNames(outerQuantifiedValue, ImmutableList.of("review", "rating"));
                    graphExpansionBuilder.addResultColumn(resultColumn(reviewRatingValue, "reviewRating"));

                    final var qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                EvaluationContext.empty());

        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                flatMapPlan(
                        typeFilterPlan(scanPlan().where(scanComparisons(unbounded())))
                                .where(recordTypes(containsAll(ImmutableSet.of("RestaurantReviewer")))),
                        descendantPlans(
                                indexPlan()
                                        .where(indexName("RestaurantRecord$name"))
                                        .and(scanComparisons(range("[[name],[name]]")))));

        assertMatchesExactly(plan.getPlan(), planMatcher);
    }
    
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanSimpleJoin() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        // with index hints (RestaurantRecord$name), plan a different query
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    var outerQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");
                    final var explodeQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()), "reviews"))));

                    var graphExpansionBuilder = GraphExpansion.builder();
                    graphExpansionBuilder.addQuantifier(outerQun);
                    graphExpansionBuilder.addQuantifier(explodeQun);
                    graphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()), "name"),
                            new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "name")));

                    final var explodeResultValue = QuantifiedObjectValue.of(explodeQun.getAlias(), explodeQun.getFlowedObjectType());
                    graphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
                    outerQun = Quantifier.forEach(GroupExpressionRef.of(
                            graphExpansionBuilder.build().buildSelect()));

                    graphExpansionBuilder = GraphExpansion.builder();
                    graphExpansionBuilder.addQuantifier(outerQun);

                    var innerQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantReviewer");
                    graphExpansionBuilder.addQuantifier(innerQun);

                    final var outerQuantifiedValue = QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType());
                    final var innerQuantifiedValue = QuantifiedObjectValue.of(innerQun.getAlias(), innerQun.getFlowedObjectType());

                    final var outerReviewerIdValue = FieldValue.ofFieldNames(outerQuantifiedValue, ImmutableList.of("review", "reviewer"));
                    final var innerReviewerIdValue = FieldValue.ofFieldName(innerQuantifiedValue, "id");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(innerReviewerIdValue, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, outerReviewerIdValue)));

                    final var reviewerNameValue = FieldValue.ofFieldName(innerQuantifiedValue, "name");
                    graphExpansionBuilder.addResultColumn(resultColumn(reviewerNameValue, "reviewerName"));
                    final var reviewRatingValue = FieldValue.ofFieldNames(outerQuantifiedValue, ImmutableList.of("review", "rating"));
                    graphExpansionBuilder.addResultColumn(resultColumn(reviewRatingValue, "reviewRating"));

                    final var qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                EvaluationContext.empty()).getPlan();

        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                flatMapPlan(
                        descendantPlans(
                                indexPlan()
                                        .where(indexName("RestaurantRecord$name"))
                                        .and(scanComparisons(range("[[name],[name]]")))),
                        typeFilterPlan(scanPlan().where(scanComparisons(range("[EQUALS $q6.review.reviewer]"))))
                                .where(recordTypes(containsAll(ImmutableSet.of("RestaurantReviewer")))));

        assertMatchesExactly(plan, planMatcher);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testMediumJoin() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        // find restaurants that where at least reviewed by two common reviewers
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    var graphExpansionBuilder = GraphExpansion.builder();

                    var reviewer1Qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantReviewer");
                    graphExpansionBuilder.addQuantifier(reviewer1Qun);

                    var reviewer2Qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantReviewer");
                    graphExpansionBuilder.addQuantifier(reviewer2Qun);

                    var restaurantQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");
                    graphExpansionBuilder.addQuantifier(restaurantQun);

                    var reviewsGraphExpansionBuilder = GraphExpansion.builder();

                    var explodeReviewsQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "reviews"))));

                    reviewsGraphExpansionBuilder.addQuantifier(explodeReviewsQun);
                    reviewsGraphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType()), "reviewer"),
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, FieldValue.ofFieldName(QuantifiedObjectValue.of(reviewer1Qun.getAlias(), reviewer1Qun.getFlowedObjectType()), "id"))));

                    var explodeResultValue = QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType());
                    reviewsGraphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
                    final var existential1Quantifier = Quantifier.existential(GroupExpressionRef.of(reviewsGraphExpansionBuilder.build().buildSelect()));

                    graphExpansionBuilder.addQuantifier(existential1Quantifier);
                    graphExpansionBuilder.addPredicate(new ExistsPredicate(existential1Quantifier.getAlias()));

                    reviewsGraphExpansionBuilder = GraphExpansion.builder();

                    explodeReviewsQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "reviews"))));

                    reviewsGraphExpansionBuilder.addQuantifier(explodeReviewsQun);
                    reviewsGraphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType()), "reviewer"),
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, FieldValue.ofFieldName(QuantifiedObjectValue.of(reviewer2Qun.getAlias(), reviewer2Qun.getFlowedObjectType()), "id"))));

                    explodeResultValue = QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType());
                    reviewsGraphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
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

                    graphExpansionBuilder.addResultColumn(resultColumn(reviewer1NameValue, "reviewer1Name"));
                    graphExpansionBuilder.addResultColumn(resultColumn(reviewer2NameValue, "reviewer2Name"));
                    graphExpansionBuilder.addResultColumn(resultColumn(restaurantNameValue, "restaurantName"));
                    graphExpansionBuilder.addResultColumn(resultColumn(restaurantNoValue, "restaurantNo"));

                    final var qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                EvaluationContext.empty());

        // TODO write a matcher when this plan becomes more stable
        Assertions.assertTrue(plan.getPlan() instanceof RecordQueryFlatMapPlan);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanFiveWayJoin() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        // find restaurants that where at least reviewed by two common reviewers
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    var graphExpansionBuilder = GraphExpansion.builder();

                    var reviewer1Qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantReviewer");
                    graphExpansionBuilder.addQuantifier(reviewer1Qun);

                    var reviewer2Qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantReviewer");
                    graphExpansionBuilder.addQuantifier(reviewer2Qun);

                    var restaurantQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");
                    graphExpansionBuilder.addQuantifier(restaurantQun);

                    var reviewsGraphExpansionBuilder = GraphExpansion.builder();

                    var explodeReviewsQun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "reviews"))));

                    reviewsGraphExpansionBuilder.addQuantifier(explodeReviewsQun);
                    reviewsGraphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType()), "reviewer"),
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, FieldValue.ofFieldName(QuantifiedObjectValue.of(reviewer1Qun.getAlias(), reviewer1Qun.getFlowedObjectType()), "id"))));

                    var explodeResultValue = QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType());
                    reviewsGraphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
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
                    reviewsGraphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
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

                    graphExpansionBuilder.addResultColumn(resultColumn(reviewer1NameValue, "reviewer1Name"));
                    graphExpansionBuilder.addResultColumn(resultColumn(reviewer2NameValue, "reviewer2Name"));
                    graphExpansionBuilder.addResultColumn(resultColumn(restaurantNameValue, "restaurantName"));
                    graphExpansionBuilder.addResultColumn(resultColumn(restaurantNoValue, "restaurantNo"));

                    final var qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                EvaluationContext.empty()).getPlan();

        // TODO write a matcher when this plan becomes more stable
        Assertions.assertTrue(plan instanceof RecordQueryFlatMapPlan);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testSimplePlanWithConstantPredicateGraph() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();
        // no index hints, plan a query
        final var plan = cascadesPlanner.planGraph(
                () -> {
                    var qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var nameValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");
                    final var restNoValue =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L)));
                    graphExpansionBuilder.addPredicate(new ConstantPredicate(true));
                    graphExpansionBuilder.addResultColumn(resultColumn(nameValue, "nameNew"));
                    graphExpansionBuilder.addResultColumn(resultColumn(restNoValue, "restNoNew"));
                    qun = Quantifier.forEach(GroupExpressionRef.of(graphExpansionBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                EvaluationContext.empty()).getPlan();

        assertMatchesExactly(plan,
                mapPlan(typeFilterPlan(
                                scanPlan()
                                        .where(scanComparisons(range("([1],>")))))
                        .where(mapResult(recordConstructorValue(exactly(ValueMatchers.fieldValueWithFieldNames("name"), ValueMatchers.fieldValueWithFieldNames("rest_no"))))));
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
