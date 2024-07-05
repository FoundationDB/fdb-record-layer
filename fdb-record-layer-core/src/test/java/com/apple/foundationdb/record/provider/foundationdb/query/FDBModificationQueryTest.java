/*
 * FDBModificationQueryTest.java
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
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.DeleteExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.InsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.UpdateExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue.LightArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.equalsObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.deletePlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.descendantPlans;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.explodePlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.flatMapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.insertPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.target;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.typeFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unorderedPrimaryKeyDistinctPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.updatePlan;
import static com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue.LightArrayConstructorValue.emptyArray;

/**
 * Tests of query planning and execution for query graphs that modify the state of the database.
 */
@Tag(Tags.RequiresFDB)
public class FDBModificationQueryTest extends FDBRecordStoreQueryTestBase {
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanDeleteExpression() throws Exception {
        final var cascadesPlanner = setUp();

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);

            // insert 2 records
            var plan = cascadesPlanner.planGraph(
                    FDBModificationQueryTest::insertGraph,
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();
            fetchResultValues(context, plan, Function.identity(), c -> {
            });

            plan = cascadesPlanner.planGraph(
                    () -> {
                        final var restaurantType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor());

                        final var allRecordTypes =
                                ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
                        var qun =
                                Quantifier.forEach(Reference.of(
                                        new FullUnorderedScanExpression(allRecordTypes,
                                                new Type.AnyRecord(false),
                                                new AccessHints())));

                        qun = Quantifier.forEach(Reference.of(
                                new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                        qun,
                                        restaurantType)));

                        var graphExpansionBuilder = GraphExpansion.builder();

                        graphExpansionBuilder.addQuantifier(qun);
                        final var restNoValue =
                                FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                        graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 100L)));
                        qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelectWithResultValue(QuantifiedObjectValue.of(qun))));

                        // make accessors and resolve them
                        qun = Quantifier.forEach(Reference.of(new DeleteExpression(qun, "RestaurantRecord")));

                        return Reference.of(LogicalSortExpression.unsorted(qun));
                    },
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();

            assertMatchesExactly(plan,
                    deletePlan(typeFilterPlan(scanPlan())));

            // dry run delete 1 record
            var resultValues = fetchResultValues(context, plan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var restNo = recordDescriptor.findFieldByName("rest_no");
                final var name = recordDescriptor.findFieldByName("name");
                if ((long)record.getField(restNo) == 100L) {
                    Assertions.assertEquals("Burger King", record.getField(name));
                } else {
                    Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            }, ExecuteProperties.newBuilder().setDryRun(true).build());
            Assertions.assertEquals(1, resultValues.size());

            final var selectPlan = cascadesPlanner.planGraph(() -> selectRecordsGraph(FDBModificationQueryTest::whereReviewsIsEmptyGraph),
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();
            // select returns 2 records
            resultValues = fetchResultValues(context, selectPlan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var rest_no = recordDescriptor.findFieldByName("rest_no");
                final var name = recordDescriptor.findFieldByName("name");
                if ((int)(long)record.getField(rest_no) == 100) {
                    Assertions.assertEquals("Burger King", record.getField(name));
                } else if ((int)(long)record.getField(rest_no) == 200) {
                    Assertions.assertEquals("Heirloom Cafe", record.getField(name)); // untouched record
                } else {
                    Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            });
            Assertions.assertEquals(2, resultValues.size());

            // wet run delete 1 record
            resultValues = fetchResultValues(context, plan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var restNo = recordDescriptor.findFieldByName("rest_no");
                final var name = recordDescriptor.findFieldByName("name");
                if ((long)record.getField(restNo) == 100L) {
                    Assertions.assertEquals("Burger King", record.getField(name));
                } else {
                    Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            });
            Assertions.assertEquals(1, resultValues.size());

            // select returns the untouched 1 record
            resultValues = fetchResultValues(context, selectPlan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var rest_no = recordDescriptor.findFieldByName("rest_no");
                final var name = recordDescriptor.findFieldByName("name");
                if ((int)(long)record.getField(rest_no) == 200) {
                    Assertions.assertEquals("Heirloom Cafe", record.getField(name)); // untouched record
                } else {
                    Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            });
            Assertions.assertEquals(1, resultValues.size());
        }
    }

    /**
     * Tests basic insert functionality.
     *
     * <pre>
     * {@code
     *   INSERT INTO RestaurantRecord
     *   VALUES (100, 'Burger King', (), (), ()),
     *          (200, 'Heirloom Cafe', (), (), ());
     * }
     * </pre>
     * @throws Exception if problem
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanInsertExpression() throws Exception {
        CascadesPlanner cascadesPlanner = setUp();

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);

            final var plan = cascadesPlanner.planGraph(
                    FDBModificationQueryTest::insertGraph,
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();

            assertMatchesExactly(plan, insertPlan(explodePlan()).where(target(equalsObject("RestaurantRecord"))));
            // dry run insert 2 records
            var resultValues = fetchResultValues(context, plan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var rest_no = recordDescriptor.findFieldByName("rest_no");
                final var name = recordDescriptor.findFieldByName("name");
                switch ((int)(long)record.getField(rest_no)) {
                    case 100:
                        Assertions.assertEquals("Burger King", record.getField(name));
                        break;
                    case 200:
                        Assertions.assertEquals("Heirloom Cafe", record.getField(name));
                        break;
                    default:
                        Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            }, ExecuteProperties.newBuilder().setDryRun(true).build());
            Assertions.assertEquals(2, resultValues.size());

            final var selectPlan = cascadesPlanner.planGraph(() -> selectRecordsGraph(FDBModificationQueryTest::whereReviewsIsEmptyGraph),
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();
            // select returns 0 record
            resultValues = fetchResultValues(context, selectPlan, record -> record, c -> {
            });
            Assertions.assertEquals(0, resultValues.size());

            // wet run insert 2 records
            resultValues = fetchResultValues(context, plan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var rest_no = recordDescriptor.findFieldByName("rest_no");
                final var name = recordDescriptor.findFieldByName("name");
                switch ((int)(long)record.getField(rest_no)) {
                    case 100:
                        Assertions.assertEquals("Burger King", record.getField(name));
                        break;
                    case 200:
                        Assertions.assertEquals("Heirloom Cafe", record.getField(name));
                        break;
                    default:
                        Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            });
            Assertions.assertEquals(2, resultValues.size());
            // select returns 2 records
            resultValues = fetchResultValues(context, selectPlan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var rest_no = recordDescriptor.findFieldByName("rest_no");
                final var name = recordDescriptor.findFieldByName("name");
                switch ((int)(long)record.getField(rest_no)) {
                    case 100:
                        Assertions.assertEquals("Burger King", record.getField(name));
                        break;
                    case 200:
                        Assertions.assertEquals("Heirloom Cafe", record.getField(name));
                        break;
                    default:
                        Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            });
            Assertions.assertEquals(2, resultValues.size());
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testInsertExistingRecordThrowsException() throws Exception {
        final var cascadesPlanner = setUp();

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);

            // insert 2 records
            var plan = cascadesPlanner.planGraph(
                    FDBModificationQueryTest::insertGraph,
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();
            fetchResultValues(context, plan, Function.identity(), c -> {
            });
            // after inserting, try inserting again, throws RecordAlreadyExistsException
            final var usedTypes = UsedTypesProperty.evaluate(plan);
            final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(usedTypes).build());
            try (RecordCursorIterator<QueryResult> cursor = plan.executePlan(recordStore, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE).asIterator()) {
                RecordCoreException ex1 = Assertions.assertThrows(RecordCoreException.class, cursor::hasNext);
                Assertions.assertTrue(ex1.getMessage().contains("record already exists"));
            }
            // dry run insert again also throws RecordAlreadyExistsException
            try (RecordCursorIterator<QueryResult> cursor = plan.executePlan(recordStore, evaluationContext, null, ExecuteProperties.newBuilder().setDryRun(true).build()).asIterator()) {
                RecordCoreException ex2 = Assertions.assertThrows(RecordCoreException.class, cursor::hasNext);
                Assertions.assertTrue(ex2.getMessage().contains("record already exists"));
            }
        }
    }

    @Nonnull
    private static Reference insertGraph() {
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
        var qun = Quantifier.forEach(Reference.of(explodeExpression));

        qun = Quantifier.forEach(Reference.of(new InsertExpression(qun,
                "RestaurantRecord",
                Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()))));
        return Reference.of(LogicalSortExpression.unsorted(qun));
    }

    /**
     * Tests that an insert using NULLs for non-nullable columns fails.
     *
     * <pre>
     * {@code
     *   INSERT INTO RestaurantRecord
     *   VALUES (100, 'Burger King', null, null, null),
     *          (200, 'Heirloom Cafe', null, null, null);
     * }
     * </pre>
     * @throws Exception if problem
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
                    var qun = Quantifier.forEach(Reference.of(explodeExpression));

                    qun = Quantifier.forEach(Reference.of(new InsertExpression(qun,
                            "RestaurantRecord",
                            Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()))));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();

        Assertions.assertThrows(RecordCoreException.class, () -> fetchResultValues(plan, this::openNestedRecordStore, Function.identity()));
    }

    @Nonnull
    private static Reference selectRecordsGraph(Function<Quantifier.ForEach, QueryPredicate> predicateCreator) {
        final var restaurantType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor());

        final var allRecordTypes =
                ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
        var qun =
                Quantifier.forEach(Reference.of(
                        new FullUnorderedScanExpression(allRecordTypes,
                                new Type.AnyRecord(false),
                                new AccessHints())));

        qun = Quantifier.forEach(Reference.of(
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
        qun = Quantifier.forEach(Reference.of(selectExpression));
        return Reference.of(LogicalSortExpression.unsorted(qun));
    }

    @Nonnull
    private static QueryPredicate whereReviewsIsEmptyGraph(@Nonnull Quantifier.ForEach qun) {
        final var reviewsValue =
                FieldValue.ofFieldName(qun.getFlowedObjectValue(), "reviews");

        return new ValuePredicate(reviewsValue, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, ImmutableList.of()));
    }

    /**
     * Tests basic update functionality.
     *
     * <pre>
     * {@code
     *   UPDATE RestaurantRecord
     *   WHERE rest_no = 100
     *   SET name = name + ' McDonald\'s'
     * }
     * </pre>
     * @throws Exception if problem
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanUpdateExpression() throws Exception {
        final var cascadesPlanner = setUp();

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);

            // insert 2 records
            var plan = cascadesPlanner.planGraph(
                    FDBModificationQueryTest::insertGraph,
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();
            fetchResultValues(context, plan, Function.identity(), c -> {
            });

            plan = planGraph(
                    () -> {
                        final var restaurantType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor());

                        final var allRecordTypes =
                                ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
                        var qun =
                                Quantifier.forEach(Reference.of(
                                        new FullUnorderedScanExpression(allRecordTypes,
                                                new Type.AnyRecord(false),
                                                new AccessHints())));

                        qun = Quantifier.forEach(Reference.of(
                                new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                        qun,
                                        restaurantType)));

                        var graphExpansionBuilder = GraphExpansion.builder();

                        graphExpansionBuilder.addQuantifier(qun);
                        final var restNoValue =
                                FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                        graphExpansionBuilder.addPredicate(new ValuePredicate(restNoValue, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 100L)));
                        qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelectWithResultValue(QuantifiedObjectValue.of(qun))));

                        // make accessors and resolve them
                        final var updatePath = FieldValue.resolveFieldPath(qun.getFlowedObjectType(), ImmutableList.of(new FieldValue.Accessor("name", -1)));
                        final var updateValue = new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_SS, FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name"), LiteralValue.ofScalar(" McDonald's"));
                        qun = Quantifier.forEach(Reference.of(new UpdateExpression(qun,
                                "RestaurantRecord",
                                restaurantType,
                                ImmutableMap.of(updatePath, updateValue))));

                        return Reference.of(LogicalSortExpression.unsorted(qun));
                    });

            assertMatchesExactly(plan,
                    updatePlan(unorderedPrimaryKeyDistinctPlan(
                            typeFilterPlan(scanPlan())))
                            .where(target(equalsObject("RestaurantRecord"))));

            var resultValues = fetchResultValues(context, plan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var oldRecordField = recordDescriptor.findFieldByName("old");
                final var oldRecordDescriptor = oldRecordField.getMessageType();
                final var oldRecord = (Message)record.getField(oldRecordField);
                final var oldRestNo = oldRecordDescriptor.findFieldByName("rest_no");
                final var oldName = oldRecordDescriptor.findFieldByName("name");
                final var newRecordField = recordDescriptor.findFieldByName("new");
                final var newRecordDescriptor = newRecordField.getMessageType();
                final var newRecord = (Message)record.getField(newRecordField);
                final var newRestNo = newRecordDescriptor.findFieldByName("rest_no");
                final var newName = newRecordDescriptor.findFieldByName("name");
                if ((long)newRecord.getField(newRestNo) == 100L) {
                    Assertions.assertEquals(100L, (long)oldRecord.getField(oldRestNo));
                    Assertions.assertEquals("Burger King", oldRecord.getField(oldName));
                    Assertions.assertEquals("Burger King McDonald's", newRecord.getField(newName));
                } else {
                    Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            });
            Assertions.assertEquals(1, resultValues.size());
            // dryRun update plan
            ExecuteProperties dryRunExecuteProperties = ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setState(ExecuteState.NO_LIMITS)
                    .setDryRun(true)
                    .build();
            var dryRunResultValues = fetchResultValues(context, plan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var oldRecordField = recordDescriptor.findFieldByName("old");
                final var oldRecordDescriptor = oldRecordField.getMessageType();
                final var oldRecord = (Message)record.getField(oldRecordField);
                final var oldRestNo = oldRecordDescriptor.findFieldByName("rest_no");
                final var oldName = oldRecordDescriptor.findFieldByName("name");
                final var newRecordField = recordDescriptor.findFieldByName("new");
                final var newRecordDescriptor = newRecordField.getMessageType();
                final var newRecord = (Message)record.getField(newRecordField);
                final var newRestNo = newRecordDescriptor.findFieldByName("rest_no");
                final var newName = newRecordDescriptor.findFieldByName("name");
                if ((long)newRecord.getField(newRestNo) == 100L) {
                    Assertions.assertEquals(100L, (long)oldRecord.getField(oldRestNo));
                    Assertions.assertEquals("Burger King McDonald's", oldRecord.getField(oldName));
                    // resultValue returns the new value, but new value shouldn't be written to database
                    // the selectPlan below should still returns Burger King McDonald's
                    Assertions.assertEquals("Burger King McDonald's McDonald's", newRecord.getField(newName));
                } else {
                    Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            }, dryRunExecuteProperties);
            Assertions.assertEquals(1, dryRunResultValues.size());

            final var selectPlan = cascadesPlanner.planGraph(() -> selectRecordsGraph(FDBModificationQueryTest::whereReviewsIsEmptyGraph),
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();
            resultValues = fetchResultValues(context, selectPlan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var rest_no = recordDescriptor.findFieldByName("rest_no");
                final var name = recordDescriptor.findFieldByName("name");
                switch ((int)(long)record.getField(rest_no)) {
                    case 100:
                        // did 1 update and 1 dry Run update, changed the value from Burger King to Burger King McDonald's
                        Assertions.assertEquals("Burger King McDonald's", record.getField(name)); //updated record because of conflict on name
                        break;
                    case 200:
                        Assertions.assertEquals("Heirloom Cafe", record.getField(name)); // untouched record
                        break;
                    default:
                        Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            });
            Assertions.assertEquals(2, resultValues.size());
        }
    }

    /**
     * Tests composability of inserts and updates in an operation normally dubbed an upsert.
     * <br>
     * This plans, executes, and verifies the equivalent of these SQL queries
     * <pre>
     * {@code
     *   INSERT INTO Restaurants(rec_no, name)
     *   VALUES (100, 'Burger King'),
     *          (200, 'Heirloom Cafe');
     * }
     * </pre>
     * and subsequently:
     * <pre>
     * {@code
     *   INSERT INTO Restaurants(rec_no, name)
     *   VALUES (300, 'Burger King'),
     *          (400, 'Bonita Burrito')
     *   ON CONFLICT(name) SET name = 'McDonald's';
     * }
     * </pre>
     * which is translated into
     * <pre>
     * {@code
     *   INSERT INTO Restaurants(rec_no, name)
     *   WHERE NOT EXISTS(UPDATE Restaurants r
     *                    WHERE v = r.name
     *                    SET name = 'McDonald's')
     *   FROM (VALUES (300, 'Burger King'),
     *                (400, 'Bonita Burrito')) v;
     * }
     * </pre>
     * @throws Exception if there is a problem
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testPlanUpsertGraph() throws Exception {
        final var cascadesPlanner = setUp();

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);

            // insert 2 records
            var plan = cascadesPlanner.planGraph(
                    FDBModificationQueryTest::insertGraph,
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();
            fetchResultValues(context, plan, Function.identity(), c -> {
            });

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
                        var outerQun = Quantifier.forEach(Reference.of(explodeExpression));

                        final var allRecordTypes =
                                ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
                        var qun =
                                Quantifier.forEach(Reference.of(
                                        new FullUnorderedScanExpression(allRecordTypes,
                                                new Type.AnyRecord(false),
                                                new AccessHints())));

                        qun = Quantifier.forEach(Reference.of(
                                new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                        qun,
                                        restaurantType)));

                        var graphExpansionBuilder = GraphExpansion.builder();

                        graphExpansionBuilder.addQuantifier(qun);
                        final var nameValue =
                                FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name");

                        final var comparandValue = FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(outerQun), 1);
                        graphExpansionBuilder.addPredicate(new ValuePredicate(nameValue, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, comparandValue)));
                        qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelectWithResultValue(QuantifiedObjectValue.of(qun))));

                        // make accessors and resolve them
                        final var namePath = FieldValue.resolveFieldPath(qun.getFlowedObjectType(), ImmutableList.of(new FieldValue.Accessor("name", -1)));

                        final var innerQun = Quantifier.existential(Reference.of(new UpdateExpression(qun,
                                "RestaurantRecord",
                                restaurantType,
                                ImmutableMap.of(namePath, LiteralValue.ofScalar("McDonald's")))));

                        graphExpansionBuilder = GraphExpansion.builder();
                        graphExpansionBuilder.addQuantifier(outerQun);
                        graphExpansionBuilder.addQuantifier(innerQun);
                        graphExpansionBuilder.addPredicate(NotPredicate.not(new ExistsPredicate(innerQun.getAlias())));
                        qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelectWithResultValue(outerQun.getFlowedObjectValue())));

                        qun = Quantifier.forEach(Reference.of(new InsertExpression(qun,
                                "RestaurantRecord",
                                Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor()))));

                        return Reference.of(LogicalSortExpression.unsorted(qun));
                    },
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();

            assertMatchesExactly(plan,
                    insertPlan(
                            flatMapPlan(explodePlan(),
                                    descendantPlans(
                                            updatePlan(fetchFromPartialRecordPlan(
                                                    unorderedPrimaryKeyDistinctPlan(
                                                            coveringIndexPlan()
                                                                    .where(indexPlanOf(indexPlan().where(indexName("RestaurantRecord$name")))))))
                                                    .where(target(equalsObject("RestaurantRecord"))))))
                            .where(target(equalsObject("RestaurantRecord"))));

            var resultValues = fetchResultValues(context, plan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var rest_no = recordDescriptor.findFieldByName("rest_no");
                final var name = recordDescriptor.findFieldByName("name");
                if ((int)(long)record.getField(rest_no) == 400) {
                    Assertions.assertEquals("Bonita Burrito", record.getField(name));
                } else {
                    Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            });
            Assertions.assertEquals(1, resultValues.size());

            final var selectPlan = cascadesPlanner.planGraph(() -> selectRecordsGraph(FDBModificationQueryTest::whereReviewsIsEmptyGraph),
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    EvaluationContext.empty()).getPlan();
            resultValues = fetchResultValues(context, selectPlan, record -> {
                final var recordDescriptor = record.getDescriptorForType();
                final var rest_no = recordDescriptor.findFieldByName("rest_no");
                final var name = recordDescriptor.findFieldByName("name");
                switch ((int)(long)record.getField(rest_no)) {
                    case 100:
                        Assertions.assertEquals("McDonald's", record.getField(name)); //updated record because of conflict on name
                        break;
                    case 200:
                        Assertions.assertEquals("Heirloom Cafe", record.getField(name)); // untouched record
                        break;
                    case 400:
                        Assertions.assertEquals("Bonita Burrito", record.getField(name)); // inserted record
                        break;
                    default:
                        Assertions.fail("unexpected record");
                }
                return record;
            }, c -> {
            });
            Assertions.assertEquals(3, resultValues.size());
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testStablePlanHash() throws Exception {
        final var cascadesPlanner = setUp();
        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);
            var plan1 = getUpdatePlan(cascadesPlanner);
            var plan2 = getUpdatePlan(cascadesPlanner);
            Assertions.assertEquals(plan1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                    plan2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);
            var plan1 = getUpdateArrayPlan(cascadesPlanner);
            var plan2 = getUpdateArrayPlan(cascadesPlanner);
            Assertions.assertEquals(plan1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                    plan2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
    }

    @NonNull
    private RecordQueryPlan getUpdatePlan(final CascadesPlanner cascadesPlanner) {
        return cascadesPlanner.planGraph(
                () -> {
                    final var reviewerType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantReviewer.getDescriptor());

                    final var allRecordTypes =
                            ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");
                    var qun =
                            Quantifier.forEach(Reference.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            new Type.AnyRecord(false),
                                            new AccessHints())));

                    qun = Quantifier.forEach(Reference.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantReviewer"),
                                    qun,
                                    reviewerType)));

                    var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var reviewerId =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "id");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(reviewerId, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 100L)));
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelectWithResultValue(QuantifiedObjectValue.of(qun))));

                    // make accessors and resolve them
                    final var updatePath = FieldValue.resolveFieldPath(qun.getFlowedObjectType(), ImmutableList.of(new FieldValue.Accessor("stats", -1), new FieldValue.Accessor("start_date", -1)));
                    final var updateValue = new LiteralValue<>(3); // integer, should cause promotion since RestaurantReview.reviewer is of type long
                    qun = Quantifier.forEach(Reference.of(new UpdateExpression(qun,
                            "RestaurantReviewer",
                            reviewerType,
                            ImmutableMap.of(updatePath, updateValue))));

                    return Reference.of(LogicalSortExpression.unsorted(qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();
    }

    @NonNull
    private RecordQueryPlan getUpdateArrayPlan(final CascadesPlanner cascadesPlanner) {
        return cascadesPlanner.planGraph(
                () -> {
                    final var recordDescriptor = TestRecords4Proto.RestaurantRecord.getDescriptor();
                    final var recordType = Type.Record.fromDescriptor(recordDescriptor);

                    final var allRecordTypes = ImmutableSet.of("RestaurantRecord", "RestaurantReviewer");

                    var qun =
                            Quantifier.forEach(Reference.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            new Type.AnyRecord(false),
                                            new AccessHints())));

                    qun = Quantifier.forEach(Reference.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("RestaurantRecord"),
                                    qun,
                                    recordType)));

                    var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var rest_no =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rest_no");

                    graphExpansionBuilder.addPredicate(new ValuePredicate(rest_no, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 100L)));
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelectWithResultValue(QuantifiedObjectValue.of(qun))));

                    final var updatePath = FieldValue.resolveFieldPath(qun.getFlowedObjectType(), ImmutableList.of(new FieldValue.Accessor("reviews", -1)));
                    final var updateValue = LightArrayConstructorValue.of(
                            RecordConstructorValue.ofUnnamed(List.of(LiteralValue.ofScalar(1), LiteralValue.ofScalar(34))),
                            RecordConstructorValue.ofUnnamed(List.of(LiteralValue.ofScalar(2), LiteralValue.ofScalar(14)))
                    );

                    qun = Quantifier.forEach(Reference.of(new UpdateExpression(qun,
                            "RestaurantRecord",
                            recordType,
                            ImmutableMap.of(updatePath, updateValue))));

                    return Reference.of(LogicalSortExpression.unsorted(qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();
    }

    @Nonnull
    private CascadesPlanner setUp() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);

            return (CascadesPlanner)planner;
        }
    }
}
