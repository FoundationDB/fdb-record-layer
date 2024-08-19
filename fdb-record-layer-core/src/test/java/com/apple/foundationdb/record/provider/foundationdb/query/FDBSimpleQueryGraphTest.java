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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.IndexAccessHint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.DatabaseObjectDependenciesPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.DerivationsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record.Field;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.ScanComparisons.anyValueComparison;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.equalities;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.containsAll;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.descendantPlans;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.explodePlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.firstOrDefaultPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.flatMapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inParameterJoinPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.isReverse;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapResult;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.recordTypes;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.typeFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.fieldValueWithFieldNames;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        return Quantifier.forEach(Reference.of(
                new FullUnorderedScanExpression(allRecordTypes,
                        new Type.AnyRecord(false),
                        hints)));
    }

    @Nonnull
    static Quantifier fullScan(@Nonnull RecordMetaData metaData) {
        return fullScan(metaData, new AccessHints());
    }

    @Nonnull
    static Quantifier fullTypeScan(@Nonnull RecordMetaData metaData, @Nonnull String typeName, @Nonnull Quantifier fullScanQun) {
        return Quantifier.forEach(Reference.of(
                new LogicalTypeFilterExpression(ImmutableSet.of(typeName),
                        fullScanQun,
                        Record.fromDescriptor(metaData.getRecordType(typeName).getDescriptor()))));
    }

    @Nonnull
    static Quantifier fullTypeScan(@Nonnull RecordMetaData metaData, @Nonnull String typeName) {
        return fullTypeScan(metaData, typeName, fullScan(metaData));
    }

    @Nonnull
    static Column<FieldValue> projectColumn(@Nonnull Quantifier qun, @Nonnull String columnName) {
        return projectColumn(qun.getFlowedObjectValue(), columnName);
    }

    @Nonnull
    static Column<FieldValue> projectColumn(@Nonnull Value value, @Nonnull String columnName) {
        return Column.of(Optional.of(columnName), FieldValue.ofFieldNameAndFuseIfPossible(value, columnName));
    }

    @Nonnull
    static <V extends Value> Column<V> resultColumn(@Nonnull V value, @Nullable String name) {
        return Column.of(Optional.ofNullable(name), value);
    }

    static RecordCursor<QueryResult> executeCascades(FDBRecordStore store, RecordQueryPlan plan) {
        return executeCascades(store, plan, Bindings.EMPTY_BINDINGS);
    }

    static RecordCursor<QueryResult> executeCascades(FDBRecordStore store, RecordQueryPlan plan, Bindings bindings) {
        Set<Type> usedTypes = UsedTypesProperty.evaluate(plan);
        TypeRepository typeRepository = TypeRepository.newBuilder()
                .addAllTypes(usedTypes)
                .build();
        EvaluationContext evaluationContext = EvaluationContext.forBindingsAndTypeRepository(bindings, typeRepository);
        return plan.executePlan(store, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE);
    }

    static <T> T getField(QueryResult result, Class<T> type, String... path) {
        Message message = result.getMessage();
        for (int i = 0; i < path.length; i++) {
            String fieldName = path[i];
            Descriptors.Descriptor messageDescriptor = message.getDescriptorForType();
            Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName(fieldName);
            assertNotNull(fieldDescriptor, () -> "expected to find field " + fieldName + " in descriptor: " + messageDescriptor);
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
    void testSimplePlanGraph() {
        CascadesPlanner cascadesPlanner = setUp();
        // no index hints, plan a query
        final var plan = planGraph(
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
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                });

        assertMatchesExactly(plan,
                mapPlan(
                        typeFilterPlan(
                                scanPlan()
                                        .where(scanComparisons(range("([1],>")))))
                        .where(mapResult(recordConstructorValue(exactly(ValueMatchers.fieldValueWithFieldNames("name"), ValueMatchers.fieldValueWithFieldNames("rest_no"))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testSimplePlanGraphReversed() {
        CascadesPlanner cascadesPlanner = setUp();
        // no index hints, plan a query
        final var plan = planGraph(
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
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    final var aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                    final var orderByValues = List.of(FieldValue.ofOrdinalNumber(qun.getFlowedObjectValue(), 1).rebase(aliasMap));
                    return Reference.of(new LogicalSortExpression(orderByValues, true, qun));
                });

        assertMatchesExactly(plan,
                mapPlan(
                        typeFilterPlan(
                                scanPlan()
                                        .where(isReverse())
                                        .where(scanComparisons(range("([1],>")))))
                        .where(mapResult(recordConstructorValue(exactly(ValueMatchers.fieldValueWithFieldNames("name"), ValueMatchers.fieldValueWithFieldNames("rest_no"))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testSimplePlanGraphWithNullableArray() {
        CascadesPlanner cascadesPlanner = setUpWithNullableArray();
        // no index hints, plan a query
        final var plan = planGraph(
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
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                });
        assertMatchesExactly(plan,
                mapPlan(
                        typeFilterPlan(
                                scanPlan()
                                        .where(scanComparisons(range("([1],>")))))
                        .where(mapResult(recordConstructorValue(exactly(fieldValueWithFieldNames("name"), fieldValueWithFieldNames("rest_no"))))));
    }

    /**
     * Test a query running a simple existential query against a FanOut index on the relevant field. This test
     * is actually designed to exercise a code path that was hit by
     * <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2881">Issue #2881</a>. This makes the
     * test somewhat brittle in the sense that if the planner changes in a way that means that it no longer needs
     * to construct certain kinds {@link com.apple.foundationdb.record.query.plan.cascades.Compensation}s to make the
     * plan work, then we may lose coverage (absent other changes to our testing strategy).
     *
     * <p>
     * There are several elements of this test that are necessary at time of writing to make it hit the code
     * path under test:
     * </p>
     * <ol>
     *     <li>A nested existential query on a single field</li>
     *     <li>The inner query has an IN predicate on that field</li>
     *     <li>The inner query projects the column (even though the result value is effectively erased by the existential quantifier)</li>
     *     <li>An index that contains that precisely that field</li>
     * </ol>
     *
     * @param inComparison whether the inner existential predicate should be an IN or EQUALS predicate
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest
    @BooleanSource
    void testSimpleExistentialPredicateOnSimpleIndex(boolean inComparison) {
        Assumptions.assumeTrue(useCascadesPlanner);
        final Index index = new Index("Restaurant$tag.value", Key.Expressions.field("tags").nest(Key.Expressions.field("values", KeyExpression.FanType.FanOut).nest("value")));
        CascadesPlanner cascadesPlanner = setUpWithNullableArray(metaDataBuilder -> metaDataBuilder.addIndex("RestaurantRecord", index));
        final var tagValueParam = "t";
        final var plan = planGraph(
                () -> {
                    // Equivalent to something like:
                    //   SELECT R.name FROM RestaurantRecord AS R WHERE EXISTS (SELECT t.value FROM R.tags AS t WHERE t.value = $t)
                    // Or, for the IN case
                    //   SELECT R.name FROM RestaurantRecord AS R WHERE EXISTS (SELECT t.value FROM R.tags AS t WHERE t.value IN $t)

                    var qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");

                    final var explodeTagsQun = Quantifier.forEach(Reference.of(new ExplodeExpression(FieldValue.ofFieldName(qun.getFlowedObjectValue(), "tags"))));
                    final var existentialQun = Quantifier.existential(Reference.of(GraphExpansion.builder()
                            .addQuantifier(explodeTagsQun)
                            .addResultColumn(projectColumn(explodeTagsQun.getFlowedObjectValue(), "value"))
                            .addPredicate(new ValuePredicate(FieldValue.ofFieldName(explodeTagsQun.getFlowedObjectValue(), "value"),
                                    new Comparisons.ParameterComparison(inComparison ? Comparisons.Type.IN : Comparisons.Type.EQUALS, tagValueParam)))
                            .build()
                            .buildSelect()));

                    qun = Quantifier.forEach(Reference.of(GraphExpansion.builder()
                            .addQuantifier(qun)
                            .addQuantifier(existentialQun)
                            .addPredicate(new ExistsPredicate(existentialQun.getAlias()))
                            .addResultColumn(projectColumn(qun.getFlowedObjectValue(), "name"))
                            .build()
                            .buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                });

        if (inComparison) {
            // IN-comparison is done via a complete scan followed by executing a full scan and then compensating
            //    flatMap(Scan(<,>) | [RestaurantRecord], map(firstOrDefault(flatMap(explode([$t]), explode([$q2.tags]) | $q4.value EQUALS $q83) || null) | $q6 NOT_NULL[(1 as _0)]))
            assertMatchesExactly(plan,
                    flatMapPlan(
                            typeFilterPlan(scanPlan().where(scanComparisons(unbounded())))
                                    .where(recordTypes(containsAll(ImmutableSet.of("RestaurantRecord")))),
                            mapPlan(
                                    predicatesFilterPlan(firstOrDefaultPlan(
                                            flatMapPlan(explodePlan(), predicatesFilterPlan(explodePlan()))
                                    )).where(predicates(valuePredicate(anyValue(), new Comparisons.NullComparison(Comparisons.Type.NOT_NULL))))
                            )
                    )
            );
            assertEquals(-1234573276, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1638799997, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            // Simple existential query with equality predicate done via a simple index scan:
            //   map(Index(Restaurant$tag.value [EQUALS $t])[($q2.name as name)])
            assertMatchesExactly(plan,
                    mapPlan(indexPlan()
                            .where(indexName(index.getName()))
                            .and(scanComparisons(range("[EQUALS $" + tagValueParam + "]")))
                    ));
            assertEquals(-1069846275, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(2087732874, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testEqualityAndSimpleExistentialPredicate() {
        Assumptions.assumeTrue(useCascadesPlanner);
        final Index index = new Index("Restaurant$name-tagValue", Key.Expressions.concat(
                Key.Expressions.field("name"),
                Key.Expressions.field("tags").nest(Key.Expressions.field("values", KeyExpression.FanType.FanOut).nest("value"))
        ));
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("RestaurantRecord", index);
        CascadesPlanner cascadesPlanner = setUpWithNullableArray(hook);

        final var nameValueParam = "name";
        final var tagValueParam = "t";
        final var plan = planGraph(
                () -> {
                    // Equivalent to something like:
                    //   SELECT R.rest_no FROM RestaurantRecord AS R WHERE R.name IN $name AND EXISTS (SELECT tag.value FROM R.tags WHERE tag.value IN $t)
                    // There's a related query where the inner IN is replaced with an equals. That query currently runs into trouble
                    // during planning while trying to calculate its ordering properties.
                    // See: https://github.com/FoundationDB/fdb-record-layer/issues/2883

                    var qun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");

                    final var explodeTagsQun = Quantifier.forEach(Reference.of(new ExplodeExpression(FieldValue.ofFieldName(qun.getFlowedObjectValue(), "tags"))));
                    final var existentialQun = Quantifier.existential(Reference.of(GraphExpansion.builder()
                            .addQuantifier(explodeTagsQun)
                            .addResultColumn(projectColumn(explodeTagsQun.getFlowedObjectValue(), "value"))
                            .addPredicate(new ValuePredicate(FieldValue.ofFieldName(explodeTagsQun.getFlowedObjectValue(), "value"),
                                    new Comparisons.ParameterComparison(Comparisons.Type.IN, tagValueParam)))
                            .build()
                            .buildSelect()));

                    qun = Quantifier.forEach(Reference.of(GraphExpansion.builder()
                            .addQuantifier(qun)
                            .addQuantifier(existentialQun)
                            .addPredicate(new ValuePredicate(FieldValue.ofFieldName(qun.getFlowedObjectValue(), "name"), new Comparisons.ParameterComparison(Comparisons.Type.IN, nameValueParam)))
                            .addPredicate(new ExistsPredicate(existentialQun.getAlias()))
                            .addResultColumn(projectColumn(qun.getFlowedObjectValue(), "rest_no"))
                            .build()
                            .buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                });

        // IN-comparison is done via a complete scan followed by executing a full scan and then compensating
        //   flatMap(Fetch(Covering(Index(RestaurantRecord$name [EQUALS $q133]) -> [name: KEY[0], rest_no: KEY[1]]) WHERE __corr_q133 IN $name), map(firstOrDefault(flatMap(explode([$t]), explode([$q2.tags]) | $q4.value EQUALS $q83) || null) | $q6 NOT_NULL[(1 as _0)]))
        assertMatchesExactly(plan,
                flatMapPlan(
                        fetchFromPartialRecordPlan(
                                inParameterJoinPlan(
                                        coveringIndexPlan()
                                                .where(indexPlanOf(indexPlan().where(indexName("RestaurantRecord$name")).and(scanComparisons(equalities(exactly(anyValueComparison()))))))
                                )
                        ),
                        mapPlan(
                                predicatesFilterPlan(firstOrDefaultPlan(
                                        flatMapPlan(explodePlan(), predicatesFilterPlan(explodePlan()))
                                )).where(predicates(valuePredicate(anyValue(), new Comparisons.NullComparison(Comparisons.Type.NOT_NULL))))
                        )
                )
        );
        assertEquals(-547779922, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(-2058055083, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testFailWithBadIndexHintGraph() {
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
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                },
                allowedIndexesOptional,
                IndexQueryabilityFilter.TRUE,
                parameterRelationshipGraph));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testPlanDifferentWithIndexHintGraph() {
        CascadesPlanner cascadesPlanner = setUp();

        // with index hints (RestaurantRecord$name), plan a different query
        final var plan = planGraph(
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
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                });

        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                mapPlan(
                        predicatesFilterPlan(
                                coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("RestaurantRecord$name")).and(scanComparisons(unbounded())))))
                                .where(predicates(only(valuePredicate(ValueMatchers.fieldValueWithFieldNames("rest_no"), new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1L))))));

        assertMatchesExactly(plan, planMatcher);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testPlanCrossProductJoin() {
        CascadesPlanner cascadesPlanner = setUp();

        // with index hints (RestaurantRecord$name), plan a different query
        final var plannedPlan = cascadesPlanner.planGraph(
                () -> {
                    var outerQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");
                    final var explodeQun =
                            Quantifier.forEach(Reference.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()), "reviews"))));

                    var graphExpansionBuilder = GraphExpansion.builder();
                    graphExpansionBuilder.addQuantifier(outerQun);
                    graphExpansionBuilder.addQuantifier(explodeQun);
                    graphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()), "name"),
                            new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "name")));

                    final var explodeResultValue = QuantifiedObjectValue.of(explodeQun.getAlias(), explodeQun.getFlowedObjectType());
                    graphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
                    outerQun = Quantifier.forEach(Reference.of(
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

                    final var qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();

        final var plan = verifySerialization(plannedPlan);

        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                flatMapPlan(
                        descendantPlans(
                                indexPlan()
                                        .where(indexName("RestaurantRecord$name"))
                                        .and(scanComparisons(range("[[name],[name]]")))),
                        typeFilterPlan(scanPlan().where(scanComparisons(unbounded())))
                                .where(recordTypes(containsAll(ImmutableSet.of("RestaurantReviewer")))));

        assertMatchesExactly(plan, planMatcher);

        final var compatibleTypeEvolutionPredicate = CompatibleTypeEvolutionPredicate.fromPlan(plannedPlan);
        final boolean isCompatible = Objects.requireNonNull(compatibleTypeEvolutionPredicate.eval(recordStore, EvaluationContext.empty()));
        Assertions.assertTrue(isCompatible);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testSimpleJoin() {
        CascadesPlanner cascadesPlanner = setUp();

        // with index hints (RestaurantRecord$name), plan a different query
        final var plan = planSimpleJoin(cascadesPlanner);

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

    /**
     * Tests incompatible type evolution, in particular an accessed field was shifted.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testMediumJoinDatabaseObjectDependencies() {
        CascadesPlanner cascadesPlanner = setUp();

        // find restaurants that where at least reviewed by two common reviewers
        final var plannedPlan = planSimpleJoin(cascadesPlanner);

        final DatabaseObjectDependenciesPredicate predicate =
                DatabaseObjectDependenciesPredicate.fromPlan(recordStore.getRecordMetaData(), plannedPlan);
        Assertions.assertTrue(Objects.requireNonNull(predicate.eval(recordStore, EvaluationContext.empty())));

        final var usedIndexes = predicate.getUsedIndexes();
        Assertions.assertTrue(usedIndexes.contains(
                new DatabaseObjectDependenciesPredicate.UsedIndex("RestaurantRecord$name", 1)));
    }

    @Nonnull
    private RecordQueryPlan planSimpleJoin(@Nonnull final CascadesPlanner cascadesPlanner) {
        // with index hints (RestaurantRecord$name), plan a different query
        return planGraph(
                () -> {
                    var outerQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");
                    final var explodeQun =
                            Quantifier.forEach(Reference.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()), "reviews"))));

                    var graphExpansionBuilder = GraphExpansion.builder();
                    graphExpansionBuilder.addQuantifier(outerQun);
                    graphExpansionBuilder.addQuantifier(explodeQun);
                    graphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()), "name"),
                            new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "name")));

                    final var explodeResultValue = QuantifiedObjectValue.of(explodeQun.getAlias(), explodeQun.getFlowedObjectType());
                    graphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
                    outerQun = Quantifier.forEach(Reference.of(
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

                    final var qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                });
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testMediumJoin() {
        CascadesPlanner cascadesPlanner = setUp();

        // find restaurants that where at least reviewed by two common reviewers
        final var plannedPlan = planMediumJoin(cascadesPlanner);

        // TODO write a matcher when this plan becomes more stable
        Assertions.assertTrue(plannedPlan instanceof RecordQueryFlatMapPlan);
        verifySerialization(plannedPlan);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testMediumJoinTypeEvolutionIdentical() {
        CascadesPlanner cascadesPlanner = setUp();

        // find restaurants that where at least reviewed by two common reviewers
        final var plannedPlan = planMediumJoin(cascadesPlanner);

        final var derivations = DerivationsProperty.evaluateDerivations(plannedPlan);
        final var simplifiedLocalValues = derivations.simplifyLocalValues();
        final var fieldAccesses = CompatibleTypeEvolutionPredicate.computeFieldAccesses(simplifiedLocalValues);
        final var compatibleTypeEvolutionPredicate = new CompatibleTypeEvolutionPredicate(fieldAccesses);

        final boolean isCompatible = Objects.requireNonNull(compatibleTypeEvolutionPredicate.eval(recordStore, EvaluationContext.empty()));
        Assertions.assertTrue(isCompatible);

        //
        // RestaurantRecord:
        // RECORD(LONG as rest_no,
        //        STRING as name,
        //        ARRAY(RECORD(LONG as reviewer,
        //                     INT as rating))(isNullable:false) as reviews,
        //        ARRAY(RECORD(STRING as value,
        //                     INT as weight))(isNullable:false) as tags,
        //        ARRAY(STRING)(isNullable:false) as customer)
        //
        final var reviewsType = Record.fromFields(false,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("reviewer")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("rating"))));

        final var tagsType = Record.fromFields(false,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("value")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("weight"))));

        final var restaurantRecordType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("rest_no")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("name")),
                        Field.of(new Type.Array(reviewsType), Optional.of("reviews")),
                        Field.of(new Type.Array(tagsType), Optional.of("tags")),
                        Field.of(new Type.Array(Type.primitiveType(Type.TypeCode.STRING)), Optional.of("customer"))));

        //
        // RestaurantReviewer:
        // RECORD(LONG as id,
        //        STRING as name,
        //        STRING as email,
        //        RECORD(LONG as start_date,
        //               STRING as school_name,
        //               STRING as hometown) as stats,
        //        INT as category)
        //
        final var statsType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("start_date")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("school_name")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("home_town"))));

        final var restaurantReviewerType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("id")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("name")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("email")),
                        Field.of(statsType, Optional.of("stats")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, true), Optional.of("category"))));

        final var fieldAccessesRestaurantRecord = fieldAccesses.get("RestaurantRecord");
        //
        // RestaurantRecord ->
        //     "[name;1;STRING]→[STRING],
        //      [reviews;2;ARRAY(RECORD(LONG as reviewer, INT as rating))(isNullable:false)]→[
        //         [reviewer;0;LONG]→[LONG]
        //      ],
        //      [rest_no;0;LONG]→[LONG]"
        //
        var childrenMap = fieldAccessesRestaurantRecord.getChildrenMap();
        Assertions.assertNotNull(childrenMap);
        var childFieldAccesses = childrenMap.get(FieldValue.ResolvedAccessor.of("name", 1));
        Assertions.assertNotNull(childFieldAccesses);
        var leafType = childFieldAccesses.getValue();
        Assertions.assertNotNull(leafType);
        Assertions.assertEquals(leafType, Type.primitiveType(Type.TypeCode.STRING, true));

        childFieldAccesses = childrenMap.get(FieldValue.ResolvedAccessor.of("reviews", 2));
        Assertions.assertNotNull(childFieldAccesses);
        childrenMap = fieldAccessesRestaurantRecord.getChildrenMap();
        Assertions.assertNotNull(childrenMap);
        childFieldAccesses = childrenMap.get(FieldValue.ResolvedAccessor.of("reviewer", 0));
        Assertions.assertNotNull(childFieldAccesses);
        leafType = childFieldAccesses.getValue();
        Assertions.assertNotNull(leafType);
        Assertions.assertEquals(leafType, Type.primitiveType(Type.TypeCode.LONG, false));

        childrenMap = fieldAccessesRestaurantRecord.getChildrenMap();
        childFieldAccesses = childrenMap.get(FieldValue.ResolvedAccessor.of("rest_no", 0));
        Assertions.assertNotNull(childFieldAccesses);
        leafType = childFieldAccesses.getValue();
        Assertions.assertNotNull(leafType);
        Assertions.assertEquals(leafType, Type.primitiveType(Type.TypeCode.LONG, false));

        Assertions.assertTrue(CompatibleTypeEvolutionPredicate.isAccessCompatibleWithCurrentType(fieldAccessesRestaurantRecord, restaurantRecordType));

        final var fieldAccessesRestaurantReviewer = fieldAccesses.get("RestaurantReviewer");
        //
        // RestaurantReviewer ->
        //     "[name;1;STRING]→[STRING],
        //      [id;0;LONG]→[LONG]"
        //
        childrenMap = fieldAccessesRestaurantReviewer.getChildrenMap();
        Assertions.assertNotNull(childrenMap);
        childFieldAccesses = childrenMap.get(FieldValue.ResolvedAccessor.of("name", 1));
        Assertions.assertNotNull(childFieldAccesses);
        leafType = childFieldAccesses.getValue();
        Assertions.assertNotNull(leafType);
        Assertions.assertEquals(leafType, Type.primitiveType(Type.TypeCode.STRING, false));

        childFieldAccesses = childrenMap.get(FieldValue.ResolvedAccessor.of("id", 0));
        Assertions.assertNotNull(childFieldAccesses);
        leafType = childFieldAccesses.getValue();
        Assertions.assertNotNull(leafType);
        Assertions.assertEquals(leafType, Type.primitiveType(Type.TypeCode.LONG, false));

        Assertions.assertTrue(CompatibleTypeEvolutionPredicate.isAccessCompatibleWithCurrentType(fieldAccessesRestaurantReviewer, restaurantReviewerType));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testMediumJoinTypeEvolutionCompatible() {
        CascadesPlanner cascadesPlanner = setUp();

        // find restaurants that where at least reviewed by two common reviewers
        final var plannedPlan = planMediumJoin(cascadesPlanner);

        final var derivations = DerivationsProperty.evaluateDerivations(plannedPlan);
        final var simplifiedLocalValues = derivations.simplifyLocalValues();
        final var fieldAccesses = CompatibleTypeEvolutionPredicate.computeFieldAccesses(simplifiedLocalValues);

        //
        // RestaurantRecord:
        // RECORD(LONG as rest_no,
        //        STRING as name,
        //        ARRAY(RECORD(LONG as reviewer,
        //                     INT as rating))(isNullable:false) as reviews,
        //        STRING new_field1,                                                  <<<< added
        //        ARRAY(RECORD(STRING new_field2,                                     <<<< added
        //                     STRING as value,
        //                     INT as weight))(isNullable:false) as tags,
        //        ARRAY(STRING)(isNullable:false) as customer                         <<<< deleted
        //        STRING new_field3)                                                  <<<< added
        //
        final var reviewsType = Record.fromFields(false,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("reviewer")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("rating"))));

        final var tagsType = Record.fromFields(false,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("new_field2")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("value")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("weight"))));

        final var restaurantRecordType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("rest_no")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("name")),
                        Field.of(new Type.Array(reviewsType), Optional.of("reviews")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("new_field1")),
                        Field.of(new Type.Array(tagsType), Optional.of("tags")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("new_field3"))));

        //
        // RestaurantReviewer:
        // RECORD(LONG as id,
        //        STRING as name,
        //        STRING new_field1,                                                  <<<< added
        //        STRING as email,
        //        RECORD(INT as start_date,                                           <<<< LONG -> INT
        //               STRING as school_name,                                       <<<< deleted
        //               STRING as hometown) as stats,
        //        INT as category)
        //
        final var statsType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.INT, true), Optional.of("start_date")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("home_town"))));

        final var restaurantReviewerType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("id")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("name")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("new_field1")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("email")),
                        Field.of(statsType, Optional.of("stats")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, true), Optional.of("category"))));

        final var fieldAccessesRestaurantRecord = fieldAccesses.get("RestaurantRecord");
        //
        // RestaurantRecord ->
        //     "[name;1;STRING]→[STRING],
        //      [reviews;2;ARRAY(RECORD(LONG as reviewer, INT as rating))(isNullable:false)]→[
        //         [reviewer;0;LONG]→[LONG]
        //      ],
        //      [rest_no;0;LONG]→[LONG]"
        //
        Assertions.assertTrue(CompatibleTypeEvolutionPredicate.isAccessCompatibleWithCurrentType(fieldAccessesRestaurantRecord, restaurantRecordType));

        final var fieldAccessesRestaurantReviewer = fieldAccesses.get("RestaurantReviewer");
        //
        // RestaurantReviewer ->
        //     "[name;1;STRING]→[STRING],
        //      [id;0;LONG]→[LONG]"
        //
        Assertions.assertTrue(CompatibleTypeEvolutionPredicate.isAccessCompatibleWithCurrentType(fieldAccessesRestaurantReviewer, restaurantReviewerType));
    }

    /**
     * Tests incompatible type evolution, in particular, an accessed field now has a different type.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testMediumJoinTypeEvolutionIncompatible1() {
        CascadesPlanner cascadesPlanner = setUp();

        // find restaurants that where at least reviewed by two common reviewers
        final var plannedPlan = planMediumJoin(cascadesPlanner);

        final var derivations = DerivationsProperty.evaluateDerivations(plannedPlan);
        final var simplifiedLocalValues = derivations.simplifyLocalValues();
        final var fieldAccesses = CompatibleTypeEvolutionPredicate.computeFieldAccesses(simplifiedLocalValues);

        //
        // RestaurantRecord:
        // RECORD(INT as rest_no,                                            <<< changed from LONG
        //        STRING as name,
        //        ARRAY(RECORD(LONG as reviewer,
        //                     INT as rating))(isNullable:false) as reviews,
        //        ARRAY(RECORD(STRING as value,
        //                     INT as weight))(isNullable:false) as tags,
        //        ARRAY(STRING)(isNullable:false) as customer)
        //
        final var reviewsType = Record.fromFields(false,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("reviewer")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("rating"))));

        final var tagsType = Record.fromFields(false,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("value")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("weight"))));

        final var restaurantRecordType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("rest_no")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("name")),
                        Field.of(new Type.Array(reviewsType), Optional.of("reviews")),
                        Field.of(new Type.Array(tagsType), Optional.of("tags")),
                        Field.of(new Type.Array(Type.primitiveType(Type.TypeCode.STRING)), Optional.of("customer"))));

        //
        // RestaurantReviewer:
        // RECORD(LONG as id,
        //        STRING as name,
        //        STRING as email,
        //        RECORD(LONG as start_date,
        //               STRING as school_name,
        //               STRING as hometown) as stats,
        //        INT as category)
        //
        final var statsType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("start_date")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("school_name")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("home_town"))));

        final var restaurantReviewerType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("id")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("name")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("email")),
                        Field.of(statsType, Optional.of("stats")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, true), Optional.of("category"))));

        final var fieldAccessesRestaurantRecord = fieldAccesses.get("RestaurantRecord");
        //
        // RestaurantRecord ->
        //     "[name;1;STRING]→[STRING],
        //      [reviews;2;ARRAY(RECORD(LONG as reviewer, INT as rating))(isNullable:false)]→[
        //         [reviewer;0;LONG]→[LONG]
        //      ],
        //      [rest_no;0;LONG]→[LONG]"
        //
        Assertions.assertFalse(CompatibleTypeEvolutionPredicate.isAccessCompatibleWithCurrentType(fieldAccessesRestaurantRecord, restaurantRecordType));

        final var fieldAccessesRestaurantReviewer = fieldAccesses.get("RestaurantReviewer");
        //
        // RestaurantReviewer ->
        //     "[name;1;STRING]→[STRING],
        //      [id;0;LONG]→[LONG]"
        //
        Assertions.assertTrue(CompatibleTypeEvolutionPredicate.isAccessCompatibleWithCurrentType(fieldAccessesRestaurantReviewer, restaurantReviewerType));
    }

    /**
     * Tests incompatible type evolution, in particular an accessed field was shifted.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testMediumJoinTypeEvolutionIncompatible2() {
        CascadesPlanner cascadesPlanner = setUp();

        // find restaurants that where at least reviewed by two common reviewers
        final var plannedPlan = planMediumJoin(cascadesPlanner);

        final var derivations = DerivationsProperty.evaluateDerivations(plannedPlan);
        final var simplifiedLocalValues = derivations.simplifyLocalValues();
        final var fieldAccesses = CompatibleTypeEvolutionPredicate.computeFieldAccesses(simplifiedLocalValues);

        //
        // RestaurantRecord:
        // RECORD(LONG as rest_no,
        //        INT new_field,
        //        STRING as name,
        //        ARRAY(RECORD(LONG as reviewer,
        //                     INT as rating))(isNullable:false) as reviews,
        //        ARRAY(RECORD(STRING as value,
        //                     INT as weight))(isNullable:false) as tags,
        //        ARRAY(STRING)(isNullable:false) as customer)
        //
        final var reviewsType = Record.fromFields(false,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("reviewer")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("rating"))));

        final var tagsType = Record.fromFields(false,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("value")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("weight"))));

        final var restaurantRecordType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("rest_no")),
                        Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("new_field")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("name")),
                        Field.of(new Type.Array(reviewsType), Optional.of("reviews")),
                        Field.of(new Type.Array(tagsType), Optional.of("tags")),
                        Field.of(new Type.Array(Type.primitiveType(Type.TypeCode.STRING)), Optional.of("customer"))));

        //
        // RestaurantReviewer:
        // RECORD(LONG as id,
        //        STRING as name,
        //        STRING as email,
        //        RECORD(LONG as start_date,
        //               STRING as school_name,
        //               STRING as hometown) as stats,
        //        INT as category)
        //
        final var statsType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("start_date")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("school_name")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("home_town"))));

        final var restaurantReviewerType = Record.fromFields(true,
                ImmutableList.of(Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("id")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("name")),
                        Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("email")),
                        Field.of(statsType, Optional.of("stats")),
                        Field.of(Type.primitiveType(Type.TypeCode.INT, true), Optional.of("category"))));

        final var fieldAccessesRestaurantRecord = fieldAccesses.get("RestaurantRecord");
        //
        // RestaurantRecord ->
        //     "[name;1;STRING]→[STRING],
        //      [reviews;2;ARRAY(RECORD(LONG as reviewer, INT as rating))(isNullable:false)]→[
        //         [reviewer;0;LONG]→[LONG]
        //      ],
        //      [rest_no;0;LONG]→[LONG]"
        //
        Assertions.assertFalse(CompatibleTypeEvolutionPredicate.isAccessCompatibleWithCurrentType(fieldAccessesRestaurantRecord, restaurantRecordType));

        final var fieldAccessesRestaurantReviewer = fieldAccesses.get("RestaurantReviewer");
        //
        // RestaurantReviewer ->
        //     "[name;1;STRING]→[STRING],
        //      [id;0;LONG]→[LONG]"
        //
        Assertions.assertTrue(CompatibleTypeEvolutionPredicate.isAccessCompatibleWithCurrentType(fieldAccessesRestaurantReviewer, restaurantReviewerType));
    }

    private RecordQueryPlan planMediumJoin(@Nonnull final CascadesPlanner cascadesPlanner) {
        // find restaurants that where at least reviewed by two common reviewers
        //
        // SELECT reviewer1.name AS reviewer1Name,
        //        reviewer2.name AS reviewer2Name,
        //        restaurant.name AS restaurantName,
        //        restaurant.rest_no AS restaurantNo
        // FROM RestaurantReviewer reviewer1,
        //      RestaurantReviewer reviewer2,
        //      RestaurantRecord restaurant
        // WHERE EXISTS(SELECT review AS review
        //              FROM RestaurantRecord restaurant, restaurant.reviews review
        //              WHERE review.reviewer = reviewer1.id) AND
        //       EXISTS(SELECT review AS review
        //              FROM RestaurantRecord restaurant, restaurant.reviews review
        //              WHERE review.reviewer = reviewer2.id)
        //
        return cascadesPlanner.planGraph(
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
                            Quantifier.forEach(Reference.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "reviews"))));

                    reviewsGraphExpansionBuilder.addQuantifier(explodeReviewsQun);
                    reviewsGraphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType()), "reviewer"),
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, FieldValue.ofFieldName(QuantifiedObjectValue.of(reviewer1Qun.getAlias(), reviewer1Qun.getFlowedObjectType()), "id"))));

                    var explodeResultValue = QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType());
                    reviewsGraphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
                    final var existential1Quantifier = Quantifier.existential(Reference.of(reviewsGraphExpansionBuilder.build().buildSelect()));

                    graphExpansionBuilder.addQuantifier(existential1Quantifier);
                    graphExpansionBuilder.addPredicate(new ExistsPredicate(existential1Quantifier.getAlias()));

                    reviewsGraphExpansionBuilder = GraphExpansion.builder();

                    explodeReviewsQun =
                            Quantifier.forEach(Reference.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "reviews"))));

                    reviewsGraphExpansionBuilder.addQuantifier(explodeReviewsQun);
                    reviewsGraphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType()), "reviewer"),
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, FieldValue.ofFieldName(QuantifiedObjectValue.of(reviewer2Qun.getAlias(), reviewer2Qun.getFlowedObjectType()), "id"))));

                    explodeResultValue = QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType());
                    reviewsGraphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
                    var existential2Quantifier = Quantifier.existential(Reference.of(reviewsGraphExpansionBuilder.build().buildSelect()));

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

                    final var qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                },
                Optional.empty(),
                IndexQueryabilityFilter.DEFAULT,
                EvaluationContext.EMPTY
        ).getPlan();
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testPlanFiveWayJoin() {
        CascadesPlanner cascadesPlanner = setUp();

        // find restaurants that where at least reviewed by two common reviewers
        final var plan = planGraph(
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
                            Quantifier.forEach(Reference.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "reviews"))));

                    reviewsGraphExpansionBuilder.addQuantifier(explodeReviewsQun);
                    reviewsGraphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType()), "reviewer"),
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, FieldValue.ofFieldName(QuantifiedObjectValue.of(reviewer1Qun.getAlias(), reviewer1Qun.getFlowedObjectType()), "id"))));

                    var explodeResultValue = QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType());
                    reviewsGraphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
                    final var existential1Quantifier = Quantifier.existential(Reference.of(reviewsGraphExpansionBuilder.build().buildSelect()));

                    graphExpansionBuilder.addQuantifier(existential1Quantifier);
                    graphExpansionBuilder.addPredicate(new ExistsPredicate(existential1Quantifier.getAlias()));

                    reviewsGraphExpansionBuilder = GraphExpansion.builder();

                    explodeReviewsQun =
                            Quantifier.forEach(Reference.of(
                                    new ExplodeExpression(FieldValue.ofFieldName(QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "reviews"))));

                    reviewsGraphExpansionBuilder.addQuantifier(explodeReviewsQun);
                    reviewsGraphExpansionBuilder.addPredicate(new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType()), "reviewer"),
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, FieldValue.ofFieldName(QuantifiedObjectValue.of(reviewer1Qun.getAlias(), reviewer1Qun.getFlowedObjectType()), "id"))));

                    explodeResultValue = QuantifiedObjectValue.of(explodeReviewsQun.getAlias(), explodeReviewsQun.getFlowedObjectType());
                    reviewsGraphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
                    var existential2Quantifier = Quantifier.existential(Reference.of(reviewsGraphExpansionBuilder.build().buildSelect()));

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

                    final var qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                });

        // TODO write a matcher when this plan becomes more stable
        Assertions.assertTrue(plan instanceof RecordQueryFlatMapPlan);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testSimplePlanWithConstantPredicateGraph() {
        CascadesPlanner cascadesPlanner = setUp();
        // no index hints, plan a query
        final var plan = planGraph(
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
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(LogicalSortExpression.unsorted(qun));
                });

        assertMatchesExactly(plan,
                mapPlan(typeFilterPlan(
                        scanPlan()
                                .where(scanComparisons(range("([1],>")))))
                        .where(mapResult(recordConstructorValue(exactly(ValueMatchers.fieldValueWithFieldNames("name"), ValueMatchers.fieldValueWithFieldNames("rest_no"))))));
    }

    @Nonnull
    private CascadesPlanner setUp() {
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
    private CascadesPlanner setUpWithNullableArray() {
        return setUpWithNullableArray(null);
    }

    @Nonnull
    private CascadesPlanner setUpWithNullableArray(@Nullable RecordMetaDataHook hook) {
        final CascadesPlanner cascadesPlanner;

        try (FDBRecordContext context = openContext()) {
            openNestedWrappedArrayRecordStore(context, hook);

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
