/*
 * FDBBitwiseFunctionQueryTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilter;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.recordTypes;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.typeFilterPlan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of indexes and queries on {@link com.apple.foundationdb.record.metadata.expressions.LongArithmethicFunctionKeyExpression}s.
 */
public class FDBLongArithmeticFunctionQueryTest extends FDBRecordStoreQueryTestBase {

    @Nonnull
    private static KeyExpression sumExpression(@Nonnull String leftField, @Nonnull String rightField) {
        return sumExpression(field(leftField), field(rightField));
    }

    @Nonnull
    private static KeyExpression sumExpression(@Nonnull KeyExpression leftExpr, @Nonnull KeyExpression rightExpr) {
        return Key.Expressions.function("add", concat(leftExpr, rightExpr));
    }

    @Nonnull
    private static KeyExpression bitMaskExpression(@Nonnull String fieldName, long mask) {
        return bitMaskExpression(field(fieldName), mask);
    }

    @Nonnull
    private static KeyExpression bitMaskExpression(@Nonnull KeyExpression fieldExpression, long mask) {
        return Key.Expressions.function("bitand", concat(fieldExpression, Key.Expressions.value(mask)));
    }

    @Nonnull
    private static Index sum2And3Index() {
        return new Index("MySimpleRecord$num_value_2+num_value_3_indexed", sumExpression("num_value_2", "num_value_3_indexed"));
    }

    @Nonnull
    private static Index maskedNumValue2Index(long mask) {
        return new Index("MySimpleRecord$num_value_2&" + mask, bitMaskExpression("num_value_2", mask));
    }

    @DualPlannerTest
    void sum2And3Query() {
        final Index sumIndex = sum2And3Index();
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", sumIndex);
        final List<TestRecords1Proto.MySimpleRecord> data = setupSimpleRecordStore(hook,
                (i, builder) -> builder.setRecNo(i).setNumValue2(i % 5).setNumValue3Indexed(i % 3));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final String sumValueParam = "sum";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.keyExpression(sumIndex.getRootExpression()).equalsParameter(sumValueParam))
                    .build();
            final RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan,
                    indexPlan()
                            .where(indexName(sumIndex.getName()))
                            .and(scanComparisons(range("[EQUALS $" + sumValueParam + "]")))
            );
            assertEquals(1466369563, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-730052000, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            LongStream.range(-1, 11).forEach(sum ->
                    assertQueryResults(data, plan,
                            Bindings.newBuilder().set(sumValueParam, sum).build(),
                            rec -> rec.getNumValue2() + rec.getNumValue3Indexed() == sum)
            );
            commit(context);
        }
    }

    @DualPlannerTest
    void numValue2MaskQuery() {
        final Index maskedIndex = maskedNumValue2Index(2);
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", maskedIndex);
        final List<TestRecords1Proto.MySimpleRecord> data = setupSimpleRecordStore(hook,
                (i, builder) -> builder.setRecNo(i).setNumValue2(i));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final String maskedValueParam = "mask";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.keyExpression(maskedIndex.getRootExpression()).equalsParameter(maskedValueParam))
                    .build();
            final RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan,
                    indexPlan()
                            .where(indexName(maskedIndex.getName()))
                            .and(scanComparisons(range("[EQUALS $" + maskedValueParam + "]")))
            );
            assertEquals(-1548863947, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(857912600, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            LongStream.range(0, 3).forEach(mask ->
                    assertQueryResults(data, plan,
                            Bindings.newBuilder().set(maskedValueParam, mask).build(),
                            rec -> (rec.getNumValue2() & 2) == mask)
            );

            TestHelpers.assertDiscardedNone(context);
            commit(context);
        }
    }

    @DualPlannerTest
    void doesNotMatchIncorrectMask() {
        final Index maskedIndex = maskedNumValue2Index(1);
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", maskedIndex);
        final List<TestRecords1Proto.MySimpleRecord> data = setupSimpleRecordStore(hook,
                (i, builder) -> builder.setRecNo(i).setNumValue2(i));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final String maskParam = "mask";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.keyExpression(bitMaskExpression("num_value_2", 2)).equalsParameter(maskParam))
                    .build();
            final RecordQueryPlan plan = planQuery(query);
            final BindingMatcher<RecordQueryTypeFilterPlan> typeFilterMatcher = typeFilterPlan(
                    scanPlan().where(scanComparisons(unbounded()))
            ).where(recordTypes(PrimitiveMatchers.containsAll(ImmutableSet.of("MySimpleRecord"))));

            if (useCascadesPlanner) {
                assertMatchesExactly(plan, predicatesFilter(QuantifierMatchers.anyQuantifier(typeFilterMatcher)));
                assertEquals(1022673764, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(1929318213, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertMatchesExactly(plan, filterPlan(typeFilterMatcher));
                assertEquals(-1686224670, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(1275558918, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }

            LongStream.range(0, 3).forEach(mask ->
                    assertQueryResults(data, plan,
                            Bindings.newBuilder().set(maskParam, mask).build(),
                            rec -> (rec.getNumValue2() & 2) == mask)
            );

            commit(context);
        }
    }

    @DualPlannerTest
    void complexIndex() {
        final Index index = new Index("complexIndex", concat(
                field("str_value_indexed"),
                sumExpression("num_value_2", "num_value_3_indexed"),
                bitMaskExpression("num_value_unique", 4))
        );
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);
        final List<TestRecords1Proto.MySimpleRecord> data = setupSimpleRecordStore(hook,
                (i, builder) -> builder.setRecNo(i).setNumValue3Indexed(i % 3).setNumValue2(i % 5).setStrValueIndexed(i % 2 == 0 ? "even" : "odd").setNumValueUnique(i));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final String strValueParam = "strValue";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.and(
                            Query.field("str_value_indexed").equalsParameter(strValueParam),
                            Query.keyExpression(sumExpression("num_value_2", "num_value_3_indexed")).greaterThanOrEquals(1),
                            Query.keyExpression(sumExpression("num_value_2", "num_value_3_indexed")).lessThanOrEquals(4)
                    ))
                    .setSort(concat(sumExpression("num_value_2", "num_value_3_indexed"), bitMaskExpression("num_value_unique", 4)))
                    .setRequiredResults(List.of(bitMaskExpression("num_value_unique", 4)))
                    .build();
            final RecordQueryPlan plan = planQuery(query);

            // Even though the only required result is in the index, there's not a clean way to surface it, so we end up not using a covering index here
            assertMatchesExactly(plan, indexPlan()
                    .where(indexName(index.getName()))
                    .and(scanComparisons(range("[EQUALS $" + strValueParam + ", [GREATER_THAN_OR_EQUALS 1 && LESS_THAN_OR_EQUALS 4]]")))
            );
            assertEquals(-83447963, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(929715426, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            for (String strValue : List.of("even", "odd")) {
                assertQueryResults(data, plan,
                        Bindings.newBuilder().set(strValueParam, strValue).build(),
                        rec -> rec.getStrValueIndexed().equals(strValue) && ((rec.getNumValue2() + rec.getNumValue3Indexed()) >= 1 && (rec.getNumValue2() + rec.getNumValue3Indexed()) <= 4),
                        rec -> Tuple.from(rec.getNumValue2() + rec.getNumValue3Indexed(), rec.getNumValueUnique() & 4));
            }

            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void complexIndexGraphQueryWithMaskInResults() {
        final Index index = new Index("complexIndex", concat(
                field("str_value_indexed"),
                sumExpression("num_value_2", "num_value_3_indexed"),
                bitMaskExpression("num_value_unique", 4))
        );
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);
        final List<TestRecords1Proto.MySimpleRecord> data = setupSimpleRecordStore(hook,
                (i, builder) -> builder.setRecNo(i).setNumValue3Indexed(i % 3).setNumValue2(i % 5).setStrValueIndexed(i % 2 == 0 ? "even" : "odd").setNumValueUnique(i));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final String strValueParam = "str";
            final String sumLowerBound = "sumLowerBound";
            final String sumUpperBound = "sumUpperBound";
            final RecordQueryPlan plan = planGraph(() -> {
                Quantifier typeQun = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final FieldValue strValue = FieldValue.ofFieldName(typeQun.getFlowedObjectValue(), "str_value_indexed");
                final FieldValue num2Value = FieldValue.ofFieldName(typeQun.getFlowedObjectValue(), "num_value_2");
                final FieldValue num3Value = FieldValue.ofFieldName(typeQun.getFlowedObjectValue(), "num_value_3_indexed");
                final FieldValue numUniqueValue = FieldValue.ofFieldName(typeQun.getFlowedObjectValue(), "num_value_unique");
                final Value sumValue = (Value) new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(num2Value, num3Value));
                final Value maskValue = (Value) new ArithmeticValue.BitAndFn().encapsulate(ImmutableList.of(
                        numUniqueValue,
                        LiteralValue.ofScalar(4L)
                ));
                SelectExpression select = GraphExpansion.builder()
                        .addQuantifier(typeQun)
                        .addPredicate(new ValuePredicate(strValue, new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, strValueParam)))
                        .addPredicate(new ValuePredicate(sumValue, new Comparisons.ParameterComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, sumLowerBound)))
                        .addPredicate(new ValuePredicate(sumValue, new Comparisons.ParameterComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, sumUpperBound)))
                        .addResultColumn(Column.of(Optional.of("sum"), sumValue))
                        .addResultColumn(Column.of(Optional.of("mask"), maskValue))
                        .addResultColumn(Column.of(Optional.of("id"), FieldValue.ofFieldName(typeQun.getFlowedObjectValue(), "rec_no")))
                        .build()
                        .buildSelect();
                Quantifier selectQun = Quantifier.forEach(Reference.of(select));
                return Reference.of(new LogicalSortExpression(ImmutableList.of(FieldValue.ofFieldName(selectQun.getFlowedObjectValue(), "sum").rebase(AliasMap.ofAliases(selectQun.getAlias(), Quantifier.current()))), false, selectQun));
            });
            // Note: This should be a covering index scan, as the mask value can be extracted from the index entries, though the matching isn't quite there
            assertMatchesExactly(plan, mapPlan(
                    indexPlan()
                            .where(indexName(index.getName()))
                            .and(scanComparisons(range("[EQUALS $" + strValueParam + ", [GREATER_THAN_OR_EQUALS $" + sumLowerBound + " && LESS_THAN_OR_EQUALS $" + sumUpperBound + "]]")))
            ));
            assertEquals(2089976284, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1454029221, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            for (String strValue : List.of("even", "odd")) {
                IntStream.range(-1, 9).forEach(lowerSum ->
                        IntStream.range(lowerSum, 10).forEach(upperSum -> {
                            Bindings bindings = Bindings.newBuilder()
                                    .set(strValueParam, strValue)
                                    .set(sumLowerBound, lowerSum)
                                    .set(sumUpperBound, upperSum)
                                    .build();

                            Set<Map<String, Number>> results = new HashSet<>();
                            try (RecordCursor<QueryResult> cursor = FDBSimpleQueryGraphTest.executeCascades(recordStore, plan, bindings)) {
                                int previousSum = Integer.MIN_VALUE;
                                for (RecordCursorResult<QueryResult> queryResult = cursor.getNext(); queryResult.hasNext(); queryResult = cursor.getNext()) {
                                    Message msg = queryResult.get().getMessage();
                                    assertNotNull(msg);
                                    Descriptors.Descriptor descriptor = msg.getDescriptorForType();
                                    Map<String, Number> result = ImmutableMap.of(
                                            "sum", (int) msg.getField(descriptor.findFieldByName("sum")),
                                            "mask", (long) msg.getField(descriptor.findFieldByName("mask")),
                                            "id", (long) msg.getField(descriptor.findFieldByName("id"))
                                    );
                                    int sumValue = result.get("sum").intValue();
                                    assertThat("Sum value should be in query predicate range", sumValue, both(greaterThanOrEqualTo(lowerSum)).and(lessThanOrEqualTo(upperSum)));
                                    assertThat("Results should be sorted by sum value", sumValue, greaterThanOrEqualTo(previousSum));
                                    results.add(result);
                                }
                            }

                            Object[] expected = data.stream()
                                    .filter(rec -> rec.getStrValueIndexed().equals(strValue))
                                    .map(rec -> ImmutableMap.of("sum", rec.getNumValue2() + rec.getNumValue3Indexed(), "mask", rec.getNumValueUnique() & 4L, "id", rec.getRecNo()))
                                    .filter(res -> {
                                        int sum = res.get("sum").intValue();
                                        return sum >= lowerSum && sum <= upperSum;
                                    })
                                    .toArray();
                            assertThat(results, containsInAnyOrder(expected));
                        })
                );
            }

            TestHelpers.assertDiscardedNone(context);
            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void matchConstantMaskValue() {
        final Index index = new Index("MySimpleRecord$num_value_unique&4", bitMaskExpression("num_value_unique", 4));
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);
        final List<TestRecords1Proto.MySimpleRecord> data = setupSimpleRecordStore(hook,
                (i, builder) -> builder.setRecNo(i).setNumValueUnique(i));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final String maskResultParam = "masked";
            final CorrelationIdentifier baseConstantId = CorrelationIdentifier.uniqueID();
            final ConstantObjectValue maskConstantValue = ConstantObjectValue.of(baseConstantId, "mask", Type.primitiveType(Type.TypeCode.LONG, false));
            for (int i = 0; i < 3; i++) {
                final long mask = (1 << i);
                final Bindings bindings = constantBindings(maskConstantValue, mask);
                final RecordQueryPlan plan = planGraph(() -> {
                    Quantifier typeQun = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
                    final Value maskValue = (Value)new ArithmeticValue.BitAndFn().encapsulate(List.of(
                            FieldValue.ofFieldName(typeQun.getFlowedObjectValue(), "num_value_unique"),
                            maskConstantValue
                    ));
                    final Quantifier selectQun = Quantifier.forEach(Reference.of(GraphExpansion.builder()
                            .addQuantifier(typeQun)
                            .addPredicate(new ValuePredicate(maskValue, new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, maskResultParam)))
                            .addResultColumn(FDBSimpleQueryGraphTest.projectColumn(typeQun.getFlowedObjectValue(), "rec_no"))
                            .build()
                            .buildSelect()));
                    return Reference.of(new LogicalSortExpression(List.of(), false, selectQun));
                }, bindings);

                if (mask == 4) {
                    // This mask value (and only this mask value) should match the literal value in the index, and so
                    // it should be able to leverage the expected index
                    assertMatchesExactly(plan, mapPlan(
                            coveringIndexPlan().where(indexPlanOf(
                                    indexPlan().where(indexName(index.getName())).and(scanComparisons(range("[EQUALS $" + maskResultParam + "]")))
                            ))
                    ));
                    assertEquals(-1043926565, plan.planHash(PlanHashable.CURRENT_LEGACY));
                    assertEquals(-1859939974, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
                } else {
                    // The other mask values do not work, and so they require executing as a scan
                    assertMatchesExactly(plan, mapPlan(
                            predicatesFilterPlan(
                                    typeFilterPlan(
                                            scanPlan().where(scanComparisons(unbounded())
                                    )
                            )
                    )));
                    // assertEquals(0, plan.planHash(PlanHashable.CURRENT_LEGACY));
                    assertEquals(-1713033856, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
                }

                for (long queryMaskValue : List.of(0L, mask, mask + 1)) {
                    final Set<Long> expectedIds = data.stream()
                            .filter(rec -> (rec.getNumValueUnique() & mask) == queryMaskValue)
                            .map(TestRecords1Proto.MySimpleRecord::getRecNo)
                            .collect(Collectors.toSet());
                    if (queryMaskValue != 0 && queryMaskValue != mask) {
                        assertThat("should not be any expected ids when the query mask is " + queryMaskValue,
                                expectedIds, empty());
                    }
                    final Bindings queryBindings = bindings.childBuilder().set(maskResultParam, queryMaskValue).build();
                    try (RecordCursor<QueryResult> cursor = FDBSimpleQueryGraphTest.executeCascades(recordStore, plan, queryBindings)) {
                        final List<Long> queriedIds = cursor.map(queryResult -> {
                            Message msg = queryResult.getMessage();
                            Descriptors.FieldDescriptor fieldDescriptor = msg.getDescriptorForType().findFieldByName("rec_no");
                            return (Long)msg.getField(fieldDescriptor);
                        }).asList().join();
                        assertThat("expected results with bit mask " + mask + " and predicate mask " + queryMaskValue,
                                queriedIds, containsInAnyOrder(expectedIds.toArray()));
                    }
                }
            }

            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void calculateFunctionFromCoveringIndexScan() {
        final Index index = new Index("MySimpleRecord$num_value_2-num_value_3_indexed", "num_value_2", "num_value_3_indexed");
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);
        final List<TestRecords1Proto.MySimpleRecord> data = setupSimpleRecordStore(hook,
                (i, builder) -> builder.setRecNo(i).setNumValue2(i % 7).setNumValue3Indexed(i % 6));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final RecordQueryPlan plan = planGraph(() -> {
                Quantifier typeQun = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final Value addValue = (Value) new ArithmeticValue.AddFn().encapsulate(List.of(
                        FieldValue.ofFieldName(typeQun.getFlowedObjectValue(), "num_value_2"),
                        FieldValue.ofFieldName(typeQun.getFlowedObjectValue(), "num_value_3_indexed")
                ));
                final SelectExpression select = GraphExpansion.builder()
                        .addQuantifier(typeQun)
                        .addResultColumn(Column.of(Optional.of("sum"), addValue))
                        .addResultColumn(Column.of(Optional.of("rec_no"), FieldValue.ofFieldName(typeQun.getFlowedObjectValue(), "rec_no")))
                        .build()
                        .buildSelect();
                Quantifier selectQun = Quantifier.forEach(Reference.of(select));
                return Reference.of(new LogicalSortExpression(ImmutableList.of(), false, selectQun));
            });
            // This should be planned as a covering index scan of the index because the functions can be calculated from the values
            // in the index. Until matching improves, we can get by with this plan
            assertMatchesExactly(plan, mapPlan(
                    typeFilterPlan(
                            scanPlan().where(scanComparisons(unbounded()))
                    )
            ));
            assertEquals(1263343956, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-2029074143, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            Set<Map<String, Object>> results = new HashSet<>();
            try (RecordCursor<QueryResult> cursor = FDBSimpleQueryGraphTest.executeCascades(recordStore, plan)) {
                for (RecordCursorResult<QueryResult> queryResult = cursor.getNext(); queryResult.hasNext(); queryResult = cursor.getNext()) {
                    Message msg = queryResult.get().getMessage();
                    assertNotNull(msg);
                    Descriptors.Descriptor descriptor = msg.getDescriptorForType();
                    results.add(ImmutableMap.of(
                            "sum", msg.getField(descriptor.findFieldByName("sum")),
                            "rec_no", msg.getField(descriptor.findFieldByName("rec_no"))
                    ));
                }
            }
            Object[] expected = data.stream()
                    .map(rec -> ImmutableMap.of("sum", rec.getNumValue2() + rec.getNumValue3Indexed(), "rec_no", rec.getRecNo()))
                    .toArray();
            assertThat(results, containsInAnyOrder(expected));

            commit(context);
        }
    }

    @Nonnull
    static Stream<Arguments> basicFunctionPlans() {
        final Stream<NonnullPair<String, Function<TestRecords1Proto.MySimpleRecord, Long>>> binaryFunctionNames = Stream.of(
                NonnullPair.of(FunctionNames.ADD, msg -> (long)(msg.getNumValue2() + msg.getNumValue3Indexed())),
                NonnullPair.of(FunctionNames.SUBTRACT, msg -> (long)(msg.getNumValue2() - msg.getNumValue3Indexed())),
                NonnullPair.of("sub", msg -> (long)(msg.getNumValue2() - msg.getNumValue3Indexed())),
                NonnullPair.of(FunctionNames.MULTIPLY, msg -> (long)(msg.getNumValue2() * msg.getNumValue3Indexed())),
                NonnullPair.of("mul", msg -> (long)(msg.getNumValue2() * msg.getNumValue3Indexed())),
                NonnullPair.of(FunctionNames.DIVIDE, msg -> (long)(msg.getNumValue2() / msg.getNumValue3Indexed())),
                NonnullPair.of("div", msg -> (long)(msg.getNumValue2() / msg.getNumValue3Indexed())),
                NonnullPair.of(FunctionNames.MOD, msg -> (long)(msg.getNumValue2() % msg.getNumValue3Indexed())),
                NonnullPair.of(FunctionNames.BITAND, msg -> (long)(msg.getNumValue2() & msg.getNumValue3Indexed())),
                NonnullPair.of(FunctionNames.BITOR, msg -> (long)(msg.getNumValue2() | msg.getNumValue3Indexed())),
                NonnullPair.of(FunctionNames.BITXOR, msg -> (long)(msg.getNumValue2() ^ msg.getNumValue3Indexed()))
        );
        final Stream<NonnullPair<String, Function<TestRecords1Proto.MySimpleRecord, Long>>> unaryFunctionNames = Stream.of(
                NonnullPair.of(FunctionNames.SUBTRACT, msg -> (long)(-1 * msg.getNumValue2())),
                NonnullPair.of("sub", msg -> (long)(-1 * msg.getNumValue2())),
                NonnullPair.of(FunctionNames.BITNOT, msg -> ~((long)msg.getNumValue2()))
        );
        return Stream.concat(
                binaryFunctionNames.map(nameAndFunc -> Arguments.of(nameAndFunc.getLeft(), nameAndFunc.getRight(), false)),
                unaryFunctionNames.map(nameAndFunc -> Arguments.of(nameAndFunc.getLeft(), nameAndFunc.getRight(), true))
        );
    }

    @ParameterizedTest(name = "basicFunctionPlans[functionName={0}, unary={2}]")
    @MethodSource
    @DualPlannerTest
    void basicFunctionPlans(@Nonnull String functionName, @Nonnull Function<TestRecords1Proto.MySimpleRecord, Long> msgFunction, boolean unary) {
        final KeyExpression args = unary ? Key.Expressions.field("num_value_2") : Key.Expressions.concatenateFields("num_value_2", "num_value_3_indexed");
        final KeyExpression expr = Key.Expressions.function(functionName, args);
        final Index index = new Index("MySimpleRecord$binaryFunction", expr);
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);
        final List<TestRecords1Proto.MySimpleRecord> data = setupSimpleRecordStore(hook,
                (i, builder) -> builder.setRecNo(i).setNumValue2(i % 4).setNumValue3Indexed((i % 5) + 1));
        final Map<Long, List<TestRecords1Proto.MySimpleRecord>> grouped = groupByFunctionExecution(msgFunction, data);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final String param = "p";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.keyExpression(expr).equalsParameter(param))
                    .build();
            if (useCascadesPlanner && unary) {
                // The cascades planner does not currently support these unary functions.
                // Once support is added, this assertion should be removed
                RecordCoreException err = assertThrows(RecordCoreException.class, () -> planQuery(query));
                assertThat(err.getMessage(), containsString("unknown function"));
                return;
            }
            final RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, indexPlan().where(scanComparisons(range("[EQUALS $" + param + "]"))));

            for (long val : grouped.keySet()) {
                final List<TestRecords1Proto.MySimpleRecord> queried = new ArrayList<>();
                try (RecordCursorIterator<FDBQueriedRecord<Message>> iter = executeQuery(plan, Bindings.newBuilder().set(param, val).build())) {
                    while (iter.hasNext()) {
                        queried.add(TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(iter.next().getRecord()).build());
                    }
                }
                assertThat(queried, containsInAnyOrder(grouped.get(val).toArray()));
            }

            commit(context);
        }
    }

    @Nonnull
    private Map<Long, List<TestRecords1Proto.MySimpleRecord>> groupByFunctionExecution(@Nonnull Function<TestRecords1Proto.MySimpleRecord, Long> func, @Nonnull List<TestRecords1Proto.MySimpleRecord> data) {
        final Map<Long, List<TestRecords1Proto.MySimpleRecord>> grouped = new HashMap<>();
        for (TestRecords1Proto.MySimpleRecord rec : data) {
            long evaluated = func.apply(rec);
            grouped.computeIfAbsent(evaluated, ignore -> new ArrayList<>())
                    .add(rec);
        }
        return grouped;
    }

    private void assertQueryResults(@Nonnull List<TestRecords1Proto.MySimpleRecord> data,
                                    @Nonnull RecordQueryPlan plan,
                                    @Nonnull Bindings bindings,
                                    @Nonnull Predicate<TestRecords1Proto.MySimpleRecord> filter) {
        assertQueryResults(data, plan, bindings, filter, null);

    }

    private void assertQueryResults(@Nonnull List<TestRecords1Proto.MySimpleRecord> data,
                                    @Nonnull RecordQueryPlan plan,
                                    @Nonnull Bindings bindings,
                                    @Nonnull Predicate<TestRecords1Proto.MySimpleRecord> filter,
                                    @Nullable Function<TestRecords1Proto.MySimpleRecord, Tuple> sortComparisonKey) {
        final List<TestRecords1Proto.MySimpleRecord> queried = new ArrayList<>();
        try (RecordCursorIterator<FDBQueriedRecord<Message>> iterator = executeQuery(plan, bindings)) {
            while (iterator.hasNext()) {
                FDBQueriedRecord<Message> rec = iterator.next();
                queried.add(TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build());
            }
        }
        if (sortComparisonKey == null) {
            assertThat(queried, containsInAnyOrder(data.stream().filter(filter).toArray()));
        } else {
            assertThat(queried, contains(data.stream().filter(filter).sorted(Comparator.comparing(sortComparisonKey)).toArray()));
        }
    }
}
