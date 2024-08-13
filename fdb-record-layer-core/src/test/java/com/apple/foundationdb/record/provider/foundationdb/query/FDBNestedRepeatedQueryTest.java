/*
 * FDBNestedRepeatedQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexAggregateFunctionCall;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.UnnestedRecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexAggregate;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexOnlyAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.ComparablePair;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.PlanHashable.CURRENT_FOR_CONTINUATION;
import static com.apple.foundationdb.record.PlanHashable.CURRENT_LEGACY;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.list;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.comparisonKey;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.composedBitmapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.composer;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.composition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inUnionComparisonKey;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inUnionOnExpressionPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.isNotReverse;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.isReverse;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.queryComponents;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unionOnExpressionPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unorderedPrimaryKeyDistinctPlan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test class for issuing queries on a record type with a nested repeated field. In most tests, this is to emulate
 * map-like behavior.
 */
@Tag(Tags.RequiresFDB)
class FDBNestedRepeatedQueryTest extends FDBRecordStoreQueryTestBase {
    private static final String PARENT_CONSTITUENT = "parent";
    private static final String OUTER = TestRecordsNestedMapProto.OuterRecord.getDescriptor().getName();
    private static final String OUTER_WITH_ENTRIES = "OuterWithEntries";
    private static final String OUTER_WITH_TWO_ENTRIES = "OuterWithTwoEntries";
    private static final KeyExpression ENTRY_EXPR = field("map").nest(field("entry", KeyExpression.FanType.FanOut));

    private static final String NESTED_CONCAT = "nestedConcat";
    private static final String CONCAT_NESTED = "concatNested";
    private static final String MAP_VALUE_IN_VALUE = "mapValueInValue";
    private static final String OTHER_MAP_KEY = "otherMapKey";
    private static final String UNNESTED_KEY_AND_VALUE = "unnestedKeyAndValue";
    private static final String UNNESTED_KEY_OTHER_VALUE = "unnestedKeyOtherValue";
    private static final String DOUBLE_UNNESTED_KEYS = "doubleUnnestedKeys";
    private static final String COUNT_BY_KEY = "countByKey";
    private static final String COUNT_BY_KEY_UNNESTED = "countByKeyUnnested";
    private static final String COUNT_BY_PAIR_OF_KEYS = "countByPairOfKeys";
    private static final String COUNT_BY_PAIR_OF_KEYS_UNNESTED = "countByPairOfKeysUnnested";
    private static final String MAX_EVER_VALUE_BY_KEY = "maxEverValueByKey";
    private static final String MAX_EVER_VALUE_BY_KEY_UNNESTED = "maxEverValueByKeyUnnested";
    private static final String MAX_EVER_RECORD_VALUE_BY_KEY = "maxEverRecordValueByKey";
    private static final String MAX_EVER_RECORD_VALUE_BY_KEY_UNNESTED = "maxEverRecordValueByKeyUnnested";
    private static final String SUM_VALUE_BY_KEY = "sumValueByKey";
    private static final String SUM_VALUE_BY_KEY_UNNESTED = "sumValueByKeyUnnested";
    private static final String SUM_WHOLE_RECORD_VALUE_BY_KEY = "sumWholeRecordValueByKey";
    private static final String SUM_WHOLE_RECORD_VALUE_BY_KEY_UNNESTED = "sumValueByKey";
    private static final String SUM_VALUE_BY_KEY_AND_OTHER = "sumValueByKeyAndOther";
    private static final String SUM_VALUE_BY_KEY_AND_OTHER_UNNESTED = "sumValueByOtherAndKeyUnnested";
    private static final String BITMAP_VALUE_BY_KEY = "bitmapValueByKey";
    private static final String BITMAP_VALUE_BY_KEY_UNNESTED = "bitmapValueByKeyUnnested";
    private static final String BITMAP_VALUE_OTHER_KEY = "bitmapValueByOuterKey";
    private static final String BITMAP_VALUE_KEY_OTHER_UNNESTED = "bitmapValueByKeyOtherUnnested";

    private static KeyExpression onEntry(Supplier<KeyExpression> toNest) {
        return field("map").nest(field("entry", KeyExpression.FanType.FanOut).nest(toNest.get()));
    }

    private static RecordMetaDataHook addUnnestedType() {
        return metaDataBuilder -> {
            UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(OUTER_WITH_ENTRIES);
            typeBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    PARENT_CONSTITUENT, ENTRY_EXPR);
        };
    }

    private static RecordMetaDataHook addDoubleUnnestedType() {
        return metaDataBuilder -> {
            UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(OUTER_WITH_TWO_ENTRIES);
            typeBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("e1", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    PARENT_CONSTITUENT, ENTRY_EXPR);
            typeBuilder.addNestedConstituent("e2", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    PARENT_CONSTITUENT, ENTRY_EXPR);
        };
    }

    private static Index nestedConcatIndex() {
        return new Index(NESTED_CONCAT, onEntry(() -> concatenateFields("key", "value")));
    }

    private static Index concatNestedIndex() {
        return new Index(CONCAT_NESTED, concat(onEntry(() -> field("key")), onEntry(() -> field("value"))));
    }

    private static Index mapValueInValueIndex() {
        return new Index(MAP_VALUE_IN_VALUE, new KeyWithValueExpression(onEntry(() -> concatenateFields("key", "value")), 1));
    }

    private static Index otherMapKeyValue() {
        return new Index(OTHER_MAP_KEY, concat(field("other_id"), onEntry(() -> field("key"))));
    }

    private static Index unnestedKeyAndValue() {
        return new Index(UNNESTED_KEY_AND_VALUE, concat(field("entry").nest("key"), field("entry").nest("value")));
    }

    private static Index unnestedKeyOtherValue() {
        return new Index(UNNESTED_KEY_OTHER_VALUE, concat(field("entry").nest("key"), field(PARENT_CONSTITUENT).nest("other_id"), field("entry").nest("value")));
    }

    private static Index doubleUnnestedKeys() {
        return new Index(DOUBLE_UNNESTED_KEYS, concat(field("e1").nest("key"), field("e2").nest("key"), field(PARENT_CONSTITUENT).nest("other_id")));
    }

    private static Index countByKey() {
        return new Index(COUNT_BY_KEY, new GroupingKeyExpression(onEntry(() -> field("key")), 0), IndexTypes.COUNT);
    }

    private static Index countByKeyUnnested() {
        return new Index(COUNT_BY_KEY_UNNESTED, new GroupingKeyExpression(field("entry").nest("key"), 0), IndexTypes.COUNT);
    }

    private static Index countByPairOfKeys() {
        return new Index(COUNT_BY_PAIR_OF_KEYS, new GroupingKeyExpression(concat(onEntry(() -> field("key")), onEntry(() -> field("key"))), 0), IndexTypes.COUNT);
    }

    private static Index countByPairOfKeysUnnested() {
        return new Index(COUNT_BY_PAIR_OF_KEYS_UNNESTED, new GroupingKeyExpression(concat(field("e1").nest("key"), field("e2").nest("key")), 0), IndexTypes.COUNT);
    }

    private static Index maxEverValueByKey() {
        return new Index(MAX_EVER_VALUE_BY_KEY, new GroupingKeyExpression(onEntry(() -> concatenateFields("key", "value")), 1), IndexTypes.MAX_EVER_TUPLE);
    }

    private static Index maxEverValueByKeyUnnested() {
        return new Index(MAX_EVER_VALUE_BY_KEY_UNNESTED, field("entry").nest("value").groupBy(field("entry").nest("key")), IndexTypes.MAX_EVER_TUPLE);
    }

    private static Index maxEverIntValueByKey() {
        return new Index(MAX_EVER_VALUE_BY_KEY, new GroupingKeyExpression(onEntry(() -> concatenateFields("key", "int_value")), 1), IndexTypes.MAX_EVER_LONG);
    }

    private static Index maxEverIntValueByKeyUnnested() {
        return new Index(MAX_EVER_VALUE_BY_KEY_UNNESTED, field("entry").nest("int_value").groupBy(field("entry").nest("key")), IndexTypes.MAX_EVER_LONG);
    }

    private static Index maxEverRecordValueByKey() {
        return new Index(MAX_EVER_RECORD_VALUE_BY_KEY, new GroupingKeyExpression(concat(onEntry(() -> field("key")), onEntry(() -> field("value"))), 1), IndexTypes.MAX_EVER_TUPLE);
    }

    private static Index maxEverRecordValueByKeyUnnested() {
        return new Index(MAX_EVER_RECORD_VALUE_BY_KEY_UNNESTED, field("e2").nest("value").groupBy(field("e1").nest("key")), IndexTypes.MAX_EVER_TUPLE);
    }

    private static Index maxEverRecordIntValueByKey() {
        return new Index(MAX_EVER_RECORD_VALUE_BY_KEY, new GroupingKeyExpression(concat(onEntry(() -> field("key")), onEntry(() -> field("int_value"))), 1), IndexTypes.MAX_EVER_LONG);
    }

    private static Index maxEverRecordIntValueByKeyUnnested() {
        return new Index(MAX_EVER_RECORD_VALUE_BY_KEY_UNNESTED, field("e2").nest("int_value").groupBy(field("e1").nest("key")), IndexTypes.MAX_EVER_LONG);
    }

    private static Index sumValueByKey() {
        return new Index(SUM_VALUE_BY_KEY, new GroupingKeyExpression(onEntry(() -> concatenateFields("key", "int_value")), 1), IndexTypes.SUM);
    }

    private static Index sumValueByKeyUnnested() {
        return new Index(SUM_VALUE_BY_KEY_UNNESTED, field("entry").nest("int_value").groupBy(field("entry").nest("key")), IndexTypes.SUM);
    }

    private static Index sumWholeRecordByKey() {
        return new Index(SUM_WHOLE_RECORD_VALUE_BY_KEY, new GroupingKeyExpression(concat(onEntry(() -> field("key")), onEntry(() -> field("int_value"))), 1), IndexTypes.SUM);
    }

    private static Index sumWholeRecordByKeyUnnested() {
        return new Index(SUM_WHOLE_RECORD_VALUE_BY_KEY_UNNESTED, field("e2").nest("int_value").groupBy(field("e1").nest("key")), IndexTypes.SUM);
    }

    private static Index sumValueByKeyAndOther() {
        return new Index(SUM_VALUE_BY_KEY_AND_OTHER, new GroupingKeyExpression(concat(field("other_id"), onEntry(() -> concatenateFields("key", "int_value"))), 1), IndexTypes.SUM);
    }

    private static Index sumValueByKeyAndOtherUnnested() {
        return new Index(SUM_VALUE_BY_KEY_AND_OTHER_UNNESTED, field("entry").nest("int_value").groupBy(field(PARENT_CONSTITUENT).nest("other_id"), field("entry").nest("key")), IndexTypes.SUM);
    }

    private static Index bitmapValueByKey() {
        return new Index(BITMAP_VALUE_BY_KEY, new GroupingKeyExpression(onEntry(() -> concatenateFields("key", "int_value")), 1), IndexTypes.BITMAP_VALUE);
    }

    private static Index bitmapValueByKeyUnnested() {
        return new Index(BITMAP_VALUE_BY_KEY_UNNESTED, field("entry").nest("int_value").groupBy(field("entry").nest("key")), IndexTypes.BITMAP_VALUE);
    }

    private static Index bitmapValueByOtherThenKey() {
        return new Index(BITMAP_VALUE_OTHER_KEY, new GroupingKeyExpression(concat(field("other_id"), onEntry(() -> concatenateFields("key", "int_value"))), 1), IndexTypes.BITMAP_VALUE);
    }

    private static Index bitmapValueByKeyOtherUnnested() {
        return new Index(BITMAP_VALUE_KEY_OTHER_UNNESTED, field("entry").nest("int_value").groupBy(concat(field("entry").nest("key"), field(PARENT_CONSTITUENT).nest("other_id"))), IndexTypes.BITMAP_VALUE);
    }

    private static QueryComponent oneEntryEquals(String keyParam, String valueParam) {
        return Query.field("map").matches(
                Query.field("entry").oneOfThem().matches(
                        Query.and(Query.field("key").equalsParameter(keyParam), Query.field("value").equalsParameter(valueParam))
                )
        );
    }

    private static RecordMetaData mapMetaData(RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsNestedMapProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    public void createOrOpenMapStore(FDBRecordContext context, RecordMetaDataHook hook) {
        createOrOpenRecordStore(context, mapMetaData(hook));
    }

    private static Set<Long> ids(Collection<TestRecordsNestedMapProto.OuterRecord> records) {
        return records.stream()
                .map(TestRecordsNestedMapProto.OuterRecord::getRecId)
                .collect(Collectors.toSet());
    }

    private static Set<Long> otherIds(Collection<TestRecordsNestedMapProto.OuterRecord> records) {
        return records.stream()
                .map(TestRecordsNestedMapProto.OuterRecord::getOtherId)
                .collect(Collectors.toSet());
    }

    private static Set<String> mapKeys(Collection<TestRecordsNestedMapProto.OuterRecord> records) {
        return records.stream()
                .flatMap(rec -> rec.getMap().getEntryList().stream())
                .map(TestRecordsNestedMapProto.MapRecord.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private static Set<String> mapValues(Collection<TestRecordsNestedMapProto.OuterRecord> records) {
        return records.stream()
                .flatMap(rec -> rec.getMap().getEntryList().stream())
                .map(TestRecordsNestedMapProto.MapRecord.Entry::getValue)
                .collect(Collectors.toSet());
    }

    private static Set<TestRecordsNestedMapProto.OuterRecord> byKey(Collection<TestRecordsNestedMapProto.OuterRecord> records, String key) {
        return records.stream()
                .filter(rec -> rec.getMap().getEntryList().stream().anyMatch(entry -> key.equals(entry.getKey())))
                .collect(Collectors.toSet());
    }

    private static List<Integer> intValuesWithKey(Collection<TestRecordsNestedMapProto.OuterRecord> records, String key) {
        return records.stream()
                .flatMap(rec -> rec.getMap().getEntryList().stream())
                .filter(entry -> entry.getKey().equals(key))
                .map(TestRecordsNestedMapProto.MapRecord.Entry::getIntValue)
                .map(Long::intValue)
                .sorted()
                .collect(Collectors.toList());
    }

    private List<TestRecordsNestedMapProto.OuterRecord> setUpData(RecordMetaDataHook hook, Function<Integer, TestRecordsNestedMapProto.MapRecord> mapCreator) {
        final List<TestRecordsNestedMapProto.OuterRecord> records = new ArrayList<>();

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            for (int i = 0; i < 20; i++) {
                TestRecordsNestedMapProto.OuterRecord rec = TestRecordsNestedMapProto.OuterRecord.newBuilder()
                        .setRecId(i)
                        .setOtherId(i % 3)
                        .setMap(mapCreator.apply(i))
                        .build();
                recordStore.saveRecord(rec);
                records.add(rec);
            }

            commit(context);
        }

        return records;
    }

    private List<TestRecordsNestedMapProto.OuterRecord> setUpData(RecordMetaDataHook hook) {
        return setUpData(hook, i -> {
            var mapBuilder = TestRecordsNestedMapProto.MapRecord.newBuilder();
            for (int j = 0; j < 5; j++) {
                mapBuilder.addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                        .setKey("" + (i + j))
                        .setValue("" + j));
            }
            return mapBuilder.build();
        });
    }

    private List<TestRecordsNestedMapProto.OuterRecord> setUpDataWithInts(@Nonnull RecordMetaDataHook hook) {
        return setUpData(hook, i -> {
            var mapBuilder = TestRecordsNestedMapProto.MapRecord.newBuilder();
            for (int j = 0; j < 10; j++) {
                mapBuilder.addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                        .setKey("" + (i + j))
                        .setIntValue(j));
            }
            return mapBuilder.build();
        });
    }

    private List<TestRecordsNestedMapProto.OuterRecord> setUpDataWithDuplicateInts(@Nonnull RecordMetaDataHook hook) {
        return setUpData(hook, i -> {
            var mapBuilder = TestRecordsNestedMapProto.MapRecord.newBuilder();
            for (int j = 0; j < 12; j++) {
                // Add map entries with a small number of keys, some of which appear multiple times
                mapBuilder.addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                        .setKey("" + (i + (j % 3)))
                        .setIntValue(j % 2));
            }
            return mapBuilder.build();
        });
    }

    @Test
    void filterOnKeyAndProjectValueWithNestedConcat() {
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex(OUTER, nestedConcatIndex());
        filterOnKeyAndProjectValue(hook, Collections.singleton(NESTED_CONCAT));
    }

    @Test
    void filterOnKeyAndProjectValueWithConcatNested() {
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex(OUTER, concatNestedIndex());
        // The planner apparently currently doesn't match this index during planning, so we don't use it
        filterOnKeyAndProjectValue(hook, Collections.emptySet());
    }

    @Test
    void filterOnKeyAndProjectValueWithCoveringIndex() {
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex(OUTER, mapValueInValueIndex());
        filterOnKeyAndProjectValue(hook, Collections.singleton(MAP_VALUE_IN_VALUE));
    }

    private void filterOnKeyAndProjectValue(RecordMetaDataHook hook, Set<String> expectedIndexes) {
        List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(hook);
        Set<String> keys = mapKeys(data);
        assertThat(keys, not(empty()));

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final String keyParam = "key";
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER)
                    .setFilter(Query.field("map").matches(Query.field("entry").oneOfThem().matches(Query.field("key").equalsParameter(keyParam))))
                    .setRequiredResults(List.of(onEntry(() -> field("value"))))
                    .build();
            RecordQueryPlan plan = planQuery(query);
            assertEquals(expectedIndexes, plan.getUsedIndexes());

            for (String key : keys) {
                EvaluationContext evalContext = EvaluationContext.forBinding(keyParam, key);
                final List<TestRecordsNestedMapProto.OuterRecord> queried = plan.execute(recordStore, evalContext)
                        .map(FDBQueriedRecord::getRecord)
                        .map(rec -> TestRecordsNestedMapProto.OuterRecord.newBuilder().mergeFrom(rec).build())
                        .asList()
                        .join();

                Set<TestRecordsNestedMapProto.OuterRecord> expected = byKey(data, key);
                assertEquals(ids(expected), ids(queried), "Queried record IDs should match expected for key " + key);

                for (TestRecordsNestedMapProto.OuterRecord queriedRecord : queried) {
                    TestRecordsNestedMapProto.OuterRecord expectedRecord = expected.stream()
                            .filter(exp -> exp.getRecId() == queriedRecord.getRecId())
                            .findFirst()
                            .orElseGet(() -> fail("Unable to find record " + queriedRecord.getRecId()));
                    assertEquals(expectedRecord.getMap(), queriedRecord.getMap(), "All map entries should be present");
                }
            }

            commit(context);
        }
    }

    @Test
    void filterOnBothKeyAndValueWithNestedConcat() {
        filterOnBothKeyAndValue(metaDataBuilder -> metaDataBuilder.addIndex(OUTER, nestedConcatIndex()),
                fetchFromPartialRecordPlan(
                        unorderedPrimaryKeyDistinctPlan(
                                coveringIndexPlan().where(indexPlanOf(
                                        indexPlan()
                                                .where(indexName(NESTED_CONCAT))
                                                .and(scanComparisons(range("[EQUALS $key, EQUALS $value]")))
                                ))
                        )
                )
        );
    }

    @Test
    void filterOnBothKeyAndValueWithConcatNested() {
        // Note that the index here contains one entry for each element in the cross-product between the keys
        // and the values. Therefore, even though its scan range includes both the key and the value, there still
        // needs to be a residual filter on top
        filterOnBothKeyAndValue(metaDataBuilder -> metaDataBuilder.addIndex(OUTER, concatNestedIndex()),
                filterPlan(
                        fetchFromPartialRecordPlan(
                                unorderedPrimaryKeyDistinctPlan(
                                        coveringIndexPlan().where(indexPlanOf(
                                                indexPlan()
                                                        .where(indexName(CONCAT_NESTED))
                                                        .and(scanComparisons(range("[EQUALS $key, EQUALS $value]")))
                                        ))
                                )
                        )
                ).where(queryComponents(only(PrimitiveMatchers.equalsObject(oneEntryEquals("key", "value")))))
        );
    }

    @Test
    void filterOnBothKeyAndValueWithCoveringIndex() {
        // Note that we should be able to push down the filter on the value column to the index,
        // though it would have to be executed as a residual filter. Effectively:
        //
        // Fetch(Covering(Index(mapValueInValue [EQUALS $key])) | (map.entry.value EQUALS $value)))
        //
        // However, the matching logic currently doesn't let that happen, so the index is only used to match the key,
        // and the value matching happens after the base record fetch.
        filterOnBothKeyAndValue(metaDataBuilder -> metaDataBuilder.addIndex(OUTER, mapValueInValueIndex()),
                filterPlan(
                        fetchFromPartialRecordPlan(
                                unorderedPrimaryKeyDistinctPlan(
                                        coveringIndexPlan().where(indexPlanOf(
                                                indexPlan()
                                                        .where(indexName(MAP_VALUE_IN_VALUE))
                                                        .and(scanComparisons(range("[EQUALS $key]")))
                                        ))
                                )
                        )
                ).where(queryComponents(only(PrimitiveMatchers.equalsObject(oneEntryEquals("key", "value")))))
        );
    }

    private void filterOnBothKeyAndValue(RecordMetaDataHook hook, BindingMatcher<? extends RecordQueryPlan> planMatcher) {
        List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(hook);
        Set<String> keys = mapKeys(data);
        assertThat(keys, not(empty()));
        Set<String> values = mapValues(data);

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final String keyParam = "key";
            final String valueParam = "value";
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER)
                    .setFilter(oneEntryEquals(keyParam, valueParam))
                    .setRequiredResults(List.of(onEntry(() -> concatenateFields("key", "value"))))
                    .build();
            RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, planMatcher);


            for (String key : keys) {
                for (String value : values) {
                    EvaluationContext evalContext = EvaluationContext.forBinding(keyParam, key).withBinding(valueParam, value);
                    final List<TestRecordsNestedMapProto.OuterRecord> queried = plan.execute(recordStore, evalContext)
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> TestRecordsNestedMapProto.OuterRecord.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join();

                    Set<TestRecordsNestedMapProto.OuterRecord> expected = data.stream()
                            .filter(rec -> rec.getMap().getEntryList().stream().anyMatch(entry -> entry.getKey().equals(key) && entry.getValue().equals(value)))
                            .collect(Collectors.toSet());
                    assertEquals(ids(expected), ids(queried), "Queried record IDs should match expected for key " + key);
                }
            }

            commit(context);
        }
    }

    @ParameterizedTest(name = "orFilterOnKeyAndOrderByValue[reverse={0}]")
    @BooleanSource
    void orFilterOnKeyAndOrderByValue(boolean reverse) {
        final Index unnestedOtherKeyValueIndex = new Index("unnestedOtherKeyValue",
                concat(field(PARENT_CONSTITUENT).nest("other_id"), field("entry").nest(concatenateFields("key", "value"))));
        final RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaData -> {
                    final RecordTypeBuilder outerRecord = metaData.getRecordType("OuterRecord");
                    outerRecord.setPrimaryKey(concatenateFields("other_id", "rec_id"));

                    metaData.addIndex(OUTER_WITH_ENTRIES, unnestedOtherKeyValueIndex);
                });
        final List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(hook);
        final Set<Long> otherIds = data.stream().map(TestRecordsNestedMapProto.OuterRecord::getOtherId).collect(Collectors.toSet());
        assertThat(otherIds, not(empty()));
        final Set<String> keys = mapKeys(data);
        assertThat(keys, not(empty()));

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final String otherParam = "otherParam";
            final String k1Param = "key1Param";
            final String k2Param = "key2Param";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER_WITH_ENTRIES)
                    .setFilter(
                            Query.and(
                                   Query.field(PARENT_CONSTITUENT).matches(
                                           Query.field("other_id").equalsParameter(otherParam)),
                                    Query.field("entry").matches(
                                            Query.or(
                                                    Query.field("key").equalsParameter(k1Param),
                                                    Query.field("key").equalsParameter(k2Param)
                                            )
                                    )
                            )
                    )
                    .setSort(field("entry").nest("value"), reverse)
                    .build();
            setOmitPrimaryKeyInUnionOrderingKey(true);
            setNormalizeNestedFields(true);

            final RecordQueryPlan plan = planQuery(query);
            assertEquals(reverse, plan.isReverse());
            final List<KeyExpression> comparisonKeyComponents = ImmutableList.<KeyExpression>builder()
                    .add(field("entry").nest("value"))
                    .addAll(recordStore.getRecordMetaData().getSyntheticRecordType(OUTER_WITH_ENTRIES).getPrimaryKey().normalizeKeyForPositions())
                    .build();
            assertThat(comparisonKeyComponents, hasSize(4));
            assertEquals(4, comparisonKeyComponents.stream().mapToInt(KeyExpression::getColumnSize).sum());
            assertMatchesExactly(plan, unionOnExpressionPlan(
                    indexPlan().where(indexName(unnestedOtherKeyValueIndex.getName())).and(scanComparisons(range("[EQUALS $" + otherParam + ", EQUALS $" + k1Param + "]"))),
                    indexPlan().where(indexName(unnestedOtherKeyValueIndex.getName())).and(scanComparisons(range("[EQUALS $" + otherParam + ", EQUALS $" + k2Param + "]")))
            ).where(comparisonKey(concat(comparisonKeyComponents))));
            assertEquals(reverse ? 1619322554L : 1619322521L, plan.planHash(CURRENT_LEGACY));
            assertEquals(reverse ? 803980444L : 809700502L, plan.planHash(CURRENT_FOR_CONTINUATION));

            for (long otherId : otherIds) {
                for (String key1 : keys) {
                    for (String key2 : keys) {
                        final List<NonnullPair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry>> expected = data.stream()
                                .filter(rec -> rec.getOtherId() == otherId)
                                .flatMap(rec -> rec.getMap().getEntryList().stream()
                                        .filter(entry -> entry.getKey().equals(key1) || entry.getKey().equals(key2))
                                        .map(entry -> NonnullPair.of(rec, entry)))
                                .sorted((p1, p2) -> {
                                    int comparison = p1.getRight().getValue().compareTo(p2.getRight().getValue());
                                    if (comparison != 0) {
                                        return reverse ? (comparison * -1) : comparison;
                                    }
                                    comparison = Long.compare(p1.getLeft().getRecId(), p2.getLeft().getRecId());
                                    if (comparison != 0) {
                                        return reverse ? (comparison * -1) : comparison;
                                    }
                                    comparison = Integer.compare(p1.getLeft().getMap().getEntryList().indexOf(p1.getRight()), p2.getLeft().getMap().getEntryList().indexOf(p2.getRight()));
                                    return reverse ? (comparison * -1) : comparison;
                                })
                                .collect(Collectors.toList());

                        final Bindings bindings = Bindings.newBuilder()
                                .set(otherParam, otherId)
                                .set(k1Param, key1)
                                .set(k2Param, key2)
                                .build();
                        final EvaluationContext evaluationContext = EvaluationContext.forBindings(bindings);
                        final List<Pair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry>> queried = plan.execute(recordStore, evaluationContext)
                                .map(FDBQueriedRecord::getSyntheticRecord)
                                .map(synthetic -> {
                                    TestRecordsNestedMapProto.OuterRecord outer = TestRecordsNestedMapProto.OuterRecord.newBuilder()
                                            .mergeFrom(synthetic.getConstituent(PARENT_CONSTITUENT).getRecord())
                                            .build();
                                    TestRecordsNestedMapProto.MapRecord.Entry entry = TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                            .mergeFrom(synthetic.getConstituent("entry").getRecord())
                                            .build();
                                    return Pair.of(outer, entry);
                                })
                                .asList()
                                .join();
                        assertEquals(expected, queried);
                    }
                }
            }
        }
    }

    @ParameterizedTest(name = "orQueryWithIdInOrdering[reverse={0}]")
    @BooleanSource
    void orQueryWithIdInOrdering(boolean reverse) {
        final Index keyIdValueIndex = new Index("synthetic$keyIdValue",
                concat(field("entry").nest("key"), field(PARENT_CONSTITUENT).nest("rec_id"), field("entry").nest("value")));
        final RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaData -> metaData.addIndex(OUTER_WITH_ENTRIES, keyIdValueIndex));
        final List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(hook);

        final Set<String> keys = mapKeys(data);
        assertThat(keys, not(empty()));

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final String key1Param = "key1Param";
            final String key2Param = "key2Param";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER_WITH_ENTRIES)
                    .setFilter(Query.field("entry").matches(
                            Query.or(Query.field("key").equalsParameter(key1Param), Query.field("key").equalsParameter(key2Param))
                    ))
                    .setSort(field(PARENT_CONSTITUENT).nest("rec_id"), reverse)
                    .build();
            setNormalizeNestedFields(true);
            setOmitPrimaryKeyInUnionOrderingKey(true);

            final RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, unionOnExpressionPlan(
                    indexPlan().where(indexName(keyIdValueIndex.getName())).and(scanComparisons(range("[EQUALS $" + key1Param + "]"))),
                    indexPlan().where(indexName(keyIdValueIndex.getName())).and(scanComparisons(range("[EQUALS $" + key2Param + "]")))
            ).where(comparisonKey(concat(field(PARENT_CONSTITUENT).nest("rec_id"), field("entry").nest("value"), recordType(), list(field(PARENT_CONSTITUENT).nest("rec_id")), list(field(UnnestedRecordType.POSITIONS_FIELD).nest("entry"))))));
            assertEquals(reverse ? -1283520453 : -1283520486, plan.planHash(CURRENT_LEGACY));
            assertEquals(reverse ? -2138865240 : -2133145182, plan.planHash(CURRENT_FOR_CONTINUATION));

            for (String key1 : keys) {
                for (String key2 : keys) {
                    final Bindings bindings = Bindings.newBuilder()
                            .set(key1Param, key1)
                            .set(key2Param, key2)
                            .build();
                    final EvaluationContext evaluationContext = EvaluationContext.forBindings(bindings);

                    final List<ComparablePair<Long, String>> expected = data.stream()
                            .flatMap(outerRecord -> outerRecord.getMap().getEntryList().stream()
                                    .filter(entry -> entry.getKey().equals(key1) || entry.getKey().equals(key2))
                                    .map(entry -> ComparablePair.of(outerRecord.getRecId(), entry.getValue()))
                            )
                            .sorted(reverse ? Comparator.reverseOrder() : Comparator.naturalOrder())
                            .collect(Collectors.toList());
                    final List<Pair<Long, String>> actual = plan.execute(recordStore, evaluationContext)
                            .map(FDBQueriedRecord::getSyntheticRecord)
                            .map(synthetic -> {
                                TestRecordsNestedMapProto.OuterRecord outerRecord = TestRecordsNestedMapProto.OuterRecord.newBuilder()
                                        .mergeFrom(synthetic.getConstituent(PARENT_CONSTITUENT).getRecord())
                                        .build();
                                TestRecordsNestedMapProto.MapRecord.Entry entry = TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                        .mergeFrom(synthetic.getConstituent("entry").getRecord())
                                        .build();
                                assertThat(entry.getKey(), either(equalTo(key1)).or(equalTo(key2)));
                                assertTrue(outerRecord.getMap().getEntryList().contains(entry), () -> "outer record should contain entry.\n  Outer record:\n" + outerRecord + "\n  Entry:\n" + entry);
                                return Pair.of(outerRecord.getRecId(), entry.getValue());
                            })
                            .asList()
                            .join();
                    assertEquals(expected, actual);
                }
            }
            commit(context);
        }
    }

    @ParameterizedTest(name = "inFilterOnKeyAndOrderByValue[reverse={0}]")
    @BooleanSource
    void inFilterOnKeyAndOrderByValue(boolean reverse) {
        final Index unnestedOtherKeyValueIndex = new Index("unnestedOtherKeyValue",
                concat(field(PARENT_CONSTITUENT).nest("other_id"), field("entry").nest(concatenateFields("key", "value"))));
        final RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaData -> {
                    final RecordTypeBuilder outerRecord = metaData.getRecordType("OuterRecord");
                    outerRecord.setPrimaryKey(concatenateFields("other_id", "rec_id"));

                    metaData.addIndex(OUTER_WITH_ENTRIES, unnestedOtherKeyValueIndex);
                });
        final List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(hook);
        final Set<Long> otherIds = data.stream().map(TestRecordsNestedMapProto.OuterRecord::getOtherId).collect(Collectors.toSet());
        assertThat(otherIds, not(empty()));
        final Set<String> keys = mapKeys(data);
        assertThat(keys, not(empty()));

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final String otherParam = "otherParam";
            final String keyListParam = "keyListParam";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER_WITH_ENTRIES)
                    .setFilter(
                            Query.and(
                                    Query.field(PARENT_CONSTITUENT).matches(
                                            Query.field("other_id").equalsParameter(otherParam)),
                                    Query.field("entry").matches(Query.field("key").in(keyListParam))
                            )
                    )
                    .setSort(field("entry").nest("value"), reverse)
                    .build();

            planner.setConfiguration(planner.getConfiguration().asBuilder()
                    .setOmitPrimaryKeyInOrderingKeyForInUnion(true)
                    .setAttemptFailedInJoinAsUnionMaxSize(100)
                    .build());
            final RecordQueryPlan plan = planQuery(query);
            assertEquals(reverse, plan.isReverse());
            final List<KeyExpression> comparisonKeyComponents = ImmutableList.<KeyExpression>builder()
                    .add(field("entry").nest("value"))
                    .addAll(recordStore.getRecordMetaData().getSyntheticRecordType(OUTER_WITH_ENTRIES).getPrimaryKey().normalizeKeyForPositions())
                    .build();
            assertThat(comparisonKeyComponents, hasSize(4));
            assertEquals(4, comparisonKeyComponents.stream().mapToInt(KeyExpression::getColumnSize).sum());
            assertMatchesExactly(plan, inUnionOnExpressionPlan(indexPlan()
                    .where(indexName(unnestedOtherKeyValueIndex.getName()))
                    .and(scanComparisons(range("[EQUALS $" + otherParam + ", EQUALS $__in_key__0]")))
                    .and(reverse ? isReverse() : isNotReverse())
            ).where(inUnionComparisonKey(concat(comparisonKeyComponents))));
            assertEquals(reverse ? -1181712345L : -1181713306L, plan.planHash(CURRENT_LEGACY));
            assertEquals(reverse ? -1128659733L : -1128480987L, plan.planHash(CURRENT_FOR_CONTINUATION));

            for (long otherId : otherIds) {
                for (String key1 : keys) {
                    for (String key2 : keys) {
                        final List<Pair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry>> expected = data.stream()
                                .filter(rec -> rec.getOtherId() == otherId)
                                .flatMap(rec -> rec.getMap().getEntryList().stream()
                                        .filter(entry -> entry.getKey().equals(key1) || entry.getKey().equals(key2))
                                        .map(entry -> Pair.of(rec, entry)))
                                .sorted((p1, p2) -> {
                                    int comparison = p1.getRight().getValue().compareTo(p2.getRight().getValue());
                                    if (comparison != 0) {
                                        return reverse ? (comparison * -1) : comparison;
                                    }
                                    comparison = Long.compare(p1.getLeft().getRecId(), p2.getLeft().getRecId());
                                    if (comparison != 0) {
                                        return reverse ? (comparison * -1) : comparison;
                                    }
                                    comparison = Integer.compare(p1.getLeft().getMap().getEntryList().indexOf(p1.getRight()), p2.getLeft().getMap().getEntryList().indexOf(p2.getRight()));
                                    return reverse ? (comparison * -1) : comparison;
                                })
                                .collect(Collectors.toList());

                        final Bindings bindings = Bindings.newBuilder()
                                .set(otherParam, otherId)
                                .set(keyListParam, List.of(key1, key2))
                                .build();
                        final EvaluationContext evaluationContext = EvaluationContext.forBindings(bindings);
                        final List<Pair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry>> queried = plan.execute(recordStore, evaluationContext)
                                .map(FDBQueriedRecord::getSyntheticRecord)
                                .map(synthetic -> {
                                    TestRecordsNestedMapProto.OuterRecord outer = TestRecordsNestedMapProto.OuterRecord.newBuilder()
                                            .mergeFrom(synthetic.getConstituent(PARENT_CONSTITUENT).getRecord())
                                            .build();
                                    TestRecordsNestedMapProto.MapRecord.Entry entry = TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                            .mergeFrom(synthetic.getConstituent("entry").getRecord())
                                            .build();
                                    return Pair.of(outer, entry);
                                })
                                .asList()
                                .join();
                        assertEquals(expected, queried);
                    }
                }
            }
        }
    }

    @ParameterizedTest(name = "inFilterOrderByValueAsOr[reverse={0}]")
    @BooleanSource
    void inFilterOrderByValueAsOr(boolean reverse) {
        final Index unnestedOtherKeyValueIndex = new Index("unnestedOtherKeyValue",
                concat(field(PARENT_CONSTITUENT).nest("other_id"), field("entry").nest(concatenateFields("key", "value"))));
        final RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaData -> {
                    final RecordTypeBuilder outerRecord = metaData.getRecordType("OuterRecord");
                    outerRecord.setPrimaryKey(concatenateFields("other_id", "rec_id"));

                    metaData.addIndex(OUTER_WITH_ENTRIES, unnestedOtherKeyValueIndex);
                });
        final List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(hook);
        final Set<Long> otherIds = data.stream().map(TestRecordsNestedMapProto.OuterRecord::getOtherId).collect(Collectors.toSet());
        assertThat(otherIds, not(empty()));
        final Set<String> keys = mapKeys(data);
        assertThat(keys, not(empty()));

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final long otherId = 1L;
            assertThat(otherId, in(otherIds));
            final List<String> keyList = List.of("2", "4", "6");
            keyList.forEach(key -> assertThat(key, in(keys)));

            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER_WITH_ENTRIES)
                    .setFilter(
                            Query.and(
                                    Query.field(PARENT_CONSTITUENT).matches(
                                            Query.field("other_id").equalsValue(otherId)),
                                    Query.field("entry").matches(Query.field("key").in(keyList))
                            )
                    )
                    .setSort(field("entry").nest("value"), reverse)
                    .build();

            planner.setConfiguration(planner.getConfiguration().asBuilder()
                    .setAttemptFailedInJoinAsOr(true)
                    .setOmitPrimaryKeyInUnionOrderingKey(true)
                    .setNormalizeNestedFields(true)
                    .build());
            // The way the Boolean expression is normalized after replacing the IN with an OR results in an expression
            // that is not matched, so query planning fails. This could be fixed by using the BooleanNormalizer
            // after re-writing the query, but this code path is more-or-less deprecated in favor of planning as
            // an in-union, so just assert that this fails until we start planning this as an ordinary union
            final RecordCoreException rce = assertThrows(RecordCoreException.class, () -> planQuery(query));
            assertThat(rce.getMessage(), containsString("Cannot sort without appropriate index"));
        }
    }

    @Test
    void evalEntryExpr() {
        List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(NO_HOOK);

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, NO_HOOK);

            for (TestRecordsNestedMapProto.OuterRecord outerRecord : data) {
                FDBStoredRecord<Message> stored = recordStore.loadRecord(Tuple.from(outerRecord.getRecId()));
                List<Key.Evaluated> evalList = ENTRY_EXPR.evaluate(stored);
                assertThat(evalList, hasSize(outerRecord.getMap().getEntryCount()));
                List<TestRecordsNestedMapProto.MapRecord.Entry> evaluatedEntries = new ArrayList<>();
                for (Key.Evaluated eval : evalList) {
                    assertEquals(ENTRY_EXPR.getColumnSize(), eval.size(), "Evaluated object " + eval + "should have expected number of columns");

                    Object nestedObj = eval.getObject(0);
                    assertNotNull(nestedObj);
                    assertThat(nestedObj, instanceOf(Message.class));
                    Message nestedMessage = (Message) nestedObj;
                    assertSame(TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(), nestedMessage.getDescriptorForType());
                    evaluatedEntries.add(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                            .mergeFrom(nestedMessage)
                            .build());
                }
                assertEquals(outerRecord.getMap().getEntryList(), evaluatedEntries);
            }

            commit(context);
        }
    }

    @Test
    void filterKeyAndProjectValueOnUnnestedMinimalIndex() {
        RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, unnestedKeyAndValue()));
        filterKeyAndProjectValueOnUnnested(hook,
                coveringIndexPlan()
                    .where(indexPlanOf(
                            indexPlan()
                                    .where(indexName(UNNESTED_KEY_AND_VALUE))
                                    .and(scanComparisons(range("[EQUALS $key]")))
                    ))
        );
    }

    @Test
    void filterKeyAndProjectValueUnnestedWithOtherFieldInIndex() {
        RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, unnestedKeyOtherValue()));
        filterKeyAndProjectValueOnUnnested(hook,
                coveringIndexPlan()
                        .where(indexPlanOf(
                                indexPlan()
                                        .where(indexName(UNNESTED_KEY_OTHER_VALUE))
                                        .and(scanComparisons(range("[EQUALS $key]")))
                        ))
        );
    }

    private void filterKeyAndProjectValueOnUnnested(RecordMetaDataHook hook, BindingMatcher<? extends RecordQueryPlan> planMatcher) {
        List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(hook);
        Set<String> keys = mapKeys(data);
        assertThat(keys, not(empty()));

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final String keyParam = "key";
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER_WITH_ENTRIES)
                    .setFilter(Query.field("entry").matches(Query.field("key").equalsParameter(keyParam)))
                    .setRequiredResults(List.of(field("entry").nest("value")))
                    .build();
            RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, planMatcher);

            final KeyExpression recIdExpr = field(PARENT_CONSTITUENT).nest("rec_id");
            final KeyExpression keyAndValueExpr = field("entry").nest(concatenateFields("key", "value"));
            for (String key : keys) {
                EvaluationContext evaluationContext = EvaluationContext.forBinding(keyParam, key);
                final List<TestRecordsNestedMapProto.OuterRecord> queried = plan.execute(recordStore, evaluationContext)
                        .map(rec -> {
                            // Nest the data back into an OuterRecord
                            long recId = rec.getPrimaryKey().getNestedTuple(1).getLong(0);
                            assertEquals(recId, recIdExpr.evaluateMessageSingleton(rec, rec.getRecord()).getLong(0));

                            Key.Evaluated entryEval = keyAndValueExpr.evaluateMessageSingleton(rec, rec.getRecord());
                            TestRecordsNestedMapProto.MapRecord.Entry entry = TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                    .setKey(entryEval.getString(0))
                                    .setValue(entryEval.getString(1))
                                    .build();

                            return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                                    .setRecId(recId)
                                    .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder().addEntry(entry))
                                    .build();
                        })
                        .asList()
                        .join();

                Set<TestRecordsNestedMapProto.OuterRecord> expected = byKey(data, key);
                assertThat(queried, hasSize(expected.size()));
                Set<Long> keySet = new HashSet<>();
                for (TestRecordsNestedMapProto.OuterRecord queriedRecord : queried) {
                    assertTrue(keySet.add(queriedRecord.getRecId()), () -> ("Duplicate key " + queriedRecord.getRecId() + " found in query results"));
                    TestRecordsNestedMapProto.OuterRecord expectedRecord = expected.stream()
                            .filter(rec -> rec.getRecId() == queriedRecord.getRecId())
                            .findFirst()
                            .orElseGet(() -> fail("Record with key " + queriedRecord.getRecId() + " not found in expected set"));

                    assertThat(queriedRecord.getMap().getEntryList(), hasSize(1));
                    TestRecordsNestedMapProto.MapRecord.Entry queriedEntry = queriedRecord.getMap().getEntry(0);
                    assertEquals(key, queriedEntry.getKey());
                    TestRecordsNestedMapProto.MapRecord.Entry expectedEntry = expectedRecord.getMap().getEntryList().stream()
                            .filter(e -> e.getKey().equals(key))
                            .findFirst()
                            .orElseGet(() -> fail("Expected record missing entry with key " + key));
                    assertEquals(expectedEntry.getValue(), queriedEntry.getValue());
                }
            }

            commit(context);
        }
    }

    @Test
    void filterKeyAndOtherIdUnnestedWithKeyValueIndex() {
        RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, unnestedKeyAndValue()));
        filterOnKeyAndOtherIdUnnested(hook,
                filterPlan(
                        indexPlan()
                                .where(indexName(UNNESTED_KEY_AND_VALUE))
                                .and(scanComparisons(range("[EQUALS $key]")))
                ).where(queryComponents(only(PrimitiveMatchers.equalsObject(Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsParameter("other"))))))
        );
    }

    @Test
    void filterKeyAndOtherIdUnnestedWithKeyOtherValueIndex() {
        RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, unnestedKeyOtherValue()));
        filterOnKeyAndOtherIdUnnested(hook,
                coveringIndexPlan()
                        .where(indexPlanOf(
                                indexPlan()
                                        .where(indexName(UNNESTED_KEY_OTHER_VALUE))
                                        .and(scanComparisons(range("[EQUALS $key, EQUALS $other]")))
                        ))
        );
    }

    @Test
    void filterKeyAndOtherIdWithSingleTypeIndex() {
        RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OUTER, otherMapKeyValue()));
        final BindingMatcher<RecordQueryPlan> anyPlanMatcher = PrimitiveMatchers.anyObject();
        RecordCoreException err = assertThrows(RecordCoreException.class, () -> filterOnKeyAndOtherIdUnnested(hook, anyPlanMatcher));
        assertThat(err.getMessage(), containsString("cannot create scan plan for a synthetic record type"));
    }

    private void filterOnKeyAndOtherIdUnnested(RecordMetaDataHook hook, BindingMatcher<? extends RecordQueryPlan> planMatcher) {
        List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(hook);
        Set<String> keys = mapKeys(data);
        assertThat(keys, not(empty()));
        Set<Long> otherIds = otherIds(data);

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final String keyParam = "key";
            final String otherParam = "other";
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER_WITH_ENTRIES)
                    .setFilter(Query.and(
                            Query.field("entry").matches(Query.field("key").equalsParameter(keyParam)),
                            Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsParameter(otherParam))))
                    .setRequiredResults(List.of(field("entry").nest("value")))
                    .build();
            RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, planMatcher);

            final KeyExpression recIdExpr = field(PARENT_CONSTITUENT).nest("rec_id");

            final KeyExpression keyAndValueExpr = field("entry").nest(concatenateFields("key", "value"));
            final KeyExpression otherExpr = field(PARENT_CONSTITUENT).nest("other_id");
            for (String key : keys) {
                Set<TestRecordsNestedMapProto.OuterRecord> recordsWithKey = byKey(data, key);
                for (long otherId : otherIds) {
                    EvaluationContext evaluationContext = EvaluationContext.forBinding(keyParam, key).withBinding(otherParam, otherId);
                    final List<TestRecordsNestedMapProto.OuterRecord> queried = plan.execute(recordStore, evaluationContext)
                            .map(rec -> {
                                // Nest the data back into an OuterRecord
                                long recId = rec.getPrimaryKey().getNestedTuple(1).getLong(0);
                                assertEquals(recId, recIdExpr.evaluateMessageSingleton(rec, rec.getRecord()).getLong(0));

                                long recOtherId = otherExpr.evaluateMessageSingleton(rec, rec.getRecord()).getLong(0);

                                Key.Evaluated entryEval = keyAndValueExpr.evaluateMessageSingleton(rec, rec.getRecord());
                                TestRecordsNestedMapProto.MapRecord.Entry entry = TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                        .setKey(entryEval.getString(0))
                                        .setValue(entryEval.getString(1))
                                        .build();

                                return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                                        .setRecId(recId)
                                        .setOtherId(recOtherId)
                                        .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder().addEntry(entry))
                                        .build();
                            })
                            .asList()
                            .join();

                    Set<TestRecordsNestedMapProto.OuterRecord> expected = recordsWithKey.stream()
                            .filter(rec -> rec.getOtherId() == otherId)
                            .collect(Collectors.toSet());
                    assertThat(queried, hasSize(expected.size()));
                    Set<Long> keySet = new HashSet<>();
                    for (TestRecordsNestedMapProto.OuterRecord queriedRecord : queried) {
                        assertTrue(keySet.add(queriedRecord.getRecId()), () -> ("Duplicate key " + queriedRecord.getRecId() + " found in query results"));
                        assertEquals(otherId, queriedRecord.getOtherId());

                        assertThat(queriedRecord.getMap().getEntryList(), hasSize(1));
                        TestRecordsNestedMapProto.MapRecord.Entry queriedEntry = queriedRecord.getMap().getEntry(0);
                        assertEquals(key, queriedEntry.getKey());

                        final TestRecordsNestedMapProto.OuterRecord expectedRecord = expected.stream()
                                .filter(rec -> rec.getRecId() == queriedRecord.getRecId())
                                .findFirst()
                                .orElseGet(() -> fail("Record with key " + queriedRecord.getRecId() + " not found in expected set"));
                        TestRecordsNestedMapProto.MapRecord.Entry expectedEntry = expectedRecord.getMap().getEntryList().stream()
                                .filter(e -> e.getKey().equals(key))
                                .findFirst()
                                .orElseGet(() -> fail("Expected record missing entry with key " + key));
                        assertEquals(expectedEntry.getValue(), queriedEntry.getValue());
                    }
                }
            }

            commit(context);
        }
    }

    @Test
    void queryOnTwoMapKeysWithDoubleUnnestedIndex() {
        RecordMetaDataHook hook = addDoubleUnnestedType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OUTER_WITH_TWO_ENTRIES, doubleUnnestedKeys()));
        queryOnTwoMapKeys(hook, indexPlan()
                .where(indexName(DOUBLE_UNNESTED_KEYS))
                .and(scanComparisons(range("[EQUALS $key1, EQUALS $key2]")))
        );
    }

    private void queryOnTwoMapKeys(RecordMetaDataHook hook, BindingMatcher<? extends RecordQueryPlan> planMatcher) {
        List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(hook);
        Set<String> keys = mapKeys(data);
        assertThat(keys, not(empty()));

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final String key1Param = "key1";
            final String key2Param = "key2";
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER_WITH_TWO_ENTRIES)
                    .setFilter(Query.and(
                            Query.field("e1").matches(Query.field("key").equalsParameter(key1Param)),
                            Query.field("e2").matches(Query.field("key").equalsParameter(key2Param))))
                    .setRequiredResults(List.of(
                            field(PARENT_CONSTITUENT).nest("rec_id"),
                            field(PARENT_CONSTITUENT).nest("other_id"),
                            field("e1").nest("value"),
                            field("e2").nest("value")
                    ))
                    .build();
            RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, planMatcher);

            final KeyExpression recIdExpr = field(PARENT_CONSTITUENT).nest("rec_id");
            final KeyExpression entry1Expr = field("e1").nest(concatenateFields("key", "value"));
            final KeyExpression entry2Expr = field("e2").nest(concatenateFields("key", "value"));
            final KeyExpression otherExpr = field(PARENT_CONSTITUENT).nest("other_id");

            for (String key1 : keys) {
                Set<TestRecordsNestedMapProto.OuterRecord> recordsWithKey = byKey(data, key1);
                for (String key2 : keys) {
                    EvaluationContext evaluationContext = EvaluationContext.forBinding(key1Param, key1).withBinding(key2Param, key2);
                    final List<TestRecordsNestedMapProto.OuterRecord> queried = plan.execute(recordStore, evaluationContext)
                            .map(rec -> {
                                // Nest the data back into an OuterRecord
                                long recId = rec.getPrimaryKey().getNestedTuple(1).getLong(0);
                                assertEquals(recId, recIdExpr.evaluateMessageSingleton(rec, rec.getRecord()).getLong(0));

                                long recOtherId = otherExpr.evaluateMessageSingleton(rec, rec.getRecord()).getLong(0);

                                final Key.Evaluated entry1Eval = entry1Expr.evaluateMessageSingleton(rec, rec.getRecord());
                                TestRecordsNestedMapProto.MapRecord.Entry entry1 = TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                        .setKey(entry1Eval.getString(0))
                                        .setValue(entry1Eval.getString(1))
                                        .build();

                                final Key.Evaluated entry2Eval = entry2Expr.evaluateMessageSingleton(rec, rec.getRecord());
                                TestRecordsNestedMapProto.MapRecord.Entry entry2 = TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                        .setKey(entry2Eval.getString(0))
                                        .setValue(entry2Eval.getString(1))
                                        .build();

                                return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                                        .setRecId(recId)
                                        .setOtherId(recOtherId)
                                        .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder().addEntry(entry1).addEntry(entry2))
                                        .build();
                            })
                            .asList()
                            .join();

                    Set<TestRecordsNestedMapProto.OuterRecord> expected = byKey(recordsWithKey, key2);
                    assertThat(queried, hasSize(expected.size()));
                    Set<Long> keySet = new HashSet<>();
                    for (TestRecordsNestedMapProto.OuterRecord queriedRecord : queried) {
                        assertTrue(keySet.add(queriedRecord.getRecId()), () -> ("Duplicate key " + queriedRecord.getRecId() + " found in query results"));
                        TestRecordsNestedMapProto.OuterRecord expectedRecord = expected.stream()
                                .filter(rec -> rec.getRecId() == queriedRecord.getRecId())
                                .findFirst()
                                .orElseGet(() -> fail("Record with key " + queriedRecord.getRecId() + " not found in expected set"));

                        assertEquals(expectedRecord.getOtherId(), queriedRecord.getOtherId());

                        assertThat(queriedRecord.getMap().getEntryList(), hasSize(2));
                        TestRecordsNestedMapProto.MapRecord.Entry queriedEntry1 = queriedRecord.getMap().getEntry(0);
                        assertEquals(key1, queriedEntry1.getKey());
                        TestRecordsNestedMapProto.MapRecord.Entry expectedEntry1 = expectedRecord.getMap().getEntryList().stream()
                                .filter(e -> e.getKey().equals(key1))
                                .findFirst()
                                .orElseGet(() -> fail("Expected record missing entry with key " + key1));
                        assertEquals(expectedEntry1.getValue(), queriedEntry1.getValue());

                        TestRecordsNestedMapProto.MapRecord.Entry queriedEntry2 = queriedRecord.getMap().getEntry(1);
                        assertEquals(key2, queriedEntry2.getKey());
                        TestRecordsNestedMapProto.MapRecord.Entry expectedEntry2 = expectedRecord.getMap().getEntryList().stream()
                                .filter(e -> e.getKey().equals(key2))
                                .findFirst()
                                .orElseGet(() -> fail("Expected record missing entry with key " + key2));
                        assertEquals(expectedEntry2.getValue(), queriedEntry2.getValue());
                    }
                }
            }

            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void countByKeyIndexes() {
        final RecordMetaDataHook hook = addUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, countByKey());
            metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, countByKeyUnnested());
        });
        final IndexAggregateFunction countOuter = new IndexAggregateFunction(FunctionNames.COUNT, onEntry(() -> field("key")), COUNT_BY_KEY);
        final IndexAggregateFunction countUnnested = new IndexAggregateFunction(FunctionNames.COUNT, field("entry").nest("key"), COUNT_BY_KEY_UNNESTED);
        testAggregateIndex(hook, setUpData(hook), countOuter, countUnnested, data -> {
            Map<String, Long> dataCountedByKey = new HashMap<>();
            data.stream()
                    .flatMap(rec -> rec.getMap().getEntryList().stream())
                    .forEach(entry -> dataCountedByKey.compute(entry.getKey(), (k, v) -> v == null ? 1L : v + 1L));
            Map<String, Tuple> asTuples = Maps.newHashMapWithExpectedSize(dataCountedByKey.size());
            dataCountedByKey.forEach((k, v) -> asTuples.put(k, Tuple.from(v)));
            return asTuples;
        }, () -> {
            final Quantifier outerQun = outerRecQun();
            final Quantifier entryQun = explodeEntryKeys(outerQun);
            final Quantifier selectWhere = selectWhereGroupByKey(outerQun, entryQun);
            final Quantifier groupBy = groupAggregateByKey(selectWhere, new CountValue.CountFn(), RecordConstructorValue.ofColumns(List.of()));
            return unsorted(selectHaving(groupBy));
        });
    }

    @Test
    void maxEverByKeyIndexes() {
        final RecordMetaDataHook hook = addUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, maxEverValueByKey());
            metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, maxEverValueByKeyUnnested());
        });
        final IndexAggregateFunction maxEverOuter = new IndexAggregateFunction(FunctionNames.MAX_EVER, onEntry(() -> field("key")), MAX_EVER_VALUE_BY_KEY);
        final IndexAggregateFunction maxEverUnnested = new IndexAggregateFunction(FunctionNames.MAX_EVER, field("entry").nest("key"), MAX_EVER_VALUE_BY_KEY_UNNESTED);
        testAggregateIndex(hook, setUpData(hook) , maxEverOuter, maxEverUnnested, data -> {
            Map<String, String> maxDataByKey = new HashMap<>();
            data.stream()
                    .flatMap(rec -> rec.getMap().getEntryList().stream())
                    .forEach(entry -> maxDataByKey.compute(entry.getKey(), (k, v) -> v == null ? entry.getValue() : (entry.getValue().compareTo(v) < 0 ? v : entry.getValue())));
            Map<String, Tuple> asTuples = Maps.newHashMapWithExpectedSize(maxDataByKey.size());
            maxDataByKey.forEach((k, v) -> asTuples.put(k, Tuple.from(v)));
            return asTuples;
        }, null);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void maxEverIntByKeyIndexes() {
        final RecordMetaDataHook hook = addUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, maxEverIntValueByKey());
            metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, maxEverIntValueByKeyUnnested());
        });
        final IndexAggregateFunction maxEverOuter = new IndexAggregateFunction(FunctionNames.MAX_EVER, onEntry(() -> field("key")), MAX_EVER_VALUE_BY_KEY);
        final IndexAggregateFunction maxEverUnnested = new IndexAggregateFunction(FunctionNames.MAX_EVER, field("entry").nest("key"), MAX_EVER_VALUE_BY_KEY_UNNESTED);
        testAggregateIndex(hook, setUpDataWithInts(hook) , maxEverOuter, maxEverUnnested, data -> {
            Map<String, Long> maxDataByKey = new HashMap<>();
            data.stream()
                    .flatMap(rec -> rec.getMap().getEntryList().stream())
                    .forEach(entry -> maxDataByKey.compute(entry.getKey(), (k, v) -> v == null ? entry.getIntValue() : Math.max(entry.getIntValue(), v)));
            Map<String, Tuple> asTuples = Maps.newHashMapWithExpectedSize(maxDataByKey.size());
            maxDataByKey.forEach((k, v) -> asTuples.put(k, Tuple.from(v)));
            return asTuples;
        }, () -> {
            final Quantifier outerQun = outerRecQun();
            final Quantifier entryQun = explodeEntryQun(outerQun, "key", "int_value");
            final Quantifier selectWhere = selectWhereGroupByKey(outerQun, entryQun);
            final Quantifier groupBy = groupAggregateByKey(selectWhere, new IndexOnlyAggregateValue.MaxEverFn(), FieldValue.ofFieldNames(selectWhere.getFlowedObjectValue(), List.of(entryQun.getAlias().getId(), "int_value")));
            return unsorted(selectHaving(groupBy));
        });
    }

    /**
     * Tests a slightly discordant type of max index. Here, the indexes are grouped by map keys, but the value is the
     * maximum value seen by <em>any</em> entry in the map. So the queries are something like: "for all records where
     * at least one map entry has a given key, find the largest value in any entry". This index is not expected to
     * be all that useful, but it's the kind of index one might accidentally construct if one did something like
     * "map.entry.value grouped by map.entry.key" using key expressions.
     */
    @Test
    void maxEverRecordValueByKeyIndexes() {
        final RecordMetaDataHook hook = addDoubleUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, maxEverRecordValueByKey());
            metaDataBuilder.addIndex(OUTER_WITH_TWO_ENTRIES, maxEverRecordValueByKeyUnnested());
        });
        final IndexAggregateFunction maxEverOuter = new IndexAggregateFunction(FunctionNames.MAX_EVER, onEntry(() -> field("key")), MAX_EVER_RECORD_VALUE_BY_KEY);
        final IndexAggregateFunction maxEverUnnested = new IndexAggregateFunction(FunctionNames.MAX_EVER, field("e1").nest("key"), MAX_EVER_RECORD_VALUE_BY_KEY_UNNESTED);
        testAggregateIndex(hook, setUpData(hook), maxEverOuter, maxEverUnnested, data -> {
            Map<String, String> maxDataByKey = new HashMap<>();
            for (TestRecordsNestedMapProto.OuterRecord outerRecord : data) {
                final String maxValueInRecord = outerRecord.getMap().getEntryList().stream()
                        .map(TestRecordsNestedMapProto.MapRecord.Entry::getValue)
                        .max(Comparator.naturalOrder())
                        .orElseGet(() -> fail("Should be at least one entry in map"));
                outerRecord.getMap().getEntryList().stream()
                        .map(TestRecordsNestedMapProto.MapRecord.Entry::getKey)
                        .forEach(key -> maxDataByKey.compute(key, (k, v) -> v == null ? maxValueInRecord : (maxValueInRecord.compareTo(v) < 0 ? v : maxValueInRecord)));
            }
            Map<String, Tuple> asTuples = Maps.newHashMapWithExpectedSize(maxDataByKey.size());
            maxDataByKey.forEach((k, v) -> asTuples.put(k, Tuple.from(v)));
            return asTuples;
        }, null);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void maxEverRecordIntValueByKeyIndexes() {
        final RecordMetaDataHook hook = addDoubleUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, maxEverRecordIntValueByKey());
            metaDataBuilder.addIndex(OUTER_WITH_TWO_ENTRIES, maxEverRecordIntValueByKeyUnnested());
        });
        final IndexAggregateFunction maxEverOuter = new IndexAggregateFunction(FunctionNames.MAX_EVER, onEntry(() -> field("key")), MAX_EVER_RECORD_VALUE_BY_KEY);
        final IndexAggregateFunction maxEverUnnested = new IndexAggregateFunction(FunctionNames.MAX_EVER, field("e1").nest("key"), MAX_EVER_RECORD_VALUE_BY_KEY_UNNESTED);
        testAggregateIndex(hook, setUpDataWithInts(hook), maxEverOuter, maxEverUnnested, data -> {
            Map<String, Long> maxDataByKey = new HashMap<>();
            for (TestRecordsNestedMapProto.OuterRecord outerRecord : data) {
                final Long maxValueInRecord = outerRecord.getMap().getEntryList().stream()
                        .map(TestRecordsNestedMapProto.MapRecord.Entry::getIntValue)
                        .max(Comparator.naturalOrder())
                        .orElseGet(() -> fail("Should be at least one entry in map"));
                outerRecord.getMap().getEntryList().stream()
                        .map(TestRecordsNestedMapProto.MapRecord.Entry::getKey)
                        .forEach(key -> maxDataByKey.compute(key, (k, v) -> v == null ? maxValueInRecord : Math.max(maxValueInRecord, v)));
            }
            Map<String, Tuple> asTuples = Maps.newHashMapWithExpectedSize(maxDataByKey.size());
            maxDataByKey.forEach((k, v) -> asTuples.put(k, Tuple.from(v)));
            return asTuples;
        }, () -> {
            final Quantifier outerQun = outerRecQun();
            final Quantifier entryKeyQun = explodeEntryQun(outerQun, "key");
            final Quantifier entryValueQun = explodeEntryQun(outerQun, "int_value");
            final Quantifier selectWhere = selectWhereGroupByKey(outerQun, entryKeyQun, entryValueQun);
            final Quantifier groupBy = groupAggregateByKey(selectWhere, new IndexOnlyAggregateValue.MaxEverFn(), FieldValue.ofFieldNames(selectWhere.getFlowedObjectValue(), List.of(entryValueQun.getAlias().getId(), "int_value")));
            return unsorted(selectHaving(groupBy));
        });
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void sumByKey() {
        final RecordMetaDataHook hook = addUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, sumValueByKey());
            metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, sumValueByKeyUnnested());
        });
        final IndexAggregateFunction sumOuter = new IndexAggregateFunction(FunctionNames.SUM, onEntry(() -> field("key")), SUM_VALUE_BY_KEY);
        final IndexAggregateFunction sumUnnested = new IndexAggregateFunction(FunctionNames.SUM, field("entry").nest("key"), SUM_VALUE_BY_KEY_UNNESTED);
        testAggregateIndex(hook, setUpDataWithInts(hook), sumOuter, sumUnnested, data -> {
            Map<String, Long> sumsByKey = new HashMap<>();
            data.stream()
                    .flatMap(rec -> rec.getMap().getEntryList().stream())
                    .forEach(entry -> sumsByKey.compute(entry.getKey(), (k, v) -> (v == null ? entry.getIntValue() : (v + entry.getIntValue()))));

            Map<String, Tuple> asTuples = Maps.newHashMapWithExpectedSize(sumsByKey.size());
            sumsByKey.forEach((k, v) -> asTuples.put(k, Tuple.from(v)));
            return asTuples;
        }, this::querySumIntValueByKey);
    }

    /**
     * Similar to {@link #maxEverRecordValueByKeyIndexes()} but with a {@link IndexTypes#SUM} index. Like with the other index,
     * this pairs each key in the record with <em>every</em> value in the record rather than just the one in the same entry.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void sumWholeRecordValueByKey() {
        final RecordMetaDataHook hook = addDoubleUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, sumWholeRecordByKey());
            metaDataBuilder.addIndex(OUTER_WITH_TWO_ENTRIES, sumWholeRecordByKeyUnnested());
        });
        final IndexAggregateFunction sumOuter = new IndexAggregateFunction(FunctionNames.SUM, onEntry(() -> field("key")), SUM_WHOLE_RECORD_VALUE_BY_KEY);
        final IndexAggregateFunction sumUnnested = new IndexAggregateFunction(FunctionNames.SUM, field("e1").nest("key"), SUM_WHOLE_RECORD_VALUE_BY_KEY_UNNESTED);
        testAggregateIndex(hook, setUpDataWithInts(hook), sumOuter, sumUnnested, data -> {
            Map<String, Long> sumsByKey = new HashMap<>();
            for (TestRecordsNestedMapProto.OuterRecord outerRecord : data) {
                long recordSum = outerRecord.getMap().getEntryList().stream()
                        .mapToLong(TestRecordsNestedMapProto.MapRecord.Entry::getIntValue)
                        .sum();
                outerRecord.getMap().getEntryList().stream()
                        .map(TestRecordsNestedMapProto.MapRecord.Entry::getKey)
                        .forEach(key -> sumsByKey.compute(key, (k, v) -> v == null ? recordSum : (v + recordSum)));
            }

            Map<String, Tuple> asTuples = Maps.newHashMapWithExpectedSize(sumsByKey.size());
            sumsByKey.forEach((k, v) -> asTuples.put(k, Tuple.from(v)));
            return asTuples;
        }, this::querySumIntValueForRecordByKey);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void sumByKeyWithDuplicates() {
        final RecordMetaDataHook hook = addUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, sumValueByKey());
            metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, sumValueByKeyUnnested());
        });
        final IndexAggregateFunction sumOuter = new IndexAggregateFunction(FunctionNames.SUM, onEntry(() -> field("key")), SUM_VALUE_BY_KEY);
        final IndexAggregateFunction sumUnnested = new IndexAggregateFunction(FunctionNames.SUM, field("entry").nest("key"), SUM_VALUE_BY_KEY_UNNESTED);
        testAggregateIndex(hook, setUpDataWithDuplicateInts(hook), sumOuter, sumUnnested, data -> {
            Map<String, Long> sumsByKey = new HashMap<>();
            data.stream()
                    .flatMap(rec -> rec.getMap().getEntryList().stream())
                    .forEach(entry -> sumsByKey.compute(entry.getKey(), (k, v) -> (v == null ? entry.getIntValue() : (v + entry.getIntValue()))));

            Map<String, Tuple> asTuples = Maps.newHashMapWithExpectedSize(sumsByKey.size());
            sumsByKey.forEach((k, v) -> asTuples.put(k, Tuple.from(v)));
            return asTuples;
        }, this::querySumIntValueByKey);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void sumWholeRecordValueWithDuplicatesByKey() {
        final RecordMetaDataHook hook = addDoubleUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, sumWholeRecordByKey());
            metaDataBuilder.addIndex(OUTER_WITH_TWO_ENTRIES, sumWholeRecordByKeyUnnested());
        });
        final IndexAggregateFunction sumOuter = new IndexAggregateFunction(FunctionNames.SUM, onEntry(() -> field("key")), SUM_WHOLE_RECORD_VALUE_BY_KEY);
        final IndexAggregateFunction sumUnnested = new IndexAggregateFunction(FunctionNames.SUM, field("e1").nest("key"), SUM_WHOLE_RECORD_VALUE_BY_KEY_UNNESTED);
        testAggregateIndex(hook, setUpDataWithDuplicateInts(hook), sumOuter, sumUnnested, data -> {
            Map<String, Long> sumsByKey = new HashMap<>();
            for (TestRecordsNestedMapProto.OuterRecord outerRecord : data) {
                long recordSum = outerRecord.getMap().getEntryList().stream()
                        .mapToLong(TestRecordsNestedMapProto.MapRecord.Entry::getIntValue)
                        .sum();
                outerRecord.getMap().getEntryList().stream()
                        .map(TestRecordsNestedMapProto.MapRecord.Entry::getKey)
                        .forEach(key -> sumsByKey.compute(key, (k, v) -> v == null ? recordSum : (v + recordSum)));
            }

            Map<String, Tuple> asTuples = Maps.newHashMapWithExpectedSize(sumsByKey.size());
            sumsByKey.forEach((k, v) -> asTuples.put(k, Tuple.from(v)));
            return asTuples;
        }, this::querySumIntValueForRecordByKey);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void sumByKeyAndOther() {
        final RecordMetaDataHook hook = addUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, sumValueByKeyAndOther());
            metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, sumValueByKeyAndOtherUnnested());
        });

        List<TestRecordsNestedMapProto.OuterRecord> data = setUpDataWithInts(hook);
        final Map<Tuple, Long> sumsByGroup = new HashMap<>();
        for (TestRecordsNestedMapProto.OuterRecord outerRecord : data) {
            for (TestRecordsNestedMapProto.MapRecord.Entry entry : outerRecord.getMap().getEntryList()) {
                Tuple group = Tuple.from(outerRecord.getOtherId(), entry.getKey());
                sumsByGroup.compute(group, (key, oldValue) -> oldValue == null ? entry.getIntValue() : oldValue + entry.getIntValue());
            }
        }

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            // Execute a Cascades query that will return each group
            final RecordQueryPlan plan = planGraph(() -> {
                final Quantifier outerQun = outerRecQun();
                final Quantifier explodeEntryQun = explodeEntryQun(outerQun, "key", "int_value");

                // Create select where, group by other_id and key

                final Quantifier selectWhereGroupBy = Quantifier.forEach(Reference.of(GraphExpansion.builder()
                        .addQuantifier(outerQun)
                        .addQuantifier(explodeEntryQun)
                        .addResultColumn(Column.of(Optional.of(outerQun.getAlias().getId()), outerQun.getFlowedObjectValue()))
                        .addResultColumn(Column.of(Optional.of(explodeEntryQun.getAlias().getId()), explodeEntryQun.getFlowedObjectValue()))
                        .build()
                        .buildSelect()));

                // Aggregate int_value (by group)
                final FieldValue aggregatedValue = FieldValue.ofFieldNames(selectWhereGroupBy.getFlowedObjectValue(), List.of(explodeEntryQun.getAlias().getId(), "int_value"));
                final Value aggregateValue = (Value)new NumericAggregationValue.Sum.SumFn().encapsulate(List.of(aggregatedValue));
                final RecordConstructorValue groupingValue = RecordConstructorValue.ofColumns(List.of(
                        Column.of(Optional.of("other_id"), FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumber(selectWhereGroupBy.getFlowedObjectValue(), 0), "other_id")),
                        Column.of(Optional.of("key"), FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumber(selectWhereGroupBy.getFlowedObjectValue(), 1), "key"))
                ));
                final GroupByExpression groupByExpression = new GroupByExpression(groupingValue, RecordConstructorValue.ofUnnamed(List.of(aggregateValue)),
                        GroupByExpression::nestedResults, selectWhereGroupBy);
                final Quantifier groupBy = Quantifier.forEach(Reference.of(groupByExpression));

                // Select both grouping keys plus the aggregate value
                final Quantifier selectHaving = Quantifier.forEach(Reference.of(GraphExpansion.builder()
                        .addQuantifier(groupBy)
                        .addResultColumn(Column.of(Optional.of("other_id"), FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupBy.getFlowedObjectValue(), 0), 0)))
                        .addResultColumn(Column.of(Optional.of("key"), FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupBy.getFlowedObjectValue(), 0), 1)))
                        .addResultColumn(Column.of(Optional.of("sum"), FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupBy.getFlowedObjectValue(), 1), 0)))
                        .build()
                        .buildSelect()));
                return unsorted(selectHaving);
            });
            assertThat(plan.getUsedIndexes(), contains(SUM_VALUE_BY_KEY_AND_OTHER));

            // Execute plan and assert that the sum by group matches expectations
            final Map<Tuple, Long> queriedSumsByGroup = Maps.newHashMapWithExpectedSize(sumsByGroup.size());
            try (RecordCursor<QueryResult> cursor = FDBSimpleQueryGraphTest.executeCascades(recordStore, plan)) {
                cursor.forEach(queryResult -> {
                    final Message queriedMessage = queryResult.getMessage();
                    final Descriptors.Descriptor recDescriptor = queriedMessage.getDescriptorForType();
                    Tuple group = Tuple.from(queriedMessage.getField(recDescriptor.findFieldByName("other_id")), queriedMessage.getField(recDescriptor.findFieldByName("key")));
                    long value = (long) queriedMessage.getField(recDescriptor.findFieldByName("sum"));
                    queriedSumsByGroup.put(group, value);
                }).join();
            }
            assertEquals(sumsByGroup, queriedSumsByGroup);

            // Evaluate index aggregate functions for each group and assert that the sum matches
            final IndexAggregateFunction sumOuter = new IndexAggregateFunction(FunctionNames.SUM, concat(field("other_id"), onEntry(() -> field("key"))), SUM_VALUE_BY_KEY_AND_OTHER);
            final IndexAggregateFunction sumUnnested = new IndexAggregateFunction(FunctionNames.SUM, concat(field(PARENT_CONSTITUENT).nest("other_id"), field("entry").nest("key")), SUM_VALUE_BY_KEY_AND_OTHER_UNNESTED);

            for (Map.Entry<Tuple, Long> groupAndSum : sumsByGroup.entrySet()) {
                final Tuple expectedSum = Tuple.from(groupAndSum.getValue());
                final Tuple group = groupAndSum.getKey();
                final Key.Evaluated groupEvaluated = Key.Evaluated.fromTuple(group);
                Tuple byOuterIndex = recordStore.evaluateAggregateFunction(List.of(OUTER), sumOuter, groupEvaluated, IsolationLevel.SERIALIZABLE)
                        .join();
                assertEquals(expectedSum, byOuterIndex, () -> "mismatched aggregate value for group " + group + " when using index on outer record");
                Tuple byUnnestedIndex = recordStore.evaluateAggregateFunction(List.of(OUTER_WITH_ENTRIES), sumUnnested, groupEvaluated, IsolationLevel.SERIALIZABLE)
                        .join();
                assertEquals(expectedSum, byUnnestedIndex, () -> "mismatched aggregate value for group " + group + " when using index on unnested record");
            }

            commit(context);
        }
    }

    private void testAggregateIndex(RecordMetaDataHook hook, List<TestRecordsNestedMapProto.OuterRecord> data,
                                    IndexAggregateFunction normalAggregate, IndexAggregateFunction unnestedAggregate,
                                    Function<List<TestRecordsNestedMapProto.OuterRecord>, Map<String, Tuple>> aggregator,
                                    @Nullable Supplier<Reference> querySupplier) {
        Map<String, Tuple> aggregatedByKey = aggregator.apply(data);
        Set<String> keys = mapKeys(data);

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            @Nullable Map<String, Tuple> cascadeResults = null;
            if (isUseCascadesPlanner() && querySupplier != null) {
                final RecordQueryPlan plan = planGraph(querySupplier);
                cascadeResults = new HashMap<>();
                try (RecordCursor<QueryResult> cascadeCursor = FDBSimpleQueryGraphTest.executeCascades(recordStore, plan)) {
                    for (RecordCursorResult<QueryResult> result = cascadeCursor.getNext(); result.hasNext(); result = cascadeCursor.getNext()) {
                        Message protoResult = result.get().getMessage();
                        String key = (String) protoResult.getField(protoResult.getDescriptorForType().findFieldByName("key"));
                        Object aggregate = protoResult.getField(protoResult.getDescriptorForType().findFieldByName("aggregate"));
                        cascadeResults.put(key, Tuple.from(aggregate));
                    }
                }
                assertEquals(keys, cascadeResults.keySet());
            }

            for (String key : keys) {
                assertThat(aggregatedByKey, hasKey(key));
                Tuple expected = aggregatedByKey.get(key);
                final Key.Evaluated groupEvaluated = Key.Evaluated.scalar(key);
                Tuple byOuterIndex = recordStore.evaluateAggregateFunction(List.of(OUTER), normalAggregate, groupEvaluated, IsolationLevel.SERIALIZABLE)
                        .join();
                assertEquals(expected, byOuterIndex, () -> "mismatched aggregate value for key " + key + " when using normal count index");
                Tuple byUnnestedIndex = recordStore.evaluateAggregateFunction(List.of(OUTER_WITH_ENTRIES), unnestedAggregate, groupEvaluated, IsolationLevel.SERIALIZABLE)
                        .join();
                assertEquals(expected, byUnnestedIndex, () -> "mismatched aggregate value for key " + key + " when using unnested count index");
                if (cascadeResults != null) {
                    assertEquals(expected, cascadeResults.get(key));
                }
            }

            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void chooseCorrectAggregateIndex() {
        final RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, sumValueByKey());
            metaDataBuilder.addIndex(OUTER, sumWholeRecordByKey());
        };
        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            // Verify that the byKey plan chooses the index that keeps entries together
            final RecordQueryPlan byKeyPlan = planGraph(this::querySumIntValueByKey);
            assertThat(byKeyPlan.getUsedIndexes(), contains(SUM_VALUE_BY_KEY));
            assertEquals(byKeyPlan, planGraph(this::querySumIntValueByKey, SUM_VALUE_BY_KEY));

            // Verify that the other byWholeRecord plan chooses the index that does not keep entries together
            final RecordQueryPlan byWholeRecordPlan = planGraph(this::querySumIntValueForRecordByKey);
            assertThat(byWholeRecordPlan.getUsedIndexes(), contains(SUM_WHOLE_RECORD_VALUE_BY_KEY));
            assertEquals(byWholeRecordPlan, planGraph(this::querySumIntValueForRecordByKey, SUM_WHOLE_RECORD_VALUE_BY_KEY));

            // Validate that when restricted to the wrong index, we fail to plan.
            assertFailsToPlan(this::querySumIntValueByKey, SUM_WHOLE_RECORD_VALUE_BY_KEY);
            assertFailsToPlan(this::querySumIntValueForRecordByKey, SUM_VALUE_BY_KEY);

            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void aggregateByGroupingKeyIndexScan() {
        final Index keyIndex = new Index("entryKeyIndex", onEntry(() -> field("key")));
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex(OUTER, keyIndex);

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            // In theory, both of these queries could be executed by going down the entryKeyIndex and grouping
            // by key
            assertFailsToPlan(this::querySumIntValueByKey);
            assertFailsToPlan(this::querySumIntValueForRecordByKey);

            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void aggregateByGroupingKeyAndValueIndexScan() {
        final RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, nestedConcatIndex());
            metaDataBuilder.addIndex(OUTER, concatNestedIndex());
        };

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            // In both cases, the index _almost_ aligns with the query. For the first query,
            // the nestedConcat index seems to contain all of the key-value pairs needed
            // to satisfy the query, and it would be sufficient to collect those values
            // up. However, this will only count each unique key-value pair once, so the
            // value under-counts. If we had a constraint, e.g., on the map entry keys,
            // so that no key appeared more than once in the same record, then this index
            // could be used without issue.
            assertFailsToPlan(this::querySumIntValueByKey);
            assertFailsToPlan(this::querySumIntValueForRecordByKey);

            commit(context);
        }
    }

    /**
     * Query for a sum where the key and value are from the same entry quantifier. So the query is something like:
     *
     * <pre>{@code
     * SELECT sum(e.int_value) FROM Outer, Outer.entry AS e GROUP BY e.key
     * }</pre>
     *
     * @return a query that sums the int values associated with a single key
     */
    @Nonnull
    private Reference querySumIntValueByKey() {
        final Quantifier outerQun = outerRecQun();
        final Quantifier entryQun = explodeEntryQun(outerQun, "key", "int_value");
        final Quantifier selectWhere = selectWhereGroupByKey(outerQun, entryQun);
        final Quantifier groupBy = groupAggregateByKey(selectWhere, new NumericAggregationValue.SumFn(), FieldValue.ofFieldNames(selectWhere.getFlowedObjectValue(), List.of(entryQun.getAlias().getId(), "int_value")));
        return unsorted(selectHaving(groupBy));
    }

    /**
     * Query for a sum where the key and value are from diferent entry quantifiers. So the query is something like:
     *
     * <pre>{@code
     * SELECT sum(e2.int_value) FROM Outer, Outer.entry AS e1, Outer.entry AS e2 GROUP BY e1.key
     * }</pre>
     *
     * @return a query that sums the int values for all entries in a record associated with a single key
     */
    @Nonnull
    private Reference querySumIntValueForRecordByKey() {
        final Quantifier outerQun = outerRecQun();
        final Quantifier entryKeyQun = explodeEntryQun(outerQun, "key");
        final Quantifier entryIntValueQun = explodeEntryQun(outerQun, "int_value");
        final Quantifier selectWhere = selectWhereGroupByKey(outerQun, entryKeyQun, entryIntValueQun);
        final Quantifier groupBy = groupAggregateByKey(selectWhere, new NumericAggregationValue.SumFn(), FieldValue.ofFieldNames(selectWhere.getFlowedObjectValue(), List.of(entryIntValueQun.getAlias().getId(), "int_value")));
        return unsorted(selectHaving(groupBy));
    }

    @Test
    void countIndexByPairOfKeys() {
        final RecordMetaDataHook hook = addDoubleUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(OUTER, countByPairOfKeys());
            metaDataBuilder.addIndex(OUTER_WITH_TWO_ENTRIES, countByPairOfKeysUnnested());
        });
        final IndexAggregateFunction countOuter = new IndexAggregateFunction(FunctionNames.COUNT, concat(onEntry(() -> field("key")), onEntry(() -> field("key"))), COUNT_BY_PAIR_OF_KEYS);
        final IndexAggregateFunction countUnnested = new IndexAggregateFunction(FunctionNames.COUNT, concat(field("e1").nest("key"), field("e2").nest("key")), COUNT_BY_PAIR_OF_KEYS_UNNESTED);

        List<TestRecordsNestedMapProto.OuterRecord> data = setUpData(hook);
        Set<String> keys = mapKeys(data);

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);
            for (String k1 : keys) {
                for (String k2 : keys) {
                    final Key.Evaluated evaluated = Key.Evaluated.concatenate(k1, k2);
                    long expectedCount = data.stream()
                            .map(rec -> rec.getMap().getEntryList())
                            .filter(entryList -> entryList.stream().anyMatch(entry -> entry.getKey().equals(k1)) && entryList.stream().anyMatch(entry -> entry.getKey().equals(k2)))
                            .count();

                    long outer = recordStore.evaluateAggregateFunction(List.of(OUTER), countOuter, evaluated, IsolationLevel.SERIALIZABLE)
                            .thenApply(t -> t.getLong(0))
                            .join();
                    assertEquals(expectedCount, outer, () -> "mismatched count for keys " + k1 + " and " + k2 + " when using normal count index");
                    long unnested = recordStore.evaluateAggregateFunction(List.of(OUTER_WITH_TWO_ENTRIES), countUnnested, evaluated, IsolationLevel.SERIALIZABLE)
                            .thenApply(t -> t.getLong(0))
                            .join();
                    assertEquals(expectedCount, unnested, () -> "mismatched count for keys " + k1 + " and " + k2 + " when using unnested count index");
                }
            }

            commit(context);
        }
    }

    @Test
    void bitmapValueIndexScan() {
        RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OUTER, bitmapValueByKey()))
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, bitmapValueByKeyUnnested()));
        List<TestRecordsNestedMapProto.OuterRecord> data = setUpDataWithInts(hook);

        Set<String> keys = mapKeys(data);
        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final Index outerIndex = recordStore.getRecordMetaData().getIndex(BITMAP_VALUE_BY_KEY);
            final Index unnestedIndex = recordStore.getRecordMetaData().getIndex(BITMAP_VALUE_BY_KEY_UNNESTED);
            for (String key : keys) {
                List<Integer> intValuesFromRecords = intValuesWithKey(data, key);
                final Tuple group = Tuple.from(key);
                assertEquals(intValuesFromRecords, collectBitsFromIndex(outerIndex, group));
                assertEquals(intValuesFromRecords, collectBitsFromIndex(unnestedIndex, group));
            }

            commit(context);
        }
    }

    @Test
    void bitmapValueQueryOnKeyAndOtherUnnested() {
        final RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, bitmapValueByKeyOtherUnnested()));
        List<TestRecordsNestedMapProto.OuterRecord> data = setUpDataWithInts(hook);
        Set<String> keys = mapKeys(data);
        Set<Long> otherIds = otherIds(data);

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final String keyParameter = "key";
            final String otherParameter = "other";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER_WITH_ENTRIES)
                    .setFilter(Query.and(
                            Query.field("entry").matches(Query.field("key").equalsParameter(keyParameter)),
                            Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsParameter(otherParameter))
                    ))
                    .setRequiredResults(List.of(field("entry").nest("int_value")))
                    .build();
            final IndexAggregateFunctionCall functionCall = new IndexAggregateFunctionCall(FunctionNames.BITMAP_VALUE, field("entry").nest("int_value").groupBy(field("entry").nest("key")));
            RecordQueryPlan plan = ComposedBitmapIndexAggregate.tryPlan((RecordQueryPlanner) planner, query, functionCall, IndexQueryabilityFilter.DEFAULT)
                    .orElseGet(() -> fail("could not plan query " + query));
            assertMatchesExactly(plan,
                    coveringIndexPlan().where(indexPlanOf(
                            indexPlan()
                                    .where(indexName(BITMAP_VALUE_KEY_OTHER_UNNESTED))
                                    .and(scanComparisons(range("[EQUALS $key, EQUALS $other]")))
                    ))
            );

            for (long otherId : otherIds) {
                Set<TestRecordsNestedMapProto.OuterRecord> withOtherId = data.stream()
                        .filter(rec -> rec.getOtherId() == otherId)
                        .collect(Collectors.toSet());
                for (String key : keys) {
                    final List<Integer> expected = intValuesWithKey(withOtherId, key);
                    final EvaluationContext evaluationContext = EvaluationContext.forBinding(keyParameter, key).withBinding(otherParameter, otherId);
                    try (RecordCursor<IndexEntry> cursor = plan.execute(recordStore, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE)
                            .map(FDBQueriedRecord::getIndexEntry)) {
                        assertEquals(expected, collectOnBits(cursor));
                    }
                }
            }

            commit(context);
        }
    }

    @Test
    void bitmapValueQueryOnKeyAndTwoOthersUnnestedWithOrsAtTopLevel() {
        // Construct a top-level OR with two expressions, each of which is on a nested field
        bitmapValueQueryOnKeyAndTwoOthersUnnested((other1Parameter, other2Parameter) ->
                Query.or(
                        Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsParameter(other1Parameter)),
                        Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsParameter(other2Parameter))
                )
        );
    }

    @Test
    void bitmapValueQueryOnKeyAndTwoOthersUnnestedWithOrsNested() {
        // Nest the OR into one child predicate
        bitmapValueQueryOnKeyAndTwoOthersUnnested((other1Parameter, other2Parameter) -> Query.field(PARENT_CONSTITUENT).matches(
                Query.or(
                        Query.field("other_id").equalsParameter(other1Parameter),
                        Query.field("other_id").equalsParameter(other2Parameter)
                )
        ));
    }

    private void bitmapValueQueryOnKeyAndTwoOthersUnnested(BiFunction<String, String, QueryComponent> otherIdFilters) {
        final RecordMetaDataHook hook = addUnnestedType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OUTER_WITH_ENTRIES, bitmapValueByKeyOtherUnnested()));
        List<TestRecordsNestedMapProto.OuterRecord> data = setUpDataWithInts(hook);
        Set<String> keys = mapKeys(data);
        Set<Long> otherIds = otherIds(data);

        try (FDBRecordContext context = openContext()) {
            createOrOpenMapStore(context, hook);

            final String keyParameter = "key";
            final String other1Parameter = "other1";
            final String other2Parameter = "other2";

            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(OUTER_WITH_ENTRIES)
                    .setFilter(Query.and(
                            Query.field("entry").matches(Query.field("key").equalsParameter(keyParameter)),
                            otherIdFilters.apply(other1Parameter, other2Parameter)
                    ))
                    .setRequiredResults(List.of(field("entry").nest("int_value")))
                    .build();
            final IndexAggregateFunctionCall functionCall = new IndexAggregateFunctionCall(FunctionNames.BITMAP_VALUE, field("entry").nest("int_value").groupBy(field("entry").nest("key")));
            RecordQueryPlan plan = ComposedBitmapIndexAggregate.tryPlan((RecordQueryPlanner) planner, query, functionCall, IndexQueryabilityFilter.DEFAULT)
                    .orElseGet(() -> fail("could not plan query " + query));
            assertMatchesExactly(plan,
                    composedBitmapPlan(ListMatcher.exactly(
                            coveringIndexPlan().where(indexPlanOf(
                                    indexPlan()
                                            .where(indexName(BITMAP_VALUE_KEY_OTHER_UNNESTED))
                                            .and(scanComparisons(range("[EQUALS $key, EQUALS $other1]")))
                            )),
                            coveringIndexPlan().where(indexPlanOf(
                                    indexPlan()
                                            .where(indexName(BITMAP_VALUE_KEY_OTHER_UNNESTED))
                                            .and(scanComparisons(range("[EQUALS $key, EQUALS $other2]")))
                            ))
                    )).where(composer(composition("[0] BITOR [1]")))
            );

            for (long other1Id : otherIds) {
                for (long other2Id : otherIds) {
                    Set<TestRecordsNestedMapProto.OuterRecord> withOtherId = data.stream()
                            .filter(rec -> rec.getOtherId() == other1Id || rec.getOtherId() == other2Id)
                            .collect(Collectors.toSet());
                    for (String key : keys) {
                        final List<Integer> expected = intValuesWithKey(withOtherId, key);
                        final EvaluationContext evaluationContext = EvaluationContext.forBinding(keyParameter, key)
                                .withBinding(other1Parameter, other1Id)
                                .withBinding(other2Parameter, other2Id);
                        try (RecordCursor<IndexEntry> cursor = plan.execute(recordStore, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE)
                                .map(FDBQueriedRecord::getIndexEntry)) {
                            assertEquals(expected, collectOnBits(cursor));
                        }
                    }
                }
            }

            commit(context);
        }
    }

    private List<Integer> collectBitsFromIndex(Index index, Tuple group) {
        return collectOnBits(recordStore.scanIndex(index, IndexScanType.BY_GROUP, TupleRange.allOf(group), null, ScanProperties.FORWARD_SCAN));
    }

    private List<Integer> collectOnBits(RecordCursor<IndexEntry> bitmapIndexEntries) {
        return bitmapIndexEntries
                .asStream()
                .flatMap(this::collectOnBits)
                .collect(Collectors.toList());
    }

    private Stream<Integer> collectOnBits(IndexEntry indexEntry) {
        byte[] bytes = indexEntry.getValue().getBytes(0);
        int start = (int) indexEntry.getKey().getLong(indexEntry.getKeySize() - 1);
        return IntStream.range(0, bytes.length)
                .flatMap(idx -> {
                    byte b = bytes[idx];
                    return IntStream.range(0, 8)
                            .filter(i -> (b & (1 << i)) != 0)
                            .map(i -> idx * 8 + start + i);
                })
                .boxed();
    }

    private Quantifier outerRecQun() {
        return FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "OuterRecord");
    }

    private Quantifier explodeEntryQun(@Nonnull Quantifier outerQun, @Nonnull String... fields) {
        ExplodeExpression explodeExpression = ExplodeExpression.explodeField((Quantifier.ForEach)outerQun, List.of("map", "entry"));
        Quantifier explodeQun = Quantifier.forEach(Reference.of(explodeExpression));
        var selectBuilder = GraphExpansion.builder();
        List<Column<? extends Value>> resultFields = Arrays.stream(fields)
                .map(fieldName -> {
                    FieldValue value = FieldValue.ofFieldName(explodeQun.getFlowedObjectValue(), fieldName);
                    return Column.of(Type.Record.Field.of(value.getResultType(), Optional.of(fieldName)), value);
                })
                .collect(Collectors.toList());
        SelectExpression select = selectBuilder.addQuantifier(explodeQun)
                .addAllResultColumns(resultFields)
                .build()
                .buildSelect();
        return Quantifier.forEach(Reference.of(select));
    }

    private void assertFailsToPlan(@Nonnull Supplier<Reference> querySupplier, String... allowedIndexes) {
        final RecordCoreException rce = assertThrows(RecordCoreException.class, () -> planGraph(querySupplier, allowedIndexes));
        assertThat(rce.getMessage(), containsString("Cascades planner could not plan query"));
    }

    @Nonnull
    private Quantifier explodeEntryKeys(@Nonnull Quantifier outerQun) {
        return explodeEntryQun(outerQun, "key");
    }

    @Nonnull
    private Quantifier selectWhereGroupByKey(Quantifier outerQun, Quantifier... entryQuns) {
        final var selectWhereBuilder = GraphExpansion.builder()
                .addQuantifier(outerQun)
                .addAllQuantifiers(List.of(entryQuns));
        selectWhereBuilder
                .addResultColumn(Column.of(Optional.of(outerQun.getAlias().getId()), outerQun.getFlowedObjectValue()));
        for (Quantifier entryQun : entryQuns) {
            selectWhereBuilder.addResultColumn(Column.of(Optional.of(entryQun.getAlias().getId()), entryQun.getFlowedObjectValue()));
        }
        return Quantifier.forEach(Reference.of(selectWhereBuilder.build().buildSelect()));
    }

    @Nonnull
    private Quantifier groupAggregateByKey(@Nonnull Quantifier selectWhere, @Nonnull BuiltInFunction<AggregateValue> aggregate, @Nonnull Value argument) {
        final Value aggregateValue = (Value) aggregate.encapsulate(List.of(argument));
        final RecordConstructorValue groupingValue = RecordConstructorValue.ofColumns(List.of(
                Column.unnamedOf(FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumber(selectWhere.getFlowedObjectValue(), 1), "key"))
        ));
        final GroupByExpression groupBy = new GroupByExpression(groupingValue, RecordConstructorValue.ofUnnamed(List.of(aggregateValue)),
                GroupByExpression::nestedResults, selectWhere);
        return Quantifier.forEach(Reference.of(groupBy));
    }

    @Nonnull
    private Quantifier selectHaving(@Nonnull Quantifier groupBy) {
        final var selectHavingBuilder = GraphExpansion.builder().addQuantifier(groupBy);
        final FieldValue groupVal = FieldValue.ofOrdinalNumber(groupBy.getFlowedObjectValue(), 0);
        final FieldValue aggregate = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupBy.getFlowedObjectValue(), 1), 0);
        final SelectExpression selectHaving = selectHavingBuilder
                .addResultColumn(Column.of(Optional.of("key"), FieldValue.ofOrdinalNumberAndFuseIfPossible(groupVal, 0)))
                .addResultColumn(Column.of(Optional.of("aggregate"), aggregate))
                .build()
                .buildSelect();
        return Quantifier.forEach(Reference.of(selectHaving));
    }

    @Nonnull
    public Reference unsorted(@Nonnull Quantifier qun) {
        return Reference.of(LogicalSortExpression.unsorted(qun));
    }
}
