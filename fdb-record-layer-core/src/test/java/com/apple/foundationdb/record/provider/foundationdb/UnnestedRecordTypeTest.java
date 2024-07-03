/*
 * UnnestedRecordTypeTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1EvolvedWithMapProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsDoubleNestedProto;
import com.apple.foundationdb.record.TestRecordsDoublyImportedMapProto;
import com.apple.foundationdb.record.TestRecordsImportedMapProto;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordFromStoredRecordPlan;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.equalsObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.queryComponents;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.recordTypes;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.typeFilterPlan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests of the {@link UnnestedRecordType} class. Some of these tests may require access to an underlying FDB record store.
 */
@Tag(Tags.RequiresFDB)
class UnnestedRecordTypeTest extends FDBRecordStoreQueryTestBase {
    @Nonnull
    private static final String OUTER = "OuterRecord";
    @Nonnull
    private static final String UNNESTED_MAP = "UnnestedMap";
    @Nonnull
    private static final String TWO_UNNESTED_MAPS = "TwoUnnestedMaps";
    @Nonnull
    private static final String DOUBLE_NESTED = "DoubleNested";

    @Nonnull
    private static final KeyExpression ENTRIES_FAN_OUT = field("map").nest(field("entry", FanType.FanOut));
    @Nonnull
    private static final String PARENT_CONSTITUENT = "parent";
    @Nonnull
    private static final String KEY_OTHER_INT_VALUE_INDEX = "keyOtherIntValue";
    @Nonnull
    private static final String KEY_ONE_KEY_TWO_VALUE_ONE_VALUE_TWO_INDEX = "keyOneKeyTwoValueOneValueTwo";
    @Nonnull
    private static final String INNER_FOO_OUTER_BAR_INNER_BAR_INDEX = "innerFooOuterBarInnerBar";
    @Nonnull
    private static final String OTHER_KEY_ID_VALUE_INDEX = "otherKeyIdValue";
    @Nonnull
    private static final String MULTI_TYPE_DOUBLE_NESTED_INDEX = "multiTypeDoubleNested";

    @Nonnull
    private static List<String> RANDOM_MAP_KEYS = List.of("foo", "bar", "baz", "quop", "asdf", "qwerty", "zop");

    @Nonnull
    static Stream<Long> randomSeeds() {
        return RandomizedTestUtils.randomSeeds(0xba5eba1L, 0xfdb5ca1eL);
    }

    @Nonnull
    private static RecordMetaData mapMetaData(@Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsNestedMapProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static RecordMetaData importedMapMetaData(@Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsImportedMapProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static RecordMetaData doublyImportedMapMetaData(@Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsDoublyImportedMapProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @SuppressWarnings("unused") // used as parameter supplier to parameterized test
    @Nonnull
    private static Stream<Function<RecordMetaDataHook, RecordMetaData>> mapMetaDataSuppliers() {
        return Stream.of(UnnestedRecordTypeTest::mapMetaData, UnnestedRecordTypeTest::importedMapMetaData, UnnestedRecordTypeTest::doublyImportedMapMetaData);
    }

    @Nonnull
    private static RecordMetaData doubleNestedMetaData(@Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsDoubleNestedProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static RecordMetaDataHook addMapType() {
        return metaDataBuilder -> {
            UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(UNNESTED_MAP);
            typeBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("map_entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    PARENT_CONSTITUENT, ENTRIES_FAN_OUT);
        };
    }

    @Nonnull
    private static RecordMetaDataHook addTwoMapsType() {
        return metaDataBuilder -> {
            UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(TWO_UNNESTED_MAPS);
            typeBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("entry_one", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    PARENT_CONSTITUENT, ENTRIES_FAN_OUT);
            typeBuilder.addNestedConstituent("entry_two", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    PARENT_CONSTITUENT, ENTRIES_FAN_OUT);
        };
    }

    @Nonnull
    private static RecordMetaDataHook addDoubleNestedType() {
        return metaDataBuilder -> {
            final UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(DOUBLE_NESTED);
            typeBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("middle", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.getDescriptor(),
                    PARENT_CONSTITUENT, field("many_middle", FanType.FanOut));
            typeBuilder.addNestedConstituent("inner", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(),
                    "middle", field("inner", FanType.FanOut));
            typeBuilder.addNestedConstituent("outer_inner", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(),
                    PARENT_CONSTITUENT, field("inner", FanType.FanOut));
        };
    }

    @Nonnull
    private static RecordMetaDataHook addKeyOtherIntValueIndex() {
        return metaDataBuilder -> {
            final KeyExpression expr = concat(
                    field("map_entry").nest("key"),
                    field(PARENT_CONSTITUENT).nest("other_id"),
                    field("map_entry").nest("int_value")
            );
            metaDataBuilder.addIndex(UNNESTED_MAP, new Index(KEY_OTHER_INT_VALUE_INDEX, expr));
        };
    }

    @Nonnull
    private static RecordMetaDataHook addKeyOneKeyTwoValueOneValueTwo() {
        return metaDataBuilder -> {
            final KeyExpression expr = new KeyWithValueExpression(concat(
                    field("entry_one").nest("key"),
                    field("entry_two").nest("key"),
                    field("entry_one").nest("value"),
                    field("entry_two").nest("int_value")
            ), 2);
            metaDataBuilder.addIndex(TWO_UNNESTED_MAPS, new Index(KEY_ONE_KEY_TWO_VALUE_ONE_VALUE_TWO_INDEX, expr));
        };
    }

    @Nonnull
    private static RecordMetaDataHook addInnerFooOuterBarInnerBarIndex() {
        return metaDataBuilder -> metaDataBuilder.addIndex(DOUBLE_NESTED, new Index(INNER_FOO_OUTER_BAR_INNER_BAR_INDEX,
                concat(field("inner").nest("foo"), field("outer_inner").nest("bar"), field("inner").nest("bar"))));
    }

    @Nonnull
    private static RecordMetaDataHook addOtherKeyIdValueIndex() {
        return metaData -> {
            final KeyExpression expr = concat(
                    field(PARENT_CONSTITUENT).nest("other_id"),
                    field("map_entry").nest("key"),
                    field(PARENT_CONSTITUENT).nest("rec_id"),
                    field("map_entry").nest("value")
            );
            metaData.addIndex(UNNESTED_MAP, new Index(OTHER_KEY_ID_VALUE_INDEX, expr));
        };
    }

    @Nonnull
    private static RecordMetaDataHook addMultiTypeDoubleUnnestedIndex() {
        return addMultiTypeDoubleUnnestedIndex(concat(field(PARENT_CONSTITUENT).nest(field("middle").nest("other_int")), field("inner").nest("foo")));
    }

    @Nonnull
    private static RecordMetaDataHook addMultiTypeDoubleUnnestedIndex(@Nonnull KeyExpression rootExpression) {
        return addDoubleNestedType().andThen(metaDataBuilder -> {
            UnnestedRecordTypeBuilder secondUnnested = metaDataBuilder.addUnnestedRecordType("MiddleUnnested");
            secondUnnested.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType("MiddleRecord"));
            secondUnnested.addNestedConstituent("inner", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(), PARENT_CONSTITUENT,
                    field("other_middle").nest(field("inner", FanType.FanOut)));

            final Index index = new Index(MULTI_TYPE_DOUBLE_NESTED_INDEX, rootExpression);
            metaDataBuilder.addMultiTypeIndex(List.of(metaDataBuilder.getIndexableRecordType(DOUBLE_NESTED), metaDataBuilder.getIndexableRecordType("MiddleUnnested")), index);
        });
    }

    @Nonnull
    private static RecordMetaDataHook setOuterAndOtherPrimaryKey(@Nonnull KeyExpression primaryKey) {
        return metaData -> {
            metaData.getRecordType(OUTER).setPrimaryKey(primaryKey);
            metaData.getRecordType("OtherRecord").setPrimaryKey(primaryKey);
        };
    }

    @Nonnull
    private static RecordMetaDataHook setOuterAndMiddlePrimaryKey(@Nonnull KeyExpression primaryKey) {
        return metaData -> {
            metaData.getRecordType(OUTER).setPrimaryKey(primaryKey);
            metaData.getRecordType("MiddleRecord").setPrimaryKey(primaryKey);
        };
    }

    @Nonnull
    private static <M> List<M> randomMessageList(@Nonnull Random r, int count, @Nonnull BiFunction<Random, Long, M> generator) {
        return LongStream.generate(r::nextLong)
                .distinct()
                .mapToObj(id -> generator.apply(r, id))
                .limit(count)
                .collect(Collectors.toList());
    }

    @Nonnull
    private static TestRecordsNestedMapProto.OuterRecord sampleMapRecord() {
        return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                .setRecId(1066)
                .setOtherId(1)
                .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder()
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("foo").setValue("bar").setIntValue(1))
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("baz").setValue("qux").setIntValue(2))
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("wid").setValue("get").setIntValue(3))
                )
                .build();
    }


    @Nonnull
    private static TestRecordsNestedMapProto.OuterRecord sampleMapRecordWithOnlyValueDifferent() {
        return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                .setRecId(1215L)
                .setOtherId(2)
                .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder()
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("a").setValue("foo").setIntValue(42))
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("a").setValue("bar").setIntValue(42))
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("a").setValue("baz").setIntValue(42))
                )
                .build();
    }


    @Nonnull
    private static TestRecordsNestedMapProto.OuterRecord sampleMapRecordWithDuplicateEntries() {
        return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                .setRecId(1415L)
                .setOtherId(3)
                .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder()
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("foo").setValue("bar").setIntValue(10))
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("wow").setValue("zaa").setIntValue(20))
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("foo").setValue("bar").setIntValue(10))
                )
                .build();
    }

    @Nonnull
    private static TestRecordsNestedMapProto.OuterRecord emptyMapRecord() {
        return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                .setRecId(1815L)
                .setOtherId(4)
                .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder())
                .build();
    }

    @Nonnull
    private static TestRecordsNestedMapProto.OuterRecord unsetMapRecord() {
        return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                .setRecId(1863L)
                .setOtherId(5)
                .build();
    }

    @Nonnull
    private static Collection<TestRecordsNestedMapProto.OuterRecord> sampleMapRecords() {
        return List.of(
                sampleMapRecord(),
                sampleMapRecordWithOnlyValueDifferent(),
                sampleMapRecordWithDuplicateEntries(),
                emptyMapRecord(),
                unsetMapRecord()
        );
    }

    @Nonnull
    private static TestRecordsNestedMapProto.OuterRecord randomMapRecord(@Nonnull Random r, long id) {
        TestRecordsNestedMapProto.MapRecord.Builder mapBuilder = TestRecordsNestedMapProto.MapRecord.newBuilder();
        Stream.generate(() -> TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                .setKey(RANDOM_MAP_KEYS.get(r.nextInt(RANDOM_MAP_KEYS.size())))
                .setIntValue(r.nextLong())
                .setValue("" + r.nextLong())
        ).limit(r.nextInt(RANDOM_MAP_KEYS.size())).forEach(mapBuilder::addEntry);

        return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                .setRecId(id)
                .setOtherId(r.nextInt(10))
                .setMap(mapBuilder)
                .build();
    }

    @Nonnull
    private static List<TestRecordsNestedMapProto.OuterRecord> randomMapRecords(@Nonnull Random r, int count) {
        return randomMessageList(r, count, UnnestedRecordTypeTest::randomMapRecord);
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord sampleDoubleNestedRecord() {
        return TestRecordsDoubleNestedProto.OuterRecord.newBuilder()
                .setRecNo(1066)
                .setOtherInt(4L)
                .setMiddle(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.newBuilder()
                        .setOtherInt(2L)
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(-1L)
                                .setBar("negative_one")
                                .build()
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(0L)
                                .setBar("zero")
                                .build()
                        )
                )
                .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                        .setFoo(1L)
                        .setBar("one")
                )
                .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                        .setFoo(2L)
                        .setBar("two")
                )
                .addManyMiddle(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.newBuilder()
                        .setOtherInt(1L)
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(3L)
                                .setBar("three")
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(4L)
                                .setBar("four")
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(5L)
                                .setBar("five")
                        )
                )
                .addManyMiddle(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.newBuilder()
                        .setOtherInt(2L)
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(6L)
                                .setBar("six")
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(7L)
                                .setBar("seven")
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(8L)
                                .setBar("eight")
                        )
                )
                .addManyMiddle(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.newBuilder()
                        .setOtherInt(3L)
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(9L)
                                .setBar("nine")
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(10L)
                                .setBar("ten")
                        )
                )
                .build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord sampleDoubleNestedWithEmptyOuterInner() {
        return sampleDoubleNestedRecord().toBuilder()
                .setRecNo(1215L)
                .clearInner()
                .build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord sampleDoubleNestedWithEmptyManyMiddleInner() {
        return sampleDoubleNestedRecord().toBuilder()
                .setRecNo(1415L)
                .clearManyMiddle()
                .build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord sampleDoubleNestedWithEmptyMiddleInners() {
        TestRecordsDoubleNestedProto.OuterRecord.Builder outerBuilder = sampleDoubleNestedRecord().toBuilder()
                .setRecNo(1815L);
        for (TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.Builder middleBuilder : outerBuilder.getManyMiddleBuilderList()) {
            middleBuilder.clearInner();
        }
        return outerBuilder.build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord sampleDoubleNestedWithOneEmptyMiddleInner() {
        return sampleDoubleNestedRecord().toBuilder()
                .setRecNo(1863L)
                .addManyMiddle(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.getDefaultInstance())
                .build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord sampleDoubleNestedWithOneEmptyMiddleInnerAtBeginning() {
        TestRecordsDoubleNestedProto.OuterRecord.Builder outerBuilder = sampleDoubleNestedRecord().toBuilder()
                .setRecNo(1867L);
        final List<TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord> middles = outerBuilder.getManyMiddleList();
        return outerBuilder
                .clearManyMiddle()
                .addManyMiddle(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.getDefaultInstance())
                .addAllManyMiddle(middles)
                .build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord emptyDoubleNestedRecord() {
        return TestRecordsDoubleNestedProto.OuterRecord.getDefaultInstance();
    }

    @Nonnull
    private static List<TestRecordsDoubleNestedProto.OuterRecord> sampleDoubleNestedRecords() {
        return List.of(
                sampleDoubleNestedRecord(),
                sampleDoubleNestedWithEmptyOuterInner(),
                sampleDoubleNestedWithEmptyManyMiddleInner(),
                sampleDoubleNestedWithEmptyMiddleInners(),
                sampleDoubleNestedWithOneEmptyMiddleInner(),
                sampleDoubleNestedWithOneEmptyMiddleInnerAtBeginning(),
                emptyDoubleNestedRecord()
        );
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OtherRecord randomOtherRecord(@Nonnull Random r, double decayRate) {
        TestRecordsDoubleNestedProto.OtherRecord.Builder builder = TestRecordsDoubleNestedProto.OtherRecord.newBuilder();
        if (r.nextDouble() < decayRate) {
            double newDecayRate = r.nextDouble() * decayRate * 0.75;
            builder.setOuter(randomDoubleNestedMapRecord(r, r.nextLong(), newDecayRate));
        }
        return builder.build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord randomInnerRecord(@Nonnull Random r, double decayRate) {
        double newDecayRate = r.nextDouble() * decayRate * 0.75;
        TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.Builder builder = TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                .setFoo(r.nextInt(20))
                .setBar("" + r.nextInt(100));
        if (r.nextDouble() < decayRate) {
            builder.setOther(randomOtherRecord(r, newDecayRate));
        }
        if (r.nextDouble() < decayRate) {
            builder.setOuter(randomDoubleNestedMapRecord(r, r.nextLong(), decayRate));
        }
        return builder.build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord randomOuterMiddle(@Nonnull Random r, double decayRate) {
        double newDecayRate = r.nextDouble() * decayRate * 0.75;
        TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.Builder builder =  TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.newBuilder()
                .setOtherInt(r.nextInt(10));
        if (r.nextDouble() < decayRate) {
            builder.setOther(randomOtherRecord(r, newDecayRate));
        }
        Stream.generate(() -> randomInnerRecord(r, newDecayRate))
                .limit(r.nextInt(5))
                .forEach(builder::addInner);
        return builder.build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.MiddleRecord randomMiddleRecord(@Nonnull Random r, long id, double decayRate) {
        double newDecayRate = r.nextDouble() * decayRate * 0.75;
        TestRecordsDoubleNestedProto.MiddleRecord.Builder builder = TestRecordsDoubleNestedProto.MiddleRecord.newBuilder()
                .setRecNo(id)
                .setOtherInt(r.nextInt(10));
        if (r.nextDouble() < decayRate) {
            builder.setMiddle(randomMiddleRecord(r, r.nextLong(), newDecayRate));
        }
        if (r.nextDouble() < decayRate) {
            builder.setOtherMiddle(randomOuterMiddle(r, newDecayRate));
        }
        return builder.build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.MiddleRecord randomMiddleRecord(@Nonnull Random r, long id) {
        return randomMiddleRecord(r, id, 1.0);
    }

    @Nonnull
    private static List<TestRecordsDoubleNestedProto.MiddleRecord> randomMiddleRecords(@Nonnull Random r, int count) {
        return randomMessageList(r, count, UnnestedRecordTypeTest::randomMiddleRecord);
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord randomDoubleNestedMapRecord(@Nonnull Random r, long id, double decayRate) {
        double newDecayRate = r.nextDouble() * decayRate * 0.75;
        TestRecordsDoubleNestedProto.OuterRecord.Builder builder = TestRecordsDoubleNestedProto.OuterRecord.newBuilder()
                .setRecNo(id)
                .setOtherInt(r.nextInt(10));
        if (r.nextDouble() < decayRate) {
            builder.setMiddle(randomOuterMiddle(r, newDecayRate));
        }
        Stream.generate(() -> randomOuterMiddle(r, newDecayRate))
                .limit(r.nextInt(5))
                .forEach(builder::addManyMiddle);
        Stream.generate(() -> randomInnerRecord(r, newDecayRate))
                .limit(r.nextInt(7))
                .forEach(builder::addInner);
        return builder.build();
    }

    @Nonnull
    private static TestRecordsDoubleNestedProto.OuterRecord randomDoubleNestedMapRecord(@Nonnull Random r, long id) {
        return randomDoubleNestedMapRecord(r, id, 1.0);
    }

    @Nonnull
    private static List<TestRecordsDoubleNestedProto.OuterRecord> randomDoubleNestedMapRecords(@Nonnull Random r, int count) {
        return randomMessageList(r, count, UnnestedRecordTypeTest::randomDoubleNestedMapRecord);
    }

    @Nonnull
    private static TestRecordsImportedMapProto.OuterRecord asImported(@Nonnull TestRecordsNestedMapProto.OuterRecord outerRecord) {
        // Copy the outer record into an imported outer record. Note that because the map type is imported, the map
        // field can just be copied over
        return TestRecordsImportedMapProto.OuterRecord.newBuilder()
                .setRecId(outerRecord.getRecId())
                .setOtherId(outerRecord.getOtherId())
                .setMap(outerRecord.getMap())
                .build();
    }

    @Nonnull
    private static TestRecordsDoublyImportedMapProto.OuterRecord asDoubleImported(@Nonnull TestRecordsNestedMapProto.OuterRecord outerRecord) {
        // Copy the outer record into an imported outer record. Note that because the map type is imported, the map
        // field can just be copied over
        var builder = TestRecordsDoublyImportedMapProto.OuterRecord.newBuilder()
                .setRecId(outerRecord.getRecId())
                .setOtherId(outerRecord.getOtherId());

        var mapBuilder = builder.getMapBuilder();
        for (TestRecordsNestedMapProto.MapRecord.Entry entry : outerRecord.getMap().getEntryList()) {
            mapBuilder.addEntry(entry);
        }

        return builder.build();
    }

    @Nonnull
    private static Message convertOuterRecord(@Nonnull RecordMetaData metaData, @Nonnull TestRecordsNestedMapProto.OuterRecord outerRecord) {
        if (metaData.getRecordsDescriptor() == TestRecordsNestedMapProto.getDescriptor()) {
            return outerRecord;
        } else if (metaData.getRecordsDescriptor() == TestRecordsImportedMapProto.getDescriptor()) {
            return asImported(outerRecord);
        } else if (metaData.getRecordsDescriptor() == TestRecordsDoublyImportedMapProto.getDescriptor()) {
            return asDoubleImported(outerRecord);
        } else {
            return fail("unknown records descriptor: " + metaData.getRecordsDescriptor());
        }
    }

    @Nonnull
    private static <C extends SyntheticRecordType.Constituent> C getConstituent(@Nonnull SyntheticRecordType<C> type, @Nonnull String name) {
        return type.getConstituents().stream()
                .filter(c -> c.getName().equals(name))
                .findFirst()
                .orElseGet(() -> fail("unable to find constituent " + name));
    }

    //
    // Meta-data tests
    //
    // Tests that assert that the meta-data operations do the right thing
    //

    @Test
    void mapTypeToAndFromProto() {
        final RecordMetaData metaData = mapMetaData(addMapType());
        assertProtoSerializationSuccessful(metaData, UNNESTED_MAP);
    }

    @Test
    void twoMapsToAndFromProto() {
        final RecordMetaData metaData = mapMetaData(addTwoMapsType());
        assertProtoSerializationSuccessful(metaData, TWO_UNNESTED_MAPS);
    }

    @Test
    void doubleNestedToAndFromProto() {
        final RecordMetaData metaData = doubleNestedMetaData(addDoubleNestedType());
        assertProtoSerializationSuccessful(metaData, DOUBLE_NESTED);
    }

    @Test
    void importedMapTypeToAndFromProto() {
        final RecordMetaData metaData = importedMapMetaData(addMapType());
        assertProtoSerializationSuccessful(metaData, UNNESTED_MAP);
    }

    @Test
    void importedTwoMapsToAndFromProto() {
        final RecordMetaData metaData = importedMapMetaData(addTwoMapsType());
        assertProtoSerializationSuccessful(metaData, TWO_UNNESTED_MAPS);
    }

    @Test
    void doubleImportedMapTypeToAndFromProto() {
        final RecordMetaData metaData = doublyImportedMapMetaData(addMapType());
        assertProtoSerializationSuccessful(metaData, UNNESTED_MAP);
    }

    @Test
    void doubleImportedTwoMapsToAndFromProto() {
        final RecordMetaData metaData = doublyImportedMapMetaData(addTwoMapsType());
        assertProtoSerializationSuccessful(metaData, TWO_UNNESTED_MAPS);
    }

    private static void assertProtoSerializationSuccessful(RecordMetaData metaData, String unnestedTypeName) {
        final UnnestedRecordType type = (UnnestedRecordType) metaData.getSyntheticRecordType(unnestedTypeName);

        RecordMetaDataProto.MetaData proto = metaData.toProto();
        RecordMetaData fromProto = RecordMetaData.build(proto);
        UnnestedRecordType typeFromProto = (UnnestedRecordType) fromProto.getSyntheticRecordType(unnestedTypeName);
        assertEquals(type.getPrimaryKey(), typeFromProto.getPrimaryKey(), "types should have the same primary key");

        assertEquals(type.getRecordTypeKey(), typeFromProto.getRecordTypeKey(), "record type keys should match");
        SyntheticRecordType.Constituent parentConstituent = typeFromProto.getConstituents().stream()
                .filter(constituent -> constituent.getName().equals(PARENT_CONSTITUENT))
                .findFirst()
                .orElseGet(() -> fail("did not find parent constituent"));
        RecordType outerTypeFromProto = fromProto.getRecordType(OUTER);
        assertSame(outerTypeFromProto, parentConstituent.getRecordType(), "parent constituent had incorrect object");

        assertEquals(type.getConstituents().size(), typeFromProto.getConstituents().size(), "types should have same number of constituents");
        for (UnnestedRecordType.NestedConstituent constituent : type.getConstituents()) {
            UnnestedRecordType.NestedConstituent constituentFromProto = typeFromProto.getConstituents().stream()
                    .filter(c -> c.getName().equals(constituent.getName()))
                    .findFirst()
                    .orElseGet(() -> fail("missing constituent " + constituent.getName()));
            // Descriptors may not be equal because protobuf descriptors don't implement a simple equals, but they should
            // serialize to the same thing
            assertEquals(constituent.getRecordType().getDescriptor().toProto(), constituentFromProto.getRecordType().getDescriptor().toProto());
            assertEquals(constituent.getParentName(), constituentFromProto.getParentName());
            assertEquals(constituent.getNestingExpression(), constituentFromProto.getNestingExpression());
        }
    }

    @Test
    void leavingOutParentTypeFails() {
        assertMetaDataFails(TestRecordsNestedMapProto.getDescriptor(), "unnested record type missing parent type",
                metaDataBuilder -> metaDataBuilder.addUnnestedRecordType("foo"));
    }

    @Test
    void duplicateParentTypesFail() {
        assertMetaDataFails(TestRecordsNestedMapProto.getDescriptor(), "cannot add duplicate parent type", metaDataBuilder -> {
            UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType("foo");
            typeBuilder.addParentConstituent("first", metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addParentConstituent("second", metaDataBuilder.getRecordType(OUTER));
        });
    }

    @Test
    void duplicateNamesFail() {
        assertMetaDataFails(TestRecordsNestedMapProto.getDescriptor(), "Could not build synthesized file descriptor", metaDataBuilder -> {
            UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType("foo");
            typeBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(), PARENT_CONSTITUENT, field("map").nest("entry", FanType.FanOut));
            typeBuilder.addNestedConstituent("entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(), PARENT_CONSTITUENT, field("map").nest("entry", FanType.FanOut));
        });
    }

    @Test
    void reservedNameFails() {
        assertMetaDataFails(TestRecordsNestedMapProto.getDescriptor(), "cannot create constituent with reserved prefix", metaDataBuilder -> {
            UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType("foo");
            typeBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("__entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(), PARENT_CONSTITUENT, field("map").nest("entry", FanType.FanOut));
        });
    }

    private void assertMetaDataFails(@Nonnull Descriptors.FileDescriptor base, @Nonnull String messageContains, @Nonnull RecordMetaDataHook hook) {
        MetaDataException err = assertThrows(MetaDataException.class, () -> {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(base);
            hook.apply(metaDataBuilder);
            metaDataBuilder.build();
        });
        assertThat(err.getMessage(), containsString(messageContains));
    }

    //
    // Unnesting tests
    //
    // Tests that assert on what synthetic records are returned by the synthetic planner given a stored record
    //

    @Nonnull
    private List<FDBSyntheticRecord> evaluateUnnesting(@Nonnull SyntheticRecordType<?> syntheticType, @Nonnull FDBStoredRecord<? extends Message> storedRecord) {
        SyntheticRecordPlanner syntheticPlanner = new SyntheticRecordPlanner(recordStore);
        SyntheticRecordFromStoredRecordPlan plan = syntheticPlanner.forType(syntheticType);
        List<FDBSyntheticRecord> syntheticRecords = plan.execute(recordStore, storedRecord)
                .asList()
                .join();

        // Validate the parent constituent matches the original stored record, and that the primary key for the type matches the definition
        assertThat(syntheticType, instanceOf(UnnestedRecordType.class));
        UnnestedRecordType unnestedRecordType = (UnnestedRecordType)syntheticType;
        for (FDBSyntheticRecord rec : syntheticRecords) {
            assertEquals(storedRecord, rec.getConstituent(PARENT_CONSTITUENT));
            assertEquals(rec.getPrimaryKey(), syntheticType.getPrimaryKey().evaluateSingleton(rec).toTuple());

            Map<String, FDBStoredRecord<? extends Message>> checkedConstituents = Maps.newHashMapWithExpectedSize(syntheticType.getConstituents().size());
            checkedConstituents.put(PARENT_CONSTITUENT, storedRecord);
            boolean done = false;
            while (!done) {
                done = true;
                for (UnnestedRecordType.NestedConstituent constituent : unnestedRecordType.getConstituents()) {
                    if (!checkedConstituents.containsKey(constituent.getName()) && checkedConstituents.containsKey(constituent.getParentName())) {
                        done = false;

                        // Validate the constituent by looking at its parent, evaluating the nesting expression, and then
                        // checking that the entry at the position specified in the primary key matches the value in the
                        // synthetic record
                        FDBStoredRecord<? extends Message> parent = checkedConstituents.get(constituent.getParentName());
                        List<Key.Evaluated> evaluatedList = constituent.getNestingExpression().evaluate(parent);
                        int primaryKeyIndex = unnestedRecordType.getConstituents().indexOf(constituent) + 1;
                        int pos = (int) rec.getPrimaryKey().getNestedTuple(primaryKeyIndex).getLong(0);
                        assertThat("Constituent " + constituent.getName() + " has index out of bounds for parent " + storedRecord + " and synthetic record " + rec, evaluatedList.size(), greaterThan(pos));
                        Message unnestedMessage = evaluatedList.get(pos).getObject(0, Message.class);
                        FDBStoredRecord<? extends Message> returnedConstituent = rec.getConstituent(constituent.getName());
                        assertNotNull(returnedConstituent);
                        assertEquals(unnestedMessage, returnedConstituent.getRecord());
                        checkedConstituents.put(constituent.getName(), returnedConstituent);
                    }
                }
            }
            // Validate that we've checked all of the constituents
            assertThat(checkedConstituents.entrySet(), hasSize(unnestedRecordType.getConstituents().size()));
        }

        return syntheticRecords;
    }

    @ParameterizedTest(name = "unnestMapType[{index}]")
    @MethodSource("mapMetaDataSuppliers")
    void unnestMapType(Function<RecordMetaDataHook, RecordMetaData> metaDataSource) {
        final RecordMetaData metaData = metaDataSource.apply(addMapType());
        final SyntheticRecordType<?> unnestedType = metaData.getSyntheticRecordType(UNNESTED_MAP);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                final FDBStoredRecord<?> stored = recordStore.saveRecord(convertOuterRecord(metaData, outerRecord));
                List<FDBSyntheticRecord> unnestedRecords = evaluateUnnesting(unnestedType, stored);
                assertThat(unnestedRecords, hasSize(outerRecord.getMap().getEntryCount()));

                final SyntheticRecordType.Constituent nestedConstituent = getConstituent(unnestedType, "map_entry");
                Collection<Matcher<? super FDBSyntheticRecord>> expected = new ArrayList<>();
                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    final TestRecordsNestedMapProto.MapRecord.Entry entry = outerRecord.getMap().getEntry(i);
                    final FDBStoredRecord<?> entryRecord = FDBStoredRecord.newBuilder(entry)
                            .setRecordType(nestedConstituent.getRecordType())
                            .setPrimaryKey(Tuple.from(i))
                            .build();

                    expected.add(equalTo(FDBSyntheticRecord.of(unnestedType, Map.of(PARENT_CONSTITUENT, stored, nestedConstituent.getName(), entryRecord))));
                }
                assertThat(unnestedRecords, containsInAnyOrder(expected));
            }
            commit(context);
        }
    }

    @ParameterizedTest(name = "unnestTwoMapsType[{index}]")
    @MethodSource("mapMetaDataSuppliers")
    void unnestTwoMapsType(Function<RecordMetaDataHook, RecordMetaData> metaDataSource) {
        final RecordMetaData metaData = metaDataSource.apply(addTwoMapsType());
        final SyntheticRecordType<?> unnestedType = metaData.getSyntheticRecordType(TWO_UNNESTED_MAPS);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                final FDBStoredRecord<?> stored = recordStore.saveRecord(convertOuterRecord(metaData, outerRecord));
                final List<FDBSyntheticRecord> unnestedRecords = evaluateUnnesting(unnestedType, stored);
                assertThat(unnestedRecords, hasSize(outerRecord.getMap().getEntryCount() * outerRecord.getMap().getEntryCount()));

                final SyntheticRecordType.Constituent entry1Constituent = getConstituent(unnestedType, "entry_one");
                final SyntheticRecordType.Constituent entry2Constituent = getConstituent(unnestedType, "entry_two");
                Collection<Matcher<? super FDBSyntheticRecord>> expected = new ArrayList<>();
                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    final TestRecordsNestedMapProto.MapRecord.Entry entry1 = outerRecord.getMap().getEntry(i);
                    final FDBStoredRecord<?> entry1Record = FDBStoredRecord.newBuilder(entry1)
                            .setRecordType(entry1Constituent.getRecordType())
                            .setPrimaryKey(Tuple.from(i))
                            .build();

                    for (int j = 0; j < outerRecord.getMap().getEntryCount(); j++) {
                        final TestRecordsNestedMapProto.MapRecord.Entry entry2 = outerRecord.getMap().getEntry(j);
                        final FDBStoredRecord<?> entry2Record = FDBStoredRecord.newBuilder(entry2)
                                .setRecordType(entry2Constituent.getRecordType())
                                .setPrimaryKey(Tuple.from(j))
                                .build();
                        expected.add(equalTo(FDBSyntheticRecord.of(unnestedType, Map.of(
                                PARENT_CONSTITUENT, stored,
                                entry1Constituent.getName(), entry1Record,
                                entry2Constituent.getName(), entry2Record))));
                    }
                }
                assertThat(unnestedRecords, containsInAnyOrder(expected));
            }
            commit(context);
        }
    }

    @Test
    void unnestDoubleNestedMapType() {
        final RecordMetaData metaData = doubleNestedMetaData(addDoubleNestedType());
        final SyntheticRecordType<?> unnestedType = metaData.getSyntheticRecordType(DOUBLE_NESTED);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            for (TestRecordsDoubleNestedProto.OuterRecord outerRecord : sampleDoubleNestedRecords()) {
                final FDBStoredRecord<?> stored = recordStore.saveRecord(outerRecord);
                final List<FDBSyntheticRecord> unnestedRecords = evaluateUnnesting(unnestedType, stored);
                assertThat(unnestedRecords, hasSize(outerRecord.getInnerCount() * outerRecord.getManyMiddleList().stream()
                        .mapToInt(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord::getInnerCount)
                        .sum()));

                final SyntheticRecordType.Constituent outerInnerConstituent = getConstituent(unnestedType, "outer_inner");
                final SyntheticRecordType.Constituent middleConstituent = getConstituent(unnestedType, "middle");
                final SyntheticRecordType.Constituent innerConstituent = getConstituent(unnestedType, "inner");
                Collection<Matcher<? super FDBSyntheticRecord>> expected = new ArrayList<>();
                for (int i = 0; i < outerRecord.getInnerCount(); i++) {
                    final FDBStoredRecord<?> firstInnerRecord = FDBStoredRecord.newBuilder(outerRecord.getInner(i))
                            .setRecordType(outerInnerConstituent.getRecordType())
                            .setPrimaryKey(Tuple.from(i))
                            .build();
                    for (int j = 0; j < outerRecord.getManyMiddleCount(); j++) {
                        final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord middle = outerRecord.getManyMiddle(j);
                        final FDBStoredRecord<?> middleRecord = FDBStoredRecord.newBuilder(middle)
                                .setRecordType(middleConstituent.getRecordType())
                                .setPrimaryKey(Tuple.from(j))
                                .build();
                        for (int k = 0; k < middle.getInnerCount(); k++) {
                            final FDBStoredRecord<?> secondInnerRecord = FDBStoredRecord.newBuilder(middle.getInner(k))
                                    .setRecordType(innerConstituent.getRecordType())
                                    .setPrimaryKey(Tuple.from(k))
                                    .build();
                            expected.add(equalTo(FDBSyntheticRecord.of(unnestedType, Map.of(
                                    PARENT_CONSTITUENT, stored,
                                    outerInnerConstituent.getName(), firstInnerRecord,
                                    middleConstituent.getName(), middleRecord,
                                    innerConstituent.getName(), secondInnerRecord))));
                        }
                    }
                }

                assertThat(unnestedRecords, containsInAnyOrder(expected));
            }
            commit(context);
        }
    }

    //
    // Loading tests
    //
    // Tests that assert on what values are returned when loadSyntheticRecord is called
    //

    @ParameterizedTest(name = "loadMapType[{index}]")
    @MethodSource("mapMetaDataSuppliers")
    void loadMapType(Function<RecordMetaDataHook, RecordMetaData> metaDataSource) {
        final RecordMetaData metaData = metaDataSource.apply(addMapType());
        final RecordType unnestedType = metaData.getSyntheticRecordType(UNNESTED_MAP);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                Message outerMessage = convertOuterRecord(metaData, outerRecord);
                FDBStoredRecord<Message> stored = recordStore.saveRecord(outerMessage);

                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    final TestRecordsNestedMapProto.MapRecord.Entry entry = outerRecord.getMap().getEntry(i);
                    final Tuple syntheticPrimaryKey = Tuple.from(unnestedType.getRecordTypeKey(), stored.getPrimaryKey(), Tuple.from(i));
                    FDBSyntheticRecord synthetic = recordStore.loadSyntheticRecord(syntheticPrimaryKey).join();
                    assertNotNull(synthetic);
                    assertEquals(syntheticPrimaryKey, synthetic.getPrimaryKey());
                    assertEquals(syntheticPrimaryKey, unnestedType.getPrimaryKey().evaluateMessageSingleton(synthetic, synthetic.getRecord()).toTuple());

                    assertEquals(outerMessage, synthetic.getConstituent(PARENT_CONSTITUENT).getRecord());
                    assertEquals(entry, synthetic.getConstituent("map_entry").getRecord());
                }
            }

            commit(context);
        }
    }

    @ParameterizedTest(name = "loadTwoMapsType[{index}]")
    @MethodSource("mapMetaDataSuppliers")
    void loadTwoMapsType(Function<RecordMetaDataHook, RecordMetaData> metaDataSource) {
        final RecordMetaData metaData = metaDataSource.apply(addTwoMapsType());
        final RecordType unnestedType = metaData.getSyntheticRecordType(TWO_UNNESTED_MAPS);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                Message outerMessage = convertOuterRecord(metaData, outerRecord);
                FDBStoredRecord<Message> stored = recordStore.saveRecord(outerMessage);

                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    final TestRecordsNestedMapProto.MapRecord.Entry entry1 = outerRecord.getMap().getEntry(i);
                    for (int j = 0; j < outerRecord.getMap().getEntryCount(); j++) {
                        final TestRecordsNestedMapProto.MapRecord.Entry entry2 = outerRecord.getMap().getEntry(j);

                        final Tuple syntheticPrimaryKey = Tuple.from(unnestedType.getRecordTypeKey(), stored.getPrimaryKey(), Tuple.from(i), Tuple.from(j));
                        FDBSyntheticRecord synthetic = recordStore.loadSyntheticRecord(syntheticPrimaryKey).join();
                        assertNotNull(synthetic);
                        assertEquals(syntheticPrimaryKey, synthetic.getPrimaryKey());
                        assertEquals(syntheticPrimaryKey, unnestedType.getPrimaryKey().evaluateMessageSingleton(synthetic, synthetic.getRecord()).toTuple());

                        assertEquals(outerMessage, synthetic.getConstituent(PARENT_CONSTITUENT).getRecord());
                        assertEquals(entry1, synthetic.getConstituent("entry_one").getRecord());
                        assertEquals(entry2, synthetic.getConstituent("entry_two").getRecord());
                    }
                }
            }

            commit(context);
        }
    }

    @Test
    void loadDoubleNestedType() {
        final RecordMetaData metaData = doubleNestedMetaData(addDoubleNestedType());
        final RecordType unnestedType = metaData.getSyntheticRecordType(DOUBLE_NESTED);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            TestRecordsDoubleNestedProto.OuterRecord outerRecord = sampleDoubleNestedRecord();
            FDBStoredRecord<Message> stored = recordStore.saveRecord(outerRecord);

            for (int i = 0; i < outerRecord.getInnerCount(); i++) {
                final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord outerInnerRecord = outerRecord.getInner(i);
                for (int j = 0; j < outerRecord.getManyMiddleCount(); j++) {
                    final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord middleRecord = outerRecord.getManyMiddle(j);
                    for (int k = 0; k < middleRecord.getInnerCount(); k++) {
                        final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord innerRecord = middleRecord.getInner(k);

                        final Tuple syntheticPrimaryKey = Tuple.from(unnestedType.getRecordTypeKey(), stored.getPrimaryKey(), Tuple.from(j), Tuple.from(k), Tuple.from(i));
                        FDBSyntheticRecord synthetic = recordStore.loadSyntheticRecord(syntheticPrimaryKey).join();
                        assertNotNull(synthetic);
                        assertEquals(syntheticPrimaryKey, synthetic.getPrimaryKey());
                        assertEquals(syntheticPrimaryKey, unnestedType.getPrimaryKey().evaluateMessageSingleton(synthetic, synthetic.getRecord()).toTuple());

                        assertEquals(outerRecord, synthetic.getConstituent(PARENT_CONSTITUENT).getRecord());
                        assertEquals(middleRecord, synthetic.getConstituent("middle").getRecord());
                        assertEquals(innerRecord, synthetic.getConstituent("inner").getRecord());
                        assertEquals(outerInnerRecord, synthetic.getConstituent("outer_inner").getRecord());
                    }
                }
            }

            commit(context);
        }
    }

    //
    // Indexing tests
    //
    // Tests that assert on what index entries are produced
    //

    @ParameterizedTest(name = "indexMapType[{index}]")
    @MethodSource("mapMetaDataSuppliers")
    void indexMapType(Function<RecordMetaDataHook, RecordMetaData> metaDataSource) {
        final RecordMetaData metaData = metaDataSource.apply(addMapType().andThen(addKeyOtherIntValueIndex()));
        final RecordType unnestedType = metaData.getSyntheticRecordType(UNNESTED_MAP);
        final Index index = metaData.getIndex(KEY_OTHER_INT_VALUE_INDEX);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                FDBStoredRecord<Message> stored = recordStore.saveRecord(convertOuterRecord(metaData, outerRecord));

                final List<IndexEntry> expected = new ArrayList<>();
                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    final TestRecordsNestedMapProto.MapRecord.Entry entry = outerRecord.getMap().getEntry(i);
                    final Tuple syntheticPrimaryKey = Tuple.from(unnestedType.getRecordTypeKey(), stored.getPrimaryKey(), Tuple.from(i));
                    expected.add(new IndexEntry(index, Tuple.from(entry.getKey(), outerRecord.getOtherId(), entry.getIntValue()).addAll(syntheticPrimaryKey), TupleHelpers.EMPTY, syntheticPrimaryKey));
                }
                expected.sort(Comparator.comparing(IndexEntry::getKey));
                List<IndexEntry> scanned = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .asList()
                        .join();
                assertEquals(expected, scanned);

                recordStore.deleteRecord(stored.getPrimaryKey());
                assertThat(recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().join(), empty());
            }

            commit(context);
        }
    }

    @ParameterizedTest(name = "indexTwoMapsType[{index}]")
    @MethodSource("mapMetaDataSuppliers")
    void indexTwoMapsType(Function<RecordMetaDataHook, RecordMetaData> metaDataSource) {
        final RecordMetaData metaData = metaDataSource.apply(addTwoMapsType().andThen(addKeyOneKeyTwoValueOneValueTwo()));
        final RecordType unnestedType = metaData.getSyntheticRecordType(TWO_UNNESTED_MAPS);
        final Index index = metaData.getIndex(KEY_ONE_KEY_TWO_VALUE_ONE_VALUE_TWO_INDEX);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                FDBStoredRecord<Message> stored = recordStore.saveRecord(convertOuterRecord(metaData, outerRecord));

                final List<IndexEntry> expected = new ArrayList<>();
                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    final TestRecordsNestedMapProto.MapRecord.Entry entry1 = outerRecord.getMap().getEntry(i);
                    for (int j = 0; j < outerRecord.getMap().getEntryCount(); j++) {
                        final TestRecordsNestedMapProto.MapRecord.Entry entry2 = outerRecord.getMap().getEntry(j);
                        final Tuple syntheticPrimaryKey = Tuple.from(unnestedType.getRecordTypeKey(), stored.getPrimaryKey(), Tuple.from(i), Tuple.from(j));
                        expected.add(new IndexEntry(index, Tuple.from(entry1.getKey(), entry2.getKey()).addAll(syntheticPrimaryKey), Tuple.from(entry1.getValue(), entry2.getIntValue()), syntheticPrimaryKey));
                    }
                }
                expected.sort(Comparator.comparing(IndexEntry::getKey));
                List<IndexEntry> scanned = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .asList()
                        .join();
                assertEquals(expected, scanned);

                recordStore.deleteRecord(stored.getPrimaryKey());
                assertThat(recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().join(), empty());
            }

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void indexDoubleNestedType(long seed) {
        final Random r = new Random(seed);
        final RecordMetaData metaData = doubleNestedMetaData(addDoubleNestedType().andThen(addInnerFooOuterBarInnerBarIndex()));
        final RecordType unnestedType = metaData.getSyntheticRecordType(DOUBLE_NESTED);
        final Index index = metaData.getIndex(INNER_FOO_OUTER_BAR_INNER_BAR_INDEX);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            TestRecordsDoubleNestedProto.OuterRecord outerRecord = randomDoubleNestedMapRecord(r, 1066L);
            FDBStoredRecord<Message> stored = recordStore.saveRecord(outerRecord);

            final List<IndexEntry> expected = new ArrayList<>();
            for (int i = 0; i < outerRecord.getInnerCount(); i++) {
                final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord outerInnerRecord = outerRecord.getInner(i);
                for (int j = 0; j < outerRecord.getManyMiddleCount(); j++) {
                    final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord middleRecord = outerRecord.getManyMiddle(j);
                    for (int k = 0; k < middleRecord.getInnerCount(); k++) {
                        final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord innerRecord = middleRecord.getInner(k);
                        final Tuple syntheticPrimaryKey = Tuple.from(unnestedType.getRecordTypeKey(), stored.getPrimaryKey(), Tuple.from(j), Tuple.from(k), Tuple.from(i));

                        expected.add(new IndexEntry(index, Tuple.from(innerRecord.getFoo(), outerInnerRecord.getBar(), innerRecord.getBar()).addAll(syntheticPrimaryKey), TupleHelpers.EMPTY, syntheticPrimaryKey));
                    }
                }
            }
            expected.sort(Comparator.comparing(IndexEntry::getKey));

            List<IndexEntry> scanned = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .join();
            assertEquals(expected, scanned);

            recordStore.deleteRecord(stored.getPrimaryKey());
            assertThat(recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().join(), empty());

            commit(context);
        }
    }

    //
    // Query tests
    //
    // Tests that assert on query plans and results
    //

    @Nonnull
    private List<Pair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry>> queryUnnestedMap(@Nonnull RecordQueryPlan plan, @Nonnull Bindings bindings) {
        EvaluationContext evaluationContext = EvaluationContext.forBindings(bindings);
        return plan.execute(recordStore, evaluationContext)
                .map(FDBQueriedRecord::getSyntheticRecord)
                .map(syntheticRecord -> Pair.of(TestRecordsNestedMapProto.OuterRecord.newBuilder().mergeFrom(syntheticRecord.getConstituent(PARENT_CONSTITUENT).getRecord()).build(), TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().mergeFrom(syntheticRecord.getConstituent("map_entry").getRecord()).build()))
                .asList()
                .join();
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void indexMatchIncludesPrimaryKeyComponent(long seed) {
        final Random r = new Random(seed);
        final List<TestRecordsNestedMapProto.OuterRecord> mapRecords = randomMapRecords(r, 50);
        final Index index = new Index("indexIncludingPrimaryKey", concat(field(PARENT_CONSTITUENT).nest("other_id"), field("map_entry").nest("key")));
        final RecordMetaData metaData = mapMetaData(setOuterAndOtherPrimaryKey(concatenateFields("other_id", "rec_id"))
                .andThen(addMapType())
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(UNNESTED_MAP, index)));

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            Set<String> mapKeys = new HashSet<>();
            for (long otherId = 0; otherId < 3; otherId++) {
                for (TestRecordsNestedMapProto.OuterRecord mapRecord : mapRecords) {
                    recordStore.saveRecord(mapRecord.toBuilder().setOtherId(otherId).build());
                    for (TestRecordsNestedMapProto.MapRecord.Entry entry : mapRecord.getMap().getEntryList()) {
                        mapKeys.add(entry.getKey());
                    }
                }
            }

            final String otherParam = "other";
            final String keyParam = "key";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(UNNESTED_MAP)
                    .setFilter(Query.and(
                            Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsParameter(otherParam)),
                            Query.field("map_entry").matches(Query.field("key").equalsParameter(keyParam))
                    ))
                    .build();
            final RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, indexPlan()
                    .where(indexName(index.getName()))
                    .and(scanComparisons(range("[EQUALS $" + otherParam + ", EQUALS $" + keyParam + ", IS " + UNNESTED_MAP + "]"))));

            for (long otherId = -1; otherId < 4; otherId++) {
                for (String key : mapKeys) {
                    List<Pair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry>> expected;
                    if (otherId < 0 || otherId >= 3) {
                        expected = Collections.emptyList();
                    } else {
                        expected = new ArrayList<>();
                        for (TestRecordsNestedMapProto.OuterRecord outerRecord : mapRecords) {
                            for (TestRecordsNestedMapProto.MapRecord.Entry entry : outerRecord.getMap().getEntryList()) {
                                if (key.equals(entry.getKey())) {
                                    expected.add(Pair.of(outerRecord.toBuilder().setOtherId(otherId).build(), entry));
                                }
                            }
                        }
                    }

                    final Bindings bindings = Bindings.newBuilder()
                            .set(otherParam, otherId)
                            .set(keyParam, key)
                            .build();
                    List<Pair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry>> results = queryUnnestedMap(plan, bindings);
                    assertThat(results, containsInAnyOrder(expected.toArray()));
                }
            }

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void indexMatchDoesNotCompareOnSingleColumnPrimaryKey(long seed) {
        final Random r = new Random(seed);
        final List<TestRecordsNestedMapProto.OuterRecord> mapRecords = randomMapRecords(r, 50);
        final Index index = new Index("indexIncludingPrimaryKey", concat(field(PARENT_CONSTITUENT).nest("other_id"), field("map_entry").nest("key")));
        final RecordMetaData metaData = mapMetaData(addMapType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(UNNESTED_MAP, index)));

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            mapRecords.forEach(recordStore::saveRecord);

            final String otherParam = "other";
            final String keyParam = "key";
            final String idParam = "id";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(UNNESTED_MAP)
                    .setFilter(Query.and(
                            Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsParameter(otherParam)),
                            Query.field("map_entry").matches(Query.field("key").equalsParameter(keyParam)),
                            Query.field(PARENT_CONSTITUENT).matches(Query.field("rec_id").equalsParameter(idParam))
                    ))
                    .build();
            // Plan: Fetch(Covering(Index(indexIncludingPrimaryKey [EQUALS $other, EQUALS $key, IS UnnestedMap]) -> [__positions: [map_entry: KEY[4, 0]], map_entry: [key: KEY[1]], parent: [other_id: KEY[0], rec_id: KEY[3, 0]]]) | parent/{rec_id EQUALS $id})
            // Note that after the matched columns, the very next column in the index contains the rec_id, but because the
            // record ID is in a nested column, the planner should not attempt to push the predicate into the scan (at
            // least until we have the right scan comparison operators to handle nested queries).
            final RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, fetchFromPartialRecordPlan(
                    filterPlan(
                            coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName(index.getName())).and(scanComparisons(range("[EQUALS $" + otherParam + ", EQUALS $" + keyParam + ", IS " + UNNESTED_MAP + "]")))))
                    ).where(queryComponents(exactly(equalsObject(Query.field(PARENT_CONSTITUENT).matches(Query.field("rec_id").equalsParameter(idParam)))))))
            );
            assertEquals(-900125310, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1035680409, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            for (TestRecordsNestedMapProto.OuterRecord mapRecord : mapRecords) {
                for (String key : RANDOM_MAP_KEYS) {
                    final Bindings bindings = Bindings.newBuilder()
                            .set(otherParam, mapRecord.getOtherId())
                            .set(idParam, mapRecord.getRecId())
                            .set(keyParam, key)
                            .build();
                    final List<Pair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry>> queried = queryUnnestedMap(plan, bindings);
                    final Object[] expected = mapRecord.getMap().getEntryList().stream()
                            .filter(entry -> key.equals(entry.getKey()))
                            .map(entry -> Pair.of(mapRecord, entry))
                            .toArray();
                    assertThat(queried, containsInAnyOrder(expected));
                }
            }

            commit(context);
        }
    }

    @Nonnull
    static Stream<Arguments> indexMatchOrdersOnSingleColumnPrimaryKey() {
        return Stream.concat(
                Stream.of(
                        Arguments.of(false, false, 0x13375ca1eL),
                        Arguments.of(false, true, 0x13375ca1eL),
                        Arguments.of(true, false, 0x0fdbfa51L),
                        Arguments.of(true, true, 0x0fdbfa51L)
                ),
                RandomizedTestUtils.randomArguments(r -> Arguments.of(r.nextBoolean(), r.nextBoolean(), r.nextLong()))
        );
    }

    @ParameterizedTest
    @MethodSource
    void indexMatchOrdersOnSingleColumnPrimaryKey(boolean reverse, boolean includeListInSort, long seed) {
        final Random r = new Random(seed);
        final List<TestRecordsNestedMapProto.OuterRecord> mapRecords = randomMapRecords(r, 30);
        final Index index = new Index("indexIncludingPrimaryKey", concat(field(PARENT_CONSTITUENT).nest("other_id"), field("map_entry").nest("key")));
        final RecordMetaData metaData = mapMetaData(addMapType()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(UNNESTED_MAP, index)));

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            mapRecords.forEach(recordStore::saveRecord);

            final String otherParam = "other";
            final String keyParam = "key";
            final KeyExpression sortBase = Key.Expressions.field(PARENT_CONSTITUENT).nest("rec_id");
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(UNNESTED_MAP)
                    .setFilter(Query.and(
                            Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsParameter(otherParam)),
                            Query.field("map_entry").matches(Query.field("key").equalsParameter(keyParam))
                    ))
                    .setSort(includeListInSort ? Key.Expressions.list(sortBase) : sortBase, reverse)
                    .build();
            if (!includeListInSort) {
                // Currently, we need to wrap the sort expression in a "list" key expression because of how the synthetic
                // record primary key is handled. Those two expressions represent equivalent sorting, so one day,
                // we should be able to remove this assert and get the same plan as the other case
                RecordCoreException rce = assertThrows(RecordCoreException.class, () -> planQuery(query));
                assertThat(rce.getMessage(), containsString("Cannot sort without appropriate index"));
                return;
            }
            final RecordQueryPlan plan = planQuery(query);
            assertEquals(reverse, plan.isReverse());
            assertMatchesExactly(plan, indexPlan()
                    .where(indexName(index.getName()))
                    .and(scanComparisons(range("[EQUALS $" + otherParam + ", EQUALS $" + keyParam + ", IS " + UNNESTED_MAP + "]")))
            );
            assertEquals(reverse ? -1478191370 : -1478191371, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? -907441561 : -907441375, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            long minOther = mapRecords.stream()
                    .mapToLong(TestRecordsNestedMapProto.OuterRecord::getOtherId)
                    .min()
                    .orElseGet(() -> fail("list should not be empty"));
            long maxOther = mapRecords.stream()
                    .mapToLong(TestRecordsNestedMapProto.OuterRecord::getOtherId)
                    .max()
                    .orElseGet(() -> fail("list should not be empty"));

            for (long otherId = minOther - 1; otherId <= maxOther + 1; otherId++) {
                for (String key : RANDOM_MAP_KEYS) {
                    final Bindings bindings = Bindings.newBuilder()
                            .set(otherParam, otherId)
                            .set(keyParam, key)
                            .build();
                    final List<Pair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry>> queried = queryUnnestedMap(plan, bindings);
                    final List<Pair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry>> expected = new ArrayList<>();
                    for (TestRecordsNestedMapProto.OuterRecord mapRecord : mapRecords) {
                        if (mapRecord.getOtherId() == otherId) {
                            for (TestRecordsNestedMapProto.MapRecord.Entry entry : mapRecord.getMap().getEntryList()) {
                                if (key.equals(entry.getKey())) {
                                    expected.add(Pair.of(mapRecord, entry));
                                }
                            }
                        }
                    }
                    assertThat(queried, containsInAnyOrder(expected.toArray()));
                    TestRecordsNestedMapProto.OuterRecord lastRec = null;
                    for (Pair<TestRecordsNestedMapProto.OuterRecord, TestRecordsNestedMapProto.MapRecord.Entry> pair : queried) {
                        TestRecordsNestedMapProto.OuterRecord currentRec = pair.getKey();
                        if (lastRec != null) {
                            if (reverse) {
                                assertThat(currentRec.getOtherId(), lessThanOrEqualTo(lastRec.getOtherId()));
                            } else {
                                assertThat(currentRec.getOtherId(), greaterThanOrEqualTo(lastRec.getOtherId()));
                            }
                        }
                        lastRec = currentRec;
                    }
                }
            }

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void multiTypeDifferentiatedByTypeKeyComparison(long seed) {
        final Random r = new Random(seed);
        final Index index = new Index("otherFooIndex", concat(field(PARENT_CONSTITUENT).nest("other_int"), field("inner").nest("foo")));
        final String middleUnnestedType = "MiddleUnnested";
        final RecordMetaData metaData = doubleNestedMetaData(setOuterAndMiddlePrimaryKey(concatenateFields("other_int", "rec_no"))
                .andThen(addDoubleNestedType())
                .andThen(metaDataBuilder -> {
                    UnnestedRecordTypeBuilder unnestedBuilder = metaDataBuilder.addUnnestedRecordType(middleUnnestedType);
                    unnestedBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType("MiddleRecord"));
                    unnestedBuilder.addNestedConstituent("inner", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(),
                            PARENT_CONSTITUENT, field("other_middle").nest("inner", FanType.FanOut));
                })
                .andThen(metaDataBuilder -> metaDataBuilder.addMultiTypeIndex(List.of(metaDataBuilder.getIndexableRecordType(DOUBLE_NESTED), metaDataBuilder.getIndexableRecordType(middleUnnestedType)), index))
        );

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            List<TestRecordsDoubleNestedProto.OuterRecord> outerRecords = randomDoubleNestedMapRecords(r, 20);
            List<TestRecordsDoubleNestedProto.MiddleRecord> middleRecords = randomMiddleRecords(r, 20);
            long minOtherId = Long.MAX_VALUE;
            long maxOtherId = Long.MIN_VALUE;
            for (TestRecordsDoubleNestedProto.OuterRecord outerRecord : outerRecords) {
                recordStore.saveRecord(outerRecord);
                minOtherId = Math.min(minOtherId, outerRecord.getOtherInt());
                maxOtherId = Math.max(maxOtherId, outerRecord.getOtherInt());
            }
            for (TestRecordsDoubleNestedProto.MiddleRecord middleRecord : middleRecords) {
                recordStore.saveRecord(middleRecord);
                minOtherId = Math.min(minOtherId, middleRecord.getOtherInt());
                maxOtherId = Math.max(maxOtherId, middleRecord.getOtherInt());
            }

            final String otherIntParam = "otherInt";
            final String fooParam = "foo";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(DOUBLE_NESTED)
                    .setFilter(Query.and(
                            Query.field(PARENT_CONSTITUENT).matches(Query.field("other_int").equalsParameter(otherIntParam)),
                            Query.field("inner").matches(Query.field("foo").equalsParameter(fooParam))
                    ))
                    .build();
            final RecordQueryPlan plan = planQuery(query);
            // Note: this type filter on DOUBLE_NESTED is unnecessary because of the record type key comparison, but it should be harmless
            assertMatchesExactly(plan, typeFilterPlan(
                    indexPlan()
                            .where(indexName(index.getName()))
                            .and(scanComparisons(range("[EQUALS $" + otherIntParam + ", EQUALS $" + fooParam + ", IS " + DOUBLE_NESTED + "]")))
                    ).where(recordTypes(PrimitiveMatchers.containsAll(ImmutableSet.of(DOUBLE_NESTED))))
            );

            final FDBStoreTimer timer = recordStore.getTimer();
            assertNotNull(timer);
            timer.reset();
            int nonEmptySets = 0;
            for (long otherId = minOtherId - 1; otherId <= maxOtherId + 1; otherId++) {
                for (long foo = 0L; foo < 4L; foo++) {
                    List<Map<String, Message>> expected = new ArrayList<>();
                    for (TestRecordsDoubleNestedProto.OuterRecord outerRecord : outerRecords) {
                        if (outerRecord.getOtherInt() == otherId) {
                            for (TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord middle : outerRecord.getManyMiddleList()) {
                                for (TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord innerRecord : middle.getInnerList()) {
                                    if (innerRecord.getFoo() == foo) {
                                        for (TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord outerInner : outerRecord.getInnerList()) {
                                            expected.add(Map.of(PARENT_CONSTITUENT, outerRecord, "middle", middle, "inner", innerRecord, "outer_inner", outerInner));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (!expected.isEmpty()) {
                        nonEmptySets++;
                    }

                    final Bindings bindings = Bindings.newBuilder()
                            .set(otherIntParam, otherId)
                            .set(fooParam, foo)
                            .build();
                    List<Map<String, Message>> queried = plan.execute(recordStore, EvaluationContext.forBindings(bindings))
                            .map(rec -> {
                                assertEquals(DOUBLE_NESTED, rec.getRecordType().getName(), () -> "record type for record " + rec + " should match type " + DOUBLE_NESTED);
                                return rec.getSyntheticRecord();
                            })
                            .map(rec -> rec.getConstituents().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (Message) e.getValue().getRecord())))
                            .asList()
                            .join();
                    assertThat(expected, containsInAnyOrder(queried.toArray()));
                }
            }
            if (!RandomizedTestUtils.includeRandomTests()) {
                // Make sure there is at least one non-empty set, as those are the cases that are the most meaningful. Do not
                // run this assertion on randomly generated seeds, though
                assertThat("There should be at least one non-empty test case", nonEmptySets, greaterThan(0));
            }
            assertEquals(timer.getCount(FDBStoreTimer.Counts.QUERY_TYPE_FILTER_PLAN_PASSED), timer.getCount(FDBStoreTimer.Counts.QUERY_TYPE_FILTER_PLAN_GIVEN));
            assertEquals(0, timer.getCount(FDBStoreTimer.Counts.QUERY_DISCARDED));

            commit(context);
        }
    }

    // Validate lifecycle things, like the record type

    @Test
    void indexAddedLater() {
        final RecordMetaData metaDataWithoutIndex = mapMetaData(metaDataBuilder -> { });
        final RecordMetaData metaDataWithIndex = mapMetaData(addMapType().andThen(addKeyOtherIntValueIndex()));
        assertThat(metaDataWithIndex.getVersion(), greaterThan(metaDataWithoutIndex.getVersion()));

        // Validate that the index shows up as having been added since the first meta-data without the index
        final Map<Index, List<RecordType>> indexesSince = metaDataWithIndex.getIndexesToBuildSince(metaDataWithoutIndex.getVersion());
        final Index index = metaDataWithIndex.getIndex(KEY_OTHER_INT_VALUE_INDEX);
        assertThat(indexesSince, hasKey(index));
        List<RecordType> typesForIndex = indexesSince.get(index);
        assertEquals(List.of(metaDataWithIndex.getRecordType(OUTER)), typesForIndex);

        // Create the store without the index
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaDataWithoutIndex);
            commit(context);
        }

        // Upgrade to the new meta-data. As the store is empty, the index can be built for for free
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaDataWithIndex);
            assertEquals(IndexState.READABLE, recordStore.getIndexState(index));
            // do not commit
        }

        // Go back to the old meta-data. Add some records so that we do not mark it readable immediately
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaDataWithoutIndex);
            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                recordStore.saveRecord(outerRecord);
            }
            commit(context);
        }

        // Upgrade again to the new meta-data. The index should begin as not-built
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaDataWithIndex);
            assertEquals(IndexState.DISABLED, recordStore.getIndexState(index));

            recordStore.rebuildIndex(index).join();
            assertEquals(IndexState.READABLE, recordStore.getIndexState(index));

            final List<IndexEntry> indexEntries = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .join();
            final List<IndexEntry> expectedEntries = new ArrayList<>();
            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    final Tuple primaryKey = Tuple.from(metaDataWithIndex.getSyntheticRecordType(UNNESTED_MAP).getRecordTypeKey(), Tuple.from(outerRecord.getRecId()), Tuple.from(i));
                    TestRecordsNestedMapProto.MapRecord.Entry entry = outerRecord.getMap().getEntry(i);
                    final Tuple indexKey = Tuple.from(entry.getKey(), outerRecord.getOtherId(), entry.getIntValue());
                    expectedEntries.add(new IndexEntry(index, indexKey.addAll(primaryKey), TupleHelpers.EMPTY, primaryKey));
                }
            }
            expectedEntries.sort(Comparator.comparing(IndexEntry::getKey));
            assertEquals(expectedEntries, indexEntries);

            commit(context);
        }
    }

    @Test
    void conflictOnRecordAddedWhenIndexBuildStarts() {
        final RecordMetaData metaData = mapMetaData(addMapType().andThen(addOtherKeyIdValueIndex()));
        final Index index = metaData.getIndex(OTHER_KEY_ID_VALUE_INDEX);
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            recordStore.markIndexDisabled(index).join();
            LongStream.range(0L, 10L).mapToObj(id ->
                    TestRecordsNestedMapProto.OuterRecord.newBuilder()
                            .setRecId(id)
                            .setOtherId(id % 2)
                            .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder()
                                    .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                            .setKey("foo")
                                            .setIntValue(id)
                                    )
                                    .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                            .setKey("bar")
                                            .setIntValue(id + 1)
                                    )
                                    .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                            .setKey("baz")
                                            .setIntValue(id + 2)
                                    )
                            )
                            .build()
            ).forEach(recordStore::saveRecord);
            commit(context);
        }

        try (FDBRecordContext context1 = openContext()) {
            createOrOpenRecordStore(context1, metaData);

            // Save a record. As the index is disabled, it does not need to update the synthetic record type indexes
            final FDBStoreTimer timer = recordStore.getTimer();
            assertNotNull(timer);
            timer.reset();
            recordStore.insertRecord(sampleMapRecord());
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));

            // Build the index and commit the transaction
            try (FDBRecordContext context2 = openContext()) {
                createOrOpenRecordStore(context2, metaData);
                recordStore.rebuildIndex(index).join();
                commit(context2);
            }

            // The initial save should not succeed. If it did, then because the record was not added to
            // the synthetic index when initially saved and the record was not visible in the store during th
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, () -> commit(context1));
        }

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            // Now that we've built the index, saving the record should also result in the synthetic type being planned
            final FDBStoreTimer timer = recordStore.getTimer();
            assertNotNull(timer);
            timer.reset();
            recordStore.insertRecord(sampleMapRecord());
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));

            commit(context);
        }
    }

    @Test
    void mapTypeAddedLater() {
        // Begin with test_records_1.proto as the basis for the meta-data
        final RecordMetaData metaDataWithoutMap = RecordMetaData.build(TestRecords1Proto.getDescriptor());

        // Construct a new meta-data that adds a new type, with an unnested record type on it
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1EvolvedWithMapProto.getDescriptor());
        metaDataBuilder.getRecordType("MySimpleRecord").setSinceVersion(metaDataWithoutMap.getVersion());
        metaDataBuilder.getRecordType("MyOtherRecord").setSinceVersion(metaDataWithoutMap.getVersion());
        metaDataBuilder.getRecordType("MyMapRecord").setSinceVersion(metaDataWithoutMap.getVersion() + 1);

        UnnestedRecordTypeBuilder unnestedRecordTypeBuilder = metaDataBuilder.addUnnestedRecordType(UNNESTED_MAP);
        unnestedRecordTypeBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType("MyMapRecord"));
        unnestedRecordTypeBuilder.addNestedConstituent("entry", TestRecords1EvolvedWithMapProto.MyMapRecord.Entry.getDescriptor(), PARENT_CONSTITUENT,
                field("map_from_str_to_long", FanType.FanOut));

        final Index unnestedIndex = new Index("Map$key_other_value", concat(field("entry").nest("key"), field(PARENT_CONSTITUENT).nest("other_str"), field("entry").nest("value")));
        unnestedIndex.setAddedVersion(metaDataWithoutMap.getVersion() + 1);
        unnestedIndex.setLastModifiedVersion(metaDataWithoutMap.getVersion() + 1);
        metaDataBuilder.addIndex(UNNESTED_MAP, unnestedIndex);

        final Index simpleIndex = new Index("MySimpleRecord$num_value_2", "num_value_2");
        unnestedIndex.setAddedVersion(metaDataWithoutMap.getVersion() + 1);
        unnestedIndex.setLastModifiedVersion(metaDataWithoutMap.getVersion() + 1);
        metaDataBuilder.addIndex("MySimpleRecord", simpleIndex);

        // Validate that the version increases and that the new index is in the set of data that is added to the type
        final RecordMetaData metaDataWithMap = metaDataBuilder.build();
        assertThat(metaDataWithMap.getVersion(), greaterThan(metaDataWithoutMap.getVersion()));
        assertThat(metaDataWithMap.getIndexesToBuildSince(metaDataWithoutMap.getVersion()), hasKey(unnestedIndex));

        // Start without the map
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaDataWithoutMap);

            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setStrValueIndexed("true")
                    .setNumValue2(10)
                    .setNumValue3Indexed(1)
                    .setNumValueUnique(100)
                    .build());
            recordStore.saveRecord(TestRecords1Proto.MyOtherRecord.newBuilder()
                    .setRecNo(1412L)
                    .setNumValue2(5)
                    .setNumValue3Indexed(2)
                    .build());

            commit(context);
        }

        // Now upgrade to the map type. As the index is on new stored types, it should be readable
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaDataWithMap);

            // The new index on MySimpleRecord is not readable as it is on pre-existing types
            assertEquals(IndexState.DISABLED, recordStore.getIndexState(simpleIndex));
            // The new unnested index is on the new MyMapRecord type, and so it can be marked as readable without a real build
            assertEquals(IndexState.READABLE, recordStore.getIndexState(unnestedIndex));

            commit(context);
        }
    }

    @Test
    void validateBuildDuringCheckVersion() {
        final RecordMetaData metaDataWithoutIndex = mapMetaData(metaDataBuilder -> { });
        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaDataWithoutIndex);
            storeBuilder = recordStore.asBuilder();
            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                recordStore.saveRecord(outerRecord);
                // Save a second OtherRecord which should not participate during the index build
                recordStore.saveRecord(TestRecordsNestedMapProto.OtherRecord.newBuilder()
                        .setRecId(-1 * outerRecord.getRecId())
                        .setOtherId(outerRecord.getOtherId())
                        .build());
            }
            commit(context);
        }

        final RecordMetaData metaDataWithIndex = mapMetaData(addMapType().andThen(addKeyOtherIntValueIndex()));
        assertThat(metaDataWithIndex.getVersion(), greaterThan(metaDataWithoutIndex.getVersion()));
        try (FDBRecordContext context = openContext()) {
            final Set<Index> newIndexesToBuild = new HashSet<>();
            final FDBStoreTimer timer = context.getTimer();
            assertNotNull(timer);
            timer.reset();
            final FDBRecordStore storeWithIndex = storeBuilder
                    .setContext(context)
                    .setMetaDataProvider(metaDataWithIndex)
                    .setUserVersionChecker(new FDBRecordStoreBase.UserVersionChecker() {
                        @Deprecated
                        @Override
                        public CompletableFuture<Integer> checkUserVersion(final int oldUserVersion, final int oldMetaDataVersion, final RecordMetaDataProvider metaData) {
                            return fail("deprecated method should not be called");
                        }

                        @Override
                        public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
                            return CompletableFuture.completedFuture(storeHeader.getUserVersion());
                        }

                        @Nonnull
                        @Override
                        public CompletableFuture<IndexState> needRebuildIndex(final Index index, final Supplier<CompletableFuture<Long>> lazyRecordCount, final Supplier<CompletableFuture<Long>> lazyEstimatedSize, final boolean indexOnNewRecordTypes) {
                            newIndexesToBuild.add(index);
                            assertFalse(indexOnNewRecordTypes, "Unnested index should not have been identified as being on new types");
                            // Return READABLE to force the store to rebuild the index during check version
                            return CompletableFuture.completedFuture(IndexState.READABLE);
                        }
                    })
                    .open();
            final Index index = metaDataWithIndex.getIndex(KEY_OTHER_INT_VALUE_INDEX);
            assertEquals(Set.of(index), newIndexesToBuild);
            assertEquals(sampleMapRecords().size(), timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));


            final List<IndexEntry> expectedEntries = new ArrayList<>();
            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                final Tuple outerPrimaryKey = Tuple.from(outerRecord.getRecId());
                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    final Tuple syntheticPrimaryKey = Tuple.from(metaDataWithIndex.getSyntheticRecordType(UNNESTED_MAP).getRecordTypeKey(), outerPrimaryKey, Tuple.from(i));
                    final TestRecordsNestedMapProto.MapRecord.Entry entry = outerRecord.getMap().getEntry(i);
                    final Tuple indexKey = Tuple.from(entry.getKey(), outerRecord.getOtherId(), entry.getIntValue());
                    expectedEntries.add(new IndexEntry(index, indexKey.addAll(syntheticPrimaryKey), TupleHelpers.EMPTY, syntheticPrimaryKey));
                }
            }
            expectedEntries.sort(Comparator.comparing(IndexEntry::getKey));
            final List<IndexEntry> scannedEntries = storeWithIndex.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .join();
            assertEquals(expectedEntries, scannedEntries);

            commit(context);
        }
    }

    // Validate deleteRecordsWhere

    @Test
    void deleteRecordsWhere() {
        final RecordMetaDataHook hook = setOuterAndOtherPrimaryKey(concatenateFields("other_id", "rec_id"))
                .andThen(addMapType())
                .andThen(addOtherKeyIdValueIndex());
        final RecordMetaData metaData = mapMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            final TestRecordsNestedMapProto.OuterRecord rec = sampleMapRecord();
            final List<FDBStoredRecord<Message>> saved = saveRecordsForDeleteRecordsWhere(rec);

            assertAllPresent(saved);
            assertThat(queryOtherKeyIdValue(rec.getOtherId()), not(empty()));
            assertThat(queryOtherKeyIdValue(rec.getOtherId() + 1), not(empty()));
            assertThat(queryOtherKeyIdValue(rec.getOtherId() - 1), not(empty()));

            recordStore.deleteRecordsWhere(Query.field("other_id").equalsValue(rec.getOtherId()));

            assertAllAbsent(saved.subList(0, 2));
            assertAllPresent(saved.subList(2, saved.size()));
            assertThat(queryOtherKeyIdValue(rec.getOtherId()), empty());
            assertThat(queryOtherKeyIdValue(rec.getOtherId() + 1), not(empty()));
            assertThat(queryOtherKeyIdValue(rec.getOtherId() - 1), not(empty()));

            commit(context);
        }
    }

    @Test
    void deleteRecordsWhereWithAnd() {
        final Index index = new Index("alignedIndex",
                concat(field(PARENT_CONSTITUENT).nest("other_int"),
                        field(PARENT_CONSTITUENT).nest(field("middle").nest("other_int")),
                        field("middle").nest("other_int"),
                        field("inner").nest("foo"),
                        field("outer_inner").nest("foo")));
        final RecordMetaDataHook hook = setOuterAndMiddlePrimaryKey(concat(field("other_int"), field("middle").nest( "other_int"), field("rec_no")))
                .andThen(addDoubleNestedType())
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(DOUBLE_NESTED, index));
        final RecordMetaData metaData = doubleNestedMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            final TestRecordsDoubleNestedProto.OuterRecord rec = sampleDoubleNestedRecord();
            final List<FDBStoredRecord<Message>> saved = new ArrayList<>();
            saved.add(recordStore.saveRecord(rec));
            saved.add(recordStore.saveRecord(rec.toBuilder().setRecNo(rec.getRecNo() + 1).build()));
            saved.add(recordStore.saveRecord(rec.toBuilder().setRecNo(rec.getRecNo() + 2).setOtherInt(rec.getOtherInt() + 1).build()));
            saved.add(recordStore.saveRecord(rec.toBuilder().setRecNo(rec.getRecNo() + 3).setOtherInt(rec.getOtherInt() - 1).build()));
            saved.add(recordStore.saveRecord(rec.toBuilder().setRecNo(rec.getRecNo() + 4).setMiddle(rec.getMiddle().toBuilder().setOtherInt(rec.getMiddle().getOtherInt() + 1)).build()));
            saved.add(recordStore.saveRecord(rec.toBuilder().setRecNo(rec.getRecNo() + 5).setMiddle(rec.getMiddle().toBuilder().setOtherInt(rec.getMiddle().getOtherInt() - 1)).build()));

            // Assert that the index contains entries corresponding to the first two records before issuing the delete
            final List<IndexEntry> entriesBeforeDelete = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().join();
            assertThat(entriesBeforeDelete.stream().filter(entry -> entry.getKey().getLong(0) == rec.getOtherInt() && entry.getKey().getLong(1) == rec.getMiddle().getOtherInt()).collect(Collectors.toList()), not(empty()));
            assertThat(entriesBeforeDelete.stream().map(entry -> entry.getPrimaryKey().getNestedTuple(1)).filter(pk -> pk.equals(saved.get(0).getPrimaryKey())).collect(Collectors.toList()), not(empty()));
            assertThat(entriesBeforeDelete.stream().map(entry -> entry.getPrimaryKey().getNestedTuple(1)).filter(pk -> pk.equals(saved.get(1).getPrimaryKey())).collect(Collectors.toList()), not(empty()));

            // Any entry that is not prefixed by (rec.getOtherInt(), rec.getMiddle().getOtherInt()) should not be deleted. Validate that the initial list is not empty so that this test is not vaccuously true
            final List<IndexEntry> entriesNotToDelete = entriesBeforeDelete.stream().filter(entry -> entry.getKey().getLong(0) != rec.getOtherInt() || entry.getKey().getLong(1) != rec.getMiddle().getOtherInt()).collect(Collectors.toList());
            assertThat(entriesNotToDelete, not(empty()));

            assertAllPresent(saved);

            recordStore.deleteRecordsWhere(Query.and(Query.field("other_int").equalsValue(rec.getOtherInt()), Query.field("middle").matches(Query.field("other_int").equalsValue(rec.getMiddle().getOtherInt()))));

            assertAllAbsent(saved.subList(0, 2));
            assertAllPresent(saved.subList(2, saved.size()));

            // Validate the index after the delete. The entries should exactly match the original entries, except missing those with the original prefix (which were from the two deleted records)
            final List<IndexEntry> entriesAfterDelete = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().join();
            assertEquals(entriesNotToDelete, entriesAfterDelete);
            assertThat(entriesAfterDelete.stream().map(entry -> entry.getPrimaryKey().getNestedTuple(1)).filter(pk -> pk.equals(saved.get(0).getPrimaryKey()) || pk.equals(saved.get(1).getPrimaryKey())).collect(Collectors.toList()), empty());

            commit(context);
        }
    }

    @Test
    void deleteWhereFailsIfNotAligned() {
        final RecordMetaDataHook hook = setOuterAndOtherPrimaryKey(concatenateFields("other_id", "rec_id"))
                .andThen(addMapType())
                .andThen(addKeyOtherIntValueIndex());
        final RecordMetaData metaData = mapMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            assertDeleteRecordsWhereFails(null, Query.field("other_id").equalsValue(42L), KEY_OTHER_INT_VALUE_INDEX);
            commit(context);
        }
    }

    @Test
    void deleteWhereWithAndFailsIfNotAligned() {
        // Note that the index is _not_ aligned with the first two columns of the primary key.
        // The first column matches, but the second column is different: the index contains the other_int field from the middle _constituent_,
        // whereas the primary key contains the other_int field from the middle _field_ (that is, parent.middle.other_int in the nested type)
        final Index index = new Index("nonAlignedIndex",
                concat(field(PARENT_CONSTITUENT).nest("other_int"),
                        field("middle").nest("other_int"),
                        field("inner").nest("foo"),
                        field("outer_inner").nest("foo")));
        final RecordMetaDataHook hook = setOuterAndMiddlePrimaryKey(concat(field("other_int"), field("middle").nest( "other_int"), field("rec_no")))
                .andThen(addDoubleNestedType())
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(DOUBLE_NESTED, index));
        final RecordMetaData metaData = doubleNestedMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            assertDeleteRecordsWhereFails(null, Query.and(Query.field("other_int").equalsValue(42L), Query.field("middle").matches(Query.field("other_int").equalsValue(42L))), index.getName());
            commit(context);
        }
    }

    @Test
    void deleteWhereWithTypeFilterFailsIfNotAligned() {
        final RecordMetaDataHook hook = setOuterAndOtherPrimaryKey(concat(recordType(), field("other_id"), field("rec_id")))
                .andThen(addMapType())
                .andThen(addKeyOtherIntValueIndex());
        final RecordMetaData metaData = mapMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            assertDeleteRecordsWhereFails(OUTER, Query.field("other_id").equalsValue(42L), KEY_OTHER_INT_VALUE_INDEX);
            commit(context);
        }
    }

    @Test
    void deleteWhereOnUnnestedNotAllowed() {
        final RecordMetaDataHook hook = setOuterAndOtherPrimaryKey(concat(recordType(), field("other_id"), field("rec_id")))
                .andThen(addMapType())
                .andThen(addOtherKeyIdValueIndex());
        final RecordMetaData metaData = mapMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            assertThrows(MetaDataException.class, () -> recordStore.deleteRecordsWhere(UNNESTED_MAP, Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsValue(42L))));
            commit(context);
        }
    }

    @Test
    void deleteSingleTypeOnMixedStoredTypesIndexNotAllowed() {
        final RecordMetaDataHook hook = setOuterAndMiddlePrimaryKey(concat(recordType(), field("rec_no")))
                .andThen(addDoubleNestedType())
                .andThen(metaDataBuilder -> {
                    UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType("OtherUnnested");
                    typeBuilder.addParentConstituent("other_parent", metaDataBuilder.getRecordType("MiddleRecord"));
                    typeBuilder.addNestedConstituent("inner", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(), "other_parent",
                            field("other_middle").nest("inner", FanType.FanOut));
                })
                .andThen(metaDataBuilder -> {
                    final Index fooIndex = new Index("fooIndex", field("inner").nest("foo"));
                    metaDataBuilder.addMultiTypeIndex(List.of(metaDataBuilder.getIndexableRecordType(DOUBLE_NESTED), metaDataBuilder.getIndexableRecordType("OtherUnnested")), fooIndex);
                });
        final RecordMetaData metaData = doubleNestedMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            assertDeleteRecordsWhereFails(OUTER, null, "fooIndex");
            commit(context);
        }
    }

    @Test
    void deleteWhereSucceedsWithDisabledIndex() {
        final RecordMetaDataHook hook = setOuterAndOtherPrimaryKey(concatenateFields("other_id", "rec_id"))
                .andThen(addMapType())
                .andThen(addKeyOtherIntValueIndex());
        final RecordMetaData metaData = mapMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            final TestRecordsNestedMapProto.OuterRecord rec = sampleMapRecord();
            final List<FDBStoredRecord<Message>> saved = saveRecordsForDeleteRecordsWhere(rec);
            assertAllPresent(saved);

            // Delete where initially fails because the index does not have the right prefix
            assertDeleteRecordsWhereFails(null, Query.field("other_id").equalsValue(rec.getOtherId()), KEY_OTHER_INT_VALUE_INDEX);
            assertAllPresent(saved);

            recordStore.markIndexDisabled(KEY_OTHER_INT_VALUE_INDEX).join();

            // Delete where should now succeed
            recordStore.deleteRecordsWhere(Query.field("other_id").equalsValue(rec.getOtherId()));
            assertAllAbsent(saved.subList(0, 2));
            assertAllPresent(saved.subList(2, saved.size()));

            commit(context);
        }
    }

    @Test
    void deleteWhereWithTypeFilter() {
        // As types are specified, make sure the primary key of the outer record is prefixed by record type
        final RecordMetaDataHook hook = setOuterAndOtherPrimaryKey(concat(recordType(), field("other_id"), field("rec_id")))
                .andThen(addMapType())
                .andThen(addOtherKeyIdValueIndex());
        final RecordMetaData metaData = mapMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            final TestRecordsNestedMapProto.OuterRecord rec = sampleMapRecord();
            final List<FDBStoredRecord<Message>> saved = saveRecordsForDeleteRecordsWhere(rec);

            assertAllPresent(saved);
            assertThat(queryOtherKeyIdValue(rec.getOtherId()), not(empty()));
            assertThat(queryOtherKeyIdValue(rec.getOtherId() + 1), not(empty()));
            assertThat(queryOtherKeyIdValue(rec.getOtherId() - 1), not(empty()));

            recordStore.deleteRecordsWhere(OUTER, Query.field("other_id").equalsValue(rec.getOtherId()));

            assertAllAbsent(saved.subList(0, 2));
            assertAllPresent(saved.subList(2, saved.size()));
            assertThat(queryOtherKeyIdValue(rec.getOtherId()), empty());
            assertThat(queryOtherKeyIdValue(rec.getOtherId() + 1), not(empty()));
            assertThat(queryOtherKeyIdValue(rec.getOtherId() - 1), not(empty()));

            commit(context);
        }
    }

    @Test
    void deleteWhereWithOnlyTypeFilter() {
        // As types are specified, make sure the primary key of the outer record is prefixed by record type
        final RecordMetaDataHook hook = setOuterAndOtherPrimaryKey(concat(recordType(), field("rec_id")))
                .andThen(addMapType())
                .andThen(addOtherKeyIdValueIndex());
        final RecordMetaData metaData = mapMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            final TestRecordsNestedMapProto.OuterRecord rec = sampleMapRecord();
            final List<FDBStoredRecord<Message>> saved = saveRecordsForDeleteRecordsWhere(rec);

            assertAllPresent(saved);
            assertThat(queryOtherKeyIdValue(rec.getOtherId()), not(empty()));
            assertThat(queryOtherKeyIdValue(rec.getOtherId() + 1), not(empty()));
            assertThat(queryOtherKeyIdValue(rec.getOtherId() - 1), not(empty()));

            recordStore.deleteRecordsWhere(OUTER, null);

            assertAllAbsent(saved);
            assertThat(queryOtherKeyIdValue(rec.getOtherId()), empty());
            assertThat(queryOtherKeyIdValue(rec.getOtherId() + 1), empty());
            assertThat(queryOtherKeyIdValue(rec.getOtherId() - 1), empty());

            commit(context);
        }
    }

    @Test
    void deleteWhereOnMultiTypeIndex() {
        // Add two unnested record types to the index (on the same stored record type)
        final RecordMetaDataHook hook = setOuterAndOtherPrimaryKey(concat(recordType(), field("other_id"), field("rec_id")))
                .andThen(addMapType())
                .andThen(addTwoMapsType())
                .andThen(metaDataBuilder -> {
                    final Index syntheticOtherIndex = new Index("syntheticOther", concat(field(PARENT_CONSTITUENT).nest("other_id"), Key.Expressions.value(42L)));
                    metaDataBuilder.addMultiTypeIndex(List.of(metaDataBuilder.getSyntheticRecordType(UNNESTED_MAP), metaDataBuilder.getSyntheticRecordType(TWO_UNNESTED_MAPS)), syntheticOtherIndex);
                });
        final RecordMetaData metaData = mapMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            final TestRecordsNestedMapProto.OuterRecord rec = sampleMapRecord();
            final FDBStoredRecord<Message> storedRecord = recordStore.saveRecord(rec);

            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordTypes(List.of(UNNESTED_MAP, TWO_UNNESTED_MAPS))
                    .setFilter(Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsValue(rec.getOtherId())))
                    .build();

            assertNotNull(recordStore.loadRecord(storedRecord.getPrimaryKey()));
            assertThat(recordStore.executeQuery(query).asList().join(), not(empty()));

            recordStore.deleteRecordsWhere(OUTER, Query.field("other_id").equalsValue(rec.getOtherId()));

            assertNull(recordStore.loadRecord(storedRecord.getPrimaryKey()));
            assertThat(recordStore.executeQuery(query).asList().join(), empty());

            commit(context);
        }
    }

    @Test
    void deleteWhereFailsWhenParentTypesHaveDifferentNames() {
        final String secondMapType = "secondMapType";
        final RecordMetaDataHook hook = setOuterAndOtherPrimaryKey(concat(recordType(), field("rec_id")))
                .andThen(addMapType())
                .andThen(metaDataBuilder -> {
                    final UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(secondMapType);
                    typeBuilder.addParentConstituent("p", metaDataBuilder.getRecordType(OUTER));
                    typeBuilder.addNestedConstituent("map_entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(), "p", ENTRIES_FAN_OUT);
                })
                .andThen(metaDataBuilder -> {
                    final Index syntheticOtherIndex = new Index("syntheticOther", field("map_entry").nest("key"));
                    metaDataBuilder.addMultiTypeIndex(List.of(metaDataBuilder.getSyntheticRecordType(UNNESTED_MAP), metaDataBuilder.getSyntheticRecordType(secondMapType)), syntheticOtherIndex);
                });
        final RecordMetaData metaData = mapMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            assertDeleteRecordsWhereFails(OUTER, null, "syntheticOther");
        }
    }

    @Test
    void deleteWhereWithDifferentTypedParents() {
        // Create a multi-type index on two different unnested types. Each one has a parent constituent named and an inner constituent, so the index is well-defined.
        // The first column in the index is middle.rec_no, which we also set as the first column in the combined primary key. This means that it should be legal to
        // perform a delete where with a middle.rec_no predicate
        final RecordMetaDataHook hook = addMultiTypeDoubleUnnestedIndex()
                .andThen(setOuterAndMiddlePrimaryKey(concat(field("middle").nest("other_int"), field("rec_no"))));
        final RecordMetaData metaData = doubleNestedMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            final Set<Long> otherIds = Set.of(1L, 2L, 3L);
            final TestRecordsDoubleNestedProto.OuterRecord outer = sampleDoubleNestedRecord();
            final Map<Long, List<FDBStoredRecord<Message>>> recordsByMiddleOther = new HashMap<>();
            for (long otherId : otherIds) {
                List<FDBStoredRecord<Message>> saved = new ArrayList<>();
                saved.add(recordStore.saveRecord(outer.toBuilder().setMiddle(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.newBuilder().setOtherInt(otherId)).build()));
                for (int i = 0; i < 5; i++) {
                    final TestRecordsDoubleNestedProto.MiddleRecord middle = TestRecordsDoubleNestedProto.MiddleRecord.newBuilder()
                            .setRecNo(i)
                            .setMiddle(TestRecordsDoubleNestedProto.MiddleRecord.newBuilder().setOtherInt(otherId))
                            .setOtherMiddle(outer.getManyMiddle(0))
                            .build();
                    saved.add(recordStore.saveRecord(middle));
                }
                recordsByMiddleOther.put(otherId, saved);
            }
            otherIds.forEach(otherId -> assertAllPresent(recordsByMiddleOther.get(otherId)));

            final String otherParam = "o";
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordTypes(List.of(DOUBLE_NESTED, "MiddleUnnested"))
                    .setFilter(Query.field(PARENT_CONSTITUENT).matches(Query.field("middle").matches(Query.field("other_int").equalsParameter(otherParam))))
                    .setSort(field("inner").nest("foo"))
                    .build();
            final RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, indexPlan()
                    .where(indexName(MULTI_TYPE_DOUBLE_NESTED_INDEX))
                    .and(scanComparisons(range("[EQUALS $" + otherParam + "]")))
            );

            final Map<Long, List<IndexEntry>> entriesByOtherId = new HashMap<>();
            otherIds.forEach(otherId -> {
                EvaluationContext evaluationContext = EvaluationContext.forBinding(otherParam, otherId);
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(recordStore, evaluationContext)) {
                    List<IndexEntry> entries = cursor.map(FDBQueriedRecord::getIndexEntry).asList().join();
                    assertThat(entries, not(empty()));
                    entriesByOtherId.put(otherId, entries);
                }
            });

            recordStore.deleteRecordsWhere(Query.field("middle").matches(Query.field("other_int").equalsValue(1L)));

            otherIds.forEach(otherId -> {
                // Assert the appropriate records have been deleted
                final List<FDBStoredRecord<Message>> saved = recordsByMiddleOther.get(otherId);
                if (otherId == 1L) {
                    assertAllAbsent(saved);
                } else {
                    assertAllPresent(saved);
                }

                // Assert the appropriate entries have been deleted and the rest are unaffected
                EvaluationContext evaluationContext = EvaluationContext.forBinding(otherParam, otherId);
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(recordStore, evaluationContext)) {
                    List<IndexEntry> entries = cursor.map(FDBQueriedRecord::getIndexEntry).asList().join();
                    if (otherId == 1L) {
                        assertThat(entries, empty());
                    } else {
                        assertEquals(entriesByOtherId.get(otherId), entries);
                    }
                }
            });

            commit(context);
        }
    }

    @Test
    void deleteWhereOnMultiTypeFailsWithAmbiguousParent() {
        // Create a multi-type index on two different unnested types. Each one has a parent constituent named and an inner constituent, so the index is well-defined.
        // The primary keys have record type prefixes, so we can delete records of a given type, but the index does not have unique prefixes for each type, so it
        // must block the delete records where.
        final RecordMetaDataHook hook = addMultiTypeDoubleUnnestedIndex()
                .andThen(setOuterAndMiddlePrimaryKey(concat(recordType(), field("middle").nest("other_int"), field("rec_no"))));
        final RecordMetaData metaData = doubleNestedMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            assertDeleteRecordsWhereFails(OUTER, null, MULTI_TYPE_DOUBLE_NESTED_INDEX);
            assertDeleteRecordsWhereFails("MiddleRecord", null, MULTI_TYPE_DOUBLE_NESTED_INDEX);

            assertDeleteRecordsWhereFails(OUTER, Query.field("middle").matches(Query.field("other_int").equalsValue(2L)), MULTI_TYPE_DOUBLE_NESTED_INDEX);
            assertDeleteRecordsWhereFails("MiddleRecord", Query.field("middle").matches(Query.field("other_int").equalsValue(2L)), MULTI_TYPE_DOUBLE_NESTED_INDEX);

            commit(context);
        }
    }

    @Test
    void deleteWhereOnMultiTypeFailsWithRecordTypePrefix() {
        // Create a multi-type index on two different unnested types. Each one has a parent constituent named and an inner constituent, so the index is well-defined.
        // The multi-type index in this case has a record type prefix. In theory, we actually could perform the delete records where, but the record type key in the
        // index matches the synthetic type's record type key, not the base type. This means we'd need to translate the record type key before deleting data from the
        // index. Until we get that working, just assert that this fails.
        final RecordMetaDataHook hook = addMultiTypeDoubleUnnestedIndex(concat(recordType(), field(PARENT_CONSTITUENT).nest(field("middle").nest("other_int")), field("inner").nest("foo")))
                .andThen(setOuterAndMiddlePrimaryKey(concat(recordType(), field("middle").nest("other_int"), field("rec_no"))));
        final RecordMetaData metaData = doubleNestedMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            assertDeleteRecordsWhereFails(OUTER, null, MULTI_TYPE_DOUBLE_NESTED_INDEX);
            assertDeleteRecordsWhereFails("MiddleRecord", null, MULTI_TYPE_DOUBLE_NESTED_INDEX);

            assertDeleteRecordsWhereFails(OUTER, Query.field("middle").matches(Query.field("other_int").equalsValue(2L)), MULTI_TYPE_DOUBLE_NESTED_INDEX);
            assertDeleteRecordsWhereFails("MiddleRecord", Query.field("middle").matches(Query.field("other_int").equalsValue(2L)), MULTI_TYPE_DOUBLE_NESTED_INDEX);

            commit(context);
        }
    }

    @Nonnull
    private List<FDBSyntheticRecord> queryOtherKeyIdValue(long otherId) {
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType(UNNESTED_MAP)
                .setFilter(Query.field(PARENT_CONSTITUENT).matches(Query.field("other_id").equalsValue(otherId)))
                .setSort(concat(field("map_entry").nest("key"), field(PARENT_CONSTITUENT).nest("rec_id")))
                .build();
        try (RecordCursor<FDBSyntheticRecord> cursor = recordStore.executeQuery(query).map(FDBQueriedRecord::getSyntheticRecord)) {
            return cursor.asList().join();
        }
    }

    @Nonnull
    private List<FDBStoredRecord<Message>> saveRecordsForDeleteRecordsWhere(TestRecordsNestedMapProto.OuterRecord baseRec) {
        final List<FDBStoredRecord<Message>> saved = new ArrayList<>();
        saved.add(recordStore.saveRecord(baseRec));
        saved.add(recordStore.saveRecord(baseRec.toBuilder().setRecId(baseRec.getRecId() + 1).build()));
        saved.add(recordStore.saveRecord(baseRec.toBuilder().setRecId(baseRec.getRecId() + 2).setOtherId(baseRec.getOtherId() + 1).build()));
        saved.add(recordStore.saveRecord(baseRec.toBuilder().setRecId(baseRec.getRecId() + 3).setOtherId(baseRec.getOtherId() + 1).build()));
        saved.add(recordStore.saveRecord(baseRec.toBuilder().setRecId(baseRec.getRecId() + 4).setOtherId(baseRec.getOtherId() - 1).build()));
        saved.add(recordStore.saveRecord(baseRec.toBuilder().setRecId(baseRec.getRecId() + 5).setOtherId(baseRec.getOtherId() - 1).build()));
        return saved;
    }

    private void assertAllPresent(final List<FDBStoredRecord<Message>> records) {
        records.forEach(rec ->
                assertNotNull(recordStore.loadRecord(rec.getPrimaryKey()), () -> ("record with primary key " + rec.getPrimaryKey() + " should be present")));
    }

    private void assertAllAbsent(final List<FDBStoredRecord<Message>> records) {
        records.forEach(rec ->
                assertNull(recordStore.loadRecord(rec.getPrimaryKey()), () -> ("record with primary key " + rec.getPrimaryKey() + " should be absent")));
    }

    private void assertDeleteRecordsWhereFails(@Nullable String typeName, @Nullable QueryComponent component, @Nonnull String indexName) {
        Query.InvalidExpressionException err = assertThrows(Query.InvalidExpressionException.class, () -> {
            if (typeName == null) {
                recordStore.deleteRecordsWhere(component);
            } else {
                recordStore.deleteRecordsWhere(typeName, component);
            }
        });
        assertThat(err.getMessage(), containsString("deleteRecordsWhere not supported by index " + indexName));
    }

}
