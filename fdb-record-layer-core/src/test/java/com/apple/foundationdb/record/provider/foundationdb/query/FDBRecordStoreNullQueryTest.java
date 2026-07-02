/*
 * FDBRecordStoreNullQueryTest.java
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

import com.apple.foundationdb.record.ProtoVersionSupplier;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsNulls2Proto;
import com.apple.foundationdb.record.TestRecordsNulls3Proto;
import com.apple.foundationdb.record.TestRecordsNulls3ExplicitProto;
import com.apple.foundationdb.record.TestRecordsNullsEditionsProto;
import com.apple.foundationdb.record.TestRecordsTupleFieldsProto;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.match.PlanMatchers;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests of alternatives for dealing with null values in queries, in particular for Protobuf 3 vs. 2.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreNullQueryTest extends FDBRecordStoreQueryTestBase {

    protected static boolean isProto3() {
        return ProtoVersionSupplier.getProtoVersion() == 3;
    }

    protected static RecordMetaData proto2MetaData() {
        return RecordMetaData.newBuilder().setRecords(TestRecordsNulls2Proto.getDescriptor()).getRecordMetaData();
    }

    protected static RecordMetaData proto3MetaData() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsNulls3Proto.getDescriptor());
        metaData.addIndex("MyNullRecord", "int_value");
        metaData.addIndex("MyNullRecord", "string_value");
        return metaData.getRecordMetaData();
    }

    protected static RecordMetaData proto3ScalarNotNullMetaData() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsNulls3Proto.getDescriptor());
        metaData.addIndex("MyNullRecord", "MyNullRecord$int_value", Key.Expressions.field("int_value", KeyExpression.FanType.None,
                Key.Evaluated.NullStandin.NOT_NULL));
        metaData.addIndex("MyNullRecord", "MyNullRecord$string_value", Key.Expressions.field("string_value", KeyExpression.FanType.None,
                Key.Evaluated.NullStandin.NOT_NULL));
        return metaData.getRecordMetaData();
    }

    protected static RecordMetaData proto3NestedMetaData() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsNulls3Proto.getDescriptor());
        metaData.addIndex("MyNullRecord", "MyNullRecord$int_value", Key.Expressions.field("nullable_int_value")
                .nest(Key.Expressions.field("value", KeyExpression.FanType.None, Key.Evaluated.NullStandin.NOT_NULL)));
        metaData.addIndex("MyNullRecord", "MyNullRecord$string_value", Key.Expressions.field("nullable_string_value")
                .nest(Key.Expressions.field("value", KeyExpression.FanType.None,
                        Key.Evaluated.NullStandin.NOT_NULL)));
        return metaData.getRecordMetaData();
    }

    protected static RecordMetaData proto3ExplicitMetaData() {
        return RecordMetaData.newBuilder().setRecords(TestRecordsNulls3ExplicitProto.getDescriptor()).getRecordMetaData();
    }

    protected static RecordMetaData protoEditionsMetaData() {
        return RecordMetaData.newBuilder().setRecords(TestRecordsNullsEditionsProto.getDescriptor()).getRecordMetaData();
    }

    @FunctionalInterface
    interface RecordBuilder<M extends Message> {
        M build(@Nonnull String name, @Nullable Integer intValue, @Nullable String stringValue);
    }

    protected static TestRecordsNulls2Proto.MyNullRecord buildRecord2(@Nonnull String name, @Nullable Integer intValue, @Nullable String stringValue) {
        TestRecordsNulls2Proto.MyNullRecord.Builder builder = TestRecordsNulls2Proto.MyNullRecord.newBuilder();
        builder.setName(name);
        if (intValue != null) {
            builder.setIntValue(intValue);
        }
        if (stringValue != null) {
            builder.setStringValue(stringValue);
        }
        return builder.build();
    }

    protected static TestRecordsNulls3Proto.MyNullRecord buildRecord3(@Nonnull String name, @Nullable Integer intValue, @Nullable String stringValue) {
        TestRecordsNulls3Proto.MyNullRecord.Builder builder = TestRecordsNulls3Proto.MyNullRecord.newBuilder();
        builder.setName(name);
        if (intValue != null) {
            builder.setIntValue(intValue);
        }
        if (stringValue != null) {
            builder.setStringValue(stringValue);
        }
        return builder.build();
    }

    protected static TestRecordsNulls3Proto.MyNullRecord buildRecord3Nested(@Nonnull String name, @Nullable Integer intValue, @Nullable String stringValue) {
        TestRecordsNulls3Proto.MyNullRecord.Builder builder = TestRecordsNulls3Proto.MyNullRecord.newBuilder();
        builder.setName(name);
        if (intValue != null) {
            builder.getNullableIntValueBuilder().setValue(intValue);
        }
        if (stringValue != null) {
            builder.getNullableStringValueBuilder().setValue(stringValue);
        }
        return builder.build();
    }

    protected static DynamicMessage buildRecord2Dynamic(@Nonnull String name, @Nullable Integer intValue, @Nullable String stringValue) {
        final Descriptors.Descriptor descriptor = TestRecordsNulls2Proto.MyNullRecord.getDescriptor();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        builder.setField(descriptor.findFieldByNumber(TestRecordsNulls3Proto.MyNullRecord.NAME_FIELD_NUMBER), name);
        if (intValue != null) {
            builder.setField(descriptor.findFieldByNumber(TestRecordsNulls3Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER), intValue);
        }
        if (stringValue != null) {
            builder.setField(descriptor.findFieldByNumber(TestRecordsNulls3Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER), stringValue);
        }
        return builder.build();
    }

    protected static DynamicMessage buildRecord3Dynamic(@Nonnull String name, @Nullable Integer intValue, @Nullable String stringValue) {
        final Descriptors.Descriptor descriptor = TestRecordsNulls3Proto.MyNullRecord.getDescriptor();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        builder.setField(descriptor.findFieldByNumber(TestRecordsNulls3Proto.MyNullRecord.NAME_FIELD_NUMBER), name);
        if (intValue != null) {
            builder.setField(descriptor.findFieldByNumber(TestRecordsNulls3Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER), intValue);
        }
        if (stringValue != null) {
            builder.setField(descriptor.findFieldByNumber(TestRecordsNulls3Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER), stringValue);
        }
        return builder.build();
    }

    protected static TestRecordsNulls3ExplicitProto.MyNullRecord buildRecord3Explicit(@Nonnull String name, @Nullable Integer intValue, @Nullable String stringValue) {
        TestRecordsNulls3ExplicitProto.MyNullRecord.Builder builder = TestRecordsNulls3ExplicitProto.MyNullRecord.newBuilder();
        builder.setName(name);
        if (intValue != null) {
            builder.setIntValue(intValue);
        }
        if (stringValue != null) {
            builder.setStringValue(stringValue);
        }
        return builder.build();
    }

    protected static TestRecordsNullsEditionsProto.MyNullRecord buildRecordEditions(@Nonnull String name, @Nullable Integer intValue, @Nullable String stringValue) {
        TestRecordsNullsEditionsProto.MyNullRecord.Builder builder = TestRecordsNullsEditionsProto.MyNullRecord.newBuilder();
        builder.setName(name);
        if (intValue != null) {
            builder.setIntValue(intValue);
        }
        if (stringValue != null) {
            builder.setStringValue(stringValue);
        }
        return builder.build();
    }

    protected <M extends Message> void saveTestRecords(@Nonnull RecordBuilder<M> builder) {
        recordStore.saveRecord(builder.build("empty", null, null));
        recordStore.saveRecord(builder.build("default", 0, ""));
        recordStore.saveRecord(builder.build("one", 1, "A"));
        recordStore.saveRecord(builder.build("two", 2, "B"));
        recordStore.saveRecord(builder.build("minus", -1, "!"));
    }

    protected List<String> executeQuery(@Nonnull RecordQuery query) {
        return recordStore.executeQuery(planQuery(query))
                .map(r -> r.getPrimaryKey().getString(0))
                .asList().join();
    }

    // Proto 2 or syntax = proto2 in Proto 3 generates hasXXX for scalar fields and so reliably distinguishes
    // missing values.
    @DualPlannerTest
    public void testProto2() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, proto2MetaData());
            saveTestRecords(FDBRecordStoreNullQueryTest::buildRecord2);

            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("int_value").equalsValue(2))
                            .build()),
                    is(Collections.singletonList("two")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").equalsValue("B"))
                            .build()),
                    is(Collections.singletonList("two")));

            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setFilter(Query.field("int_value").isNull())
                    .build()),
                    is(Collections.singletonList("empty")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setFilter(Query.field("int_value").equalsValue(0))
                    .build()),
                    is(Collections.singletonList("default")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").isNull())
                            .build()),
                    is(Collections.singletonList("empty")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").equalsValue(""))
                            .build()),
                    is(Collections.singletonList("default")));

            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setSort(Key.Expressions.field("int_value"))
                    .build()),
                    is(Arrays.asList("empty", "minus", "default", "one", "two")));
        }
    }

    public static Stream<Arguments> testProto3Params() {
        return Stream.of(false, true)
                .flatMap(dynamic -> Stream.of(false, true).map(notNull -> Arguments.of(
                        (dynamic ? "dynamic" : "generated") + (notNull ? " NOT NULL" : ""), dynamic, notNull)));
    }

    // Proto3 scalar fields do not distinguish missing from default value, even if set explicitly.
    // They both return false for hasField, which means they look like null by default.
    // Key.Evaluated.NullStandin.NOT_NULL gives consistent behavior for Proto 2 and Proto 3.
    @ParameterizedTest(name = "testProto3({0})")
    @MethodSource("testProto3Params")
    public void testProto3(String testName, boolean buildDynamic, boolean scalarFieldsNotNull) {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, scalarFieldsNotNull ? proto3ScalarNotNullMetaData() : proto3MetaData());
            saveTestRecords(buildDynamic ?
                            FDBRecordStoreNullQueryTest::buildRecord3Dynamic :
                            FDBRecordStoreNullQueryTest::buildRecord3);

            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("int_value").equalsValue(2))
                            .build()),
                    is(Collections.singletonList("two")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").equalsValue("B"))
                            .build()),
                    is(Collections.singletonList("two")));

            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setFilter(Query.field("int_value").isNull())
                    .build()),
                    is(scalarFieldsNotNull ?
                                Collections.emptyList() :
                                isProto3() ?
                                Arrays.asList("default", "empty") :
                                Collections.singletonList("empty")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setFilter(Query.field("int_value").equalsValue(0))
                    .build()),
                    is(scalarFieldsNotNull ?
                                Arrays.asList("default", "empty") :
                                isProto3() ?
                                Collections.emptyList() :
                                Collections.singletonList("default")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").isNull())
                            .build()),
                    is(scalarFieldsNotNull ?
                                Collections.emptyList() :
                                isProto3() ?
                                Arrays.asList("default", "empty") :
                                Collections.singletonList("empty")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").equalsValue(""))
                            .build()),
                    is(scalarFieldsNotNull ?
                                Arrays.asList("default", "empty") :
                                isProto3() ?
                                Collections.emptyList() :
                                Collections.singletonList("default")));

            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setSort(Key.Expressions.field("int_value"))
                    .build()),
                    is(scalarFieldsNotNull ?
                                Arrays.asList("minus", "default", "empty", "one", "two") :
                                isProto3() ?
                                Arrays.asList("default", "empty", "minus", "one", "two") :
                                Arrays.asList("empty", "minus", "default", "one", "two")));
        }
    }

    // A nested message has reliable hasField semantics in both Proto 2 and Proto 3.
    // It can hold a nullable scalar value at the cost of two extra bytes in the non-null case.
    // TODO: An alternative KeyExpression for an indicator field would use two bytes in the null case, which is
    //  a better space tradeoff, but a more complicated data model for the user to maintain.
    @Test
    public void testProto3Nested() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, proto3NestedMetaData());
            saveTestRecords(FDBRecordStoreNullQueryTest::buildRecord3Nested);

            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("nullable_int_value").matches(Query.field("value").equalsValue(2)))
                            .build()),
                    is(Collections.singletonList("two")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("nullable_string_value").matches(Query.field("value").equalsValue("B")))
                            .build()),
                    is(Collections.singletonList("two")));

            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("nullable_int_value").matches(Query.field("value").isNull()))
                            .build()),
                    is(Collections.singletonList("empty")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("nullable_int_value").matches(Query.field("value").equalsValue(0)))
                            .build()),
                    is(Collections.singletonList("default")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("nullable_string_value").matches(Query.field("value").isNull()))
                            .build()),
                    is(Collections.singletonList("empty")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("nullable_string_value").matches(Query.field("value").equalsValue("")))
                            .build()),
                    is(Collections.singletonList("default")));

            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setSort(Key.Expressions.field("nullable_int_value").nest(Key.Expressions.field("value")))
                            .build()),
                    is(Arrays.asList("empty", "minus", "default", "one", "two")));
        }
    }

    // Explicit presence ("optional") correctly distguishes as well (results same as proto2).
    @Test
    public void testProto3Explicit() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, proto3ExplicitMetaData());
            saveTestRecords(FDBRecordStoreNullQueryTest::buildRecord3Explicit);

            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("int_value").equalsValue(2))
                            .build()),
                    is(Collections.singletonList("two")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").equalsValue("B"))
                            .build()),
                    is(Collections.singletonList("two")));

            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setFilter(Query.field("int_value").isNull())
                    .build()),
                    is(Collections.singletonList("empty")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setFilter(Query.field("int_value").equalsValue(0))
                    .build()),
                    is(Collections.singletonList("default")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").isNull())
                            .build()),
                    is(Collections.singletonList("empty")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").equalsValue(""))
                            .build()),
                    is(Collections.singletonList("default")));

            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setSort(Key.Expressions.field("int_value"))
                    .build()),
                    is(Arrays.asList("empty", "minus", "default", "one", "two")));
        }
    }

    // Explicit presence is now the default.
    @Test
    public void testProtoEditions() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, protoEditionsMetaData());
            saveTestRecords(FDBRecordStoreNullQueryTest::buildRecordEditions);

            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("int_value").equalsValue(2))
                            .build()),
                    is(Collections.singletonList("two")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").equalsValue("B"))
                            .build()),
                    is(Collections.singletonList("two")));

            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setFilter(Query.field("int_value").isNull())
                    .build()),
                    is(Collections.singletonList("empty")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setFilter(Query.field("int_value").equalsValue(0))
                    .build()),
                    is(Collections.singletonList("default")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").isNull())
                            .build()),
                    is(Collections.singletonList("empty")));
            assertThat(executeQuery(RecordQuery.newBuilder()
                            .setRecordType("MyNullRecord")
                            .setFilter(Query.field("string_value").equalsValue(""))
                            .build()),
                    is(Collections.singletonList("default")));

            assertThat(executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MyNullRecord")
                    .setSort(Key.Expressions.field("int_value"))
                    .build()),
                    is(Arrays.asList("empty", "minus", "default", "one", "two")));
        }
    }

    @Test
    public void testCompareSerialization() throws Exception {
        final TestRecordsNulls2Proto.MyNullRecord emptyProto2 = buildRecord2("record", null, null);
        final TestRecordsNulls3Proto.MyNullRecord emptyProto3 = buildRecord3("record", null, null);
        final DynamicMessage emptyDynamic2 = buildRecord2Dynamic("record", null, null);
        final DynamicMessage emptyDynamic3 = buildRecord3Dynamic("record", null, null);

        final TestRecordsNulls2Proto.MyNullRecord defaultProto2 = buildRecord2("record", 0, "");
        final TestRecordsNulls3Proto.MyNullRecord defaultProto3 = buildRecord3("record", 0, "");
        final DynamicMessage defaultDynamic2 = buildRecord2Dynamic("record", 0, "");
        final DynamicMessage defaultDynamic3 = buildRecord3Dynamic("record", 0, "");

        final TestRecordsNulls2Proto.MyNullRecord oneProto2 = buildRecord2("record", 1, "A");
        final TestRecordsNulls3Proto.MyNullRecord oneProto3 = buildRecord3("record", 1, "A");
        final DynamicMessage oneDynamic2 = buildRecord2Dynamic("record", 1, "A");
        final DynamicMessage oneDynamic3 = buildRecord3Dynamic("record", 1, "A");

        checkHasField(emptyProto2, emptyProto3, emptyDynamic2, emptyDynamic3,
                defaultProto2, defaultProto3, defaultDynamic2, defaultDynamic3,
                oneProto2, oneProto3, oneDynamic2, oneDynamic3);

        final byte[] emptySerialized = emptyProto2.toByteArray();
        assertThat("empty serialized from proto3 should be the same as proto2", emptyProto3.toByteArray(), equalTo(emptySerialized));
        assertThat("empty serialized from dynamic2 should be the same as proto2", emptyDynamic2.toByteArray(), equalTo(emptySerialized));
        assertThat("empty serialized from dynamic3 should be the same as proto2", emptyDynamic3.toByteArray(), equalTo(emptySerialized));

        final byte[] defaultSerialized = defaultProto2.toByteArray();
        assertThat("empty and default serialized by proto2 should differ", defaultSerialized, not(equalTo(emptySerialized)));
        assertThat("default serialized from dynamic2 should be the same as proto2", defaultDynamic2.toByteArray(), equalTo(defaultSerialized));
        if (isProto3()) {
            // Fields set to default cannot be distinguished from cleared, so not serialized.
            assertThat("default serialized from proto3 should be the same as proto2 empty", defaultProto3.toByteArray(), equalTo(emptySerialized));
            assertThat("default serialized from dynamic3 should be the same as proto2 empty", defaultDynamic3.toByteArray(), equalTo(emptySerialized));
        } else {
            assertThat("default serialized from proto3 should be the same as proto2", defaultProto3.toByteArray(), equalTo(defaultSerialized));
            assertThat("default serialized from dynamic3 should be the same as proto2", defaultDynamic3.toByteArray(), equalTo(defaultSerialized));
        }

        final byte[] oneSerialized = oneProto2.toByteArray();
        assertThat("empty and one serialized by proto2 should differ", oneSerialized, not(equalTo(emptySerialized)));
        assertThat("one serialized from proto3 should be the same as proto2", oneProto3.toByteArray(), equalTo(oneSerialized));
        assertThat("one serialized from dynamic2 should be the same as proto2", oneDynamic2.toByteArray(), equalTo(oneSerialized));
        assertThat("one serialized from dynamic3 should be the same as proto2", oneDynamic3.toByteArray(), equalTo(oneSerialized));

        final TestRecordsNulls2Proto.MyNullRecord emptyProto2x = TestRecordsNulls2Proto.MyNullRecord.parseFrom(emptySerialized);
        final TestRecordsNulls3Proto.MyNullRecord emptyProto3x = TestRecordsNulls3Proto.MyNullRecord.parseFrom(emptySerialized);
        final DynamicMessage emptyDynamic2x = DynamicMessage.parseFrom(TestRecordsNulls2Proto.MyNullRecord.getDescriptor(), emptySerialized);
        final DynamicMessage emptyDynamic3x = DynamicMessage.parseFrom(TestRecordsNulls3Proto.MyNullRecord.getDescriptor(), emptySerialized);

        final TestRecordsNulls2Proto.MyNullRecord defaultProto2x = TestRecordsNulls2Proto.MyNullRecord.parseFrom(defaultSerialized);
        final TestRecordsNulls3Proto.MyNullRecord defaultProto3x = TestRecordsNulls3Proto.MyNullRecord.parseFrom(defaultSerialized);
        final DynamicMessage defaultDynamic2x = DynamicMessage.parseFrom(TestRecordsNulls2Proto.MyNullRecord.getDescriptor(), defaultSerialized);
        final DynamicMessage defaultDynamic3x = DynamicMessage.parseFrom(TestRecordsNulls3Proto.MyNullRecord.getDescriptor(), defaultSerialized);

        final TestRecordsNulls2Proto.MyNullRecord oneProto2x = TestRecordsNulls2Proto.MyNullRecord.parseFrom(oneSerialized);
        final TestRecordsNulls3Proto.MyNullRecord oneProto3x = TestRecordsNulls3Proto.MyNullRecord.parseFrom(oneSerialized);
        final DynamicMessage oneDynamic2x = DynamicMessage.parseFrom(TestRecordsNulls2Proto.MyNullRecord.getDescriptor(), oneSerialized);
        final DynamicMessage oneDynamic3x = DynamicMessage.parseFrom(TestRecordsNulls3Proto.MyNullRecord.getDescriptor(), oneSerialized);

        checkHasField(emptyProto2x, emptyProto3x, emptyDynamic2x, emptyDynamic3x,
                defaultProto2x, defaultProto3x, defaultDynamic2x, defaultDynamic3x,
                oneProto2x, oneProto3x, oneDynamic2x, oneDynamic3x);

    }

    private void checkHasField(TestRecordsNulls2Proto.MyNullRecord emptyProto2, TestRecordsNulls3Proto.MyNullRecord emptyProto3, DynamicMessage emptyDynamic2, DynamicMessage emptyDynamic3,
                               TestRecordsNulls2Proto.MyNullRecord defaultProto2, TestRecordsNulls3Proto.MyNullRecord defaultProto3, DynamicMessage defaultDynamic2, DynamicMessage defaultDynamic3,
                               TestRecordsNulls2Proto.MyNullRecord oneProto2, TestRecordsNulls3Proto.MyNullRecord oneProto3, DynamicMessage oneDynamic2, DynamicMessage oneDynamic3) {
        assertThat("!emptyProto2.hasIntValue", !emptyProto2.hasIntValue());
        assertThat("!emptyProto2.hasStringValue", !emptyProto2.hasStringValue());
        assertThat("defaultProto2.hasIntValue", defaultProto2.hasIntValue());
        assertThat("defaultProto2.hasStringValue", defaultProto2.hasStringValue());
        assertThat("oneProto2.hasIntValue", oneProto2.hasIntValue());
        assertThat("oneProto2.hasStringValue", oneProto2.hasStringValue());

        assertThat("!emptyProto2.hasField(int_field)", !emptyProto2.hasField(emptyProto2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
        assertThat("!emptyProto2.hasField(string_field)", !emptyProto2.hasField(emptyProto2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
        assertThat("defaultProto2.hasField(int_field)", defaultProto2.hasField(emptyProto2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
        assertThat("defaultProto2.hasField(string_field)", defaultProto2.hasField(emptyProto2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
        assertThat("oneProto2.hasField(int_field)", oneProto2.hasField(oneProto2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
        assertThat("oneProto2.hasField(string_field)", oneProto2.hasField(oneProto2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));

        assertThat("!emptyProto3.hasField(int_field)", !emptyProto3.hasField(emptyProto3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
        assertThat("!emptyProto3.hasField(string_field)", !emptyProto3.hasField(emptyProto3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
        if (isProto3()) {
            // Setting to the default value is like clearing.
            assertThat("!defaultProto3.hasField(int_field)", !defaultProto3.hasField(emptyProto3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
            assertThat("!defaultProto3.hasField(string_field)", !defaultProto3.hasField(emptyProto3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
        } else {
            assertThat("defaultProto3.hasField(int_field)", defaultProto3.hasField(emptyProto3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
            assertThat("defaultProto3.hasField(string_field)", defaultProto3.hasField(emptyProto3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
        }
        assertThat("oneProto3.hasField(int_field)", oneProto3.hasField(oneProto3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
        assertThat("oneProto3.hasField(string_field)", oneProto3.hasField(oneProto3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));

        assertThat("!emptyDynamic2.hasField(int_field)", !emptyDynamic2.hasField(emptyDynamic2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
        assertThat("!emptyDynamic2.hasField(string_field)", !emptyDynamic2.hasField(emptyDynamic2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
        assertThat("defaultDynamic2.hasField(int_field)", defaultDynamic2.hasField(emptyDynamic2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
        assertThat("defaultDynamic2.hasField(string_field)", defaultDynamic2.hasField(emptyDynamic2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
        assertThat("oneDynamic2.hasField(int_field)", oneDynamic2.hasField(oneDynamic2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
        assertThat("oneDynamic2.hasField(string_field)", oneDynamic2.hasField(oneDynamic2.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));

        assertThat("!emptyDynamic3.hasField(int_field)", !emptyDynamic3.hasField(emptyDynamic3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
        assertThat("!emptyDynamic3.hasField(string_field)", !emptyDynamic3.hasField(emptyDynamic3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
        if (isProto3()) {
            // Setting to the default value is like clearing.
            assertThat("!defaultDynamic3.hasField(int_field)", !defaultDynamic3.hasField(emptyDynamic3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
            assertThat("!defaultDynamic3.hasField(string_field)", !defaultDynamic3.hasField(emptyDynamic3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
        } else {
            assertThat("defaultDynamic3.hasField(int_field)", defaultDynamic3.hasField(emptyDynamic3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
            assertThat("defaultDynamic3.hasField(string_field)", defaultDynamic3.hasField(emptyDynamic3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
        }
        assertThat("oneDynamic3.hasField(int_field)", oneDynamic3.hasField(oneDynamic3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.INT_VALUE_FIELD_NUMBER)));
        assertThat("oneDynamic3.hasField(string_field)", oneDynamic3.hasField(oneDynamic3.getDescriptorForType().findFieldByNumber(TestRecordsNulls2Proto.MyNullRecord.STRING_VALUE_FIELD_NUMBER)));
    }

    protected static RecordMetaData tupleFieldsMetaData() {
        return RecordMetaData.build(TestRecordsTupleFieldsProto.getDescriptor());
    }

    @Test
    public void testTupleFields() throws Exception {
        final UUID nullId = UUID.randomUUID();
        final UUID emptyId = UUID.randomUUID();
        final UUID defaultId = UUID.randomUUID();
        final UUID otherId = UUID.randomUUID();

        final TestRecordsTupleFieldsProto.MyFieldsRecord nullRecord = TestRecordsTupleFieldsProto.MyFieldsRecord.newBuilder()
                .setUuid(TupleFieldsHelper.toProto(nullId))
                .build();
        final TestRecordsTupleFieldsProto.MyFieldsRecord emptyRecord = TestRecordsTupleFieldsProto.MyFieldsRecord.newBuilder()
                .setUuid(TupleFieldsHelper.toProto(emptyId))
                .setFdouble(TupleFieldsProto.NullableDouble.getDefaultInstance())
                .setFfloat(TupleFieldsProto.NullableFloat.getDefaultInstance())
                .setFint32(TupleFieldsProto.NullableInt32.getDefaultInstance())
                .setFint64(TupleFieldsProto.NullableInt64.getDefaultInstance())
                .setFbool(TupleFieldsProto.NullableBool.getDefaultInstance())
                .setFstring(TupleFieldsProto.NullableString.getDefaultInstance())
                .setFbytes(TupleFieldsProto.NullableBytes.getDefaultInstance())
                .build();
        final TestRecordsTupleFieldsProto.MyFieldsRecord defaultRecord = TestRecordsTupleFieldsProto.MyFieldsRecord.newBuilder()
                .setUuid(TupleFieldsHelper.toProto(defaultId))
                .setFdouble(TupleFieldsHelper.toProto(0.0))
                .setFfloat(TupleFieldsHelper.toProto(0.0f))
                .setFint32(TupleFieldsHelper.toProto(0))
                .setFint64(TupleFieldsHelper.toProto(0L))
                .setFbool(TupleFieldsHelper.toProto(false))
                .setFstring(TupleFieldsHelper.toProto(""))
                .setFbytes(TupleFieldsHelper.toProto(ByteString.EMPTY))
                .build();
        final TestRecordsTupleFieldsProto.MyFieldsRecord otherRecord = TestRecordsTupleFieldsProto.MyFieldsRecord.newBuilder()
                .setUuid(TupleFieldsHelper.toProto(otherId))
                .setFdouble(TupleFieldsHelper.toProto(1.0))
                .setFfloat(TupleFieldsHelper.toProto(1.0f))
                .setFint32(TupleFieldsHelper.toProto(1))
                .setFint64(TupleFieldsHelper.toProto(1L))
                .setFbool(TupleFieldsHelper.toProto(true))
                .setFstring(TupleFieldsHelper.toProto(" "))
                .setFbytes(TupleFieldsHelper.toProto(ByteString.copyFrom(" ", "UTF-8")))
                .build();
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, tupleFieldsMetaData());

            recordStore.saveRecord(nullRecord);
            recordStore.saveRecord(emptyRecord);
            recordStore.saveRecord(defaultRecord);
            recordStore.saveRecord(otherRecord);
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, tupleFieldsMetaData());

            assertEquals(nullId, fieldsRecordId(recordStore.loadRecord(Tuple.from(nullId))));
            assertEquals(otherId, fieldsRecordId(recordStore.loadRecord(Tuple.from(otherId))));

            assertEquals(Collections.singleton(nullId), fieldsRecordQuery(context, Query.field("fdouble").isNull()));
            assertEquals(Collections.singleton(nullId), fieldsRecordQuery(context, Query.field("ffloat").isNull()));
            assertEquals(Collections.singleton(nullId), fieldsRecordQuery(context, Query.field("fint32").isNull()));
            assertEquals(Collections.singleton(nullId), fieldsRecordQuery(context, Query.field("fint64").isNull()));
            assertEquals(Collections.singleton(nullId), fieldsRecordQuery(context, Query.field("fbool").isNull()));
            assertEquals(Collections.singleton(nullId), fieldsRecordQuery(context, Query.field("fstring").isNull()));
            assertEquals(Collections.singleton(nullId), fieldsRecordQuery(context, Query.field("fbytes").isNull()));

            assertEquals(ImmutableSet.of(emptyId, defaultId), fieldsRecordQuery(context, Query.field("fdouble").equalsValue(0.0)));
            assertEquals(ImmutableSet.of(emptyId, defaultId), fieldsRecordQuery(context, Query.field("ffloat").equalsValue(0.0f)));
            assertEquals(ImmutableSet.of(emptyId, defaultId), fieldsRecordQuery(context, Query.field("fint32").equalsValue(0)));
            assertEquals(ImmutableSet.of(emptyId, defaultId), fieldsRecordQuery(context, Query.field("fint64").equalsValue(0L)));
            assertEquals(ImmutableSet.of(emptyId, defaultId), fieldsRecordQuery(context, Query.field("fbool").equalsValue(false)));
            assertEquals(ImmutableSet.of(emptyId, defaultId), fieldsRecordQuery(context, Query.field("fstring").equalsValue("")));
            assertEquals(ImmutableSet.of(emptyId, defaultId), fieldsRecordQuery(context, Query.field("fbytes").equalsValue(ByteString.EMPTY)));

            assertEquals(Collections.singleton(otherId), fieldsRecordQuery(context, Query.field("fdouble").equalsValue(1.0)));
            assertEquals(Collections.singleton(otherId), fieldsRecordQuery(context, Query.field("ffloat").equalsValue(1.0f)));
            assertEquals(Collections.singleton(otherId), fieldsRecordQuery(context, Query.field("fint32").equalsValue(1)));
            assertEquals(Collections.singleton(otherId), fieldsRecordQuery(context, Query.field("fint64").equalsValue(1L)));
            assertEquals(Collections.singleton(otherId), fieldsRecordQuery(context, Query.field("fbool").equalsValue(true)));
            assertEquals(Collections.singleton(otherId), fieldsRecordQuery(context, Query.field("fstring").equalsValue(" ")));
            assertEquals(Collections.singleton(otherId), fieldsRecordQuery(context, Query.field("fbytes").equalsValue(ByteString.copyFrom(" ", "UTF-8"))));

            {
                final RecordQueryPlan coveringPlan = planQuery(RecordQuery.newBuilder()
                        .setRecordType("MyFieldsRecord")
                        .setFilter(Query.field("fint64").greaterThan(0L))
                        .setRequiredResults(Stream.of("uuid", "fint64").map(Key.Expressions::field).collect(Collectors.toList()))
                        .build());
                assertThat(coveringPlan, PlanMatchers.coveringIndexScan(PlanMatchers.indexScan(PlanMatchers.indexName("MyFieldsRecord$fint64"))));
                final List<Pair<UUID, Long>> results = recordStore.executeQuery(coveringPlan).map(rec -> {
                    final TestRecordsTupleFieldsProto.MyFieldsRecord myrec = TestRecordsTupleFieldsProto.MyFieldsRecord.newBuilder().mergeFrom(rec.getRecord()).build();
                    return Pair.of(TupleFieldsHelper.fromProto(myrec.getUuid()), TupleFieldsHelper.fromProto(myrec.getFint64()));
                }).asList().join();
                assertEquals(Collections.singletonList(Pair.of(otherId, 1L)), results);
            }

            context.commit();
        }

    }

    protected Set<UUID> fieldsRecordQuery(@Nonnull FDBRecordContext context, @Nonnull QueryComponent filter) {
        final RecordQueryPlan plan = planQuery(RecordQuery.newBuilder().setRecordType("MyFieldsRecord").setFilter(filter).build());
        return new HashSet<>(recordStore.executeQuery(plan).map(this::fieldsRecordId).asList().join());
    }

    protected UUID fieldsRecordId(@Nullable FDBRecord<Message> record) {
        if (record == null) {
            return null;
        } else {
            final TestRecordsTupleFieldsProto.MyFieldsRecord fieldsRecord = TestRecordsTupleFieldsProto.MyFieldsRecord.newBuilder().mergeFrom(record.getRecord()).build();
            return TupleFieldsHelper.fromProto(fieldsRecord.getUuid());
        }
    }

}
