/*
 * FDBRecordStoreQueryTestBase.java
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
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.TestRecords5Proto;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.TestRecordsTupleFieldsProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanResult;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A base class for common infrastructure used by tests in {@link com.apple.foundationdb.record.provider.foundationdb.query}.
 */
public abstract class FDBRecordStoreQueryTestBase extends FDBRecordStoreTestBase {
    protected List<TestRecords1Proto.MySimpleRecord> setupSimpleRecordStore(RecordMetaDataHook recordMetaDataHook,
                                                                            BiConsumer<Integer, TestRecords1Proto.MySimpleRecord.Builder> buildRecord) {
        ImmutableList.Builder<TestRecords1Proto.MySimpleRecord> listBuilder = ImmutableList.builder();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, recordMetaDataHook);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recordBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                buildRecord.accept(i, recordBuilder);
                TestRecords1Proto.MySimpleRecord rec = recordBuilder.build();
                recordStore.saveRecord(rec);
                listBuilder.add(rec);
            }
            commit(context);
        }
        return listBuilder.build();
    }

    protected int querySimpleRecordStore(RecordMetaDataHook recordMetaDataHook, RecordQueryPlan plan,
                                         Supplier<EvaluationContext> contextSupplier,
                                         TestHelpers.DangerousConsumer<TestRecords1Proto.MySimpleRecord> checkRecord)
            throws Exception {
        return querySimpleRecordStore(recordMetaDataHook, plan, contextSupplier, checkRecord, context -> { });
    }

    protected int querySimpleRecordStore(RecordMetaDataHook recordMetaDataHook, RecordQueryPlan plan,
                                         Supplier<EvaluationContext> contextSupplier,
                                         TestHelpers.DangerousConsumer<TestRecords1Proto.MySimpleRecord> checkRecord,
                                         TestHelpers.DangerousConsumer<FDBRecordContext> checkDiscarded)
            throws Exception {
        return querySimpleRecordStore(recordMetaDataHook, plan, contextSupplier, checkRecord, null, false, checkDiscarded);
    }

    protected int querySimpleRecordStore(RecordMetaDataHook recordMetaDataHook, RecordQueryPlan plan,
                                         Supplier<EvaluationContext> contextSupplier,
                                         TestHelpers.DangerousConsumer<TestRecords1Proto.MySimpleRecord> checkRecord,
                                         @Nullable Function<TestRecords1Proto.MySimpleRecord, Tuple> sortKey,
                                         boolean reverse,
                                         TestHelpers.DangerousConsumer<FDBRecordContext> checkDiscarded)
            throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, recordMetaDataHook);
            int i = 0;
            @Nullable Tuple sortValue = null;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = plan.execute(recordStore, contextSupplier.get()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord myrec = TestRecords1Proto.MySimpleRecord.newBuilder()
                            .mergeFrom(rec.getRecord())
                            .build();
                    checkRecord.accept(myrec);
                    if (sortKey != null) {
                        Tuple nextSortValue = sortKey.apply(myrec);
                        if (sortValue != null) {
                            assertThat(nextSortValue, reverse ? lessThanOrEqualTo(sortValue) : greaterThanOrEqualTo(sortValue));
                        }
                        sortValue = nextSortValue;
                    }
                    i++;
                }
            }
            checkDiscarded.accept(context);
            clearStoreCounter(context); // TODO a hack until this gets refactored properly
            return i;
        }
    }

    /**
     * A query execution utility that can handle continuations. This is very similar to the above {@link #querySimpleRecordStore}
     * with the additional support for {@link ExecuteProperties} and continuation.
     * This method returns the last result encountered. In the case where the row limit was encountered, this would be the one
     * result that contains the continuation that should be used on the next call.
     * @param recordMetaDataHook Metadata hook to invoke while opening store
     * @param plan the plan to execute
     * @param contextSupplier provider method to get execution context
     * @param continuation execution continuation
     * @param executeProperties execution properties to pass into the execute method
     * @param checkNumRecords Consumer that verifies correct number of records returned
     * @param checkRecord Consumer that asserts every record retrieved
     * @param checkDiscarded Consumer that asserts the number of discarded records
     * @return the last result from the cursor
     * @throws Throwable any thrown exception, or its cause if the exception is a {@link ExecutionException}
     */
    protected RecordCursorResult<FDBQueriedRecord<Message>> querySimpleRecordStoreWithContinuation(@Nonnull RecordMetaDataHook recordMetaDataHook,
                                                                                                   @Nonnull RecordQueryPlan plan,
                                                                                                   @Nonnull Supplier<EvaluationContext> contextSupplier,
                                                                                                   @Nullable byte[] continuation,
                                                                                                   @Nonnull ExecuteProperties executeProperties,
                                                                                                   @Nonnull Consumer<Integer> checkNumRecords,
                                                                                                   @Nonnull Consumer<TestRecords1Proto.MySimpleRecord.Builder> checkRecord,
                                                                                                   @Nonnull Consumer<FDBRecordContext> checkDiscarded)
            throws Throwable {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, recordMetaDataHook);
            AtomicInteger i = new AtomicInteger(0);
            CompletableFuture<RecordCursorResult<FDBQueriedRecord<Message>>> lastResult;
            RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(recordStore, contextSupplier.get(), continuation, executeProperties);
            lastResult = cursor.forEachResult(result -> {
                TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                myrec.mergeFrom(result.get().getRecord());
                checkRecord.accept(myrec);
                i.incrementAndGet();
            });
            lastResult.get();
            checkNumRecords.accept(i.get());
            checkDiscarded.accept(context);
            clearStoreCounter(context); // TODO a hack until this gets refactored properly
            return lastResult.get();
        } catch (ExecutionException ex) {
            throw ex.getCause();
        }
    }

    protected void complexQuerySetup(RecordMetaDataHook hook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValueUnique(1000 - i);
                recBuilder.setNumValue2(i % 3);
                recBuilder.setNumValue3Indexed(i % 5);

                for (int j = 0; j < i % 10; j++) {
                    recBuilder.addRepeater(j);
                }
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
    }

    @Nonnull
    protected List<Map<String, Object>> queryAsMaps(@Nonnull RecordQueryPlan plan, @Nonnull Bindings bindings) {
        final TypeRepository types = TypeRepository.newBuilder()
                .addAllTypes(UsedTypesProperty.evaluate(plan))
                .build();
        final EvaluationContext evaluationContext = EvaluationContext.forBindingsAndTypeRepository(bindings, types);
        try (RecordCursor<QueryResult> resultCursor = plan.executePlan(recordStore, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE)) {
            List<Map<String, Object>> maps = new ArrayList<>();
            for (RecordCursorResult<QueryResult> res = resultCursor.getNext(); res.hasNext(); res = resultCursor.getNext()) {
                Message message = res.get().getMessage();
                Map<String, Object> asMap = message.getAllFields()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(e -> e.getKey().getName(), Map.Entry::getValue));
                maps.add(asMap);
            }
            return maps;
        }
    }

    @Nonnull
    protected static Bindings constantBindings(@Nonnull ConstantObjectValue constant, @Nonnull Object value) {
        return constantBindings(Map.of(constant, value));
    }

    @Nonnull
    protected static Bindings constantBindings(@Nonnull Map<ConstantObjectValue, Object> constantMap) {
        Map<CorrelationIdentifier, ImmutableMap.Builder<String, Object>> byAlias = new HashMap<>();
        for (Map.Entry<ConstantObjectValue, Object> constant :  constantMap.entrySet()) {
            byAlias.computeIfAbsent(constant.getKey().getAlias(), ignore -> ImmutableMap.builder())
                    .put(constant.getKey().getConstantId(), constant.getValue());
        }
        Bindings.Builder bindingsBuilder = Bindings.newBuilder();
        for (Map.Entry<CorrelationIdentifier, ImmutableMap.Builder<String, Object>> aliasEntry : byAlias.entrySet()) {
            bindingsBuilder.set(Bindings.Internal.CONSTANT.bindingName(aliasEntry.getKey().getId()), aliasEntry.getValue().build());
        }
        return bindingsBuilder.build();
    }

    @Nonnull
    protected RecordMetaDataHook complexQuerySetupHook() {
        return metaData -> {
            metaData.addIndex("MySimpleRecord", new Index("multi_index",
                    "str_value_indexed", "num_value_2", "num_value_3_indexed"));
            metaData.addIndex("MySimpleRecord", "repeater$fanout", field("repeater", FanType.FanOut));
        };
    }

    protected void openHierarchicalRecordStore(FDBRecordContext context) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords3Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(globalCountIndex());
        metaDataBuilder.getRecordType("MyHierarchicalRecord").setPrimaryKey(
                concatenateFields("parent_path", "child_name"));
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    protected void openNestedRecordStore(FDBRecordContext context) {
        openNestedRecordStore(context, null);
    }

    protected void openNestedRecordStore(FDBRecordContext context, @Nullable RecordMetaDataHook hook) {
        createOrOpenRecordStore(context, nestedMetaData(hook));
    }

    protected void openNestedWrappedArrayRecordStore(@Nonnull FDBRecordContext context, @Nullable RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords4WrapperProto.getDescriptor());
        metaDataBuilder.addUniversalIndex(globalCountIndex());
        metaDataBuilder.addIndex("RestaurantRecord", "review_rating", field("reviews", FanType.None).nest(field("values", FanType.FanOut).nest("rating")));
        metaDataBuilder.addIndex("RestaurantRecord", "tag", field("tags", FanType.None).nest(field("values", FanType.FanOut).nest(
                concatenateFields("value", "weight"))));
        metaDataBuilder.addIndex("RestaurantRecord", "customers", field("customer", FanType.None).nest(field("values", FanType.FanOut)));
        metaDataBuilder.addIndex("RestaurantRecord", "customers-name", concat(field("customer", FanType.None).nest(field("values", FanType.FanOut)), field("name")));
        metaDataBuilder.addIndex("RestaurantReviewer", "stats$school", field("stats").nest(field("start_date")));
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
        if (hook != null) {
            hook.apply(metaDataBuilder);
        }
    }

    protected RecordMetaData nestedMetaData(@Nullable RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords4Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(globalCountIndex());
        metaDataBuilder.addIndex("RestaurantRecord", "review_rating", field("reviews", FanType.FanOut).nest("rating"));
        metaDataBuilder.addIndex("RestaurantRecord", "tag", field("tags", FanType.FanOut).nest(
                concatenateFields("value", "weight")));
        metaDataBuilder.addIndex("RestaurantRecord", "customers", field("customer", FanType.FanOut));
        metaDataBuilder.addIndex("RestaurantRecord", "customers-name", concat(field("customer", FanType.FanOut), field("name")));
        metaDataBuilder.addIndex("RestaurantReviewer", "stats$school", field("stats").nest(field("start_date")));
        if (hook != null) {
            hook.apply(metaDataBuilder);
        }
        return metaDataBuilder.getRecordMetaData();
    }

    protected void nestedWithAndSetup(@Nullable RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context, hook);

            TestRecords4Proto.RestaurantReviewer reviewer1 = TestRecords4Proto.RestaurantReviewer.newBuilder()
                    .setId(1L)
                    .setName("Person McPersonface")
                    .setEmail("pmp@example.com")
                    .setStats(
                            TestRecords4Proto.ReviewerStats.newBuilder()
                                    .setStartDate(0L)
                                    .setSchoolName("University of Learning")
                                    .setHometown("Home Town")

                    )
                    .setCategory(1)
                    .build();

            TestRecords4Proto.RestaurantReviewer reviewer2 = TestRecords4Proto.RestaurantReviewer.newBuilder()
                    .setId(2L)
                    .setName("Newt A. Robot")
                    .setEmail("newtarobot@example.com")
                    .setStats(
                            TestRecords4Proto.ReviewerStats.newBuilder()
                                    .setStartDate(1066L)
                                    .setSchoolName("Human University")
                                    .setHometown("Real Place")
                    )
                    .setCategory(1)
                    .build();

            recordStore.saveRecord(reviewer1);
            recordStore.saveRecord(reviewer2);

            commit(context);
        }
    }

    protected void openDoublyNestedRecordStore(FDBRecordContext context) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords5Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(globalCountIndex());
        metaDataBuilder.addIndex("CalendarEvent", "alarm_start", field("alarmIndex").nest(
                field("recurrence", FanType.FanOut).nest("start")));
        metaDataBuilder.addIndex("CalendarEvent", "event_start", field("eventIndex").nest(
                field("recurrence", FanType.FanOut).nest("start")));
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    public void openConcatNestedRecordStore(FDBRecordContext context) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords5Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(globalCountIndex());
        metaDataBuilder.addIndex("CalendarEvent", "versions", concat(field("alarmIndex").nest("version"),
                field("eventIndex").nest("version")));
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    protected List<Object> fetchResultValues(RecordQueryPlan plan, final int fieldNumber, Opener opener,
                                             TestHelpers.DangerousConsumer<FDBRecordContext> checkDiscarded) throws Exception {
        return fetchResultValues(plan, opener,
                rec -> rec.getField(rec.getDescriptorForType().findFieldByNumber(fieldNumber)), checkDiscarded);
    }

    protected <T> List<T> fetchResultValues(RecordQueryPlan plan, Opener opener, Function<Message, T> rowHandler) throws Exception {
        return fetchResultValues(plan, opener, rowHandler, context -> { });
    }

    protected List<Object> fetchResultValues(RecordQueryPlan plan, final int fieldNumber, Opener opener) throws Exception {
        return fetchResultValues(plan, fieldNumber, opener, context -> { });
    }

    protected <T> List<T> fetchResultValues(RecordQueryPlan plan, Opener opener, Function<Message, T> rowHandler,
                                            TestHelpers.DangerousConsumer<FDBRecordContext> checkDiscarded) throws Exception {
        try (FDBRecordContext context = openContext()) {
            opener.open(context);
            return fetchResultValues(context, plan, rowHandler, checkDiscarded);
        }
    }

    protected <T> List<T> fetchResultValues(FDBRecordContext context, RecordQueryPlan plan, Function<Message, T> rowHandler,
                                            TestHelpers.DangerousConsumer<FDBRecordContext> checkDiscarded) throws Exception {
        return fetchResultValues(context, plan, rowHandler, checkDiscarded, ExecuteProperties.SERIAL_EXECUTE);
    }

    protected <T> List<T> fetchResultValues(FDBRecordContext context, RecordQueryPlan plan, Function<Message, T> rowHandler,
                                        TestHelpers.DangerousConsumer<FDBRecordContext> checkDiscarded, ExecuteProperties executeProperties) throws Exception {
        final var usedTypes = UsedTypesProperty.evaluate(plan);
        List<T> result = new ArrayList<>();
        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(usedTypes).build());
        try (RecordCursorIterator<QueryResult> cursor = plan.executePlan(recordStore, evaluationContext, null, executeProperties).asIterator()) {
            while (cursor.hasNext()) {
                Message message = Verify.verifyNotNull(cursor.next()).getMessage();
                result.add(rowHandler.apply(message));
            }
        }
        checkDiscarded.accept(context);
        clearStoreCounter(context); // TODO a hack until this gets refactored properly
        return result;
    }

    protected void setupRecordsWithHeader(RecordMetaDataHook recordMetaDataHook,
                                          BiConsumer<Integer, TestRecordsWithHeaderProto.MyRecord.Builder> buildRecord) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, recordMetaDataHook);

            for (int i = 0; i < 100; i++) {
                TestRecordsWithHeaderProto.MyRecord.Builder record = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                buildRecord.accept(i, record);
                recordStore.saveRecord(record.build());
            }
            commit(context);
        }
    }

    protected void queryRecordsWithHeader(RecordMetaDataHook recordMetaDataHook, RecordQueryPlan plan, byte[] continuation, int limit,
                                          TestHelpers.DangerousConsumer<RecordCursor<TestRecordsWithHeaderProto.MyRecord.Builder>> handleResults) throws Exception {
        queryRecordsWithHeader(recordMetaDataHook, plan, continuation, limit, handleResults, context -> { });
    }


    protected void queryRecordsWithHeader(RecordMetaDataHook recordMetaDataHook, RecordQueryPlan plan, byte[] continuation, int limit,
                                          TestHelpers.DangerousConsumer<RecordCursor<TestRecordsWithHeaderProto.MyRecord.Builder>> handleResults,
                                          TestHelpers.DangerousConsumer<FDBRecordContext> checkDiscarded) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, recordMetaDataHook);
            final RecordCursor<TestRecordsWithHeaderProto.MyRecord.Builder> cursor = recordStore.executeQuery(plan, continuation, ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(limit)
                            .build())
                    .map(r -> TestRecordsWithHeaderProto.MyRecord.newBuilder().mergeFrom(r.getRecord()));
            handleResults.accept(cursor);
            checkDiscarded.accept(context);
            clearStoreCounter(context); // TODO a hack until this gets refactored properly
        }
    }

    protected void queryRecordsWithHeader(RecordMetaDataHook recordMetaDataHook, RecordQueryPlan plan,
                                          TestHelpers.DangerousConsumer<RecordCursor<TestRecordsWithHeaderProto.MyRecord.Builder>> handleResults) throws Exception {
        queryRecordsWithHeader(recordMetaDataHook, plan, handleResults, context -> { });
    }

    protected void queryRecordsWithHeader(RecordMetaDataHook recordMetaDataHook, RecordQueryPlan plan,
                                          TestHelpers.DangerousConsumer<RecordCursor<TestRecordsWithHeaderProto.MyRecord.Builder>> handleResults,
                                          TestHelpers.DangerousConsumer<FDBRecordContext> checkDiscarded) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, recordMetaDataHook);
            final RecordCursor<TestRecordsWithHeaderProto.MyRecord.Builder> cursor = recordStore.executeQuery(plan)
                    .map(r -> TestRecordsWithHeaderProto.MyRecord.newBuilder().mergeFrom(r.getRecord()));
            handleResults.accept(cursor);
            checkDiscarded.accept(context);
            clearStoreCounter(context); // TODO a hack until this gets refactored properly
        }
    }


    @Nonnull
    protected RecordMetaDataHook complexPrimaryKeyHook() {
        return complexPrimaryKeyHook(false);
    }

    @Nonnull
    protected RecordMetaDataHook complexPrimaryKeyHook(boolean skipNum3) {
        return metaData -> {
            RecordTypeBuilder recordType = metaData.getRecordType("MySimpleRecord");
            recordType.setPrimaryKey(concatenateFields("str_value_indexed", "num_value_unique"));
            metaData.addIndex(recordType, new Index("str_value_2_index",
                    "str_value_indexed", "num_value_2"));
            if (skipNum3) {
                // Same fields in different order as str_value_3_index.
                metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
            }
            metaData.addIndex(recordType, new Index("str_value_3_index",
                    "str_value_indexed", "num_value_3_indexed"));
        };
    }

    protected void openEnumRecordStore(FDBRecordContext context, RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsEnumProto.getDescriptor());
        if (hook != null) {
            hook.apply(metaData);
        }
        createOrOpenRecordStore(context, metaData.getRecordMetaData());
    }

    protected void setupEnumShapes(RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            openEnumRecordStore(context, hook);
            int n = 0;
            for (TestRecordsEnumProto.MyShapeRecord.Size size : TestRecordsEnumProto.MyShapeRecord.Size.values()) {
                for (TestRecordsEnumProto.MyShapeRecord.Color color : TestRecordsEnumProto.MyShapeRecord.Color.values()) {
                    for (TestRecordsEnumProto.MyShapeRecord.Shape shape : TestRecordsEnumProto.MyShapeRecord.Shape.values()) {
                        TestRecordsEnumProto.MyShapeRecord.Builder rec = TestRecordsEnumProto.MyShapeRecord.newBuilder();
                        rec.setRecName(size.name() + "-" + color.name() + "-" + shape.name());
                        rec.setRecNo(++n);
                        rec.setSize(size);
                        rec.setColor(color);
                        rec.setShape(shape);
                        recordStore.saveRecord(rec.build());
                    }
                }
            }
            commit(context);
        }
    }

    protected List<UUID> setupTupleFields(@Nonnull FDBRecordContext context) throws Exception {
        openAnyRecordStore(TestRecordsTupleFieldsProto.getDescriptor(), context);
        final List<UUID> uuids = IntStream.rangeClosed(1, 10).mapToObj(i -> UUID.randomUUID())
                .sorted(Comparisons::compare).collect(Collectors.toList());
        for (int i = 0; i < uuids.size(); i++) {
            TestRecordsTupleFieldsProto.MyFieldsRecord.Builder rec = TestRecordsTupleFieldsProto.MyFieldsRecord.newBuilder();
            rec.setUuid(TupleFieldsHelper.toProto(uuids.get(i)));
            if (i != 3) {
                rec.setFint32(TupleFieldsHelper.toProto(i));
            }
            if (i != 6) {
                rec.setFstring(TupleFieldsHelper.toProto("s" + i));
            }
            recordStore.saveRecord(rec.build());
        }
        return uuids;
    }

    protected KeyExpression primaryKey(String recordType) {
        return recordStore.getRecordMetaData().getRecordType(recordType).getPrimaryKey();
    }

    protected void clearStoreCounter(@Nonnull FDBRecordContext context) {
        if (context.getTimer() != null) {
            context.getTimer().reset();
        }
    }

    protected void setNormalizeNestedFields(boolean normalizeNestedFields) {
        if (planner instanceof RecordQueryPlanner) {
            planner.setConfiguration(planner.getConfiguration().asBuilder().setNormalizeNestedFields(normalizeNestedFields).build());
        }
    }

    protected void setDeferFetchAfterUnionAndIntersection(boolean shouldDeferFetch) {
        if (planner instanceof RecordQueryPlanner) {
            RecordQueryPlanner recordQueryPlanner = (RecordQueryPlanner)planner;
            recordQueryPlanner.setConfiguration(recordQueryPlanner.getConfiguration()
                    .asBuilder()
                    .setDeferFetchAfterUnionAndIntersection(shouldDeferFetch)
                    .build());
        }
    }

    protected void setOmitPrimaryKeyInUnionOrderingKey(boolean shouldOmitPrimaryKeyInUnionOrderingKey) {
        if (planner instanceof RecordQueryPlanner) {
            RecordQueryPlanner recordQueryPlanner = (RecordQueryPlanner)planner;
            recordQueryPlanner.setConfiguration(recordQueryPlanner.getConfiguration()
                    .asBuilder()
                    .setOmitPrimaryKeyInUnionOrderingKey(shouldOmitPrimaryKeyInUnionOrderingKey)
                    .setOmitPrimaryKeyInOrderingKeyForInUnion(shouldOmitPrimaryKeyInUnionOrderingKey)
                    .build());
        }
    }

    protected void setOptimizeForIndexFilters(boolean shouldOptimizeForIndexFilters) {
        assertTrue(planner instanceof RecordQueryPlanner);
        RecordQueryPlanner recordQueryPlanner = (RecordQueryPlanner)planner;
        recordQueryPlanner.setConfiguration(recordQueryPlanner.getConfiguration()
                .asBuilder()
                .setOptimizeForIndexFilters(shouldOptimizeForIndexFilters)
                .build());
    }

    /**
     * For the cascades planner, plan the query, serialize the plan to bytes, parse those bytes and reconstruct the plan.
     * @param query the query
     * @return the plan that is either just planned or planned, then serialized, deserialized, and reconstructed
     */
    @Nonnull
    protected RecordQueryPlan planQuery(@Nonnull final RecordQuery query) {
        return planQuery(this.planner, query);
    }

    /**
     * For the cascades planner, plan the query, serialize the plan to bytes, parse those bytes and reconstruct the plan.
     * @param planner the planner to use
     * @param query the query
     * @return the plan that is either just planned or planned, then serialized, deserialized, and reconstructed
     */
    @Nonnull
    protected static RecordQueryPlan planQuery(@Nonnull final QueryPlanner planner, @Nonnull final RecordQuery query) {
        final RecordQueryPlan plannedPlan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            return plannedPlan;
        }
        assertThat(planner, instanceOf(CascadesPlanner.class));
        return verifySerialization(plannedPlan);
    }

    @Nonnull
    protected RecordQueryPlan planGraph(@Nonnull Supplier<Reference> querySupplier, @Nonnull String... allowedIndexes) {
        return planGraph(querySupplier, Bindings.EMPTY_BINDINGS, allowedIndexes);
    }

    @Nonnull
    protected RecordQueryPlan planGraph(@Nonnull Supplier<Reference> querySupplier, @Nonnull Bindings bindings, @Nonnull String... allowedIndexes) {
        assertThat(planner, instanceOf(CascadesPlanner.class));
        final CascadesPlanner cascadesPlanner = (CascadesPlanner)planner;
        final Optional<Collection<String>> allowedIndexesOptional;
        if (allowedIndexes.length > 0) {
            allowedIndexesOptional = Optional.of(List.of(allowedIndexes));
        } else {
            allowedIndexesOptional = Optional.empty();
        }
        final QueryPlanResult planResult = cascadesPlanner.planGraph(querySupplier, allowedIndexesOptional, IndexQueryabilityFilter.DEFAULT, EvaluationContext.forBindings(bindings));
        return verifySerialization(planResult.getPlan());
    }

    /**
     * Serialize the plan to bytes, parse those bytes, reconstruct, and compare the deserialized plan against the
     * original plan.
     * @param plan the original plan
     * @return the deserialized and verified plan
     */
    @Nonnull
    protected static RecordQueryPlan verifySerialization(@Nonnull final RecordQueryPlan plan) {
        PlanSerializationContext serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        final PRecordQueryPlan planProto = plan.toRecordQueryPlanProto(serializationContext);
        final byte[] serializedPlan = planProto.toByteArray();
        final PRecordQueryPlan parsedPlanProto;
        try {
            parsedPlanProto = PRecordQueryPlan.parseFrom(serializedPlan);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        final PlanSerializationContext deserializationContext = new PlanSerializationContext(new DefaultPlanSerializationRegistry(), PlanHashable.CURRENT_FOR_CONTINUATION);
        final RecordQueryPlan deserializedPlan =
                RecordQueryPlan.fromRecordQueryPlanProto(deserializationContext, parsedPlanProto);
        Assertions.assertEquals(plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), deserializedPlan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        Assertions.assertTrue(plan.structuralEquals(deserializedPlan));
        return deserializedPlan;
    }

    @Nonnull
    protected RecordCursorIterator<FDBQueriedRecord<Message>> executeQuery(@Nonnull final RecordQueryPlan plan) {
        return executeQuery(plan, Bindings.EMPTY_BINDINGS);
    }

    @Nonnull
    protected RecordCursorIterator<FDBQueriedRecord<Message>> executeQuery(@Nonnull final RecordQueryPlan plan, @Nonnull Bindings bindings) {
        final var usedTypes = UsedTypesProperty.evaluate(plan);
        final var typeRepository = TypeRepository.newBuilder().addAllTypes(usedTypes).build();
        return plan.execute(recordStore, EvaluationContext.forBindingsAndTypeRepository(bindings, typeRepository)).asIterator();
    }

    protected static class Holder<T> {
        public T value;
    }

    protected static void assertMatches(@Nonnull final RecordQueryPlan plan, @Nonnull BindingMatcher<? extends RecordQueryPlan> planMatcher) {
        final boolean matches = planMatcher.matches(plan);
        if (matches) {
            return;
        }
        System.err.println(plan + "\n does not match");
        System.err.println(planMatcher.explainMatcher(RecordQueryPlan.class, "plan", ""));
        fail();
    }

    protected static void assertMatchesExactly(@Nonnull final RecordQueryPlan plan, @Nonnull BindingMatcher<? extends RecordQueryPlan> planMatcher) {
        final boolean matchesExactly = planMatcher.matchesExactly(plan);
        if (matchesExactly) {
            return;
        }
        System.err.println(plan + "\n does not match exactly");
        System.err.println(planMatcher.explainMatcher(RecordQueryPlan.class, "plan", ""));
        fail();
    }
}
