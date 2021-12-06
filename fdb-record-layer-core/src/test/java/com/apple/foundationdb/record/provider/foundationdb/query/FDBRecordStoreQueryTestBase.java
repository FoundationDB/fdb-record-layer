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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords5Proto;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.TestRecordsTupleFieldsProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A base class for common infrastructure used by tests in {@link com.apple.foundationdb.record.provider.foundationdb.query}.
 */
public abstract class FDBRecordStoreQueryTestBase extends FDBRecordStoreTestBase {
    protected void setupSimpleRecordStore(RecordMetaDataHook recordMetaDataHook,
                                          BiConsumer<Integer, TestRecords1Proto.MySimpleRecord.Builder> buildRecord)
            throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, recordMetaDataHook);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder record = TestRecords1Proto.MySimpleRecord.newBuilder();
                buildRecord.accept(i, record);
                recordStore.saveRecord(record.build());
            }
            commit(context);
        }
    }

    protected int querySimpleRecordStore(RecordMetaDataHook recordMetaDataHook, RecordQueryPlan plan,
                                         Supplier<EvaluationContext> contextSupplier,
                                         TestHelpers.DangerousConsumer<TestRecords1Proto.MySimpleRecord.Builder> checkRecord)
            throws Exception {
        return querySimpleRecordStore(recordMetaDataHook, plan, contextSupplier, checkRecord, context -> { });
    }

    protected int querySimpleRecordStore(RecordMetaDataHook recordMetaDataHook, RecordQueryPlan plan,
                                         Supplier<EvaluationContext> contextSupplier,
                                         TestHelpers.DangerousConsumer<TestRecords1Proto.MySimpleRecord.Builder> checkRecord,
                                         TestHelpers.DangerousConsumer<FDBRecordContext> checkDiscarded)
            throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, recordMetaDataHook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = plan.execute(recordStore, contextSupplier.get()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    checkRecord.accept(myrec);
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
    protected RecordMetaDataHook complexQuerySetupHook() {
        return metaData -> {
            metaData.addIndex("MySimpleRecord", new Index("multi_index",
                    "str_value_indexed", "num_value_2", "num_value_3_indexed"));
            metaData.addIndex("MySimpleRecord", "repeater$fanout", field("repeater", FanType.FanOut));
        };
    }

    protected void openHierarchicalRecordStore(FDBRecordContext context) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords3Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(COUNT_INDEX);
        metaDataBuilder.getRecordType("MyHierarchicalRecord").setPrimaryKey(
                concatenateFields("parent_path", "child_name"));
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    protected void openNestedRecordStore(FDBRecordContext context) throws Exception {
        openNestedRecordStore(context, null);
    }

    protected void openNestedRecordStore(FDBRecordContext context, @Nullable RecordMetaDataHook hook) throws Exception {
        createOrOpenRecordStore(context, nestedMetaData(hook));
    }

    protected RecordMetaData nestedMetaData(@Nullable RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords4Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(COUNT_INDEX);
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

    protected void nestedWithAndSetup(@Nullable RecordMetaDataHook hook) throws Exception {
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
                    ).build();

            TestRecords4Proto.RestaurantReviewer reviewer2 = TestRecords4Proto.RestaurantReviewer.newBuilder()
                    .setId(2L)
                    .setName("Newt A. Robot")
                    .setEmail("newtarobot@example.com")
                    .setStats(
                            TestRecords4Proto.ReviewerStats.newBuilder()
                                    .setStartDate(1066L)
                                    .setSchoolName("Human University")
                                    .setHometown("Real Place")
                    ).build();

            recordStore.saveRecord(reviewer1);
            recordStore.saveRecord(reviewer2);

            commit(context);
        }
    }

    protected void openDoublyNestedRecordStore(FDBRecordContext context) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords5Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(COUNT_INDEX);
        metaDataBuilder.addIndex("CalendarEvent", "alarm_start", field("alarmIndex").nest(
                field("recurrence", FanType.FanOut).nest("start")));
        metaDataBuilder.addIndex("CalendarEvent", "event_start", field("eventIndex").nest(
                field("recurrence", FanType.FanOut).nest("start")));
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    public void openConcatNestedRecordStore(FDBRecordContext context) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords5Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(COUNT_INDEX);
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
        List<T> result = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            opener.open(context);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    result.add(rowHandler.apply(rec.getRecord()));
                }
            }
            checkDiscarded.accept(context);
            clearStoreCounter(context); // TODO a hack until this gets refactored properly
        }
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

    protected void openEnumRecordStore(FDBRecordContext context, RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsEnumProto.getDescriptor());
        if (hook != null) {
            hook.apply(metaData);
        }
        createOrOpenRecordStore(context, metaData.getRecordMetaData());
    }

    protected void setupEnumShapes(RecordMetaDataHook hook) throws Exception {
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
                rec.setFstring(TupleFieldsHelper.toProto(String.format("s%d", i)));
            }
            recordStore.saveRecord(rec.build());
        }
        return uuids;
    }

    protected KeyExpression primaryKey(String recordType) {
        return recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getPrimaryKey();
    }

    protected void clearStoreCounter(@Nonnull FDBRecordContext context) {
        if (context.getTimer() != null) {
            context.getTimer().reset();
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

    protected void setOptimizeForIndexFilters(boolean shouldOptimizeForIndexFilters) {
        assertTrue(planner instanceof RecordQueryPlanner);
        RecordQueryPlanner recordQueryPlanner = (RecordQueryPlanner)planner;
        recordQueryPlanner.setConfiguration(recordQueryPlanner.getConfiguration()
                .asBuilder()
                .setOptimizeForIndexFilters(shouldOptimizeForIndexFilters)
                .build());
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
