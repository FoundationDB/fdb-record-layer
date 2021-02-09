/*
 * FunctionKeyRecordTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords8Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;

/**
 * Tests for {@link FunctionKeyExpression} with records.
 */
@Tag(Tags.RequiresFDB)
public class FunctionKeyRecordTest extends FDBRecordStoreTestBase {

    private void openRecordStore(@Nonnull FDBRecordContext context, @Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords8Proto.getDescriptor());
        hook.apply(metaData);
        createOrOpenRecordStore(context, metaData.getRecordMetaData());
    }

    @Test
    public void testWriteRecord() throws Exception {
        RecordMetaDataHook hook = metadata -> {
            metadata.getRecordType("StringRecordId").setPrimaryKey(
                    function("regex", concat(field("rec_id"), value("/s(\\d+):(.*)"),
                            value("LONG"), value("STRING"))));
        };

        // Create some records
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(TestRecords8Proto.StringRecordId.newBuilder()
                        .setRecId("/s" + i + ":foo_" + i)
                        .setIntValue(i)
                        .build());
            }
            commit(context);
        }

        // Try to read then back using our new computed key
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (int i = 0; i < 5; i++) {
                FDBStoredRecord<Message> message = recordStore.loadRecord(Tuple.from(i, "foo_" + i));
                Assertions.assertNotNull(message, "Failed to load record");
                TestRecords8Proto.StringRecordId record = TestRecords8Proto.StringRecordId.newBuilder().mergeFrom(message.getRecord()).build();
                Assertions.assertEquals("/s" + i + ":foo_" + i, record.getRecId());
                Assertions.assertEquals(i, record.getIntValue());

                if ((i % 2) == 0) {
                    recordStore.deleteRecord(Tuple.from(i, "foo_" + i));
                }
            }
            commit(context);
        }

        // Verify the deleted records
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (int i = 0; i < 5; i++) {
                FDBStoredRecord<Message> message = recordStore.loadRecord(Tuple.from(i, "foo_" + i));
                if ((i % 2) == 0) {
                    Assertions.assertNull(message, "Failed to delete record");
                } else {
                    Assertions.assertNotNull(message, "Record was deleted");
                }
            }
            commit(context);
        }
    }

    private RecordMetaDataHook setupQueryData() throws Exception {
        // We should still be able to query and perform deletes against the leading field that is not
        // produced by the results of the function.
        RecordMetaDataHook hook = metadata -> {
            metadata.getRecordType("StringRecordId").setPrimaryKey(
                    concat(field("int_value"),
                            function("regex", concat(field("rec_id"), value("/s:(foo_\\d+).*"), value("STRING")))));
        };

        // Create some records
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            recordStore.deleteAllRecords();
            for (int i = 0; i < 10; i++) {
                recordStore.saveRecord(TestRecords8Proto.StringRecordId.newBuilder()
                        .setRecId("/s:foo_" + i + "_blah")
                        .setIntValue(i % 3)
                        .setStrField("hello_" + i)
                        .setLongField(i)
                        .build());
            }
            commit(context);
        }

        return hook;
    }

    private TestRecords8Proto.StringRecordId validateQueryData(FDBRecord<Message> message) {
        TestRecords8Proto.StringRecordId record = TestRecords8Proto.StringRecordId.newBuilder().mergeFrom(
                message.getRecord()).build();
        Assertions.assertEquals("/s:foo_" + record.getLongField() + "_blah", record.getRecId());
        Assertions.assertEquals(record.getLongField() % 3, record.getIntValue());
        Assertions.assertEquals("hello_" + record.getLongField(), record.getStrField());
        Assertions.assertTrue(record.getLongField() >= 0 && record.getLongField() < 10,
                "Invalid value for long field");
        return record;
    }

    @Test
    public void testQueryByNonKeyField() throws Exception {
        RecordMetaDataHook hook = setupQueryData();

        // Query by non-primary key field and make sure that works.
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("StringRecordId")
                .setFilter(Query.or(
                        Query.field("str_field").equalsValue("hello_0"),
                        Query.field("str_field").equalsValue("hello_3"),
                        Query.field("str_field").equalsValue("hello_6")))
                .build();
        RecordQueryPlan plan = planner.plan(query);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                AtomicInteger count = new AtomicInteger();
                cursor.forEach(queriedRecord -> {
                    TestRecords8Proto.StringRecordId record = validateQueryData(queriedRecord);
                    Assertions.assertTrue(record.getIntValue() >= 0 && record.getIntValue() <= 2,
                            "Unexpected record returned");
                    count.incrementAndGet();
                }).get();

                Assertions.assertEquals(3, count.get(), "Too few records returned");
            }
        }
    }

    @Test
    public void testQueryByLeadingPortionOfKey() throws Exception {
        RecordMetaDataHook hook = setupQueryData();

        // Query by leading portion of primary key that is not hidden behind the function
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("StringRecordId")
                .setFilter(Query.and(
                        Query.field("int_value").greaterThanOrEquals(1),
                        Query.field("int_value").lessThanOrEquals(2)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        Assertions.assertEquals("Scan([[1],[2]])", plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                AtomicInteger count = new AtomicInteger();
                cursor.forEach(queriedRecord -> {
                    TestRecords8Proto.StringRecordId record = validateQueryData(queriedRecord);
                    Assertions.assertTrue(record.getLongField() % 3 == 1 || record.getLongField() % 3 == 2);
                    count.incrementAndGet();
                }).get();

                Assertions.assertEquals(6, count.get(), "Too few records returned");
            }
        }
    }

    @Test
    public void testQueryByRecordId() throws Exception {
        RecordMetaDataHook hook = setupQueryData();

        // Query by leading portion of primary key that is not hidden behind the function
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("StringRecordId")
                .setFilter(Query.field("rec_id").equalsValue("/s:foo_3_blah"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        Assertions.assertEquals("Scan(<,>) | rec_id EQUALS /s:foo_3_blah", plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                AtomicInteger count = new AtomicInteger();
                cursor.forEach(queriedRecord -> {
                    TestRecords8Proto.StringRecordId record = validateQueryData(queriedRecord);
                    Assertions.assertEquals("/s:foo_3_blah", record.getRecId());
                    count.incrementAndGet();
                }).get();

                Assertions.assertEquals(1, count.get(), "Too few records returned");
            }
        }
    }

    @Test
    public void testDeleteWhere() throws Exception {
        RecordMetaDataHook hook = setupQueryData();
        // Delete where int_value = 0
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            recordStore.deleteRecordsWhere(Query.field("int_value").equalsValue(0));
            commit(context);
        }


        // See that they got deleted
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (int i = 0; i < 10; i++) {
                FDBStoredRecord<Message> message = recordStore.loadRecord(Tuple.from(i % 3, "foo_" + i));
                if ((i % 3) == 0) {
                    Assertions.assertNull(message, "Record should have been deleted!");
                } else {
                    Assertions.assertNotNull(message, "Failed to load record: " + i);
                    TestRecords8Proto.StringRecordId record = TestRecords8Proto.StringRecordId.newBuilder().mergeFrom(message.getRecord()).build();
                    Assertions.assertEquals("/s:foo_" + i + "_blah", record.getRecId());
                    Assertions.assertEquals(i % 3, record.getIntValue());
                }
            }
            commit(context);
        }
    }

    @Test
    public void testCoveringIndexFunction() throws Exception {
        // This index parses each entry of str_array_field of the form "X:Y:Z" and produces a keyWithValue
        // index of the form X -> (Y, Z).
        final Index index = new Index("covering",
                keyWithValue(function("regex", concat(field("str_array_field", KeyExpression.FanType.FanOut), value("(\\d+):(\\w+):(\\d+)"),
                        value("LONG"), value("STRING"), value("LONG"))), 1));

        final RecordMetaDataHook hook = metadata -> {
            RecordTypeBuilder type = metadata.getRecordType("StringRecordId");
            type.setPrimaryKey(field("rec_id"));
            metadata.addIndex(type, index);
        };

        final BiFunction<Integer, Integer, String> makeId = (id1, id2) ->
                id1 + ":" + Character.toString((char) ('a' + id2)) + ":" + id2;

        // Create some records
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (int i = 0; i < 5; i++) {
                TestRecords8Proto.StringRecordId.Builder builder =
                        TestRecords8Proto.StringRecordId.newBuilder()
                                .setRecId("record_" + i)
                                .setIntValue(i);
                for (int j = 0; j < 4; j++) {
                    builder.addStrArrayField(makeId.apply((i * 4) + j, j));
                }

                recordStore.saveRecord(builder.build());
            }
            commit(context);
        }

        Function<FDBIndexedRecord<Message>, TestRecords8Proto.StringRecordId> validate = message -> {
            final Tuple key = message.getIndexEntry().getKey();
            final Tuple value = message.getIndexEntry().getValue();

            // Key is X,<record_id> where X is the first part of the str_array_field
            Assertions.assertEquals(2, key.size());
            // Value is the last two pieces of the str_array_field
            Assertions.assertEquals(2, value.size());

            // Check the record itself
            final TestRecords8Proto.StringRecordId record = TestRecords8Proto.StringRecordId.newBuilder().mergeFrom(
                    message.getRecord()).build();

            // Get the portions of the key and the value that are needed to reconstruct the
            // full value that is supposed to be stored in the str_array_field.
            final int id1 = (int) key.getLong(0);
            final int id2 = (int) value.getLong(1);

            final int recordId = (id1 / 4);

            Assertions.assertEquals(recordId, record.getIntValue());
            Assertions.assertEquals("record_" + recordId, record.getRecId());
            Assertions.assertTrue(record.getStrArrayFieldList().contains(makeId.apply(id1, id2)),
                    "str_array_field does not contain entry");

            Assertions.assertEquals(Character.toString((char) ('a' + id2)), value.getString(0));
            return record;
        };

        // We can't query based upon a covering index, but we can do a direct index scan. First,
        // let's just scan all of the records
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);

            List<FDBIndexedRecord<Message>> messages = recordStore.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                    recordStore.scanIndexRecords(index.getName(), IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList());
            Assertions.assertEquals(20, messages.size(), "Wrong record count");
            for (FDBIndexedRecord<Message> message : messages) {
                validate.apply(message);
            }
        }

        // Next, scan a subset of them based upon the first value of the covering index
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);

            TupleRange range = new TupleRange(Tuple.from(2), Tuple.from(4),
                    EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE);

            List<FDBIndexedRecord<Message>> messages = recordStore.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                    recordStore.scanIndexRecords(index.getName(), IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN).asList());
            Assertions.assertEquals(2, messages.size(), "Wrong record count");
            for (FDBIndexedRecord<Message> message : messages) {
                TestRecords8Proto.StringRecordId record = validate.apply(message);
                Assertions.assertTrue(record.getIntValue() == 0, "Invalid int value");
            }
        }
    }

    /**
     * Function registry for {@link RegexSplitter}.
     */
    @AutoService(FunctionKeyExpression.Factory.class)
    public static class TestFunctionRegistry implements FunctionKeyExpression.Factory {
        @Nonnull
        @Override
        public List<FunctionKeyExpression.Builder> getBuilders() {
            return Collections.singletonList(new FunctionKeyExpression.BiFunctionBuilder("regex", RegexSplitter::new));
        }
    }

    /**
     * A function that uses a regular expression to split a field value.
     */
    public static class RegexSplitter extends FunctionKeyExpression {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Regex-Splitter");

        enum KeyType {
            LONG,
            STRING
        }

        private final List<KeyType> types = new ArrayList<>();
        private final KeyExpression sourceExpression;
        private final Pattern pattern;

        public RegexSplitter(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
            if (!(arguments instanceof ThenKeyExpression)) {
                throw new InvalidExpressionException("Expected concat() for arguments");
            }

            List<KeyExpression> children = ((ThenKeyExpression) arguments).getChildren();
            this.sourceExpression = children.get(0);
            if (sourceExpression.getColumnSize() != 1) {
                throw new InvalidExpressionException("First argument must produce a column count of 1")
                        .addLogInfo("column_count", sourceExpression.getColumnSize());
            }

            final String regex = getString(children, 1);
            try {
                this.pattern = Pattern.compile(regex);
            } catch (PatternSyntaxException e) {
                throw new InvalidExpressionException("Invalid regular expression")
                        .addLogInfo("regex", regex);
            }

            for (int i = 2; i < children.size(); i++) {
                types.add(getKeyType(children, i));
            }
        }

        private KeyType getKeyType(List<KeyExpression> arguments, int idx) {
            return KeyType.valueOf(getString(arguments, idx));
        }

        private String getString(List<KeyExpression> arguments, int idx) {
            Object value = getValue(arguments, idx).getValue();
            if (value instanceof String) {
                return (String) value;
            }
            throw new InvalidExpressionException("Expected STRING value for argument")
                    .addLogInfo("argument_index", idx);
        }

        private LiteralKeyExpression<?> getValue(@Nonnull List<KeyExpression> arguments, int idx) {
            KeyExpression child = arguments.get(idx);
            if (!(child instanceof LiteralKeyExpression)) {
                throw new InvalidExpressionException("Expected value() expression")
                        .addLogInfo("argument_index", idx);
            }
            return (LiteralKeyExpression) child;
        }

        @Override
        public int getMinArguments() {
            return 3;
        }

        @Override
        public int getMaxArguments() {
            return Integer.MAX_VALUE;
        }

        @Nonnull
        @Override
        public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
                                                                        @Nullable Message message,
                                                                        @Nonnull Key.Evaluated arguments) {
            final String origValue = arguments.getString(0);
            final Matcher matcher  = pattern.matcher(origValue);
            if (! matcher.matches()) {
                throw new InvalidResultException("Pattern match failed").addLogInfo(
                        "pattern", pattern.toString());
            }

            if (matcher.groupCount() < types.size()) {
                throw new InvalidResultException("Pattern returned too few groups").addLogInfo(
                        "pattern", pattern.toString(),
                        "expected_groups", types.size(),
                        "actual_groups", matcher.groupCount());
            }

            List<Object> values = new ArrayList<>(matcher.groupCount());
            for (int i = 0; i < types.size(); i++) {
                String str = matcher.group(i + 1);
                switch (types.get(i)) {
                    case LONG:
                        values.add(Long.valueOf(str));
                        break;
                    case STRING:
                        values.add(str);
                        break;
                    default:
                        throw new InvalidResultException("Unexpected type").addLogInfo(
                                "type", types.get(i));
                }
            }

            return Collections.singletonList(Key.Evaluated.concatenate(values));
        }

        @Override
        public boolean createsDuplicates() {
            return sourceExpression.createsDuplicates();
        }

        @Override
        public int getColumnSize() {
            return types.size();
        }

        @Override
        public int planHash(@Nonnull final PlanHashable.PlanHashKind hashKind) {
            return super.basePlanHash(hashKind, BASE_HASH);
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return super.baseQueryHash(hashKind, BASE_HASH);
        }
    }
}
