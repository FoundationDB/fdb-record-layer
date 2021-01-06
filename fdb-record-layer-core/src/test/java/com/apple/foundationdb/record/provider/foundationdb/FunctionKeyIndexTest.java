/*
 * FunctionKeyIndexTest.java
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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestDataTypesProto;
import com.apple.foundationdb.record.TestDataTypesProto.TypesRecord;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.UnstoredRecord;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.empty;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for indexes having a {@link FunctionKeyExpression}.
 */
@Tag(Tags.RequiresFDB)
public class FunctionKeyIndexTest extends FDBRecordStoreTestBase {

    static class Records {
        private List<TypesRecord> records = new ArrayList<>();

        private long recordId = Math.abs(new Random().nextLong());

        public void add(int intValue, String strValue, String ... strRest) {
            if (strRest.length == 0) {
                add(intValue, strValue, (List<String>) null);
            } else {
                List<String> strList = new ArrayList<>(strRest.length + 1);
                strList.add(strValue);
                Collections.addAll(strList, strRest);
                add(intValue, null, strList);
            }
        }

        public void add(int intValue, List<String> strList) {
            add(intValue, null, strList);
        }

        public void add(int intValue, String strValue, List<String> strListValue) {
            TypesRecord.Builder builder = TypesRecord.newBuilder()
                    .setLongValue(recordId)
                    .setIntValue(intValue);
            if (strValue != null) {
                builder.setStrValue(strValue);
            }
            if (strListValue != null) {
                builder.addAllStrListValue(strListValue);
            }
            records.add(builder.build());
            ++recordId;
        }

        public int size() {
            return records.size();
        }

        public boolean contains(TypesRecord record) {
            return records.contains(record);
        }

        public void save(FDBRecordStore recordStore) {
            for (TypesRecord record : records) {
                recordStore.saveRecord(record);
            }
        }

        public static Records create() {
            return new Records();
        }
    }


    protected void openRecordStore(FDBRecordContext context, Index ... indexes) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestDataTypesProto.getDescriptor());
        RecordTypeBuilder scalarRecordType = metaDataBuilder.getRecordType("TypesRecord");
        for (Index index : indexes) {
            metaDataBuilder.addIndex(scalarRecordType, index);
        }
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    protected void saveRecords(Records records, Index ... indexes) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, indexes);
            records.save(recordStore);
            commit(context);
        }
    }

    protected List<FDBIndexedRecord<Message>> getIndexRecords(FDBRecordStore store, String indexName) {
        return store.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                store.scanIndexRecords(indexName, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .asList());
    }

    protected List<IndexEntry> getIndexKeyValues(FDBRecordStore store, Index index, IndexScanType scanType) {
        return store.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                store.scanIndex(index, scanType, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .asList());
    }

    protected static TypesRecord fromMessage(Message message) {
        return TypesRecord.newBuilder().mergeFrom(message).build();
    }

    @Test
    public void testIndexScan() throws Exception {
        // Note: the substr() function is defined by KeyExpressionTest.java
        Index index = new Index("substr_index", function("substr", concat(field("str_value"), value(0), value(3))));
        Records records = Records.create();
        for (int i = 0; i < 10; i++) {
            records.add(i, "ab" + Character.toString((char) ('c' + i)) + "_" + i);
        }
        saveRecords(records, index);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            TupleRange range = new TupleRange(Tuple.from("abd"), Tuple.from("abg_5"), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
            List<FDBIndexedRecord<Message>> results = recordStore.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                    recordStore.scanIndexRecords(index.getName(), IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN).asList());
            assertEquals(4, results.size());
            for (FDBIndexedRecord<Message> indexedRecord : results) {
                TypesRecord record = fromMessage(indexedRecord.getRecord());
                assertTrue(records.contains(record), "Record does not exist");
                assertEquals(record.getStrValue().substring(0, 3), indexedRecord.getIndexEntry().getKey().getString(0));
            }
        }
    }

    @Test
    public void testUniqueIndexNoViolation() throws Exception {
        // Note: the substr() function is defined by KeyExpressionTest.java
        Index index = new Index("substr_unique_index", function("substr", concat(field("str_value"), value(0), value(3))),
                empty(), IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Records records = Records.create();
        for (int i = 0; i < 10; i++) {
            records.add(i, "ab" + Character.toString((char) ('c' + i)) + "_" + i);
        }
        saveRecords(records, index);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            int count = 0;
            for (FDBIndexedRecord<Message> indexedRecord : getIndexRecords(recordStore, "substr_unique_index")) {
                TypesRecord record = fromMessage(indexedRecord.getRecord());
                assertTrue(records.contains(record), "Record does not exist");
                assertEquals(record.getStrValue().substring(0, 3), indexedRecord.getIndexEntry().getKey().getString(0));
                ++count;
            }
            assertEquals(records.size(), count);
        }
    }

    @Test
    public void testUniqueViolation() throws Exception {
        Index index = new Index("substr_unique_index", function("substr", concat(field("str_value"), value(0), value(2))),
                empty(), IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Records records = Records.create();
        for (int i = 0; i < 5; i++) {
            records.add(i, "a" + Character.toString((char) ('b' + (i % 3))) + "_" + i);
        }
        assertThrows(RecordIndexUniquenessViolation.class, () -> saveRecords(records, index));
    }

    @Test
    public void testCountIndex() throws Exception {
        Index index = new Index("group_index",
                new GroupingKeyExpression(function("substr", concat(field("str_value"), value(0), value(3))), 0),
                IndexTypes.COUNT);
        Records records = Records.create();
        for (int i = 0; i < 10; i++) {
            records.add(i, "ab" + Character.toString((char) ('c' + (i % 3))) + "_" + i);
        }
        saveRecords(records, index);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            List<IndexEntry> keyValues = getIndexKeyValues(recordStore, index, IndexScanType.BY_GROUP);
            assertEquals(Tuple.from("abc"), keyValues.get(0).getKey());
            assertEquals(Tuple.from(4), keyValues.get(0).getValue());
            assertEquals(Tuple.from("abd"), keyValues.get(1).getKey());
            assertEquals(Tuple.from(3), keyValues.get(1).getValue());
            assertEquals(Tuple.from("abe"), keyValues.get(2).getKey());
            assertEquals(Tuple.from(3), keyValues.get(2).getValue());
        }
    }

    @Test
    public void testQueryIgnoresFunctionIndexes() throws Exception {
        testQueryFunctionIndex(false);
    }

    // Make sure that index selection still works on "normal" indexes and nothing chokes when it hits the
    // function index definition.
    @Test
    public void testQueryFunctionIndex() throws Exception {
        testQueryFunctionIndex(true);
    }

    private void testQueryFunctionIndex(boolean functionQuery) throws Exception {
        Index funcIndex = new Index("substr_index", function("substr", concat(field("str_value"), value(0), value(3))),
                IndexTypes.VALUE);
        Index normalIndex = new Index("normal_index", field("str_value"), IndexTypes.VALUE);

        Records records = Records.create();
        for (int i = 0; i < 10; i++) {
            String strValue = "ab" + Character.toString((char) ('c' + i)) + "_" + i;
            records.add(i, strValue);
        }
        saveRecords(records, funcIndex, normalIndex);

        QueryComponent filter;
        if (functionQuery) {
            filter = Query.and(
                    Query.keyExpression(funcIndex.getRootExpression()).greaterThanOrEquals("abd"),
                    Query.keyExpression(funcIndex.getRootExpression()).lessThanOrEquals("abg"));
        } else {
            filter = Query.and(
                    Query.field("str_value").greaterThanOrEquals("abd"),
                    Query.field("str_value").lessThanOrEquals("abg"));
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("TypesRecord")
                .setFilter(filter)
                .build();
        RecordQueryPlan plan = planner.plan(query);

        if (functionQuery) {
            assertThat(plan, indexScan(allOf(indexName(funcIndex.getName()), bounds(hasTupleString("[[abd],[abg]]")))));
            assertEquals(316561162, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1854693510, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1386544645, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            // Here I'm really just making sure that (a) the substr_index is not selected, because the
            // function call doesn't appear in the query anyway and (b) that the planner doesn't throw
            // an exception or do something wonky as a result of the presence of this index.
            assertThat(plan, indexScan(allOf(indexName(normalIndex.getName()), bounds(hasTupleString("[[abd],[abg]]")))));
            assertEquals(1189784448, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1432694864, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(964545999, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, funcIndex, normalIndex);
            int count = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> queriedRecord = cursor.next();
                    TypesRecord record = fromMessage(queriedRecord.getRecord());
                    assertTrue(records.contains(record));
                    String str = functionQuery ? record.getStrValue().substring(0, 3) : record.getStrValue();
                    assertThat(str, greaterThanOrEqualTo("abd"));
                    assertThat(str, lessThanOrEqualTo("abg"));
                    ++count;
                }
            }
            assertEquals(functionQuery ? 4 : 3, count);
            assertDiscardedNone(context);
        }
    }

    @Test
    public void testFanOutFunction() throws Exception {
        Index index = new Index("str_fields_index", function("indexStrFields"), IndexTypes.VALUE);
        Records records = Records.create();
        records.add(0, "a,b,c");
        for (int i = 1; i < 10; i++) {
            records.add(i, "a,b,c_" + i, "a_" + i + ",b,c", "a,b_" + i + ",c");
        }
        saveRecords(records, index);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            List<IndexEntry> keyValues = getIndexKeyValues(recordStore, index, IndexScanType.BY_VALUE);
            int count = 0;
            for (IndexEntry kv : keyValues) {
                Tuple key = kv.getKey();
                assertEquals(4, key.size(), "Wrong key length");
                assertTrue(key.getString(0).startsWith("a"), "Invalid value in first key element: " + key.get(0));
                assertTrue(key.getString(1).startsWith("b"), "Invalid value in second key element: " + key.get(1));
                assertTrue(key.getString(2).startsWith("c"), "Invalid value in third key element: " + key.get(2));
                ++count;
            }
            assertEquals(28, count, "Too few index keys");
        }
    }

    @Test
    public void testFanOutFunctionEvaluation() {
        final KeyExpression expression = function("indexStrFields");
        assertEquals(Collections.singletonList(Key.Evaluated.concatenate("a", "b", "c")),
                expression.evaluate(new UnstoredRecord<>(TypesRecord.newBuilder().setStrValue("a,b,c").build())));
        assertEquals(Collections.singletonList(Key.Evaluated.concatenate("a", "b", "c")),
                expression.evaluate(new UnstoredRecord<>(TypesRecord.newBuilder().setStrValue("a,b,c,d").build())));
        assertEquals(Arrays.asList(Key.Evaluated.concatenate("a", "b", "c"), Key.Evaluated.concatenate("d", "e", "f")),
                expression.evaluate(new UnstoredRecord<>(TypesRecord.newBuilder().addStrListValue("a,b,c").addStrListValue("d,e,f").build())));
        assertEquals(Arrays.asList(Key.Evaluated.concatenate("a", "b", "c"), Key.Evaluated.concatenate("d", "e", "f")),
                expression.evaluate(new UnstoredRecord<>(TypesRecord.newBuilder().setStrValue("a,b,c").addStrListValue("d,e,f").build())));
        assertThrows(KeyExpression.InvalidResultException.class,
                () -> expression.evaluate(new UnstoredRecord<>(TypesRecord.newBuilder().setStrValue("a,b").build())));
        assertEquals(Collections.emptyList(),
                expression.evaluate(new UnstoredRecord<>(TypesRecord.getDefaultInstance())));
        assertEquals(Collections.emptyList(),
                expression.evaluate(null));
    }

    /**
     * Function registry for {@link IndexStrFields}.
     */
    @AutoService(FunctionKeyExpression.Factory.class)
    public static class TestFunctionRegistry implements FunctionKeyExpression.Factory {
        @Nonnull
        @Override
        public List<FunctionKeyExpression.Builder> getBuilders() {
            return Collections.singletonList(new FunctionKeyExpression.BiFunctionBuilder("indexStrFields", IndexStrFields::new));
        }
    }

    /**
     * A function that finds all string fields or string list fields, and splits them out by commas into discrete
     * "subfields" and produces and index by those values.
     */
    public static class IndexStrFields extends FunctionKeyExpression {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Index-Str-Fields");

        public IndexStrFields(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 0;
        }

        @Override
        public int getMaxArguments() {
            return 0;
        }

        @Nonnull
        @Override
        public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
                                                                        @Nullable Message message,
                                                                        @Nonnull Key.Evaluated arguments) {
            if (message == null) {
                return Collections.emptyList();
            }
            List<Key.Evaluated> keys = new ArrayList<>();
            Descriptors.Descriptor descriptor = message.getDescriptorForType();
            Descriptors.FieldDescriptor strField = descriptor.findFieldByNumber(TypesRecord.STR_VALUE_FIELD_NUMBER);
            Descriptors.FieldDescriptor strListField = descriptor.findFieldByNumber(TypesRecord.STR_LIST_VALUE_FIELD_NUMBER);
            if (message.hasField(strField)) {
                keys.add(toKey((String) message.getField(strField)));
            }
            final int len = message.getRepeatedFieldCount(strListField);
            for (int i = 0; i < len; i++) {
                keys.add(toKey((String) message.getRepeatedField(strListField, i)));
            }
            return keys;
        }

        private Key.Evaluated toKey(@Nonnull String value) {
            String[] values = value.split(",");
            if (values.length < 3) {
                throw new InvalidResultException(
                        "Expected at least three values, but got: " + values.length);
            }
            return Key.Evaluated.concatenate(values[0], values[1], values[2]);
        }

        @Override
        public boolean createsDuplicates() {
            return true;
        }

        @Override
        public int getColumnSize() {
            return 3;
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
