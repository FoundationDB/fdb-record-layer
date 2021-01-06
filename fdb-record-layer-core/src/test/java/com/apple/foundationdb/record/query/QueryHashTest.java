/*
 * QueryHashTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.QueryHashable.QueryHashKind;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for QueryHash on queries.
 */
@Tag(Tags.RequiresFDB)
public class QueryHashTest extends FDBRecordStoreQueryTestBase {

    @Test
    public void testSingleEqualsFilter() throws Exception {
        RecordQuery qyery1 = createQuery("MySimpleRecord", Query.field("num_value_2").equalsValue(1));
        RecordQuery qyery2 = createQuery("MySimpleRecord", Query.field("num_value_2").equalsValue(2));
        RecordQuery query3 = createQuery("MySimpleRecord", Query.field("num_value_2").equalsParameter("3"));

        assertEquals(947189211, qyery1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(947189211, qyery2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that the value and the parameter hashes are different.
        assertEquals(820394820, query3.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testSingleGtFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord", Query.field("num_value_2").greaterThan(1));
        RecordQuery query2 = createQuery("MySimpleRecord", Query.field("num_value_2").greaterThan(2));

        assertEquals(2043962708, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(2043962708, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testSingleGteFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord", Query.field("num_value_2").greaterThanOrEquals(1));
        RecordQuery query2 = createQuery("MySimpleRecord", Query.field("num_value_2").greaterThanOrEquals(2));

        assertEquals(-349000904, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-349000904, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testOrEqualsFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_2").equalsValue(2)));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").equalsValue(3),
                        Query.field("num_value_2").equalsValue(4)));
        RecordQuery query3 = createQuery("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").equalsParameter("5"),
                        Query.field("num_value_2").equalsParameter("6")));

        assertEquals(969320472, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(969320472, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that the value and the parameter hashes are different.
        assertEquals(1206867256, query3.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testAndGtFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("num_value_2").greaterThan(1),
                        Query.field("num_value_2").lessThan(3)));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("num_value_2").greaterThan(2),
                        Query.field("num_value_2").lessThan(4)));

        assertEquals(1154381483, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1154381483, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testOrGtFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").greaterThan(1),
                        Query.field("num_value_2").lessThan(3)));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").greaterThan(2),
                        Query.field("num_value_2").lessThan(4)));

        assertEquals(1661143543, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1661143543, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }


    @Test
    public void testNotEqualsFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord", Query.not(Query.field("num_value_2").equalsValue(1)));
        RecordQuery query2 = createQuery("MySimpleRecord", Query.not(Query.field("num_value_2").equalsValue(2)));
        RecordQuery query3 = createQuery("MySimpleRecord", Query.not(Query.field("num_value_2").equalsParameter("3")));

        assertEquals(-1665088707, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1665088707, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that value and parameter hashes are not the same
        assertEquals(-1791883098, query3.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testNotGtFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord", Query.not(Query.field("num_value_2").greaterThan(1)));
        RecordQuery query2 = createQuery("MySimpleRecord", Query.not(Query.field("num_value_2").greaterThan(2)));

        assertEquals(-568315210, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-568315210, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testRank() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord", Query.rank("num_value_2").equalsValue(2L));
        RecordQuery query2 = createQuery("MySimpleRecord", Query.rank("num_value_2").equalsValue(3L));

        assertEquals(273555036, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(273555036, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testComplexQuery1g() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("a"),
                        Query.field("num_value_3_indexed").equalsValue(3)));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("b"),
                        Query.field("num_value_3_indexed").equalsValue(3)));

        assertEquals(-897358891, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-897358891, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testComplexQueryAndWithIncompatibleFilters() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").startsWith("e"),
                        Query.field("num_value_3_indexed").equalsValue(3)));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").startsWith("f"),
                        Query.field("num_value_3_indexed").equalsValue(3)));

        assertEquals(-429556342, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-429556342, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void intersectionVersusRange() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaData -> {
                metaData.addIndex("MySimpleRecord", "num_value_2");
                metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
                metaData.addIndex("MySimpleRecord", new Index("index_2_3", "num_value_2", "num_value_3_indexed"));
            });
        }
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("q"),
                        Query.field("num_value_unique").equalsValue(0),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("w"),
                        Query.field("num_value_unique").equalsValue(0),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)));
        RecordQuery query3 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("w"),
                        Query.field("num_value_unique").equalsValue(0),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(3),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)));

        assertEquals(593786046, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(593786046, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(593786046, query3.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void sortedIntersectionBounded() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2)),
                field("num_value_3_indexed"));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(3),
                        Query.field("num_value_3_indexed").equalsValue(4)),
                field("num_value_3_indexed"));

        assertEquals(-1605161468, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1605161468, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void sortedIntersectionUnbound() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2)),
                field("rec_no"));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(3),
                        Query.field("num_value_3_indexed").equalsValue(4)),
                field("rec_no"));

        assertEquals(1122317778, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1122317778, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void collateNoIndex() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.keyExpression(function("collate_jre", field("str_value_indexed"))).equalsValue("a"),
                null,
                Collections.singletonList(field("str_value_indexed")));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.keyExpression(function("collate_jre", field("str_value_indexed"))).equalsValue("b"),
                null,
                Collections.singletonList(field("str_value_indexed")));

        assertEquals(-207710934, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-207710934, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void coveringIndex() throws Exception {
        // Note how the name field needs to be repeated in the value because it can't be recovered from an index
        // entry after transformation to a collation key.
        final KeyExpression collateKey = function("collate_jre", concat(field("str_value_indexed"), value("da_DK")));
        final KeyExpression indexKey = keyWithValue(concat(collateKey, field("str_value_indexed")), 1);
        final RecordMetaDataHook hook = md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.addIndex("MySimpleRecord", "collated_name", indexKey);
        };
        runHook(hook);
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.keyExpression(collateKey).lessThan("a"),
                null,
                Collections.singletonList(field("str_value_indexed")));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.keyExpression(collateKey).lessThan("b"),
                null,
                Collections.singletonList(field("str_value_indexed")));

        // TODO: https://github.com/FoundationDB/fdb-record-layer/issues/1087
        //        assertEquals(379475762, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        //        assertEquals(379475762, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void compareParameter() throws Exception {
        final KeyExpression key = function("collate_jre", concat(field("str_value_indexed"), value("de_DE")));
        final RecordMetaDataHook hook = md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.addIndex("MySimpleRecord", "collated_name", key);
        };
        runHook(hook);
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.keyExpression(key).equalsParameter("name"),
                null,
                Collections.singletonList(field("str_value_indexed")));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.keyExpression(key).equalsParameter("no-name"),
                null,
                Collections.singletonList(field("str_value_indexed")));

        // TODO: https://github.com/FoundationDB/fdb-record-layer/issues/1087
        //        assertEquals(2024130897, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        //        assertEquals(2024130897, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));

    }

    @Test
    public void coveringSimple() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.field("num_value_unique").greaterThan(990),
                field("num_value_unique"),
                Collections.singletonList(field("num_value_unique")));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.field("num_value_unique").greaterThan(7766),
                field("num_value_unique"),
                Collections.singletonList(field("num_value_unique")));

        assertEquals(1019582585, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1019582585, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void coveringSimpleInsufficient() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.field("num_value_unique").greaterThan(990),
                field("num_value_unique"),
                Arrays.asList(field("num_value_unique"), field("num_value_3_indexed")));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.field("num_value_unique").greaterThan(7766),
                field("num_value_unique"),
                Arrays.asList(field("num_value_unique"), field("num_value_3_indexed")));

        assertEquals(1019582585, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1019582585, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void coveringWithAdditionalFilter() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "num_value_3_indexed", "num_value_2"));
        };
        runHook(hook);

        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("num_value_3_indexed").lessThan(1),
                        Query.field("num_value_2").lessThan(2)),
                null,
                Collections.singletonList(field("num_value_3_indexed")));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.and(
                        Query.field("num_value_3_indexed").lessThan(3),
                        Query.field("num_value_2").lessThan(4)),
                null,
                Collections.singletonList(field("num_value_3_indexed")));

        assertEquals(1306898425, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1306898425, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testMultiRecordTypeIndexScan() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            commit(context);
        }

        RecordQuery query1 = createQuery(
                Arrays.asList("MySimpleRecord", "MySimpleRecord2"),
                Query.field("etag").equalsValue(7),
                null, null);
        RecordQuery query2 = createQuery(
                Arrays.asList("MySimpleRecord", "MySimpleRecord2"),
                Query.field("etag").equalsValue(8),
                null, null);

        assertEquals(1598221676, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1598221676, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testInQueryNoIndex() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.field("num_value_2").in(asList(0, 2)),
                null, null);
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.field("num_value_2").in(asList(1, 3)),
                null, null);

        assertEquals(-936101258, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-936101258, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testInQueryNoIndexWithParameter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.field("num_value_2").in("valuesThree"),   // num_value_2 is i%3
                null, null);
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.field("num_value_2").in("valuesFour"),   // num_value_2 is i%3
                null, null);

        assertEquals(1554768926, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1554768926, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testInQueryIndex() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.field("num_value_3_indexed").in(asList(1, 2, 3, 4)),
                null, null);
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.field("num_value_3_indexed").in(asList(5, 6)),
                null, null);

        assertEquals(-193254231, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-193254231, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testNotInQuery() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.not(Query.field("num_value_2").in(asList(0, 2))),
                null, null);
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.not(Query.field("num_value_2").in(asList(1, 3))),
                null, null);

        assertEquals(746588120, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(746588120, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testFullTextCovering() throws Exception {
        final List<TestRecordsTextProto.SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.FRENCH
        ));

        try (FDBRecordContext context = openContext()) {
            setupTextStore(context);
            documents.forEach(recordStore::saveRecord);
            commit(context);
        }

        RecordQuery query1 = createQuery("SimpleDocument",
                Query.field("text").text().contains("civil"),
                null, null);
        RecordQuery query2 = createQuery("SimpleDocument",
                Query.field("text").text().contains("duty"),
                null, null);

        assertEquals(269491435, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(269491435, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testTextWithcontainsAll() throws Exception {
        final List<TestRecordsTextProto.SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.FRENCH
        ));

        try (FDBRecordContext context = openContext()) {
            setupTextStore(context);
            documents.forEach(recordStore::saveRecord);
            commit(context);
        }

        RecordQuery query1 = createQuery("SimpleDocument",
                Query.field("text").text().containsAll("civil", 5),
                null, null);
        RecordQuery query2 = createQuery("SimpleDocument",
                Query.field("text").text().containsAll("duty", 1),
                null, null);

        assertEquals(-97516745, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-97516745, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testTextWithContainsAllPrefix() throws Exception {
        final List<TestRecordsTextProto.SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.FRENCH
        ));

        try (FDBRecordContext context = openContext()) {
            setupTextStore(context);
            documents.forEach(recordStore::saveRecord);
            commit(context);
        }

        RecordQuery query1 = createQuery("SimpleDocument",
                Query.field("text").text().containsAllPrefixes("civil", true, 1L, 2.0),
                null, null);
        RecordQuery query2 = createQuery("SimpleDocument",
                Query.field("text").text().containsAllPrefixes("duty", true, 3L, 4.0),
                null, null);

        assertEquals(1603894354, query1.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1603894354, query2.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }


    private RecordQuery createQuery(String recordType, QueryComponent filter) {
        return createQuery(recordType, filter, null);
    }

    private RecordQuery createQuery(String recordType, QueryComponent filter, KeyExpression sort) {
        return createQuery(recordType, filter, sort, null);
    }

    private RecordQuery createQuery(String recordType, QueryComponent filter, KeyExpression sort, List<KeyExpression> requiredResults) {
        return createQuery(Collections.singletonList(recordType), filter, sort, requiredResults);
    }

    private RecordQuery createQuery(List<String> recordTypes, QueryComponent filter, KeyExpression sort, List<KeyExpression> requiredResults) {
        RecordQuery.Builder builder = RecordQuery.newBuilder()
                .setRecordTypes(recordTypes)
                .setFilter(filter);
        if (sort != null) {
            builder.setSort(sort, false);
        }
        if (requiredResults != null) {
            builder.setRequiredResults(requiredResults);
        }
        return builder.build();
    }

    protected void runHook(RecordMetaDataHook hook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            commit(context);
        }
    }

    protected void setupTextStore(FDBRecordContext context) throws Exception {
        setupTextStore(context, store -> {
        });
    }

    protected void setupTextStore(FDBRecordContext context, RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }

}
