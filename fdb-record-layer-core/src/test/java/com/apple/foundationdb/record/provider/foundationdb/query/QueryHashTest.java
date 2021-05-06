/*
 * QueryHashTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
        final RecordQuery qyery1 = createQuery("MySimpleRecord", Query.field("num_value_2").equalsValue(1));
        final RecordQuery qyery2 = createQuery("MySimpleRecord", Query.field("num_value_2").equalsValue(2));
        final RecordQuery query3 = createQuery("MySimpleRecord", Query.field("num_value_2").equalsParameter("x"));
        final RecordQuery query4 = createQuery("MySimpleRecord", Query.field("num_value_2").equalsParameter("y"));

        assertHash(qyery1, 602828722, 947189211);
        assertHash(qyery2, 602858513, 947189211);
        // Note that the value and the parameter hashes are different.
        assertHash(query3, 970715026, 820394820);
        assertHash(query4, 970744817, 820394820);
    }

    @Test
    public void testSingleGtFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord", Query.field("num_value_2").greaterThan(1));
        RecordQuery query2 = createQuery("MySimpleRecord", Query.field("num_value_2").greaterThan(2));

        assertHash(query1, 243068761, 2043962708);
        assertHash(query2, 243098552, 2043962708);
    }

    @Test
    public void testSingleGteFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord", Query.field("num_value_2").greaterThanOrEquals(1));
        RecordQuery query2 = createQuery("MySimpleRecord", Query.field("num_value_2").greaterThanOrEquals(2));

        assertHash(query1, -924359179, -349000904);
        assertHash(query2, -924329388, -349000904);
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

        assertHash(query1, -1460250793, 969320472);
        assertHash(query2, -1458344169, 969320472);
        // Note that the value and the parameter hashes are different.
        assertHash(query3, 1658304439, 1206867256);
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

        assertHash(query1, -1995304341, 1154381483);
        assertHash(query2, -1994351029, 1154381483);
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

        assertHash(query1, -1488542281, 1661143543);
        assertHash(query2, -1487588969, 1661143543);
    }


    @Test
    public void testNotEqualsFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord", Query.not(Query.field("num_value_2").equalsValue(1)));
        RecordQuery query2 = createQuery("MySimpleRecord", Query.not(Query.field("num_value_2").equalsValue(2)));
        RecordQuery query3 = createQuery("MySimpleRecord", Query.not(Query.field("num_value_2").equalsParameter("3")));

        assertHash(query1, -2009449196, -1665088707);
        assertHash(query2, -2009419405, -1665088707);
        // Note that value and parameter hashes are not the same
        assertHash(query3, -1643618471, -1791883098);
    }

    @Test
    public void testNotGtFilter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord", Query.not(Query.field("num_value_2").greaterThan(1)));
        RecordQuery query2 = createQuery("MySimpleRecord", Query.not(Query.field("num_value_2").greaterThan(2)));

        assertHash(query1, 1925758139, -568315210);
        assertHash(query2, 1925787930, -568315210);
    }

    @Test
    public void testRank() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord", Query.rank("num_value_2").equalsValue(2L));
        RecordQuery query2 = createQuery("MySimpleRecord", Query.rank("num_value_2").equalsValue(3L));

        assertHash(query1, -70775662, 273555036);
        assertHash(query2, -70745871, 273555036);
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

        assertHash(query1, 1056724947, -897358891);
        assertHash(query2, 1057648468, -897358891);
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

        assertHash(query1, -1617571134, -429556342);
        assertHash(query2, -1616647613, -429556342);
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

        assertHash(query1, -1326035963, 593786046);
        assertHash(query2, 540891455, 593786046);
        assertHash(query3, 541814976, 593786046);
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

        assertHash(query1, 260234563, -1605161468);
        assertHash(query2, 262141187, -1605161468);
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

        assertHash(query1, -1307253487, 1122317778);
        assertHash(query2, -1305346863, 1122317778);
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

        assertHash(query1, 509275846, -207710934);
        assertHash(query2, -1298178617, -207710934);
    }

    @Test
    public void coveringIndex() throws Exception {
        // Note how the name field needs to be repeated in the value because it can't be recovered from an index
        // entry after transformation to a collation key.
        final KeyExpression collateKey1 = function("collate_jre", concat(field("str_value_indexed"), value("da_DK")));
        final KeyExpression collateKey2 = function("collate_jre", concat(field("str_value_indexed"), value("en_US")));
        final KeyExpression indexKey1 = keyWithValue(concat(collateKey1, field("str_value_indexed")), 1);
        final KeyExpression indexKey2 = keyWithValue(concat(collateKey2, field("str_value_indexed")), 1);
        final RecordMetaDataHook hook = md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.addIndex("MySimpleRecord", "collated_name1", indexKey1);
            md.addIndex("MySimpleRecord", "collated_name2", indexKey2);
        };
        runHook(hook);
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.keyExpression(collateKey1).lessThan("a"),
                null,
                Collections.singletonList(field("str_value_indexed")));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.keyExpression(collateKey2).lessThan("b"),
                null,
                Collections.singletonList(field("str_value_indexed")));

        assertHash(query1, 1814534013, -313391822);
        assertHash(query2, -1718938913, -313391822);
    }

    @Test
    public void compareParameter() throws Exception {
        final KeyExpression key1 = function("collate_jre", concat(field("str_value_indexed"), value("de_DE")));
        final KeyExpression key2 = function("collate_jre", concat(field("str_value_indexed"), value("en_EN")));
        final RecordMetaDataHook hook = md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.addIndex("MySimpleRecord", "collated_name1", key1);
            md.addIndex("MySimpleRecord", "collated_name2", key2);
        };
        runHook(hook);
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.keyExpression(key1).equalsParameter("name"),
                null,
                Collections.singletonList(field("str_value_indexed")));
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.keyExpression(key2).equalsParameter("no-name"),
                null,
                Collections.singletonList(field("str_value_indexed")));

        assertHash(query1, 334321807, 2089264010);
        assertHash(query2, -1234725093, 2089264010);
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

        assertHash(query1, -751848063, 1019582585);
        assertHash(query2, -549984247, 1019582585);
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

        assertHash(query1, -751848063, 1019582585);
        assertHash(query2, -549984247, 1019582585);
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

        assertHash(query1, -920276680, 1306898425);
        assertHash(query2, -918370056, 1306898425);
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

        assertHash(query1, 1254039933, 1598221676);
        assertHash(query2, 1254069724, 1598221676);
    }

    @Test
    public void testInQueryNoIndex() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.field("num_value_2").in(asList(0, 2)),
                null, null);
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.field("num_value_2").in(asList(1, 3)),
                null, null);

        assertHash(query1, -1564723747, -936101258);
        assertHash(query2, -1535171075, -936101258);
    }

    @Test
    public void testInQueryNoIndexWithParameter() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.field("num_value_2").in("valuesThree"),   // num_value_2 is i%3
                null, null);
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.field("num_value_2").in("valuesFour"),   // num_value_2 is i%3
                null, null);

        assertHash(query1, -1521180076, 1554768926);
        assertHash(query2, 998194952, 1554768926);
    }

    @Test
    public void testInQueryIndex() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.field("num_value_3_indexed").in(asList(1, 2, 3, 4)),
                null, null);
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.field("num_value_3_indexed").in(asList(5, 6)),
                null, null);

        assertHash(query1, 88717328, -193254231);
        assertHash(query2, -675036881, -193254231);
    }

    @Test
    public void testNotInQuery() throws Exception {
        RecordQuery query1 = createQuery("MySimpleRecord",
                Query.not(Query.field("num_value_2").in(asList(0, 2))),
                null, null);
        RecordQuery query2 = createQuery("MySimpleRecord",
                Query.not(Query.field("num_value_2").in(asList(1, 3))),
                null, null);

        assertHash(query1, 117965631, 746588120);
        assertHash(query2, 147518303, 746588120);
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

        assertHash(query1, 1728588239, 269491435);
        assertHash(query2, -42098996, 269491435);
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

        assertHash(query1, -398367993, -97516745);
        assertHash(query2, 544783406, -97516745);
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

        assertHash(query1, 46244221, 1603894354);
        assertHash(query2, 989514784, 1603894354);
    }


    private void assertHash(final RecordQuery query, final int withLiterals, final int withoutLiterals) {
        assertEquals(withLiterals, query.queryHash(QueryHashKind.STRUCTURAL_WITH_LITERALS));
        assertEquals(withoutLiterals, query.queryHash(QueryHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
