/*
 * FDBCollateQueryTest.java
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
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.expressions.CollateFunctionKeyExpressionFactoryJRE;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests related to planning and executing queries with string collation.
 */
@Tag(Tags.RequiresFDB)
public abstract class FDBCollateQueryTest extends FDBRecordStoreQueryTestBase {

    @Nonnull
    protected final String collateFunctionName;

    protected FDBCollateQueryTest(@Nonnull String collateFunctionName) {
        this.collateFunctionName = collateFunctionName;
    }

    /**
     * Test with JRE collators.
     */
    @SuppressWarnings("checkstyle:abbreviationaswordinname") // Allow JRE here.
    public static class FDBCollateJREQueryTest extends FDBCollateQueryTest {
        public FDBCollateJREQueryTest() {
            super(CollateFunctionKeyExpressionFactoryJRE.FUNCTION_NAME);
        }
    }

    protected static final String[] NAMES = {
        "Ampère", "Gauß", "Ørsted", "Faraday", "Ångström", "Stokes", "Maxwell"
    };

    protected void loadNames(RecordMetaDataHook hook) throws Exception {
        loadNames(NAMES, hook);
    }

    protected void loadNames(String[] names, RecordMetaDataHook hook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            for (int i = 0; i < names.length; i++) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(100 + i)
                        .setStrValueIndexed(names[i])
                        .build();
                recordStore.saveRecord(record);
            }
            commit(context);
        }
    }

    protected List<String> queryNames(RecordQuery query, RecordMetaDataHook hook, String... bindings) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            Bindings.Builder builder = Bindings.newBuilder();
            for (int i = 0; i < bindings.length; i += 2) {
                builder.set(bindings[i], bindings[i + 1]);
            }
            return recordStore.planQuery(query).execute(recordStore, EvaluationContext.forBindings(builder.build())).map(r -> {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .mergeFrom(r.getRecord())
                        .build();
                return record.getStrValueIndexed();
            }).asList().join();
        }
    }

    protected static final KeyExpression NAME_FIELD = field("str_value_indexed");

    protected void sortOnly(String locale, String... expected) throws Exception {
        sortOnly(locale, NAMES, expected);
    }

    protected void sortOnly(String locale, String[] names, String... expected) throws Exception {
        final KeyExpression key;
        final RecordMetaDataHook hook;
        if (locale == null) {
            key = NAME_FIELD;
            hook = NO_HOOK;
        } else {
            key = function(collateFunctionName, concat(NAME_FIELD, value(locale)));
            hook = md -> {
                md.removeIndex("MySimpleRecord$str_value_indexed");
                md.addIndex("MySimpleRecord", "collated_name", key);
            };
        }
        loadNames(names, hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(key)
                .setRequiredResults(Arrays.asList(NAME_FIELD))
                .build();
        final List<String> actual = queryNames(query, hook);
        assertEquals(Arrays.asList(expected), actual);
    }

    @Test
    public void sortOnlyUnicode() throws Exception {
        sortOnly(null, "Ampère", "Faraday", "Gauß", "Maxwell", "Stokes", "Ångström", "Ørsted");
    }

    @Test
    public void sortOnlyDa() throws Exception {
        sortOnly("da_DK", "Ampère", "Faraday", "Gauß", "Maxwell", "Stokes", "Ørsted", "Ångström");
    }

    @Test
    public void sortOnlyFr() throws Exception {
        if (CollateFunctionKeyExpressionFactoryJRE.FUNCTION_NAME.equals(collateFunctionName)) {
            // Does not recognize Ø as an accented O.
            sortOnly("fr_FR", "Ampère", "Ångström", "Faraday", "Gauß", "Maxwell", "Stokes", "Ørsted");
        } else {
            sortOnly("fr_FR", "Ampère", "Ångström", "Faraday", "Gauß", "Maxwell", "Ørsted", "Stokes");
        }
    }

    @Test
    public void noIndex() throws Exception {
        loadNames(NO_HOOK);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.keyExpression(function(collateFunctionName, NAME_FIELD)).equalsValue("ampere"))
                .setRequiredResults(Arrays.asList(NAME_FIELD))
                .build();
        final List<String> expected = Arrays.asList("Ampère");
        final List<String> actual = queryNames(query, NO_HOOK);
        assertEquals(expected, actual);
    }

    @Test
    public void rangeScan() throws Exception {
        final KeyExpression key = function(collateFunctionName, concat(NAME_FIELD, value("da_DK")));
        final RecordMetaDataHook hook = md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.addIndex("MySimpleRecord", "collated_name", key);
        };
        loadNames(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.keyExpression(key).greaterThan("Z"))
                .setRequiredResults(Arrays.asList(NAME_FIELD))
                .build();
        final List<String> actual = queryNames(query, hook);
        final List<String> expected = Arrays.asList("Ørsted", "Ångström");
        assertEquals(expected, actual);
        RecordQueryPlan plan = planQuery(query);
        assertThat(plan, indexScan("collated_name"));
    }

    @Test
    public void coveringIndex() throws Exception {
        // Note how the name field needs to be repeated in the value because it can't be recovered from an index
        // entry after transformation to a collation key.
        final KeyExpression collateKey = function(collateFunctionName, concat(NAME_FIELD, value("da_DK")));
        final KeyExpression indexKey = keyWithValue(concat(collateKey, NAME_FIELD), 1);
        final RecordMetaDataHook hook = md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.addIndex("MySimpleRecord", "collated_name", indexKey);
        };
        loadNames(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.keyExpression(collateKey).lessThan("B"))
                .setRequiredResults(Arrays.asList(NAME_FIELD))
                .build();
        final List<String> actual = queryNames(query, hook);
        final List<String> expected = Arrays.asList("Ampère");
        assertEquals(expected, actual);
        RecordQueryPlan plan = planQuery(query);
        assertThat(plan, coveringIndexScan(indexScan("collated_name")));
    }

    @Test
    public void compareParameter() throws Exception {
        final KeyExpression key = function(collateFunctionName, concat(NAME_FIELD, value("de_DE")));
        final RecordMetaDataHook hook = md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.addIndex("MySimpleRecord", "collated_name", key);
        };
        loadNames(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.keyExpression(key).equalsParameter("name"))
                .setRequiredResults(Arrays.asList(NAME_FIELD))
                .build();
        final List<String> actual = queryNames(query, hook, "name", "gauss");
        final List<String> expected = Arrays.asList("Gauß");
        assertEquals(expected, actual);
        RecordQueryPlan plan = planQuery(query);
        assertThat(plan, indexScan(allOf(indexName("collated_name"), bounds(hasTupleString("[EQUALS " + collateFunctionName + "($name)]")))));
    }

}
