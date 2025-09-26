/*
 * QueryPlanFullySortedTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerRegistryImpl;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link com.apple.foundationdb.record.query.plan.plans.QueryPlan#isStrictlySorted}.
 */
@Tag(Tags.RequiresFDB)
public class QueryPlanFullySortedTest extends FDBRecordStoreQueryTestBase {

    RecordMetaData metaData;
    QueryPlanner planner;

    protected void setup(@Nullable RecordMetaDataHook hook) {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        if (hook != null) {
            hook.apply(builder);
        }
        metaData = builder.getRecordMetaData();
        planner = isUseCascadesPlanner() ?
                  new CascadesPlanner(metaData, new RecordStoreState(null, null), IndexMaintainerRegistryImpl.instance()) :
                  new RecordQueryPlanner(metaData, new RecordStoreState(null, null));
    }

    @DualPlannerTest
    public void unsorted() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("a"))
                .build();
        assertFalse(planQuery(planner, query).isStrictlySorted());
    }

    @DualPlannerTest
    public void singleSort() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(Key.Expressions.field("str_value_indexed"))
                .build();
        assertFalse(planQuery(planner, query).isStrictlySorted());
    }

    @DualPlannerTest
    public void primaryKey() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(Key.Expressions.field("rec_no"))
                .build();
        assertTrue(planQuery(planner, query).isStrictlySorted());
    }

    private static void compoundPrimaryKey(@Nonnull RecordMetaDataBuilder metaData) {
        metaData.getRecordType("MySimpleRecord").setPrimaryKey(Key.Expressions.concatenateFields("num_value_2", "rec_no"));
        metaData.addIndex("MySimpleRecord", "multi", Key.Expressions.concatenateFields("num_value_2", "num_value_3_indexed"));
    }

    @DualPlannerTest
    public void primaryKeyPartial() {
        setup(QueryPlanFullySortedTest::compoundPrimaryKey);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .setSort(Key.Expressions.field("num_value_3_indexed"))
                .build();
        assertFalse(planQuery(planner, query).isStrictlySorted());
    }

    @DualPlannerTest
    public void primaryKeyFull() {
        setup(QueryPlanFullySortedTest::compoundPrimaryKey);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .setSort(Key.Expressions.concatenateFields("num_value_3_indexed", "rec_no"))
                .build();
        final var plan = planQuery(planner, query);
        assertTrue(plan.isStrictlySorted());
    }

    @DualPlannerTest
    public void intersection() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_3_indexed").equalsValue(1), Query.field("str_value_indexed").equalsValue("a")))
                .setSort(Key.Expressions.field("rec_no"))
                .build();
        assertTrue(planQuery(planner, query).isStrictlySorted());
    }

    @DualPlannerTest
    public void union() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(Query.field("num_value_3_indexed").equalsValue(1), Query.field("str_value_indexed").equalsValue("a")))
                .setSort(Key.Expressions.field("rec_no"))
                .build();
        assertTrue(planQuery(planner, query).isStrictlySorted());
    }

    @DualPlannerTest
    public void sortingUnion() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(Query.field("num_value_3_indexed").equalsValue(1), Query.field("num_value_3_indexed").equalsValue(3)))
                .setSort(Key.Expressions.field("num_value_3_indexed"))
                .build();
        assertFalse(planQuery(planner, query).isStrictlySorted());
    }

    @DualPlannerTest
    public void sortingUnionFull() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(Query.field("num_value_3_indexed").equalsValue(1), Query.field("num_value_3_indexed").equalsValue(3)))
                .setSort(Key.Expressions.concatenateFields("num_value_3_indexed", "rec_no"))
                .build();
        assertTrue(planQuery(planner, query).isStrictlySorted());
    }

    private static void compoundUnique(@Nonnull RecordMetaDataBuilder metaData) {
        metaData.addIndex("MySimpleRecord", new Index("multi_unique",
                Key.Expressions.concatenateFields("num_value_2", "num_value_unique"),
                IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
    }

    @DualPlannerTest
    public void sortingUniquePrefix() {
        setup(QueryPlanFullySortedTest::compoundUnique);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(Key.Expressions.field("num_value_2"))
                .build();
        assertFalse(planQuery(planner, query).isStrictlySorted());
    }

    @DualPlannerTest
    public void sortingUnique() {
        setup(QueryPlanFullySortedTest::compoundUnique);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_2").equalsValue(1), Query.field("num_value_unique").lessThan(10)))
                .setSort(Key.Expressions.field("num_value_unique"))
                .build();
        assertTrue(planQuery(planner, query).isStrictlySorted());
    }


}
