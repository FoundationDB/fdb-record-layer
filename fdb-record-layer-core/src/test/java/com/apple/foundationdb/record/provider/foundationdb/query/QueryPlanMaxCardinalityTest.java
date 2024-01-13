/*
 * QueryPlanMaxCardinalityTest.java
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
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link com.apple.foundationdb.record.query.plan.plans.QueryPlan#maxCardinality}.
 */
@Tag(Tags.RequiresFDB)
public class QueryPlanMaxCardinalityTest extends FDBRecordStoreQueryTestBase {

    RecordMetaData metaData;
    QueryPlanner planner;

    protected void setup(@Nullable RecordMetaDataHook hook) {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        if (hook != null) {
            hook.apply(builder);
        }
        metaData = builder.getRecordMetaData();
        planner = new RecordQueryPlanner(metaData, new RecordStoreState(null, null));
    }

    @Test
    public void nonUniqueIndex() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("x"))
                .build();
        assertEquals(QueryPlan.UNKNOWN_MAX_CARDINALITY, planQuery(query).maxCardinality(metaData));
    }

    @Test
    public void uniqueIndex() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").equalsValue(1))
                .build();
        assertEquals(1, planQuery(query).maxCardinality(metaData));
    }

    @Test
    public void uniqueIndexInequality() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").lessThanOrEquals(1))
                .build();
        assertEquals(QueryPlan.UNKNOWN_MAX_CARDINALITY, planQuery(query).maxCardinality(metaData));
    }

    @Test
    public void uniqueIndexPartial() {
        setup(md -> {
            md.removeIndex("MySimpleRecord$num_value_unique");
            md.addIndex("MySimpleRecord", new Index("MySimpleIndex", Key.Expressions.concatenateFields("num_value_unique", "num_value_2"), IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        });
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").equalsValue(1))
                .build();
        assertEquals(QueryPlan.UNKNOWN_MAX_CARDINALITY, planQuery(query).maxCardinality(metaData));
    }

    @Test
    public void primaryKey() {
        setup(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("rec_no").equalsValue(1L))
                .build();
        assertEquals(1, planQuery(query).maxCardinality(metaData));
    }

    @Test
    public void primaryKeyPartial() {
        setup(md -> {
            md.getRecordType("MySimpleRecord").setPrimaryKey(Key.Expressions.concatenateFields("num_value_2", "rec_no"));
        });
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        assertEquals(QueryPlan.UNKNOWN_MAX_CARDINALITY, planQuery(query).maxCardinality(metaData));
    }

}
