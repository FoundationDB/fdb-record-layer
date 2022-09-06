/*
 * FDBNullableArrayFieldQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.TestRecords6Proto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FDBNullableArrayFieldQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that equality checks against repeated fields can scan an index scan with a FanType of Concatenate.
     */
    @Test
    void queryFilterByNullableArrayField() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openNestedWrappedArrayRecordStore(context);

            TestRecords4WrapperProto.RestaurantRecord record = TestRecords4WrapperProto.RestaurantRecord.newBuilder()
                    .setRestNo(1L)
                    .setName("r1")
                    .setCustomer(TestRecords4WrapperProto.StringList.newBuilder().addValues("c1").addValues("c2"))
                    .build();
            recordStore.saveRecord(record);
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantRecord")
                .setFilter(Query.field("name").equalsValue("r1"))
                .build();

        // Index(s1$concat [[[aaa, bbb]],[[aaa, bbb]]])
        RecordQueryPlan plan = planner.plan(query);
        /*
        assertThat(plan, indexScan(allOf(indexName("s1$concat"), bounds(hasTupleString("[[[aaa, bbb]],[[aaa, bbb]]]")))));
        assertEquals(2088320916, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1316657522, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(2118586752, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        */
        assertEquals(Arrays.asList("c1", "c2"), fetchResultValues(plan,
                TestRecords4WrapperProto.RestaurantRecord.CUSTOMER_FIELD_NUMBER,
                this::openNestedWrappedArrayRecordStore,
                TestHelpers::assertDiscardedNone));
    }

}
