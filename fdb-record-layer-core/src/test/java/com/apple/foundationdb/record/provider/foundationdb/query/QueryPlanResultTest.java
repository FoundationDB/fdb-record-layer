/*
 * QueryPlanResultTest.java
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

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.QueryPlanInfoKeys;
import com.apple.foundationdb.record.query.plan.QueryPlanResult;
import com.apple.test.Tags;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests for QueryPlanResult by cascades planner.
 */
@Tag(Tags.RequiresFDB)
public class QueryPlanResultTest extends FDBRecordStoreQueryTestBase {

    @BeforeEach
    public void setup() throws Exception {
        setUseCascadesPlanner(true);
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
    }

    @Test
    public void testSingleEqualsFilter() throws Exception {
        QueryPlanResult res = createPlan(Collections.singletonList("MySimpleRecord"), Query.field("num_value_2").equalsValue(1), null, null);
        Integer taskCount = res.getPlanInfo().get(QueryPlanInfoKeys.TOTAL_TASK_COUNT);
        Assertions.assertNotNull(taskCount);
        Assertions.assertTrue(taskCount > 0);
        Integer maxQueueSize = res.getPlanInfo().get(QueryPlanInfoKeys.MAX_TASK_QUEUE_SIZE);
        Assertions.assertNotNull(maxQueueSize);
        Assertions.assertTrue(maxQueueSize > 0);
    }

    private QueryPlanResult createPlan(List<String> recordTypes, QueryComponent filter, KeyExpression sort, List<KeyExpression> requiredResults) {
        RecordQuery.Builder builder = RecordQuery.newBuilder()
                .setRecordTypes(recordTypes)
                .setFilter(filter);
        if (sort != null) {
            builder.setSort(sort, false);
        }
        if (requiredResults != null) {
            builder.setRequiredResults(requiredResults);
        }
        return planner.planQuery(builder.build());
    }
}
