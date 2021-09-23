/*
 * QueryPlanHashTest.java
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

package com.apple.foundationdb.record.query.plan.debug;

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.debug.LogPlanDebugger;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests for PlanHash on query plans.
 */
@Tag(Tags.RequiresFDB)
public class LogPlanDebuggerTest extends FDBRecordStoreQueryTestBase {

    @BeforeEach
    public void setup() throws Exception {
        setUseRewritePlanner(true);
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
    }

    @Test
    public void testDebuggerAllActions() throws Exception {
        Debugger.setDebugger(new LogPlanDebugger());
        Debugger.setup();

        createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1));
    }

    @Test
    public void testDebuggerSomeActions() throws Exception {
        Debugger.setDebugger(new LogPlanDebugger(LogPlanDebugger.DebuggerAction.ON_SETUP, LogPlanDebugger.DebuggerAction.ON_QUERY, LogPlanDebugger.DebuggerAction.ON_DONE));
        Debugger.setup();

        createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1));
    }

    @Test
    public void testNoDebugger() throws Exception {
        createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1));
    }

    @Test
    public void testRemoveDebugger() throws Exception {
        Debugger.setDebugger(new LogPlanDebugger());
        Debugger.setup();
        Debugger.removeDebugger();

        createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1));
    }

    private RecordQueryPlan createPlan(String recordType, QueryComponent filter) {
        return createPlan(Collections.singletonList(recordType), filter, null, null);
    }

    private RecordQueryPlan createPlan(List<String> recordTypes, QueryComponent filter, KeyExpression sort, List<KeyExpression> requiredResults) {
        RecordQuery.Builder builder = RecordQuery.newBuilder()
                .setRecordTypes(recordTypes)
                .setFilter(filter);
        if (sort != null) {
            builder.setSort(sort, false);
        }
        if (requiredResults != null) {
            builder.setRequiredResults(requiredResults);
        }
        return planner.plan(builder.build());
    }
}
