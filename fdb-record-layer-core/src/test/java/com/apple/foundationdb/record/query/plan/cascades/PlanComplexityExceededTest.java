/*
 * PlanComplexityExceededTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanComplexityException;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;

/**
 * Test the "plan too complex" settings for the cascades planner.
 */
@Tag(Tags.RequiresFDB)
public class PlanComplexityExceededTest extends FDBRecordStoreQueryTestBase {

    private CascadesPlanner cascadesPlanner;

    @BeforeEach
    void setup() throws Exception {
        setUseCascadesPlanner(true);
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        cascadesPlanner = (CascadesPlanner)planner;
    }

    @Test
    void testPlanSucceeds() {
        createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1));
    }

    @Test
    void testPlanQueueTooLarge() {
        cascadesPlanner.setConfiguration(RecordQueryPlannerConfiguration.builder().setMaxTaskQueueSize(1).build());
        RecordQueryPlanComplexityException err = Assertions.assertThrows(RecordQueryPlanComplexityException.class,
                () -> createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1)));
        assertThat(err.getLogInfo(), hasEntry(LogMessageKeys.MAX_TASK_QUEUE_SIZE.toString(), 1));
        assertThat(err.getLogInfo(), hasKey(LogMessageKeys.TASK_QUEUE_SIZE.toString()));
    }

    @Test
    void testPlanTooManyTasks() {
        cascadesPlanner.setConfiguration(RecordQueryPlannerConfiguration.builder().setMaxTotalTaskCount(1).build());
        RecordQueryPlanComplexityException err = Assertions.assertThrows(RecordQueryPlanComplexityException.class,
                () -> createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1)));
        assertThat(err.getLogInfo(), hasEntry(LogMessageKeys.MAX_TASK_COUNT.toString(), 1));
        assertThat(err.getLogInfo(), hasKey(LogMessageKeys.TASK_COUNT.toString()));
    }

    @Test
    void testPlanMatchesRuleCallTooManyTimes() {
        cascadesPlanner.setConfiguration(RecordQueryPlannerConfiguration.builder().setMaxNumMatchesPerRuleCall(2).build());
        RecordQueryPlanComplexityException err = Assertions.assertThrows(RecordQueryPlanComplexityException.class,
                () -> createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1)));
        assertThat(err.getLogInfo(), hasEntry(LogMessageKeys.MAX_RULE_MATCHES_COUNT.toString(), 2));
        assertThat(err.getLogInfo(), hasKey(LogMessageKeys.RULE_MATCHES_COUNT.toString()));
        assertThat(err.getLogInfo(), hasKey(LogMessageKeys.RULE.toString()));
    }

    private RecordQueryPlan createPlan(String recordType, QueryComponent filter) {
        return createPlan(recordType, filter, null);
    }

    private RecordQueryPlan createPlan(String recordType, QueryComponent filter, KeyExpression sort) {
        return createPlan(recordType, filter, sort, null);
    }

    private RecordQueryPlan createPlan(String recordType, QueryComponent filter, KeyExpression sort, List<KeyExpression> requiredResults) {
        return createPlan(Collections.singletonList(recordType), filter, sort, requiredResults);
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
        return planQuery(builder.build());
    }

}
