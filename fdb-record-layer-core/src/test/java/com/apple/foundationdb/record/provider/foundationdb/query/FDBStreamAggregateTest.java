/*
 * FDBStreamAggregateTest.java
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregatePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.AggregateValues;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests related to planning and executing queries with string collation.
 */
@Tag(Tags.RequiresFDB)
public class FDBStreamAggregateTest extends FDBRecordStoreQueryTestBase {

    @BeforeEach
    public void setup() throws Exception {
        populateDB(5);
    }

    @Test
    public void noAggregateGroupByNone() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            RecordQueryPlan plan = new AggregatePlanBuilder()
                    .build();

            List<FDBQueriedRecord<Message>> result = execute(plan);
        }
    }

    @Test
    public void aggregateOneGroupByOne() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder();
            RecordQueryPlan plan = builder
                    .withAggregateValue("num_value_2", "SumInteger")
                    .withGroupCriterion("num_value_3_indexed")
                    .build();

            List<FDBQueriedRecord<Message>> result = execute(plan);
        }
    }

    @Test
    public void aggregateOneGroupByNone() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            RecordQueryPlan plan = new AggregatePlanBuilder()
                    .withAggregateValue("num_value_2", "SumInteger")
                    .build();

            List<FDBQueriedRecord<Message>> result = execute(plan);
        }
    }

    @Test
    public void noAggregateGroupByOne() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            RecordQueryPlan plan = new AggregatePlanBuilder()
                    .withGroupCriterion("num_value_3_indexed")
                    .build();

            List<FDBQueriedRecord<Message>> result = execute(plan);
        }
    }

    @Test
    public void aggregateOneGroupByTwo() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            RecordQueryPlan plan = new AggregatePlanBuilder()
                    .withAggregateValue("num_value_2", "SumInteger")
                    .withGroupCriterion("num_value_3_indexed")
                    .withGroupCriterion("str_value_indexed")
                    .build();

            List<FDBQueriedRecord<Message>> result = execute(plan);
        }
    }

    @Test
    public void aggregateTwoGroupByTwo() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            RecordQueryPlan plan = new AggregatePlanBuilder()
                    .withAggregateValue("num_value_2", "SumInteger")
                    .withAggregateValue("num_value_2", "MinInteger")
                    .withGroupCriterion("num_value_3_indexed")
                    .withGroupCriterion("str_value_indexed")
                    .build();

            List<FDBQueriedRecord<Message>> result = execute(plan);
        }
    }

    @Test
    public void aggregateThreeGroupByTwo() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            RecordQueryPlan plan = new AggregatePlanBuilder()
                    .withAggregateValue("num_value_2", "SumInteger")
                    .withAggregateValue("num_value_2", "MinInteger")
                    .withAggregateValue("num_value_2", "AvgInteger")
                    .withGroupCriterion("num_value_3_indexed")
                    .withGroupCriterion("str_value_indexed")
                    .build();

            List<FDBQueriedRecord<Message>> result = execute(plan);
        }
    }

    private void populateDB(final int numRecords) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            for (int i = 0; i <= numRecords; i++) {
                recBuilder.setRecNo(i);
                recBuilder.setNumValue2(i);
                recBuilder.setNumValue3Indexed(i / 2); // some field that changes every 2nd record
                recBuilder.setStrValueIndexed(Integer.toString(i / 3)); // some field that changes every 3rd record
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
    }

    @Nonnull
    private List<FDBQueriedRecord<Message>> execute(final RecordQueryPlan plan) throws InterruptedException, java.util.concurrent.ExecutionException {
        Bindings bindings = Bindings.newBuilder().build();
        return plan.execute(recordStore, EvaluationContext.forBindings(bindings)).asList().get();
    }

    private static class AggregatePlanBuilder {
        private final Quantifier.Physical quantifier;
        private final List<AggregateValue<?>> aggregateValues;
        private final List<Value> groupValues;

        public AggregatePlanBuilder() {
            this.quantifier = createBaseQuantifier();
            aggregateValues = new ArrayList<>();
            groupValues = new ArrayList<>();
        }

        public AggregatePlanBuilder withAggregateValue(final String fieldName, final String aggregateType) {
            this.aggregateValues.add(createAggregateValue(createValue(fieldName), aggregateType));
            return this;
        }

        public AggregatePlanBuilder withGroupCriterion(final String fieldName) {
            this.groupValues.add(createValue(fieldName));
            return this;
        }

        public RecordQueryPlan build() {
            return new RecordQueryStreamingAggregatePlan(quantifier, groupValues, aggregateValues);
        }

        private Value createValue(final String fieldName) {
            return new FieldValue(QuantifiedColumnValue.of(quantifier.getAlias(), 1), Collections.singletonList(fieldName));
        }

        private Quantifier.Physical createBaseQuantifier() {
            RecordQueryScanPlan scanPlan = new RecordQueryScanPlan(ImmutableSet.of("MySimpleRecord", "MyOtherRecord"), ScanComparisons.EMPTY, false);
            RecordQueryTypeFilterPlan filterPlan = new RecordQueryTypeFilterPlan(scanPlan, Collections.singleton("MySimpleRecord"));
            return Quantifier.physical(GroupExpressionRef.of(filterPlan));
        }

        private AggregateValue<?> createAggregateValue(Value value, String aggregateType) {
            switch (aggregateType) {
                case ("SumInteger"):
                    return new AggregateValues.SumInteger(value);
                case ("MinInteger"):
                    return new AggregateValues.MinInteger(value);
                case ("AvgInteger"):
                    return new AggregateValues.AvgInteger(value);
                default:
                    throw new IllegalArgumentException("Cannot parse function name " + aggregateType);
            }
        }
    }
}
