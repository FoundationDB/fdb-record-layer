/*
 * FDBComparatorPlanAbortEnabledTest.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class FDBComparatorPlanAbortEnabledTest extends FDBComparatorPlanTest {
    @BeforeEach
    void setup() {
        this.setAbortOnComparisonFailure(true);
    }

    @Override
    protected void assertDifferentPlans(final RecordQueryPlan planUnderTest, final int numValue2, final int atMostDiscarded, final int totalCount) throws Exception {
        // In the case where the abort flag is enabled, the plan execution should fail and throw an exception
        Assertions.assertThrows(RecordCoreException.class, () -> querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(numValue2)),
                context -> assertDiscardedAtMost(atMostDiscarded, context)));
    }

    @Override
    protected RecordCursorResult<FDBQueriedRecord<Message>> assertDifferentPlansWithContinuation(
            final RecordQueryPlan planUnderTest,
            final byte[] continuation,
            final int returnedRowLimit,
            final int scannedRowLimit,
            final int numRecords,
            final int numValue2,
            final int atMostDiscarded,
            final boolean hasNext,
            final RecordCursor.NoNextReason noNextReason) throws Throwable {
        // In the case where the abort flag is enabled, the plan execution should fail and throw an exception
        Assertions.assertThrows(RecordCoreException.class, () -> querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, executePropertiesFor(returnedRowLimit, scannedRowLimit),
                count -> assertThat(count, is(numRecords)),
                record -> assertThat(record.getNumValue2(), is(numValue2)),
                context -> assertDiscardedAtMost(atMostDiscarded, context)));
        return null;
    }
}
