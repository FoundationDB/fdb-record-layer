/*
 * FDBComparatorPlanAbortDisabledTest.java
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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;

public class FDBComparatorPlanAbortDisabledTest extends FDBComparatorPlanTest {
    @BeforeEach
    void setup() {
        this.setAbortOnComparisonFailure(false);
    }

    @Override
    protected void assertDifferentPlans(final RecordQueryPlan planUnderTest, final int numValue2, final int atMostDiscarded, final int totalCount) throws Exception {
        // For the case where the abort flag is disabled, the comparator plan will swallow the failure (issue a log only)
        // and continue, so the result should be indistinguishable from when the comparison succeeds
        assertSamePlans(planUnderTest, numValue2, atMostDiscarded, totalCount);
    }

    @Override
    protected RecordCursorResult<FDBQueriedRecord<Message>> assertDifferentPlansWithContinuation(final RecordQueryPlan planUnderTest, final byte[] continuation, final int returnedRowLimit, final int scannedRowLimit, final int numRecords, final int numValue2, final int atMostDiscarded, final boolean hasNext, final RecordCursor.NoNextReason noNextReason) throws Throwable {
        // For the case where the abort flag is disabled, the comparator plan will swallow the failure (issue a log only)
        // and continue, so the result should be indistinguishable from when the comparison succeeds
        return assertSamePlansWithContinuation(planUnderTest, continuation, returnedRowLimit, scannedRowLimit, numRecords, numValue2, atMostDiscarded, hasNext, noNextReason);
    }
}
