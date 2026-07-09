/*
 * ScanMatcher.java
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

package com.apple.foundationdb.record.query.plan.match;

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;

import static org.hamcrest.Matchers.anything;

/**
 * A plan matcher for {@link RecordQueryScanPlan}.
 */
public class ScanMatcher extends TypeSafeMatcher<RecordQueryPlan> {
    @Nonnull
    private final Matcher<? super RecordQueryScanPlan> planMatcher;

    public ScanMatcher() {
        this(anything());
    }

    public ScanMatcher(@Nonnull Matcher<? super RecordQueryScanPlan> planMatcher) {
        this.planMatcher = planMatcher;
    }

    @Override
    public boolean matchesSafely(@Nonnull RecordQueryPlan plan) {
        return plan instanceof RecordQueryScanPlan &&
                planMatcher.matches(plan);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Scan(");
        planMatcher.describeTo(description);
        description.appendText(")");
    }
}
