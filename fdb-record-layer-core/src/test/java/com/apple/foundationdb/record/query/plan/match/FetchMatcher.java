/*
 * FetchMatcher.java
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

package com.apple.foundationdb.record.query.plan.match;

import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;

/**
 * Plan matcher for {@link RecordQueryFetchFromPartialRecordPlan} over a child matcher.
 */
public class FetchMatcher extends PlanMatcherWithChild {
    public FetchMatcher(@Nonnull final Matcher<RecordQueryPlan> childMatcher) {
        super(childMatcher);
    }

    @Override
    public boolean matchesSafely(@Nonnull final RecordQueryPlan plan) {
        return plan instanceof RecordQueryFetchFromPartialRecordPlan && super.matchesSafely(plan);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Fetch(");
        super.describeTo(description);
        description.appendText(")");
    }
}
