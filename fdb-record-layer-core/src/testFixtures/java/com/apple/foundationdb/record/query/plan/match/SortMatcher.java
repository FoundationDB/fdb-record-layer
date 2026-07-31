/*
 * FilterMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortKey;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortPlan;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;

/**
 * Plan matcher for a sort applied to a child plan matcher.
 */
public class SortMatcher extends PlanMatcherWithChild {
    @Nonnull
    private final Matcher<RecordQuerySortKey> keyMatcher;

    public SortMatcher(@Nonnull Matcher<RecordQuerySortKey> keyMatcher, @Nonnull Matcher<RecordQueryPlan> childMatcher) {
        super(childMatcher);
        this.keyMatcher = keyMatcher;
    }

    @Override
    public boolean matchesSafely(@Nonnull RecordQueryPlan plan) {
        final RecordQuerySortKey key;
        if (plan instanceof RecordQuerySortPlan) {
            key = ((RecordQuerySortPlan)plan).getKey();
        } else {
            return false;
        }
        return keyMatcher.matches(key) && super.matchesSafely(plan);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Sort(");
        keyMatcher.describeTo(description);
        description.appendText("; ");
        super.describeTo(description);
        description.appendText(")");
    }

}
