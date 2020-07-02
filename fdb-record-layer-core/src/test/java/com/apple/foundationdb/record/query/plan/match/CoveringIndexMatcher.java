/*
 * CoveringIndexMatcher.java
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

import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;

/**
 * A plan matcher for {@link RecordQueryCoveringIndexPlan}.
 */
public class CoveringIndexMatcher extends TypeSafeMatcher<RecordQueryPlan> {
    @Nonnull
    private final Matcher<? super RecordQueryPlanWithIndex> childMatcher;

    public CoveringIndexMatcher(@Nonnull Matcher<? super RecordQueryPlanWithIndex> childMatcher) {
        this.childMatcher = childMatcher;
    }

    @Override
    public boolean matchesSafely(@Nonnull RecordQueryPlan plan) {
        return plan instanceof RecordQueryCoveringIndexPlan &&
                childMatcher.matches(((RecordQueryCoveringIndexPlan)plan).getIndexPlan());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("CoveringIndex(");
        childMatcher.describeTo(description);
        description.appendText(")");
    }
}
