/*
 * OverscanIndexMatcher.java
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

package com.apple.foundationdb.record.query.plan.match;

import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryOverscanIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Plan matcher for the {@link RecordQueryOverscanIndexPlan}. Allows for test assertions to be made
 * both that a plan is of the correct type and that the underlying index plan furthermore
 * matches expectations.
 */
public class OverscanIndexMatcher extends TypeSafeMatcher<RecordQueryPlan> {
    private final Matcher<? super RecordQueryIndexPlan> indexPlanMatcher;

    public OverscanIndexMatcher(Matcher<? super RecordQueryIndexPlan> indexPlanMatcher) {
        this.indexPlanMatcher = indexPlanMatcher;
    }

    @Override
    protected boolean matchesSafely(final RecordQueryPlan item) {
        return item instanceof RecordQueryOverscanIndexPlan
               && indexPlanMatcher.matches(((RecordQueryOverscanIndexPlan)item).getIndexPlan());
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Overscan(");
        description.appendDescriptionOf(indexPlanMatcher);
        description.appendText(")");
    }
}
