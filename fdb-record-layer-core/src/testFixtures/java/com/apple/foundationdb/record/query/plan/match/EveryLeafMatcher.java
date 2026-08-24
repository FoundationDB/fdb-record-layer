/*
 * EveryLeafMatcher.java
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
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;

/**
 * Matches only if *every* leaf (i.e. plan without a descendant) matches the given matcher.
 */
public class EveryLeafMatcher extends TypeSafeMatcher<RecordQueryPlan> {
    @Nonnull
    private final Matcher<RecordQueryPlan> matcher;

    public EveryLeafMatcher(@Nonnull Matcher<RecordQueryPlan> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matchesSafely(@Nonnull RecordQueryPlan plan) {
        if (plan.getChildren().isEmpty()) { // plan is a leaf
            return matcher.matches(plan);
        }
        return plan.getChildren().stream().allMatch(this::matchesSafely);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("every leaf ");
        matcher.describeTo(description);
    }
}
