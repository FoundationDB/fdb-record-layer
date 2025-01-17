/*
 * ComposedBitmapIndexMatcher.java
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

import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A plan matcher for {@link ComposedBitmapIndexQueryPlan}.
 */
public class ComposedBitmapIndexMatcher extends TypeSafeMatcher<RecordQueryPlan> {
    @Nonnull
    private final Matcher<ComposedBitmapIndexQueryPlan.ComposerBase> composerMatcher;
    @Nonnull
    private final List<Matcher<RecordQueryPlan>> childMatchers;

    public ComposedBitmapIndexMatcher(@Nonnull Matcher<ComposedBitmapIndexQueryPlan.ComposerBase> composerMatcher,
                                      @Nonnull List<Matcher<RecordQueryPlan>> childMatchers) {
        this.composerMatcher = composerMatcher;
        this.childMatchers = childMatchers;
    }

    @Override
    public boolean matchesSafely(@Nonnull RecordQueryPlan plan) {
        if (!(plan instanceof ComposedBitmapIndexQueryPlan)) {
            return false;
        }
        ComposedBitmapIndexQueryPlan composedBitmapPlan = (ComposedBitmapIndexQueryPlan)plan;
        if (!composerMatcher.matches(composedBitmapPlan.getComposer())) {
            return false;
        }
        List<RecordQueryCoveringIndexPlan> childPlans = composedBitmapPlan.getIndexPlans();
        if (childPlans.size() != childMatchers.size()) {
            return false;
        }
        for (int i = 0; i < childMatchers.size(); i++) {
            if (!childMatchers.get(i).matches(childPlans.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("ComposedBitmapIndexQueryPlan(");
        composerMatcher.describeTo(description);
        for (Matcher<RecordQueryPlan> childMatcher : childMatchers) {
            description.appendText(", ");
            childMatcher.describeTo(description);
        }
        description.appendText(")");
    }
}
