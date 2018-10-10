/*
 * PlanMatcherWithChildren.java
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/**
 * A plan matcher with child matchers to apply to the plan children in order.
 */
public abstract class PlanMatcherWithChildren extends TypeSafeMatcher<RecordQueryPlan> {
    @Nonnull
    private final Collection<Matcher<RecordQueryPlan>> childMatchers;

    public PlanMatcherWithChildren(@Nonnull Collection<Matcher<RecordQueryPlan>> childMatchers) {
        this.childMatchers = childMatchers;
    }

    /**
     * Check if the given matchers can be matched to unique children.
     * Caution: this only checks that each given matcher can be matched onto a unique child; there may be children
     * that are not matched by any matcher, and this check will still pass!
     */
    @Override
    public boolean matchesSafely(@Nonnull RecordQueryPlan plan) {
        Collection<Matcher<RecordQueryPlan>> remaining = new HashSet<>(childMatchers);
        for (RecordQueryPlan child : plan.getChildren()) {
            for (Matcher<RecordQueryPlan> matcher : remaining) {
                if (matcher.matches(child)) {
                    remaining.remove(matcher);
                    break;
                }
            }
        }
        return remaining.isEmpty();
    }

    @Override
    public void describeTo(Description description) {
        Iterator<Matcher<RecordQueryPlan>> matchIterator = childMatchers.iterator();
        if (!matchIterator.hasNext()) {
            return;
        }

        matchIterator.next().describeTo(description);
        while (matchIterator.hasNext()) {
            description.appendText(", ");
            matchIterator.next().describeTo(description);
        }
    }
}
