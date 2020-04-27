/*
 * FilterMatcher.java
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

import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicateFilterPlan;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.match.PredicateMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Plan matcher for a filter applied to a child plan matcher.
 */
public class FilterMatcher extends PlanMatcherWithChild {
    @Nonnull
    private final Matcher<QueryPredicate> predicateMatcher;

    public FilterMatcher(@Nonnull Matcher<QueryPredicate> predicateMatcher, @Nonnull Matcher<RecordQueryPlan> childMatcher) {
        super(childMatcher);
        this.predicateMatcher = predicateMatcher;
    }

    @Override
    public boolean matchesSafely(@Nonnull RecordQueryPlan plan) {
        final QueryPredicate predicate;
        if (plan instanceof RecordQueryFilterPlan) {
            predicate = ((RecordQueryFilterPlan)plan).getFilter()
                    .normalizeForPlanner(PredicateMatchers.BlankSource.INSTANCE, Function.identity());
        } else if (plan instanceof RecordQueryPredicateFilterPlan) {
            predicate = ((RecordQueryPredicateFilterPlan)plan).getFilter();
        } else {
            return false;
        }
        return predicateMatcher.matches(predicate) && super.matchesSafely(plan);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Filter(");
        predicateMatcher.describeTo(description);
        description.appendText("; ");
        super.describeTo(description);
        description.appendText(")");
    }

}
