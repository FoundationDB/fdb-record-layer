/*
 * QueryPredicateDescendantMatcher.java
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

import com.apple.foundationdb.record.query.plan.cascades.predicates.AndOrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;

/**
 * Matches any (non-strict) descendant of the query component with the given matcher. Note that a component is its own
 * non-strict descendant.
 */
public class QueryPredicateDescendantMatcher extends TypeSafeMatcher<QueryPredicate> {
    @Nonnull
    private final Matcher<QueryPredicate> matcher;

    public QueryPredicateDescendantMatcher(@Nonnull Matcher<QueryPredicate> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matchesSafely(@Nonnull QueryPredicate component) {
        if (matcher.matches(component)) {
            return true;
        }
        if (component instanceof NotPredicate) {
            return matchesSafely(((NotPredicate)component).getChild());
        }
        if (component instanceof AndOrPredicate) {
            return ((AndOrPredicate)component).getChildren().stream().anyMatch(this::matchesSafely);
        }
        return false;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("..");
        matcher.describeTo(description);
    }
}
