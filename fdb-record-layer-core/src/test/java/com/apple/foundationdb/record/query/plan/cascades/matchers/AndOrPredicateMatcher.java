/*
 * AndOrPredicateMatcher.java
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

package com.apple.foundationdb.record.query.plan.cascades.matchers;

import com.apple.foundationdb.record.query.plan.cascades.predicates.AndOrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * A base class with common logic for the {@link AndPredicateMatcher} and {@link AndOrPredicateMatcher}.
 */
abstract class AndOrPredicateMatcher extends TypeSafeMatcher<QueryPredicate> {
    @Nonnull
    private final Matcher<Collection<QueryPredicate>> childrenMatchers;

    protected AndOrPredicateMatcher(@Nonnull Matcher<Collection<QueryPredicate>> childrenMatchers) {
        this.childrenMatchers = childrenMatchers;
    }

    @Override
    protected boolean matchesSafely(QueryPredicate predicate) {
        if (!(predicate instanceof AndOrPredicate)) {
            return false;
        }
        return childrenMatchers.matches(((AndOrPredicate)predicate).getChildren());
    }

    @Override
    public void describeTo(Description description) {
        childrenMatchers.describeTo(description);
    }
}
