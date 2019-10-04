/*
 * AndOrPredicateMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates.match;

import com.apple.foundationdb.record.query.predicates.AndOrPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/**
 * A base class with common logic for the {@link AndPredicateMatcher} and {@link AndOrPredicateMatcher}.
 */
abstract class AndOrPredicateMatcher extends TypeSafeMatcher<QueryPredicate> {
    @Nonnull
    private final Collection<Matcher<QueryPredicate>> childMatchers;

    protected AndOrPredicateMatcher(@Nonnull Collection<Matcher<QueryPredicate>> childMatchers) {
        this.childMatchers = childMatchers;
    }

    @Override
    protected boolean matchesSafely(QueryPredicate predicate) {
        if (!(predicate instanceof AndOrPredicate)) {
            return false;
        }
        Collection<Matcher<QueryPredicate>> remaining = new HashSet<>(childMatchers);
        for (QueryPredicate child : ((AndOrPredicate)predicate).getChildren()) {
            for (Matcher<QueryPredicate> matcher : remaining) {
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
        Iterator<Matcher<QueryPredicate>> matchIterator = childMatchers.iterator();
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
