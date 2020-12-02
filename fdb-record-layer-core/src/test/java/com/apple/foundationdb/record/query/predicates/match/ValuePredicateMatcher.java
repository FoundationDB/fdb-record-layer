/*
 * ValuePredicateMatcher.java
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

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;

/**
 * A Hamcrest matcher for an {@link com.apple.foundationdb.record.query.predicates.ValuePredicate},
 * with sub-matchers for the {@link com.apple.foundationdb.record.query.predicates.Value} and
 * {@link Comparisons.Comparison}.
 */
public class ValuePredicateMatcher extends TypeSafeMatcher<QueryPredicate> {
    @Nonnull
    private final Matcher<Value> valueMatcher;
    @Nonnull
    private final Matcher<Comparisons.Comparison> comparisonMatcher;

    public ValuePredicateMatcher(@Nonnull Matcher<Value> valueMatcher, @Nonnull Matcher<Comparisons.Comparison> comparisonMatcher) {
        this.valueMatcher = valueMatcher;
        this.comparisonMatcher = comparisonMatcher;
    }

    @Override
    protected boolean matchesSafely(QueryPredicate predicate) {
        return predicate instanceof ValuePredicate &&
               valueMatcher.matches(((ValuePredicate)predicate).getValue()) &&
               comparisonMatcher.matches(((ValuePredicate)predicate).getComparison());
    }

    @Override
    public void describeTo(Description description) {
        valueMatcher.describeTo(description);
        description.appendText(" ");
        comparisonMatcher.describeTo(description);
    }
}
