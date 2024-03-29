/*
 * AndPredicateMatcher.java
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

import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * A Hamcrest matcher for the presence of an {@link AndPredicate}.
 */
public class AndPredicateMatcher extends AndOrPredicateMatcher {
    public AndPredicateMatcher(@Nonnull Matcher<Collection<QueryPredicate>> childMatchers) {
        super(childMatchers);
    }

    @Override
    protected boolean matchesSafely(QueryPredicate predicate) {
        return predicate instanceof AndPredicate && super.matchesSafely(predicate);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("And(");
        super.describeTo(description);
        description.appendText(")");
    }
}
