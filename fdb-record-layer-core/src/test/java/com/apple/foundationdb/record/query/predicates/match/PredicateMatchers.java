/*
 * PredicateMatchers.java
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

import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.view.FieldValueMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.ValueMatcher;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Various utility methods for building Hamcrest matchers that match simple instances of both {@link QueryComponent}
 * and {@link QueryPredicate} predicates.
 */
public class PredicateMatchers {
    public static ValueMatcher field(@Nonnull String fieldName) {
        return new FieldValueMatcher(fieldName);
    }

    public static ValueMatcher field(@Nonnull String... fieldNames) {
        return new FieldValueMatcher(ImmutableList.copyOf(fieldNames));
    }

    public static Matcher<QueryPredicate> and(@Nonnull Matcher<Collection<QueryPredicate>> childrenMatcher) {
        return new AndPredicateMatcher(childrenMatcher);
    }

    private PredicateMatchers() {}
}
