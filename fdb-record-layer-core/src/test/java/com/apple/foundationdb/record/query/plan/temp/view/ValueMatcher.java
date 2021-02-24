/*
 * ValueMatcher.java
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

package com.apple.foundationdb.record.query.plan.temp.view;

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.match.ValuePredicateMatcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;

/**
 * A base class for matchers that should support the fluent syntax for constructing matchers on
 * {@link com.apple.foundationdb.record.query.predicates.ValuePredicate}s.
 */
public abstract class ValueMatcher extends TypeSafeMatcher<Value> {
    @Nonnull
    public ValuePredicateMatcher equalsValue(@Nonnull Object comparand) {
        return new ValuePredicateMatcher(this,
                Matchers.equalTo(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, comparand)));
    }

    @Nonnull
    public ValuePredicateMatcher notEquals(@Nonnull Object comparand) {
        return new ValuePredicateMatcher(this,
                Matchers.equalTo(new Comparisons.SimpleComparison(Comparisons.Type.NOT_EQUALS, comparand)));
    }

    @Nonnull
    public ValuePredicateMatcher greaterThan(@Nonnull Object comparand) {
        return new ValuePredicateMatcher(this,
                Matchers.equalTo(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, comparand)));
    }

    @Nonnull
    public ValuePredicateMatcher lessThan(@Nonnull Object comparand) {
        return new ValuePredicateMatcher(this,
                Matchers.equalTo(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, comparand)));
    }

    @Nonnull
    public ValuePredicateMatcher startsWith(@Nonnull Object comparand) {
        return new ValuePredicateMatcher(this,
                Matchers.equalTo(new Comparisons.SimpleComparison(Comparisons.Type.STARTS_WITH, comparand)));
    }

    @Nonnull
    public ValuePredicateMatcher notNull() {
        return new ValuePredicateMatcher(this,
                Matchers.equalTo(new Comparisons.NullComparison(Comparisons.Type.NOT_NULL)));
    }
}
