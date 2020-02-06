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
import com.apple.foundationdb.record.query.plan.temp.view.ElementMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.FieldElementMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.RepeatedFieldSourceMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.apple.foundationdb.record.query.plan.temp.view.SourceEntry;
import com.apple.foundationdb.record.query.plan.temp.view.ValueElementMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Various utility methods for building Hamcrest matchers that match simple instances of both {@link QueryComponent}
 * and {@link QueryPredicate} predicates.
 */
public class PredicateMatchers {
    public static ElementMatcher field(@Nonnull String fieldName) {
        return new FieldElementMatcher(fieldName);
    }

    public static ElementMatcher field(@Nonnull String... fieldNames) {
        return new FieldElementMatcher(ImmutableList.copyOf(fieldNames));
    }

    public static ElementMatcher valueFrom(@Nonnull Matcher<Source> sourceMatcher) {
        return new ValueElementMatcher(sourceMatcher);
    }

    public static Matcher<Source> repeatedField(@Nonnull String fieldName) {
        return new RepeatedFieldSourceMatcher(fieldName);
    }

    public static Matcher<QueryPredicate> and(@Nonnull Matcher<Collection<QueryPredicate>> childrenMatcher) {
        return new AndPredicateMatcher(childrenMatcher);
    }

    public static Matcher<QueryPredicate> equivalentTo(@Nonnull QueryComponent component) {
        return Matchers.equalTo(component.normalizeForPlanner(BlankSource.INSTANCE));
    }

    /**
     * A source representing an empty stream of values for testing purposes.
     */
    public static class BlankSource extends Source {
        public static final BlankSource INSTANCE  = new BlankSource();

        @Override
        public Set<Source> getSources() {
            return Collections.singleton(this);
        }

        @Override
        public boolean supportsSourceIn(@Nonnull ViewExpressionComparisons comparisons, @Nonnull Source other) {
            return false;
        }

        @Nonnull
        @Override
        public Source withSourceMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource) {
            return this;
        }

        @Nonnull
        @Override
        protected Stream<SourceEntry> evalSourceEntriesFor(@Nonnull SourceEntry entry) {
            return Stream.empty();
        }

        @Override
        public String toString() {
            return "ANY";
        }
    }

    private PredicateMatchers() {}
}
