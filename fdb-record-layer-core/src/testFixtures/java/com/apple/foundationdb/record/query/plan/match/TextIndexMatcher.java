/*
 * TextIndexMatcher.java
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

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;

/**
 * A plan matcher for {@link RecordQueryTextIndexPlan}.
 */
public class TextIndexMatcher extends TypeSafeMatcher<RecordQueryPlan> {

    private final Matcher<? super RecordQueryTextIndexPlan> planMatcher;

    public TextIndexMatcher(@Nonnull Matcher<? super RecordQueryTextIndexPlan> planMatcher) {
        this.planMatcher = planMatcher;
    }

    @Override
    protected boolean matchesSafely(RecordQueryPlan plan) {
        return plan instanceof RecordQueryTextIndexPlan && planMatcher.matches(plan);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("TextIndex(");
        planMatcher.describeTo(description);
        description.appendText(")");
    }

    /**
     * Matcher on the grouping comparisons of a text index scan plan.
     */
    public static class GroupingMatcher extends TypeSafeMatcher<RecordQueryTextIndexPlan> {
        @Nonnull
        private final Matcher<? super ScanComparisons> comparisonsMatcher;

        public GroupingMatcher(@Nonnull Matcher<? super ScanComparisons> comparisonsMatcher) {
            this.comparisonsMatcher = comparisonsMatcher;
        }

        @Override
        protected boolean matchesSafely(RecordQueryTextIndexPlan recordQueryTextIndexPlan) {
            return comparisonsMatcher.matches(recordQueryTextIndexPlan.getTextScan().getGroupingComparisons());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("grouping=(");
            comparisonsMatcher.describeTo(description);
            description.appendText(")");
        }
    }

    /**
     * Matcher on the suffix comparisons of a text index scan plan.
     */
    public static class SuffixMatcher extends TypeSafeMatcher<RecordQueryTextIndexPlan> {
        @Nonnull
        private final Matcher<? super ScanComparisons> comparisonsMatcher;

        public SuffixMatcher(@Nonnull Matcher<? super ScanComparisons> comparisonsMatcher) {
            this.comparisonsMatcher = comparisonsMatcher;
        }

        @Override
        protected boolean matchesSafely(RecordQueryTextIndexPlan recordQueryTextIndexPlan) {
            return comparisonsMatcher.matches(recordQueryTextIndexPlan.getTextScan().getSuffixComparisons());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("suffix=(");
            comparisonsMatcher.describeTo(description);
            description.appendText(")");
        }
    }

    /**
     * Matcher on the text comparison of a text index scan plan.
     */
    public static class TextComparisonMatcher extends TypeSafeMatcher<RecordQueryTextIndexPlan> {
        private final Matcher<? super Comparisons.TextComparison> comparisonMatcher;

        public TextComparisonMatcher(@Nonnull Matcher<? super Comparisons.TextComparison> comparisonMatcher) {
            this.comparisonMatcher = comparisonMatcher;
        }

        @Override
        protected boolean matchesSafely(RecordQueryTextIndexPlan recordQueryTextIndexPlan) {
            return comparisonMatcher.matches(recordQueryTextIndexPlan.getTextScan().getTextComparison());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("comparison=(");
            comparisonMatcher.describeTo(description);
            description.appendText(")");
        }
    }
}
