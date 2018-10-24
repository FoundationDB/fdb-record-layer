/*
 * IndexMatcher.java
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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;

/**
 * A plan matcher for {@link RecordQueryIndexPlan}.
 */
public class IndexMatcher extends TypeSafeMatcher<RecordQueryPlan> {
    @Nonnull
    private final Matcher<? super RecordQueryIndexPlan> planMatcher;

    public IndexMatcher(@Nonnull Matcher<? super RecordQueryIndexPlan> planMatcher) {
        this.planMatcher = planMatcher;
    }

    @Override
    public boolean matchesSafely(@Nonnull RecordQueryPlan plan) {
        return plan instanceof RecordQueryIndexPlan && planMatcher.matches(plan);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Index(");
        planMatcher.describeTo(description);
        description.appendText(")");
    }

    /**
     * Match the index plan's name.
     */
    public static class NameMatcher extends TypeSafeMatcher<RecordQueryPlanWithIndex> {
        @Nonnull
        private final Matcher<String> indexNameMatcher;

        public NameMatcher(@Nonnull Matcher<String> indexNameMatcher) {
            this.indexNameMatcher = indexNameMatcher;
        }

        @Override
        public boolean matchesSafely(@Nonnull RecordQueryPlanWithIndex plan) {
            return indexNameMatcher.matches(plan.getIndexName());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("indexName=(");
            indexNameMatcher.describeTo(description);
            description.appendText(")");
        }
    }

    /**
     * Match the index plan's bounds.
     */
    public static class BoundsMatcher extends TypeSafeMatcher<RecordQueryPlanWithComparisons> {
        @Nonnull
        private final Matcher<ScanComparisons> boundsMatcher;

        public BoundsMatcher(@Nonnull Matcher<ScanComparisons> boundsMatcher) {
            this.boundsMatcher = boundsMatcher;
        }

        @Override
        public boolean matchesSafely(@Nonnull RecordQueryPlanWithComparisons plan) {
            return boundsMatcher.matches(plan.getComparisons());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("bounds=(");
            boundsMatcher.describeTo(description);
            description.appendText(")");
        }
    }

    /**
     * Match the index plan's {@link IndexScanType}.
     */
    public static class ScanTypeMatcher extends TypeSafeMatcher<RecordQueryPlanWithIndex> {
        @Nonnull
        private final Matcher<IndexScanType> scanTypeMatcher;

        public ScanTypeMatcher(@Nonnull Matcher<IndexScanType> scanTypeMatcher) {
            this.scanTypeMatcher = scanTypeMatcher;
        }

        @Override
        public boolean matchesSafely(@Nonnull RecordQueryPlanWithIndex plan) {
            return scanTypeMatcher.matches(plan.getScanType());
        }

        @Override
        public void describeTo(Description description) {
            scanTypeMatcher.describeTo(description);
        }
    }
}
