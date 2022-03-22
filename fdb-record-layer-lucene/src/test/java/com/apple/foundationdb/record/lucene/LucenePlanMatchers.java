/*
 * FDBLuceneQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;

public class LucenePlanMatchers {

    public static Matcher<RecordQueryIndexPlan> scanParams(@Nonnull Matcher<IndexScanParameters> scanMatcher) {
        return new ScanParamsMatcher(scanMatcher);
    }

    public static Matcher<IndexScanParameters> query(@Nonnull Matcher<LuceneQueryClause> queryMatcher) {
        return new QueryMatcher(queryMatcher);
    }

    public static Matcher<IndexScanParameters> group(@Nonnull Matcher<ScanComparisons> boundsMatcher) {
        return new GroupBoundsMatcher(boundsMatcher);
    }

    public static class ScanParamsMatcher extends TypeSafeMatcher<RecordQueryIndexPlan> {
        @Nonnull
        private final Matcher<IndexScanParameters> scanMatcher;

        public ScanParamsMatcher(@Nonnull Matcher<IndexScanParameters> scanMatcher) {
            this.scanMatcher = scanMatcher;
        }

        @Override
        public boolean matchesSafely(@Nonnull RecordQueryIndexPlan plan) {
            return scanMatcher.matches(plan.getScanParameters());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("scan=(");
            scanMatcher.describeTo(description);
            description.appendText(")");
        }
    }
    
    public static class QueryMatcher extends TypeSafeMatcher<IndexScanParameters> {
        @Nonnull
        private final Matcher<LuceneQueryClause> queryMatcher;

        public QueryMatcher(@Nonnull Matcher<LuceneQueryClause> queryMatcher) {
            this.queryMatcher = queryMatcher;
        }

        @Override
        public boolean matchesSafely(@Nonnull IndexScanParameters scan) {
            return scan instanceof LuceneScanQueryParameters && queryMatcher.matches(((LuceneScanQueryParameters)scan).getQuery());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("query=(");
            queryMatcher.describeTo(description);
            description.appendText(")");
        }
    }

    public static class GroupBoundsMatcher extends TypeSafeMatcher<IndexScanParameters> {
        @Nonnull
        private final Matcher<ScanComparisons> boundsMatcher;

        public GroupBoundsMatcher(@Nonnull Matcher<ScanComparisons> boundsMatcher) {
            this.boundsMatcher = boundsMatcher;
        }

        @Override
        public boolean matchesSafely(@Nonnull IndexScanParameters scan) {
            return scan instanceof LuceneScanQueryParameters && boundsMatcher.matches(((LuceneScanQueryParameters)scan).getGroupComparisons());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("group=(");
            boundsMatcher.describeTo(description);
            description.appendText(")");
        }
    }

}
