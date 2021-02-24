/*
 * PlanMatchers.java
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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScoreForRankPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Helper methods for building plan matchers.
 */
public class PlanMatchers {
    public static Matcher<RecordQueryPlan> scan() {
        return new ScanMatcher();
    }

    public static Matcher<RecordQueryPlan> scan(@Nonnull Matcher<? super RecordQueryScanPlan> scanPlanMatcher) {
        return new ScanMatcher(scanPlanMatcher);
    }

    public static Matcher<RecordQueryPlan> indexScan(@Nonnull Matcher<? super RecordQueryIndexPlan> planMatcher) {
        return new IndexMatcher(planMatcher);
    }

    public static Matcher<RecordQueryPlan> indexScan(@Nonnull final String indexName) {
        return indexScan(indexName(indexName));
    }

    public static Matcher<RecordQueryPlan> textIndexScan(@Nonnull Matcher<? super RecordQueryTextIndexPlan> planMatcher) {
        return new TextIndexMatcher(planMatcher);
    }

    public static Matcher<RecordQueryTextIndexPlan> groupingBounds(@Nonnull Matcher<ScanComparisons> boundsMatcher) {
        return new TextIndexMatcher.GroupingMatcher(boundsMatcher);
    }

    public static Matcher<RecordQueryTextIndexPlan> suffixBounds(@Nonnull Matcher<ScanComparisons> boundsMatcher) {
        return new TextIndexMatcher.SuffixMatcher(boundsMatcher);
    }

    public static Matcher<RecordQueryTextIndexPlan> textComparison(@Nonnull Matcher<? super Comparisons.TextComparison> comparisonMatcher) {
        return new TextIndexMatcher.TextComparisonMatcher(comparisonMatcher);
    }

    public static Matcher<RecordQueryPlanWithIndex> indexName(@Nonnull Matcher<String> indexNameMatcher) {
        return new IndexMatcher.NameMatcher(indexNameMatcher);
    }

    public static Matcher<RecordQueryPlanWithIndex> indexName(@Nonnull String indexName) {
        return indexName(equalTo(indexName));
    }

    public static Matcher<RecordQueryPlanWithIndex> indexScanType(@Nonnull Matcher<IndexScanType> scanTypeMatcher) {
        return new IndexMatcher.ScanTypeMatcher(scanTypeMatcher);
    }

    public static Matcher<RecordQueryPlanWithIndex> indexScanType(@Nonnull IndexScanType scanType) {
        return new IndexMatcher.ScanTypeMatcher(equalTo(scanType));
    }

    public static Matcher<RecordQueryPlanWithComparisons> bounds(@Nonnull Matcher<ScanComparisons> boundsMatcher) {
        return new IndexMatcher.BoundsMatcher(boundsMatcher);
    }

    public static Matcher<RecordQueryPlanWithComparisons> unbounded() {
        return new IndexMatcher.BoundsMatcher(new ScanComparisonsEmptyMatcher());
    }

    public static Matcher<ScanComparisons> hasTupleString(@Nonnull Matcher<String> stringMatcher) {
        return new ScanComparisonsStringMatcher(stringMatcher);
    }

    public static Matcher<ScanComparisons> hasTupleString(@Nonnull String string) {
        return hasTupleString(equalTo(string));
    }

    public static Matcher<RecordQueryPlan> coveringIndexScan(@Nonnull Matcher<? super RecordQueryPlan> childMatcher) {
        return new CoveringIndexMatcher(childMatcher);
    }

    public static Matcher<RecordQueryPlan> filter(@Nonnull Matcher<QueryPredicate> filterMatcher,
                                                  @Nonnull Matcher<RecordQueryPlan> childMatcher) {
        return new FilterMatcher(filterMatcher, childMatcher);
    }

    public static Matcher<RecordQueryPlan> filter(@Nonnull QueryComponent component,
                                                  @Nonnull Matcher<RecordQueryPlan> childMatcher) {
        return new FilterMatcherWithComponent(component, childMatcher);
    }

    public static Matcher<RecordQueryPlan> anyFilter(@Nonnull Matcher<RecordQueryPlan> childMatcher) {
        return new AnyFilterMatcher(childMatcher);
    }

    public static Matcher<RecordQueryPlan> typeFilter(@Nonnull Matcher<Iterable<? extends String>> typeMatcher,
                                                      @Nonnull Matcher<RecordQueryPlan> childMatcher) {
        return new TypeFilterMatcher(typeMatcher, childMatcher);
    }

    public static Matcher<RecordQueryPlan> union(@Nonnull Matcher<RecordQueryPlan> oneMatcher,
                                                 @Nonnull Matcher<RecordQueryPlan> otherMatcher) {
        return new UnionMatcher(Arrays.asList(oneMatcher, otherMatcher)); // order of arguments does not matter
    }

    public static Matcher<RecordQueryPlan> union(@Nonnull List<Matcher<RecordQueryPlan>> childMatchers) {
        return new UnionMatcher(childMatchers); // order of child arguments does not matter
    }

    public static Matcher<RecordQueryPlan> union(@Nonnull Matcher<RecordQueryPlan> oneMatcher,
                                                 @Nonnull Matcher<RecordQueryPlan> otherMatcher,
                                                 @Nonnull Matcher<KeyExpression> comparisonKeyMatcher) {
        return new UnionMatcher(Arrays.asList(oneMatcher, otherMatcher), comparisonKeyMatcher); // order of child arguments does not matter
    }

    public static Matcher<RecordQueryPlan> union(@Nonnull List<Matcher<RecordQueryPlan>> childMatchers,
                                                 @Nonnull Matcher<KeyExpression> comparisonKeyMatcher) {
        return new UnionMatcher(childMatchers, comparisonKeyMatcher); // order of child arguments does not matter
    }

    public static Matcher<RecordQueryPlan> unorderedUnion(@Nonnull Matcher<RecordQueryPlan> oneMatcher,
                                                          @Nonnull Matcher<RecordQueryPlan> otherMatcher) {
        return new UnorderedUnionMatcher(Arrays.asList(oneMatcher, otherMatcher)); // order of arguments does not matter
    }

    public static Matcher<RecordQueryPlan> unorderedUnion(@Nonnull List<Matcher<RecordQueryPlan>> childMatchers) {
        return new UnorderedUnionMatcher(childMatchers); // order of child arguments does not matter
    }

    public static Matcher<RecordQueryPlan> intersection(@Nonnull Matcher<RecordQueryPlan> oneMatcher,
                                                        @Nonnull Matcher<RecordQueryPlan> otherMatcher) {
        return new IntersectionMatcher(Arrays.asList(oneMatcher, otherMatcher)); // order of arguments does not matter
    }

    public static Matcher<RecordQueryPlan> intersection(@Nonnull Matcher<RecordQueryPlan> oneMatcher,
                                                 @Nonnull Matcher<RecordQueryPlan> otherMatcher,
                                                 @Nonnull Matcher<KeyExpression> comparisonKeyMatcher) {
        return new IntersectionMatcher(Arrays.asList(oneMatcher, otherMatcher), comparisonKeyMatcher); // order of child arguments does not matter
    }

    public static Matcher<RecordQueryPlan> intersection(@Nonnull List<Matcher<RecordQueryPlan>> childMatchers) {
        return new IntersectionMatcher(childMatchers); // order of arguments does not matter
    }

    public static Matcher<RecordQueryPlan> intersection(@Nonnull List<Matcher<RecordQueryPlan>> childMatchers,
                                                        @Nonnull Matcher<KeyExpression> comparisonKeyMatcher) {
        return new IntersectionMatcher(childMatchers, comparisonKeyMatcher); // order of arguments does not matter
    }

    public static Matcher<RecordQueryPlan> fetch(@Nonnull Matcher<RecordQueryPlan> childMatcher) {
        return new FetchMatcher(childMatcher);
    }

    public static Matcher<RecordQueryPlan> inValues(@Nonnull Matcher<Iterable<?>> listMatcher,
                                              @Nonnull Matcher<RecordQueryPlan> childMatcher) {
        return new InValueJoinMatcher(listMatcher, childMatcher);
    }

    public static Matcher<RecordQueryPlan> inParameter(@Nonnull Matcher<String> bindingMatcher,
                                              @Nonnull Matcher<RecordQueryPlan> childMatcher) {
        return new InParameterJoinMatcher(bindingMatcher, childMatcher);
    }

    public static Matcher<RecordQueryPlan> scoreForRank(@Nonnull Matcher<Iterable<? extends RecordQueryScoreForRankPlan.ScoreForRank>> rankMatchers,
                                                        @Nonnull Matcher<RecordQueryPlan> childMatcher) {
        return new ScoreForRankMatcher(rankMatchers, childMatcher);
    }

    public static Matcher<RecordQueryPlan> primaryKeyDistinct(@Nonnull Matcher<RecordQueryPlan> childMatcher) {
        return new UnorderedPrimaryKeyDistinctMatcher(childMatcher);
    }

    public static Matcher<RecordQueryPlan> anyParent(@Nonnull Matcher<RecordQueryPlan> childMatcher) {
        return new AnyParentMatcher(childMatcher);
    }

    public static Matcher<RecordQueryPlan> anyParent(@Nonnull Collection<Matcher<RecordQueryPlan>> childMatcher) {
        return new AnyParentMatcher(childMatcher);
    }

    public static Matcher<RecordQueryPlan> descendant(@Nonnull Matcher<RecordQueryPlan> matcher) {
        return new DescendantMatcher(matcher);
    }

    public static Matcher<QueryPredicate> queryPredicateDescendant(@Nonnull Matcher<QueryPredicate> matcher) {
        return new QueryPredicateDescendantMatcher(matcher);
    }

    public static Matcher<RecordQueryPlan> everyLeaf(@Nonnull Matcher<RecordQueryPlan> matcher) {
        return new EveryLeafMatcher(matcher);
    }

    public static Matcher<RecordQueryPlan> hasNoDescendant(@Nonnull Matcher<RecordQueryPlan> matcher) {
        return not(descendant(matcher));
    }

    public static Matcher<RecordQueryPlan> compositeBitmap(@Nonnull Matcher<ComposedBitmapIndexQueryPlan.ComposerBase> composerMatcher,
                                                           @Nonnull List<Matcher<RecordQueryPlan>> childMatchers) {
        return new ComposedBitmapIndexMatcher(composerMatcher, childMatchers);
    }
}
