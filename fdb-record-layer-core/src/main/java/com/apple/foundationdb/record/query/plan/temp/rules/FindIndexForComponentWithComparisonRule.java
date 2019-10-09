/*
 * FindIndexForComponentWithComparisonRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.query.expressions.ComponentWithComparison;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.IndexEntrySource;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionComparisons;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A rewrite rule that turns a logical filter on the results of a full scan (without a sort) into an index scan on
 * a compatibly ordered index.
 *
 * Like the {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner}
 */
@API(API.Status.EXPERIMENTAL)
public class FindIndexForComponentWithComparisonRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<ComponentWithComparison> filterMatcher = TypeMatcher.of(ComponentWithComparison.class);
    private static final ExpressionMatcher<FullUnorderedScanExpression> scanMatcher = TypeMatcher.of(FullUnorderedScanExpression.class);
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class, filterMatcher, scanMatcher);

    public FindIndexForComponentWithComparisonRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ComponentWithComparison singleField = call.get(filterMatcher);
        if (ScanComparisons.getComparisonType(singleField.getComparison()).equals(ScanComparisons.ComparisonType.NONE)) {
            // This comparison cannot be accomplished with a single scan.
            return;
        }

        for (IndexEntrySource indexEntrySource : call.getContext().getIndexEntrySources()) {
            final KeyExpressionComparisons comparisons = indexEntrySource.getEmptyComparisons();
            Optional<KeyExpressionComparisons> matchedComparisons = comparisons.matchWith(singleField);
            if (matchedComparisons.isPresent()) {
                call.yield(call.ref(new IndexEntrySourceScanExpression(indexEntrySource, IndexScanType.BY_VALUE,
                        matchedComparisons.get(), false)));
            }
        }
    }
}
