/*
 * SortToIndexRule.java
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
import com.apple.foundationdb.record.query.plan.temp.IndexEntrySource;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A rule for implementing a {@link LogicalSortExpression} as a scan of any appropriately-ordered index.
 * The rule's logic mirrors {@link FilterWithElementWithComparisonRule}, but applied to sorts rather than filters.
 */
@API(API.Status.EXPERIMENTAL)
public class SortToIndexRule extends PlannerRule<LogicalSortExpression> {
    private static final ExpressionMatcher<FullUnorderedScanExpression> innerMatcher = TypeMatcher.of(FullUnorderedScanExpression.class);
    private static final ExpressionMatcher<LogicalSortExpression> root = TypeMatcher.of(LogicalSortExpression.class, innerMatcher);

    public SortToIndexRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalSortExpression logicalSort = call.get(root);
        final boolean reverse = call.get(root).isReverse();

        for (IndexEntrySource indexEntrySource : call.getContext().getIndexEntrySources()) {
            final ViewExpressionComparisons sortExpression = indexEntrySource.getEmptyComparisons();
            final Optional<ViewExpressionComparisons> matchedViewExpression = sortExpression.matchWithSort(logicalSort.getSortPrefix());
            if (matchedViewExpression.isPresent()) {
                RelationalPlannerExpression indexScan = new IndexEntrySourceScanExpression(
                        indexEntrySource, IndexScanType.BY_VALUE, matchedViewExpression.get(), reverse);
                if (logicalSort.getSortSuffix().isEmpty()) {
                    call.yield(call.ref(indexScan));
                } else {
                    call.yield(call.ref(new LogicalSortExpression(
                            logicalSort.getSortPrefix(),
                            logicalSort.getSortSuffix(),
                            reverse,
                            indexScan)));

                }
            }
        }
    }
}
