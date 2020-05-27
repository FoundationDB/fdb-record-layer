/*
 * PushSortIntoExistingIndexRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A rule that pushes a sort into a compatibly-ordered index scan, including one with existing comparisons
 * (from filters or other sort orders) that might affect whether the index is compatibly ordered.
 */
@API(API.Status.EXPERIMENTAL)
public class PushSortIntoExistingIndexRule extends PlannerRule<LogicalSortExpression> {
    private static final ExpressionMatcher<IndexEntrySourceScanExpression> indexScanMatcher = TypeMatcher.of(IndexEntrySourceScanExpression.class);
    private static final ExpressionMatcher<LogicalSortExpression> root =
            TypeMatcher.of(LogicalSortExpression.class,
                    QuantifierMatcher.forEach(indexScanMatcher));

    public PushSortIntoExistingIndexRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalSortExpression sortExpression = call.get(root);
        final IndexEntrySourceScanExpression scan = call.get(indexScanMatcher);

        final Optional<ViewExpressionComparisons> matchedViewExpression = scan.getComparisons()
                .matchWithSort(sortExpression.getSortPrefix());
        if (matchedViewExpression.isPresent() && scan.isReverse() == sortExpression.isReverse()) {
            RelationalExpression indexScan = new IndexEntrySourceScanExpression(
                    scan.getIndexEntrySource(), scan.getScanType(), matchedViewExpression.get(), scan.isReverse());
            if (sortExpression.getSortSuffix().isEmpty()) {
                call.yield(call.ref(indexScan));
            } else {
                call.yield(call.ref(new LogicalSortExpression(
                        sortExpression.getSortPrefix(),
                        sortExpression.getSortSuffix(),
                        sortExpression.isReverse(),
                        indexScan)));

            }
        }
    }
}
