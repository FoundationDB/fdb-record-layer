/*
 * FilterWithElementWithComparisonRule.java
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.IndexEntrySource;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A rewrite rule that turns a logical filter on the results of a full scan (without a sort) into an index scan on
 * a compatibly ordered index.
 *
 * <pre>
 * {@code
 *      +----------------------------+                               +-------------------------------------+
 *      |                            |                               |                                     |
 *      |  LogicalFilterExpression   |                               |  IndexEntrySourceScanExpression     |
 *      |          element <>= val   |                               |                        scan ranges  |
 *      |                            |                               |                                     |
 *      +-------------+--------------+                               +-------------------------------------+
 *                    |                    +------------------>
 *                    |
 *                    |
 *     +--------------+----------------+
 *     |                               |
 *     |  FullUnorderedScanExpression  |
 *     |                               |
 *     +-------------------------------+
 * }
 * </pre>
 *
 * Like the {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner}
 */
@API(API.Status.EXPERIMENTAL)
public class FilterWithElementWithComparisonRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<FullUnorderedScanExpression> scanMatcher = TypeMatcher.of(FullUnorderedScanExpression.class);
    private static final ExpressionMatcher<ElementPredicate> predMatcher = TypeMatcher.of(ElementPredicate.class);
    private static final ExpressionMatcher<LogicalFilterExpression> rootMatcher =
            TypeWithPredicateMatcher.ofPredicate(LogicalFilterExpression.class,
                    predMatcher,
                    QuantifierMatcher.forEach(scanMatcher));

    public FilterWithElementWithComparisonRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ElementPredicate pred = call.get(predMatcher);
        if (ScanComparisons.getComparisonType(pred.getComparison()).equals(ScanComparisons.ComparisonType.NONE)) {
            // This comparison cannot be accomplished with a single scan.
            return;
        }
        final LogicalFilterExpression logicalFilterExpression = call.get(rootMatcher);
        if (!logicalFilterExpression.getCorrelatedTo().isEmpty()) {
            // TODO revisit later -- this is too restrictive
            // LogicalFilterExpressions can express correlations while IndexEntrySourceScanExpressions cannot,
            // Do not transform if the root is correlated!
            return;
        }

        for (IndexEntrySource indexEntrySource : call.getContext().getIndexEntrySources()) {
            Optional<ViewExpressionComparisons> matchedComparisons = indexEntrySource.getEmptyComparisons().matchWith(pred);
            if (matchedComparisons.isPresent()) {
                call.yield(call.ref(new IndexEntrySourceScanExpression(indexEntrySource, IndexScanType.BY_VALUE,
                        matchedComparisons.get(), false)));
            }
        }
    }
}
