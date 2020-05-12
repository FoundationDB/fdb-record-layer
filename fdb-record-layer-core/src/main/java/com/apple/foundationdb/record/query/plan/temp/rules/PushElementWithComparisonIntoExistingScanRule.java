/*
 * PushElementWithComparisonIntoExistingScanRule.java
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
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A simple rule that looks for a {@link com.apple.foundationdb.record.query.expressions.ComponentWithComparison}
 * predicate and a compatibly ordered index scan comparison down to the index scan.
 */
@API(API.Status.EXPERIMENTAL)
public class PushElementWithComparisonIntoExistingScanRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<ElementPredicate> filterMatcher = TypeMatcher.of(ElementPredicate.class);
    private static final ExpressionMatcher<IndexEntrySourceScanExpression> indexScanMatcher = TypeMatcher.of(IndexEntrySourceScanExpression.class);
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeWithPredicateMatcher.ofPredicate(LogicalFilterExpression.class, filterMatcher, indexScanMatcher);

    public PushElementWithComparisonIntoExistingScanRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        IndexEntrySourceScanExpression indexScan = call.get(indexScanMatcher);
        ElementPredicate filter = call.get(filterMatcher);

        final Optional<ViewExpressionComparisons> matchedComparisons = indexScan.getComparisons().matchWith(filter);
        if (matchedComparisons.isPresent()) {
            call.yield(call.ref(new IndexEntrySourceScanExpression(indexScan.getIndexEntrySource(), indexScan.getScanType(),
                            matchedComparisons.get(), indexScan.isReverse())));
        }
    }
}
