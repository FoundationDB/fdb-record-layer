/*
 * PushFieldWithComparisonIntoExistingIndexScanRule.java
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
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionComparisons;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIndexScanExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A simple rule that looks for a filter (with an equality comparison on a field) and a trivial but compatibly
 * ordered index scan with no existing comparisons and pushes the equality comparison down to the index scan.
 */
@API(API.Status.EXPERIMENTAL)
public class PushFieldWithComparisonIntoExistingIndexScanRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<FieldWithComparison> filterMatcher = TypeMatcher.of(FieldWithComparison.class);
    private static final ExpressionMatcher<LogicalIndexScanExpression> indexScanMatcher = TypeMatcher.of(LogicalIndexScanExpression.class);
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class, filterMatcher, indexScanMatcher);

    public PushFieldWithComparisonIntoExistingIndexScanRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        LogicalIndexScanExpression indexScan = call.get(indexScanMatcher);
        FieldWithComparison filter = call.get(filterMatcher);

        final Optional<KeyExpressionComparisons> matchedComparisons = indexScan.getComparisons().matchWith(filter);
        if (matchedComparisons.isPresent()) {
            call.yield(call.ref(new LogicalIndexScanExpression(indexScan.getIndexName(), indexScan.getScanType(),
                            matchedComparisons.get(), indexScan.isReverse())));
            return ChangesMade.MADE_CHANGES;
        }
        return ChangesMade.NO_CHANGE;
    }
}
