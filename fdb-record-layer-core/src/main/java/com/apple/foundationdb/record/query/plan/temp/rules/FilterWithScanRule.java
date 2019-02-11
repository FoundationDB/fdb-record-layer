/*
 * FilterWithScanRule.java
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

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A simple rule that looks for a filter (with an equality comparison on a field) and a trivial but compatibly
 * ordered index scan with no existing comparisons and pushes the equality comparison down to the index scan.
 */
@API(API.Status.EXPERIMENTAL)
public class FilterWithScanRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<Comparisons.Comparison> comparisonMatcher = TypeMatcher.of(Comparisons.Comparison.class);
    private static final ExpressionMatcher<FieldWithComparison> filterMatcher = TypeMatcher.of(FieldWithComparison.class, comparisonMatcher);
    private static final ExpressionMatcher<RecordQueryIndexPlan> indexScanMatcher = TypeMatcher.of(RecordQueryIndexPlan.class);
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class, filterMatcher, indexScanMatcher);

    public FilterWithScanRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        RecordQueryIndexPlan indexScan = call.get(indexScanMatcher);
        Comparisons.Comparison comparison = call.get(comparisonMatcher);
        FieldWithComparison filter = call.get(filterMatcher);

        Set<String> indexNames = call.getContext().getIndexes().stream().map(Index::getName).collect(Collectors.toSet());
        if (!indexNames.contains(indexScan.getIndexName())) {
            return ChangesMade.NO_CHANGE;
        }
        KeyExpression indexExpression = call.getContext().getIndexByName(indexScan.getIndexName()).getRootExpression();

        if (indexScan.getComparisons().isEmpty() &&
                comparison.getType().equals(Comparisons.Type.EQUALS) &&
                Key.Expressions.field(filter.getFieldName()).isPrefixKey(indexExpression)) {
            call.yield(call.ref(new RecordQueryIndexPlan(
                    indexScan.getIndexName(),
                    indexScan.getScanType(),
                    ScanComparisons.from(new Comparisons.SimpleComparison(comparison.getType(), comparison.getComparand())),
                    indexScan.isReverse())));
            return ChangesMade.MADE_CHANGES;
        }
        return ChangesMade.NO_CHANGE;
    }
}
