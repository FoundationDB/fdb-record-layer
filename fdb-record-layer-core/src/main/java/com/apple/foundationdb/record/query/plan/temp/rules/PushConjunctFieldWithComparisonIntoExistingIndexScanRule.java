/*
 * PushConjunctFieldWithComparisonIntoExistingIndexScanRule.java
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
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionComparisons;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIndexScanExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildWithRestMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * A rule that selects one of the {@link FieldWithComparison} conjuncts from an AND and tries to push it into an existing
 * logical index scan.
 * @see PushFieldWithComparisonIntoExistingIndexScanRule for a very similar rule for {@code FieldWithComparison}s that are not conjuncts
 */
@API(API.Status.EXPERIMENTAL)
public class PushConjunctFieldWithComparisonIntoExistingIndexScanRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<FieldWithComparison> filterMatcher = TypeMatcher.of(FieldWithComparison.class);
    private static final ReferenceMatcher<QueryComponent> otherFilterMatchers = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<AndComponent> andMatcher = TypeMatcher.of(AndComponent.class,
            AnyChildWithRestMatcher.anyMatchingWithRest(filterMatcher, otherFilterMatchers));
    private static final ExpressionMatcher<LogicalIndexScanExpression> indexScanMatcher = TypeMatcher.of(LogicalIndexScanExpression.class);
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class, andMatcher, indexScanMatcher);

    public PushConjunctFieldWithComparisonIntoExistingIndexScanRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalIndexScanExpression indexScan = call.get(indexScanMatcher);
        final FieldWithComparison field = call.get(filterMatcher);
        final List<ExpressionRef<QueryComponent>> residualFields = call.getBindings().getAll(otherFilterMatchers);

        final Optional<KeyExpressionComparisons> matchedComparisons = indexScan.getComparisons().matchWith(field);
        if (matchedComparisons.isPresent()) {
            final LogicalIndexScanExpression newIndexScan = new LogicalIndexScanExpression(indexScan.getIndexName(),
                    indexScan.getScanType(), matchedComparisons.get(), indexScan.isReverse());
            if (residualFields.isEmpty()) {
                call.yield(call.ref(newIndexScan));
                return ChangesMade.MADE_CHANGES;
            }

            final ExpressionRef<QueryComponent> residualFilter;
            if (residualFields.size() > 1) {
                residualFilter = call.ref(new AndComponent(residualFields));
            } else {
                residualFilter = residualFields.get(0);
            }
            call.yield(call.ref(new LogicalFilterExpression(residualFilter, call.ref(newIndexScan))));
            return ChangesMade.MADE_CHANGES;
        }
        return ChangesMade.NO_CHANGE;
    }
}
