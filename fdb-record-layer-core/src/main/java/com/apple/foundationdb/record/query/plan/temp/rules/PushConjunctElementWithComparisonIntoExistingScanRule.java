/*
 * PushConjunctElementWithComparisonIntoExistingScanRule.java
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
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildWithRestMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * A rule that selects one of the {@link com.apple.foundationdb.record.query.expressions.ComponentWithComparison}
 * conjuncts from an AND and tries to push it into an existing logical scan.
 * @see PushElementWithComparisonIntoExistingScanRule for a very similar rule for {@code FieldWithComparison}s that are not conjuncts
 */
@API(API.Status.EXPERIMENTAL)
public class PushConjunctElementWithComparisonIntoExistingScanRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<ElementPredicate> filterMatcher = TypeMatcher.of(ElementPredicate.class);
    private static final ReferenceMatcher<QueryPredicate> otherFilterMatchers = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<AndPredicate> andMatcher = TypeMatcher.of(AndPredicate.class,
            AnyChildWithRestMatcher.anyMatchingWithRest(filterMatcher, otherFilterMatchers));
    private static final ExpressionMatcher<IndexEntrySourceScanExpression> indexScanMatcher = TypeMatcher.of(IndexEntrySourceScanExpression.class);
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class, andMatcher, indexScanMatcher);

    public PushConjunctElementWithComparisonIntoExistingScanRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalFilterExpression filterExpression = call.get(root);
        final IndexEntrySourceScanExpression indexScan = call.get(indexScanMatcher);
        final ElementPredicate field = call.get(filterMatcher);
        final List<ExpressionRef<QueryPredicate>> residualFields = call.getBindings().getAll(otherFilterMatchers);

        final Optional<ViewExpressionComparisons> matchedComparisons = indexScan.getComparisons().matchWith(field);
        if (matchedComparisons.isPresent()) {
            final IndexEntrySourceScanExpression newIndexScan = new IndexEntrySourceScanExpression(
                    indexScan.getIndexEntrySource(), indexScan.getScanType(), matchedComparisons.get(), indexScan.isReverse());
            if (residualFields.isEmpty()) {
                call.yield(call.ref(newIndexScan));
                return;
            }

            final ExpressionRef<QueryPredicate> residualFilter;
            if (residualFields.size() > 1) {
                residualFilter = call.ref(new AndPredicate(residualFields));
            } else {
                residualFilter = residualFields.get(0);
            }
            call.yield(call.ref(new LogicalFilterExpression(filterExpression.getBaseSource(), residualFilter, call.ref(newIndexScan))));
        }
    }
}
