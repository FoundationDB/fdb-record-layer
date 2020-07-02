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
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildWithRestMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A rule that selects one of the {@link com.apple.foundationdb.record.query.expressions.ComponentWithComparison}
 * conjuncts from an AND and tries to push it into an existing logical scan.
 * @see PushElementWithComparisonIntoExistingScanRule for a very similar rule for {@code FieldWithComparison}s that are not conjuncts
 *
 * <pre>
 * {@code
 *       +-----------------------------+                 +-----------------------------+
 *       |                             |                 |                             |
 *       |  LogicalFilterExpression    |                 |  LogicalFilterExpression    |
 *       |   elementPred ^ otherPreds  |                 |                 otherPreds' |
 *       |                             |                 |                             |
 *       +-------------+---------------+                 +-------------+---------------+
 *                     |                     +------>                  |
 *                     | qun                                           | newQun
 *                     |                                               |
 *     +---------------+------------------+            +---------------+------------------+
 *     |                                  |            |                                  |
 *     |  IndexEntrySourceScanExpression  |            |  IndexEntrySourceScanExpression  |
 *     |                     comparisons  |            |  elementComparison ∪ comparisons |
 *     |                                  |            |                                  |
 *     +----------------------------------+            +----------------------------------+
 * }
 * </pre>
 *
 * or if there are no other predicates
 *
 * <pre>
 * {@code
 *       +-----------------------------+               +----------------------------------+
 *       |                             |               |                                  |
 *       |  LogicalFilterExpression    |     +------>  |  IndexEntrySourceScanExpression  |
 *       |                elementPred  |               |  elementComparison ∪ comparisons |
 *       |                             |               |                                  |
 *       +-------------+---------------+               +----------------------------------+
 *                     |
 *                     | qun
 *                     |
 *     +---------------+------------------+
 *     |                                  |
 *     |  IndexEntrySourceScanExpression  |
 *     |                     comparisons  |
 *     |                                  |
 *     +----------------------------------+
 * }
 * </pre>
 *
 * where otherPreds' are rebased with respect to a translation from qun to newQun and elementComparison is computed
 * from elementPred.
 */
@API(API.Status.EXPERIMENTAL)
public class PushConjunctElementWithComparisonIntoExistingScanRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<ElementPredicate> elementPredMatcher = TypeMatcher.of(ElementPredicate.class);
    private static final ExpressionMatcher<QueryPredicate> otherPredMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<AndPredicate> andMatcher = TypeMatcher.of(AndPredicate.class,
            AnyChildWithRestMatcher.anyMatchingWithRest(elementPredMatcher, otherPredMatcher));
    private static final ExpressionMatcher<IndexEntrySourceScanExpression> indexScanMatcher = TypeMatcher.of(IndexEntrySourceScanExpression.class);
    private static final QuantifierMatcher<Quantifier.ForEach> qunMatcher = QuantifierMatcher.forEach(indexScanMatcher);
    private static final ExpressionMatcher<LogicalFilterExpression> root =
            TypeWithPredicateMatcher.ofPredicate(LogicalFilterExpression.class,
                    andMatcher,
                    qunMatcher);

    public PushConjunctElementWithComparisonIntoExistingScanRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ElementPredicate elementPred = call.get(elementPredMatcher);
        final List<QueryPredicate> otherPreds = call.getBindings().getAll(otherPredMatcher);
        final IndexEntrySourceScanExpression indexScan = call.get(indexScanMatcher);
        final Quantifier.ForEach qun = call.get(qunMatcher);
        final LogicalFilterExpression filterExpression = call.get(root);

        final Optional<ViewExpressionComparisons> matchedComparisons = indexScan.getComparisons().matchWith(elementPred);
        if (matchedComparisons.isPresent()) {
            final IndexEntrySourceScanExpression newIndexScan = new IndexEntrySourceScanExpression(
                    indexScan.getIndexEntrySource(), indexScan.getScanType(), matchedComparisons.get(), indexScan.isReverse());
            if (otherPreds.isEmpty()) {
                call.yield(call.ref(newIndexScan));
                return;
            }

            final Quantifier.ForEach newQun = Quantifier.forEach(call.ref(newIndexScan));
            final AliasMap translationMap = Quantifiers.translate(qun, newQun);
            final QueryPredicate residualRebasedFilter;
            if (otherPreds.size() > 1) {
                residualRebasedFilter = new AndPredicate(
                        otherPreds.stream()
                                .map(otherPred -> otherPred.rebase(translationMap))
                                .collect(Collectors.toList()));
            } else {
                residualRebasedFilter = otherPreds.get(0).rebase(translationMap);
            }
            call.yield(call.ref(new LogicalFilterExpression(filterExpression.getBaseSource(), residualRebasedFilter, newQun)));
        }
    }
}
